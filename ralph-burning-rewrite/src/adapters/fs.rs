use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::contexts::agent_execution::model::RawOutputReference;
use crate::contexts::agent_execution::service::RawOutputPort;
use crate::contexts::agent_execution::session::{PersistedSessions, SessionStorePort};
use crate::contexts::automation_runtime::model::{
    DaemonJournalEvent, DaemonTask, LeaseRecord, WorktreeLease,
};
use crate::contexts::automation_runtime::DaemonStorePort;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ArtifactRecord, JournalEvent, PayloadRecord, ProjectRecord, RollbackPoint, RunSnapshot,
    RuntimeLogEntry, SessionStore,
};
use crate::contexts::project_run_record::service::{
    ActiveProjectPort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    ProjectStorePort, RollbackPointStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogStorePort, RuntimeLogWritePort,
};
use crate::shared::domain::{ProjectId, StageId, WorkspaceConfig};
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

    pub(crate) fn workspace_root_path(base_dir: &Path) -> PathBuf {
        base_dir.join(WORKSPACE_DIR)
    }

    pub(crate) fn daemon_root(base_dir: &Path) -> PathBuf {
        Self::workspace_root_path(base_dir).join("daemon")
    }

    fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(project_id.as_str())
    }

    fn pending_delete_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(format!(".{}.pending-delete", project_id))
    }

    fn append_line(path: &Path, line: &str) -> AppResult<()> {
        let parent = path.parent().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path '{}' has no parent directory", path.display()),
            ))
        })?;
        fs::create_dir_all(parent)?;

        let needs_separator = match fs::read(path) {
            Ok(contents) => contents.last().is_some_and(|byte| *byte != b'\n'),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        };

        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if needs_separator {
            file.write_all(b"\n")?;
        }
        writeln!(file, "{}", line)?;
        file.sync_all()?;
        Ok(())
    }

    fn write_create_new(path: &Path, contents: &str) -> AppResult<()> {
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

        match fs::hard_link(&temp_path, path) {
            Ok(()) => {
                let _ = fs::remove_file(&temp_path);
                Ok(())
            }
            Err(error) => {
                let _ = fs::remove_file(&temp_path);
                Err(error.into())
            }
        }
    }
}

/// Filesystem-backed implementation of `ProjectStorePort`.
pub struct FsProjectStore;

impl ProjectStorePort for FsProjectStore {
    fn project_exists(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        if !project_root.is_dir() {
            return Ok(false);
        }
        if !project_root.join(PROJECT_CONFIG_FILE).is_file() {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/project.toml", project_id),
                details: "project directory exists but canonical project.toml is missing"
                    .to_owned(),
            });
        }
        Ok(true)
    }

    fn read_project_record(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        let path = project_root.join(PROJECT_CONFIG_FILE);
        let raw = fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                if project_root.is_dir() {
                    // Directory exists but project.toml is missing — corruption
                    AppError::CorruptRecord {
                        file: format!("projects/{}/project.toml", project_id),
                        details: "project directory exists but canonical project.toml is missing"
                            .to_owned(),
                    }
                } else {
                    AppError::ProjectNotFound {
                        project_id: project_id.to_string(),
                    }
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
            // Skip hidden directories (staging/trash from transactional operations)
            if name_str.starts_with('.') {
                continue;
            }
            if let Ok(pid) = ProjectId::new(name_str.as_ref()) {
                if !entry.path().join(PROJECT_CONFIG_FILE).is_file() {
                    return Err(AppError::CorruptRecord {
                        file: format!("projects/{}/project.toml", name_str),
                        details: "project directory exists but canonical project.toml is missing"
                            .to_owned(),
                    });
                }
                ids.push(pid);
            }
        }

        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    fn stage_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        if !project_root.is_dir() {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }

        let pending_path = FileSystem::pending_delete_path(base_dir, project_id);

        // Clean up any leftover pending-delete from a previous crash
        if pending_path.exists() {
            fs::remove_dir_all(&pending_path)?;
        }

        // Atomic rename to pending-delete location — project becomes
        // invisible to list/show but data stays on disk.
        fs::rename(&project_root, &pending_path)?;
        Ok(())
    }

    fn commit_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let pending_path = FileSystem::pending_delete_path(base_dir, project_id);
        if pending_path.is_dir() {
            fs::remove_dir_all(&pending_path)?;
        }
        Ok(())
    }

    fn rollback_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        let pending_path = FileSystem::pending_delete_path(base_dir, project_id);

        if !pending_path.is_dir() {
            return Ok(());
        }

        fs::rename(&pending_path, &project_root)?;
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
            let project_toml = toml::to_string_pretty(record)
                .map_err(|e| AppError::Io(std::io::Error::other(e.to_string())))?;
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
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(AppError::CorruptRecord {
                file: format!("projects/{}/journal.ndjson", project_id),
                details: "canonical file is missing".to_owned(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    fn append_event(&self, base_dir: &Path, project_id: &ProjectId, line: &str) -> AppResult<()> {
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
                let record: PayloadRecord =
                    serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                        file: format!(
                            "history/payloads/{}",
                            path.file_name().unwrap_or_default().to_string_lossy()
                        ),
                        details: e.to_string(),
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
                            path.file_name().unwrap_or_default().to_string_lossy()
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

        // Only read the newest/latest log file per spec:
        // "run tail --logs appends the newest runtime logs"
        if let Some(path) = paths.last() {
            let contents = fs::read_to_string(path)?;
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
    fn read_run_snapshot(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<RunSnapshot> {
        let path = FileSystem::project_root(base_dir, project_id).join(RUN_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => {
                let snapshot: RunSnapshot =
                    serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                        file: format!("projects/{}/run.json", project_id),
                        details: e.to_string(),
                    })?;
                // Semantic validation: reject inconsistent status/active_run combinations
                snapshot
                    .validate_semantics()
                    .map_err(|details| AppError::CorruptRecord {
                        file: format!("projects/{}/run.json", project_id),
                        details,
                    })?;
                Ok(snapshot)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id),
                details: "canonical file is missing".to_owned(),
            }),
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

    fn write_active_project(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        FileSystem::write_active_project(
            &FileSystem::workspace_root_path(base_dir),
            project_id.as_str(),
        )
    }
}

/// Filesystem-backed implementation of `RawOutputPort`.
pub struct FsRawOutputStore;

impl RawOutputPort for FsRawOutputStore {
    fn persist_raw_output(
        &self,
        project_root: &Path,
        invocation_id: &str,
        contents: &str,
    ) -> AppResult<RawOutputReference> {
        let path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        FileSystem::write_atomic(&path, contents)?;
        Ok(RawOutputReference::Stored(path))
    }
}

/// Filesystem-backed implementation of `SessionStorePort`.
pub struct FsSessionStore;

impl SessionStorePort for FsSessionStore {
    fn load_sessions(&self, project_root: &Path) -> AppResult<PersistedSessions> {
        let path = project_root.join(SESSIONS_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
                file: format!("{}/{}", project_root.display(), SESSIONS_FILE),
                details: error.to_string(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(AppError::CorruptRecord {
                    file: format!("{}/{}", project_root.display(), SESSIONS_FILE),
                    details: "canonical file is missing".to_owned(),
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save_sessions(&self, project_root: &Path, sessions: &PersistedSessions) -> AppResult<()> {
        let path = project_root.join(SESSIONS_FILE);
        let contents = serde_json::to_string_pretty(sessions)?;
        FileSystem::write_atomic(&path, &contents)
    }
}

/// Filesystem-backed implementation of `RunSnapshotWritePort`.
pub struct FsRunSnapshotWriteStore;

impl RunSnapshotWritePort for FsRunSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        let path = FileSystem::project_root(base_dir, project_id).join(RUN_FILE);
        let contents = serde_json::to_string_pretty(snapshot)?;
        FileSystem::write_atomic(&path, &contents)
    }
}

/// Filesystem-backed implementation of `RollbackPointStorePort`.
pub struct FsRollbackPointStore;

impl RollbackPointStorePort for FsRollbackPointStore {
    fn write_rollback_point(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        rollback_point: &RollbackPoint,
    ) -> AppResult<()> {
        let path = FileSystem::project_root(base_dir, project_id)
            .join("rollback")
            .join(format!("{}.json", rollback_point.rollback_id));
        let contents = serde_json::to_string_pretty(rollback_point)?;
        FileSystem::write_atomic(&path, &contents)
    }

    fn list_rollback_points(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RollbackPoint>> {
        let dir = FileSystem::project_root(base_dir, project_id).join("rollback");
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut points = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let raw = fs::read_to_string(&path)?;
                let point: RollbackPoint =
                    serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
                        file: format!(
                            "rollback/{}",
                            path.file_name().unwrap_or_default().to_string_lossy()
                        ),
                        details: error.to_string(),
                    })?;
                points.push(point);
            }
        }

        points.sort_by_key(|point| point.created_at);
        Ok(points)
    }

    fn read_rollback_point_by_stage(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        stage_id: StageId,
    ) -> AppResult<Option<RollbackPoint>> {
        Ok(self
            .list_rollback_points(base_dir, project_id)?
            .into_iter()
            .filter(|point| point.stage_id == stage_id)
            .max_by_key(|point| point.created_at))
    }
}

/// Filesystem-backed implementation of `PayloadArtifactWritePort`.
pub struct FsPayloadArtifactWriteStore;

impl PayloadArtifactWritePort for FsPayloadArtifactWriteStore {
    fn write_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload: &PayloadRecord,
        artifact: &ArtifactRecord,
    ) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, project_id);

        let payload_final = project_root
            .join("history/payloads")
            .join(format!("{}.json", payload.payload_id));
        let artifact_final = project_root
            .join("history/artifacts")
            .join(format!("{}.json", artifact.artifact_id));

        // Stage both files under runtime/temp/ first so that no canonical
        // history file exists until both writes succeed. This prevents
        // orphaned payloads from leaking into durable history on partial failure.
        let staging_dir = project_root.join("runtime/temp");
        fs::create_dir_all(&staging_dir)?;

        let payload_staging = staging_dir.join(format!(".{}.payload.staging", payload.payload_id));
        let artifact_staging =
            staging_dir.join(format!(".{}.artifact.staging", artifact.artifact_id));

        let payload_json = serde_json::to_string_pretty(payload)?;
        let artifact_json = serde_json::to_string_pretty(artifact)?;

        // Write payload to staging
        FileSystem::write_atomic(&payload_staging, &payload_json)?;

        // Write artifact to staging — if this fails, clean up payload staging
        if let Err(e) = FileSystem::write_atomic(&artifact_staging, &artifact_json) {
            let _ = fs::remove_file(&payload_staging);
            return Err(e);
        }

        // Rename payload staging into canonical location
        if let Err(e) = fs::rename(&payload_staging, &payload_final) {
            let _ = fs::remove_file(&payload_staging);
            let _ = fs::remove_file(&artifact_staging);
            return Err(e.into());
        }

        // Rename artifact staging into canonical location
        if let Err(e) = fs::rename(&artifact_staging, &artifact_final) {
            // Roll back the payload that was already moved to canonical location
            // and remove the orphaned staged artifact file
            let _ = fs::remove_file(&payload_final);
            let _ = fs::remove_file(&artifact_staging);
            return Err(e.into());
        }

        Ok(())
    }

    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, project_id);

        let payload_path = project_root
            .join("history/payloads")
            .join(format!("{}.json", payload_id));
        let artifact_path = project_root
            .join("history/artifacts")
            .join(format!("{}.json", artifact_id));

        // Remove both files, propagating errors so the caller knows about
        // leaked durable history. NotFound is not an error (already cleaned up).
        let mut errors = Vec::new();
        if let Err(e) = fs::remove_file(&artifact_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                errors.push(format!("artifact {}: {}", artifact_id, e));
            }
        }
        if let Err(e) = fs::remove_file(&payload_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                errors.push(format!("payload {}: {}", payload_id, e));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(AppError::Io(std::io::Error::other(format!(
                "failed to remove payload/artifact pair: {}",
                errors.join("; ")
            ))))
        }
    }
}

/// Filesystem-backed implementation of `AmendmentQueuePort`.
pub struct FsAmendmentQueueStore;

impl crate::contexts::project_run_record::service::AmendmentQueuePort for FsAmendmentQueueStore {
    fn write_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment: &crate::contexts::project_run_record::model::QueuedAmendment,
    ) -> AppResult<()> {
        // Test-only injection seam: fail amendment writes after N successful writes.
        // Format: "N" where N is the number of successful writes before failure.
        // E.g., "1" means the first write succeeds, the second fails.
        if let Ok(after_str) = std::env::var("RALPH_BURNING_TEST_AMENDMENT_WRITE_FAIL_AFTER") {
            use std::sync::atomic::{AtomicU32, Ordering};
            static WRITE_COUNT: AtomicU32 = AtomicU32::new(0);
            let threshold: u32 = after_str.parse().unwrap_or(0);
            let current = WRITE_COUNT.fetch_add(1, Ordering::SeqCst);
            if current >= threshold {
                return Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected amendment write failure for testing",
                )));
            }
        }
        let dir = FileSystem::project_root(base_dir, project_id).join("amendments");
        fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.json", amendment.amendment_id));
        let contents = serde_json::to_string_pretty(amendment)?;
        FileSystem::write_atomic(&path, &contents)
    }

    fn list_pending_amendments(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<crate::contexts::project_run_record::model::QueuedAmendment>> {
        let dir = FileSystem::project_root(base_dir, project_id).join("amendments");
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut amendments = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let raw = fs::read_to_string(&path)?;
                let amendment: crate::contexts::project_run_record::model::QueuedAmendment =
                    serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                        file: format!(
                            "amendments/{}",
                            path.file_name().unwrap_or_default().to_string_lossy()
                        ),
                        details: e.to_string(),
                    })?;
                amendments.push(amendment);
            }
        }

        amendments.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.batch_sequence.cmp(&b.batch_sequence))
        });
        Ok(amendments)
    }

    fn remove_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let path = FileSystem::project_root(base_dir, project_id)
            .join("amendments")
            .join(format!("{}.json", amendment_id));
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn drain_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<u32> {
        let dir = FileSystem::project_root(base_dir, project_id).join("amendments");
        if !dir.is_dir() {
            return Ok(0);
        }

        let mut count = 0u32;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                fs::remove_file(&path)?;
                count += 1;
            }
        }
        Ok(count)
    }

    fn has_pending_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
        let dir = FileSystem::project_root(base_dir, project_id).join("amendments");
        if !dir.is_dir() {
            return Ok(false);
        }

        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|ext| ext == "json") {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Filesystem-backed implementation of `RuntimeLogWritePort`.
pub struct FsRuntimeLogWriteStore;

impl RuntimeLogWritePort for FsRuntimeLogWriteStore {
    fn append_runtime_log(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        entry: &RuntimeLogEntry,
    ) -> AppResult<()> {
        let dir = FileSystem::project_root(base_dir, project_id).join("runtime/logs");
        fs::create_dir_all(&dir)?;
        let path = dir.join("run.ndjson");
        let line = serde_json::to_string(entry)?;
        FileSystem::append_line(&path, &line)
    }
}

// ── Automation runtime filesystem store ────────────────────────────────────

const DAEMON_TASKS_DIR: &str = "tasks";
const DAEMON_LEASES_DIR: &str = "leases";
const DAEMON_JOURNAL_FILE: &str = "journal.ndjson";

pub struct FsDaemonStore;

impl FsDaemonStore {
    fn tasks_dir(base_dir: &Path) -> PathBuf {
        FileSystem::daemon_root(base_dir).join(DAEMON_TASKS_DIR)
    }

    fn leases_dir(base_dir: &Path) -> PathBuf {
        FileSystem::daemon_root(base_dir).join(DAEMON_LEASES_DIR)
    }

    fn task_path(base_dir: &Path, task_id: &str) -> PathBuf {
        Self::tasks_dir(base_dir).join(format!("{task_id}.json"))
    }

    fn lease_path(base_dir: &Path, lease_id: &str) -> PathBuf {
        Self::leases_dir(base_dir).join(format!("{lease_id}.json"))
    }

    fn writer_lock_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::leases_dir(base_dir).join(format!("writer-{}.lock", project_id.as_str()))
    }

    fn daemon_journal_path(base_dir: &Path) -> PathBuf {
        FileSystem::daemon_root(base_dir).join(DAEMON_JOURNAL_FILE)
    }

    fn read_json_file<T: serde::de::DeserializeOwned>(path: &Path, file: String) -> AppResult<T> {
        let raw = fs::read_to_string(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                AppError::CorruptRecord {
                    file: file.clone(),
                    details: "canonical file is missing".to_owned(),
                }
            } else {
                error.into()
            }
        })?;
        serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
            file,
            details: error.to_string(),
        })
    }
}

impl DaemonStorePort for FsDaemonStore {
    fn list_tasks(&self, base_dir: &Path) -> AppResult<Vec<DaemonTask>> {
        let dir = Self::tasks_dir(base_dir);
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut tasks = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let file = format!(
                "daemon/tasks/{}",
                path.file_name().unwrap_or_default().to_string_lossy()
            );
            tasks.push(Self::read_json_file(&path, file)?);
        }

        tasks.sort_by(|a: &DaemonTask, b: &DaemonTask| {
            a.status
                .is_terminal()
                .cmp(&b.status.is_terminal())
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.task_id.cmp(&b.task_id))
        });
        Ok(tasks)
    }

    fn read_task(&self, base_dir: &Path, task_id: &str) -> AppResult<DaemonTask> {
        Self::read_json_file(
            &Self::task_path(base_dir, task_id),
            format!("daemon/tasks/{task_id}.json"),
        )
    }

    fn create_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
        let contents = serde_json::to_string_pretty(task)?;
        FileSystem::write_create_new(&Self::task_path(base_dir, &task.task_id), &contents)
    }

    fn write_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
        let contents = serde_json::to_string_pretty(task)?;
        FileSystem::write_atomic(&Self::task_path(base_dir, &task.task_id), &contents)
    }

    fn list_leases(&self, base_dir: &Path) -> AppResult<Vec<WorktreeLease>> {
        Ok(self
            .list_lease_records(base_dir)?
            .into_iter()
            .filter_map(|record| match record {
                LeaseRecord::Worktree(lease) => Some(lease),
                LeaseRecord::CliWriter(_) => None,
            })
            .collect())
    }

    fn read_lease(&self, base_dir: &Path, lease_id: &str) -> AppResult<WorktreeLease> {
        match self.read_lease_record(base_dir, lease_id)? {
            LeaseRecord::Worktree(lease) => Ok(lease),
            LeaseRecord::CliWriter(_) => Err(AppError::CorruptRecord {
                file: format!("daemon/leases/{lease_id}.json"),
                details: "lease record is not a worktree lease".to_owned(),
            }),
        }
    }

    fn write_lease(&self, base_dir: &Path, lease: &WorktreeLease) -> AppResult<()> {
        self.write_lease_record(base_dir, &LeaseRecord::from(lease.clone()))
    }

    fn list_lease_records(&self, base_dir: &Path) -> AppResult<Vec<LeaseRecord>> {
        let dir = Self::leases_dir(base_dir);
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut leases = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let file = format!(
                "daemon/leases/{}",
                path.file_name().unwrap_or_default().to_string_lossy()
            );
            leases.push(Self::read_json_file(&path, file)?);
        }
        leases.sort_by(|a: &LeaseRecord, b: &LeaseRecord| {
            a.acquired_at()
                .cmp(b.acquired_at())
                .then_with(|| a.lease_id().cmp(b.lease_id()))
        });
        Ok(leases)
    }

    fn read_lease_record(&self, base_dir: &Path, lease_id: &str) -> AppResult<LeaseRecord> {
        Self::read_json_file(
            &Self::lease_path(base_dir, lease_id),
            format!("daemon/leases/{lease_id}.json"),
        )
    }

    fn write_lease_record(&self, base_dir: &Path, lease: &LeaseRecord) -> AppResult<()> {
        let contents = serde_json::to_string_pretty(lease)?;
        FileSystem::write_atomic(&Self::lease_path(base_dir, lease.lease_id()), &contents)
    }

    fn remove_lease(
        &self,
        base_dir: &Path,
        lease_id: &str,
    ) -> AppResult<crate::contexts::automation_runtime::ResourceCleanupOutcome> {
        use crate::contexts::automation_runtime::ResourceCleanupOutcome;
        match fs::remove_file(Self::lease_path(base_dir, lease_id)) {
            Ok(()) => Ok(ResourceCleanupOutcome::Removed),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(ResourceCleanupOutcome::AlreadyAbsent)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn read_daemon_journal(&self, base_dir: &Path) -> AppResult<Vec<DaemonJournalEvent>> {
        let path = Self::daemon_journal_path(base_dir);
        match fs::read_to_string(&path) {
            Ok(contents) => {
                let mut events = Vec::new();
                for line in contents.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let event: DaemonJournalEvent =
                        serde_json::from_str(trimmed).map_err(|error| AppError::CorruptRecord {
                            file: "daemon/journal.ndjson".to_owned(),
                            details: error.to_string(),
                        })?;
                    events.push(event);
                }
                Ok(events)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    fn append_daemon_journal_event(
        &self,
        base_dir: &Path,
        event: &DaemonJournalEvent,
    ) -> AppResult<()> {
        let line = serde_json::to_string(event)?;
        FileSystem::append_line(&Self::daemon_journal_path(base_dir), &line)
    }

    fn acquire_writer_lock(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        lease_id: &str,
    ) -> AppResult<()> {
        let path = Self::writer_lock_path(base_dir, project_id);
        match FileSystem::write_create_new(&path, lease_id) {
            Ok(()) => Ok(()),
            Err(AppError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(AppError::ProjectWriterLockHeld {
                    project_id: project_id.to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn release_writer_lock(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        expected_owner: &str,
    ) -> AppResult<crate::contexts::automation_runtime::WriterLockReleaseOutcome> {
        use crate::contexts::automation_runtime::WriterLockReleaseOutcome;
        use std::io::Read as _;
        let path = Self::writer_lock_path(base_dir, project_id);

        // ── Phase 1: read-only ownership check ──────────────────────────
        // Open via fd so we can later compare inodes after the rename to
        // detect a concurrent file replacement.
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(WriterLockReleaseOutcome::AlreadyAbsent);
            }
            Err(e) => return Err(e.into()),
        };

        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        if contents != expected_owner {
            return Ok(WriterLockReleaseOutcome::OwnerMismatch {
                actual_owner: contents,
            });
        }

        // ── Phase 2: atomic rename-to-staging ───────────────────────────
        // Content matches. Atomically move the lock file to a staging path
        // so the subsequent delete cannot affect a replacement lock that
        // appeared after our read. The staging target is unique per releaser
        // (keyed by expected_owner) so two concurrent releasers cannot
        // clobber each other's staging file.
        #[cfg(test)]
        release_lock_testing::invoke_pre_rename_hook();

        let staging = path.with_extension(format!(
            "lock.releasing.{}",
            expected_owner.replace('/', "_")
        ));
        match fs::rename(&path, &staging) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(WriterLockReleaseOutcome::AlreadyAbsent);
            }
            Err(e) => return Err(e.into()),
        }

        #[cfg(test)]
        release_lock_testing::invoke_post_rename_hook();

        // ── Phase 3: inode verification ─────────────────────────────────
        // Verify the renamed file is the same inode we read from. Between
        // phase 1 (open) and phase 2 (rename), another writer could have
        // replaced the file; the rename would then move their file to
        // staging. Detect this by comparing the fd inode with the staging
        // inode.
        {
            use std::os::unix::fs::MetadataExt;
            let fd_meta = match file.metadata() {
                Ok(m) => m,
                Err(e) => {
                    // fstat on the open fd failed. Restore canonical lock
                    // visibility before returning the error.
                    let _ = fs::rename(&staging, &path);
                    return Err(e.into());
                }
            };
            let staged_meta = match fs::metadata(&staging) {
                Ok(m) => m,
                Err(e) => {
                    // Cannot verify — restore canonical lock visibility
                    // before returning the error so the lock is not
                    // stranded at the staging path.
                    let _ = fs::rename(&staging, &path);
                    return Err(e.into());
                }
            };
            if fd_meta.ino() != staged_meta.ino() || fd_meta.dev() != staged_meta.dev() {
                // A different file was at the canonical path when we
                // renamed. Restore it via hard_link (which fails safely
                // if a new lock already exists at the canonical path).
                drop(file);
                match fs::hard_link(&staging, &path) {
                    Ok(()) => {
                        let _ = fs::remove_file(&staging);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        // Canonical path already has a new lock — the
                        // staging copy has been superseded; safe to
                        // delete it.
                        let _ = fs::remove_file(&staging);
                    }
                    Err(_) => {
                        // Cannot restore to canonical path due to an
                        // unexpected I/O error. Leave the staging file
                        // intact for later recovery — do NOT delete it.
                    }
                }
                return Ok(WriterLockReleaseOutcome::OwnerMismatch {
                    actual_owner: "(replaced)".to_owned(),
                });
            }
        }

        // ── Phase 4: delete the verified staging file ───────────────────
        // The staging path is unique per expected_owner, so no concurrent
        // releaser can rename a different lock onto our staging slot. This
        // delete operates on the exact file instance we verified.
        drop(file);

        #[cfg(test)]
        release_lock_testing::invoke_post_verify_hook();

        match fs::remove_file(&staging) {
            Ok(()) => Ok(WriterLockReleaseOutcome::Released),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(WriterLockReleaseOutcome::Released)
            }
            Err(e) => Err(e.into()),
        }
    }
}

// ── Test hook for TOCTOU-safe writer-lock release ───────────────────────────

#[cfg(test)]
pub mod release_lock_testing {
    use std::cell::RefCell;

    thread_local! {
        static PRE_RENAME_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
        static POST_RENAME_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
        static POST_VERIFY_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    /// Hook that fires after the content check passes but before the atomic
    /// rename-to-staging. Tests can swap the file at the canonical path here
    /// to exercise the inode-mismatch detection after rename.
    pub fn set_pre_rename_hook(f: impl FnOnce() + 'static) {
        PRE_RENAME_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(f));
        });
    }

    /// Hook that fires after the atomic rename-to-staging succeeds but
    /// before the inode verification reads staging metadata. Tests can
    /// manipulate the staging file here to exercise verification failures.
    pub fn set_post_rename_hook(f: impl FnOnce() + 'static) {
        POST_RENAME_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(f));
        });
    }

    /// Hook that fires after all verification (content + inode) has passed
    /// but before the staging file is deleted. Tests can create a new lock
    /// at the canonical path here to prove it survives the release.
    pub fn set_post_verify_hook(f: impl FnOnce() + 'static) {
        POST_VERIFY_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(f));
        });
    }

    pub(crate) fn invoke_pre_rename_hook() {
        PRE_RENAME_HOOK.with(|h| {
            if let Some(f) = h.borrow_mut().take() {
                f();
            }
        });
    }

    pub(crate) fn invoke_post_rename_hook() {
        POST_RENAME_HOOK.with(|h| {
            if let Some(f) = h.borrow_mut().take() {
                f();
            }
        });
    }

    pub(crate) fn invoke_post_verify_hook() {
        POST_VERIFY_HOOK.with(|h| {
            if let Some(f) = h.borrow_mut().take() {
                f();
            }
        });
    }
}

// ── Requirements drafting filesystem store ──────────────────────────────────

use crate::contexts::requirements_drafting::model::{
    PersistedAnswers, RequirementsJournalEvent, RequirementsRun,
};
use crate::contexts::requirements_drafting::service::RequirementsStorePort;

const REQUIREMENTS_DIR: &str = "requirements";

/// Required subdirectories inside a requirements run.
const REQUIREMENTS_RUN_SUBDIRS: &[&str] = &[
    "history/payloads",
    "history/artifacts",
    "runtime/logs",
    "runtime/backend",
    "runtime/temp",
    "seed",
];

/// Filesystem-backed implementation of `RequirementsStorePort`.
pub struct FsRequirementsStore;

impl RequirementsStorePort for FsRequirementsStore {
    fn create_run_dir(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
        let run_root = requirements_run_root(base_dir, run_id);
        fs::create_dir_all(&run_root)?;
        for subdir in REQUIREMENTS_RUN_SUBDIRS {
            fs::create_dir_all(run_root.join(subdir))?;
        }
        // Agent execution expects sessions.json in the project root (run root).
        let sessions_path = run_root.join(SESSIONS_FILE);
        if !sessions_path.exists() {
            let empty = serde_json::to_string_pretty(
                &crate::contexts::agent_execution::session::PersistedSessions::empty(),
            )?;
            FileSystem::write_atomic(&sessions_path, &empty)?;
        }

        // Spec requires every requirements run to persist answers.toml and
        // answers.json, even when questions are skipped (quick mode) or answers
        // have not yet been submitted.
        let answers_toml_path = run_root.join("answers.toml");
        if !answers_toml_path.exists() {
            FileSystem::write_atomic(
                &answers_toml_path,
                "# No clarifying questions for this run.\n",
            )?;
        }
        let answers_json_path = run_root.join("answers.json");
        if !answers_json_path.exists() {
            let empty_answers = serde_json::to_string_pretty(&PersistedAnswers {
                answers: Vec::new(),
            })?;
            FileSystem::write_atomic(&answers_json_path, &empty_answers)?;
        }

        Ok(())
    }

    fn write_run(&self, base_dir: &Path, run_id: &str, run: &RequirementsRun) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id).join("run.json");
        let contents = serde_json::to_string_pretty(run)?;
        FileSystem::write_atomic(&path, &contents)
    }

    fn read_run(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsRun> {
        let path = requirements_run_root(base_dir, run_id).join("run.json");
        match fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                file: format!("requirements/{}/run.json", run_id),
                details: e.to_string(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(AppError::InvalidRequirementsState {
                    run_id: run_id.to_owned(),
                    details: "requirements run not found".to_owned(),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    fn append_journal_event(
        &self,
        base_dir: &Path,
        run_id: &str,
        event: &RequirementsJournalEvent,
    ) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id).join("journal.ndjson");
        let line = serde_json::to_string(event)?;
        FileSystem::append_line(&path, &line)
    }

    fn read_journal(
        &self,
        base_dir: &Path,
        run_id: &str,
    ) -> AppResult<Vec<RequirementsJournalEvent>> {
        let path = requirements_run_root(base_dir, run_id).join("journal.ndjson");
        match fs::read_to_string(&path) {
            Ok(contents) => {
                let mut events = Vec::new();
                for line in contents.lines() {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let event: RequirementsJournalEvent =
                        serde_json::from_str(trimmed).map_err(|e| AppError::CorruptRecord {
                            file: format!("requirements/{}/journal.ndjson", run_id),
                            details: e.to_string(),
                        })?;
                    events.push(event);
                }
                Ok(events)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn write_payload(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        payload: &serde_json::Value,
    ) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id)
            .join("history/payloads")
            .join(format!("{payload_id}.json"));
        let contents = serde_json::to_string_pretty(payload)?;
        FileSystem::write_atomic(&path, &contents)
    }

    fn write_artifact(
        &self,
        base_dir: &Path,
        run_id: &str,
        artifact_id: &str,
        content: &str,
    ) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id)
            .join("history/artifacts")
            .join(format!("{artifact_id}.md"));
        FileSystem::write_atomic(&path, content)
    }

    fn read_payload(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
    ) -> AppResult<serde_json::Value> {
        let path = requirements_run_root(base_dir, run_id)
            .join("history/payloads")
            .join(format!("{payload_id}.json"));
        let contents = FileSystem::read_to_string(&path)?;
        serde_json::from_str(&contents).map_err(|e| AppError::CorruptRecord {
            file: format!(
                "requirements/{}/history/payloads/{}.json",
                run_id, payload_id
            ),
            details: e.to_string(),
        })
    }

    fn write_payload_artifact_pair_atomic(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        payload: &serde_json::Value,
        artifact_id: &str,
        artifact: &str,
    ) -> AppResult<()> {
        let run_root = requirements_run_root(base_dir, run_id);
        let staging_dir = run_root.join("runtime/temp");
        fs::create_dir_all(&staging_dir)?;

        let payload_final = run_root
            .join("history/payloads")
            .join(format!("{payload_id}.json"));
        let artifact_final = run_root
            .join("history/artifacts")
            .join(format!("{artifact_id}.md"));

        let payload_staging = staging_dir.join(format!(".{payload_id}.payload.staging"));
        let artifact_staging = staging_dir.join(format!(".{artifact_id}.artifact.staging"));

        let payload_contents = serde_json::to_string_pretty(payload)?;

        // Write payload to staging
        FileSystem::write_atomic(&payload_staging, &payload_contents)?;

        // Write artifact to staging — if this fails, clean up payload staging
        if let Err(e) = FileSystem::write_atomic(&artifact_staging, artifact) {
            let _ = fs::remove_file(&payload_staging);
            return Err(e);
        }

        // Rename payload staging into canonical location
        if let Err(e) = fs::rename(&payload_staging, &payload_final) {
            let _ = fs::remove_file(&payload_staging);
            let _ = fs::remove_file(&artifact_staging);
            return Err(e.into());
        }

        // Rename artifact staging into canonical location
        if let Err(e) = fs::rename(&artifact_staging, &artifact_final) {
            // Roll back the payload that was already moved to canonical location
            // and remove the orphaned staged artifact file
            let _ = fs::remove_file(&payload_final);
            let _ = fs::remove_file(&artifact_staging);
            return Err(e.into());
        }

        Ok(())
    }

    fn write_answers_toml(&self, base_dir: &Path, run_id: &str, template: &str) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id).join("answers.toml");
        FileSystem::write_atomic(&path, template)
    }

    fn read_answers_toml(&self, base_dir: &Path, run_id: &str) -> AppResult<String> {
        let path = requirements_run_root(base_dir, run_id).join("answers.toml");
        FileSystem::read_to_string(&path)
    }

    fn write_answers_json(
        &self,
        base_dir: &Path,
        run_id: &str,
        answers: &PersistedAnswers,
    ) -> AppResult<()> {
        let path = requirements_run_root(base_dir, run_id).join("answers.json");
        let contents = serde_json::to_string_pretty(answers)?;
        FileSystem::write_atomic(&path, &contents)
    }

    fn read_answers_json(&self, base_dir: &Path, run_id: &str) -> AppResult<PersistedAnswers> {
        let path = requirements_run_root(base_dir, run_id).join("answers.json");
        let contents = FileSystem::read_to_string(&path)?;
        serde_json::from_str(&contents).map_err(|e| AppError::CorruptRecord {
            file: format!("requirements/{}/answers.json", run_id),
            details: e.to_string(),
        })
    }

    fn write_seed_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        project_json: &serde_json::Value,
        prompt_md: &str,
    ) -> AppResult<()> {
        let seed_dir = requirements_run_root(base_dir, run_id).join("seed");
        fs::create_dir_all(&seed_dir)?;

        let project_path = seed_dir.join("project.json");
        let prompt_path = seed_dir.join("prompt.md");

        // Write project.json first
        let project_contents = serde_json::to_string_pretty(project_json)?;
        FileSystem::write_atomic(&project_path, &project_contents)?;

        // Test injection: simulate prompt.md write failure after project.json succeeds
        if std::env::var("RALPH_BURNING_TEST_SEED_PROMPT_WRITE_FAIL").is_ok() {
            let _ = fs::remove_file(&project_path);
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected prompt.md write failure",
            )));
        }

        // Write prompt.md — if this fails, remove project.json too
        if let Err(e) = FileSystem::write_atomic(&prompt_path, prompt_md) {
            let _ = fs::remove_file(&project_path);
            return Err(e);
        }

        Ok(())
    }

    fn remove_seed_pair(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
        let seed_dir = requirements_run_root(base_dir, run_id).join("seed");
        let _ = fs::remove_file(seed_dir.join("project.json"));
        let _ = fs::remove_file(seed_dir.join("prompt.md"));
        Ok(())
    }

    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()> {
        let run_root = requirements_run_root(base_dir, run_id);
        let _ = fs::remove_file(
            run_root
                .join("history/payloads")
                .join(format!("{payload_id}.json")),
        );
        let _ = fs::remove_file(
            run_root
                .join("history/artifacts")
                .join(format!("{artifact_id}.md")),
        );
        Ok(())
    }

    fn answers_toml_path(&self, base_dir: &Path, run_id: &str) -> PathBuf {
        requirements_run_root(base_dir, run_id).join("answers.toml")
    }

    fn seed_prompt_path(&self, base_dir: &Path, run_id: &str) -> PathBuf {
        requirements_run_root(base_dir, run_id)
            .join("seed")
            .join("prompt.md")
    }
}

fn requirements_run_root(base_dir: &Path, run_id: &str) -> PathBuf {
    FileSystem::workspace_root_path(base_dir)
        .join(REQUIREMENTS_DIR)
        .join(run_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::automation_runtime::{DaemonStorePort, WriterLockReleaseOutcome};
    use crate::shared::domain::ProjectId;
    use tempfile::tempdir;

    #[test]
    fn release_writer_lock_detects_replaced_file_via_inode() {
        // Regression: exercises the window where a lock file is replaced
        // between the content read and the rename-to-staging. The
        // pre-rename hook simulates another writer deleting and recreating
        // the lock after the content check passes. The rename then moves
        // the replacement to staging, where the inode comparison detects
        // the swap and restores the replacement lock.
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("toctou-test".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path =
            FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let lock_path_clone = lock_path.clone();

        // Install hook: after content read matches "owner-A", replace
        // the file on disk with a new inode containing "owner-B".
        release_lock_testing::set_pre_rename_hook(move || {
            std::fs::remove_file(&lock_path_clone).expect("remove old lock");
            std::fs::write(&lock_path_clone, "owner-B").expect("write replacement");
        });

        let outcome = store
            .release_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("release should not error");

        // The inode changed, so release must fail closed.
        assert!(
            matches!(outcome, WriterLockReleaseOutcome::OwnerMismatch { .. }),
            "expected OwnerMismatch due to inode change, got: {outcome:?}"
        );

        // The replacement lock must survive — it was restored to the
        // canonical path via hard_link.
        assert_eq!(
            std::fs::read_to_string(&lock_path).expect("read replacement"),
            "owner-B",
            "replacement lock must not be deleted"
        );

        // Cleanup
        std::fs::remove_file(&lock_path).expect("cleanup");
    }

    #[test]
    fn release_writer_lock_post_check_replacement_survives() {
        // Regression: after all identity checks pass (content + inode)
        // and the staging file is about to be deleted, a new lock created
        // at the canonical path must survive because the release only
        // removes the staging copy, never the canonical path.
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("post-check".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path =
            FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let lock_path_clone = lock_path.clone();

        // Install hook: after inode verification passes, create a new
        // lock at the canonical path to simulate a new writer acquiring
        // between our final check and the staging delete.
        release_lock_testing::set_post_verify_hook(move || {
            std::fs::write(&lock_path_clone, "owner-B")
                .expect("write new lock");
        });

        let outcome = store
            .release_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("release should not error");

        // Release should succeed — we deleted the verified staging copy.
        assert!(
            matches!(outcome, WriterLockReleaseOutcome::Released),
            "expected Released, got: {outcome:?}"
        );

        // The new lock at the canonical path must survive.
        assert_eq!(
            std::fs::read_to_string(&lock_path).expect("read new lock"),
            "owner-B",
            "post-check replacement lock must survive"
        );

        // Cleanup
        std::fs::remove_file(&lock_path).expect("cleanup");
    }

    #[test]
    fn release_writer_lock_post_rename_verification_failure_preserves_lock() {
        // Regression: after rename-to-staging succeeds, if the staging
        // file becomes unavailable (e.g. deleted by another process),
        // the release must return an error without stranding or deleting
        // any lock artifact at the canonical path.
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("post-rename-fail".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path =
            FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let staging_path = lock_path.with_extension("lock.releasing.owner-A");
        let lock_path_clone = lock_path.clone();
        let staging_clone = staging_path.clone();

        // After rename-to-staging: delete the staging file and place a
        // replacement lock at the canonical path. This simulates the
        // staging file becoming unavailable while a new writer acquires.
        release_lock_testing::set_post_rename_hook(move || {
            std::fs::remove_file(&staging_clone).expect("remove staging");
            std::fs::write(&lock_path_clone, "owner-B")
                .expect("write replacement lock");
        });

        let result = store
            .release_writer_lock(temp.path(), &project_id, "owner-A");

        // Must return an error (verification failed — staging metadata
        // could not be read).
        assert!(
            result.is_err(),
            "expected error on verification failure, got: {result:?}"
        );

        // The replacement lock at the canonical path must survive
        // untouched — the release must not have deleted it.
        assert_eq!(
            std::fs::read_to_string(&lock_path).expect("read canonical"),
            "owner-B",
            "replacement lock at canonical path must be untouched"
        );

        // Cleanup
        std::fs::remove_file(&lock_path).expect("cleanup");
    }

    #[test]
    fn release_writer_lock_unique_staging_prevents_cross_releaser_clobber() {
        // Regression: with a shared staging path, releaser B could rename
        // its lock onto releaser A's staging file between A's inode check
        // and A's final delete, causing A to delete B's validated lock.
        // With per-owner staging paths this race is impossible: each
        // releaser stages to a distinct path.
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("staging-race".to_owned()).expect("valid id");

        // Acquire lock as owner-A.
        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire A");

        let lock_path =
            FsDaemonStore::writer_lock_path(temp.path(), &project_id);

        // Compute both staging paths to verify they are distinct.
        let staging_a = lock_path.with_extension("lock.releasing.owner-A");
        let staging_b = lock_path.with_extension("lock.releasing.owner-B");
        assert_ne!(staging_a, staging_b, "staging paths must differ per owner");

        // Release owner-A. After inode verification passes but before the
        // staging file is deleted, simulate owner-B's release placing its
        // own staging file at the owner-B staging path.
        let staging_b_clone = staging_b.clone();
        release_lock_testing::set_post_verify_hook(move || {
            // Simulate owner-B having renamed its lock to its own staging
            // path. With the old fixed staging path, this would have
            // clobbered owner-A's staging; now it lands on a separate path.
            std::fs::write(&staging_b_clone, "owner-B")
                .expect("write B staging");
        });

        let outcome = store
            .release_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("release A should not error");

        assert!(
            matches!(outcome, WriterLockReleaseOutcome::Released),
            "expected Released for owner-A, got: {outcome:?}"
        );

        // Owner-A's staging file was deleted.
        assert!(
            !staging_a.exists(),
            "owner-A staging must be deleted after release"
        );

        // Owner-B's staging file survived — not clobbered by A's delete.
        assert_eq!(
            std::fs::read_to_string(&staging_b).expect("read B staging"),
            "owner-B",
            "owner-B staging must survive owner-A's release"
        );

        // Cleanup
        std::fs::remove_file(&staging_b).expect("cleanup B staging");
    }
}

