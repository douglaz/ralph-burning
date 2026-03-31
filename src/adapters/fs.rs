use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use nix::fcntl::{Flock, FlockArg};

use crate::contexts::agent_execution::model::RawOutputReference;
use crate::contexts::agent_execution::service::RawOutputPort;
use crate::contexts::agent_execution::session::{PersistedSessions, SessionStorePort};
use crate::contexts::automation_runtime::model::{
    DaemonJournalEvent, DaemonTask, LeaseRecord, WorktreeLease,
};
use crate::contexts::automation_runtime::repo_registry::{
    parse_repo_slug, RepoRegistration, RepoRegistryPort,
};
use crate::contexts::automation_runtime::DaemonStorePort;
use crate::contexts::milestone_record::model::{
    collapse_task_run_attempts, find_matching_running_task_run, matching_finalized_task_runs,
    render_completion_journal_details, render_start_journal_details, CompletionJournalDetails,
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneRecord, MilestoneSnapshot,
    StartJournalDetails, TaskRunEntry, TaskRunOutcome,
};
use crate::contexts::milestone_record::service::{
    MilestoneJournalPort, MilestonePlanPort, MilestoneSnapshotPort, MilestoneStorePort,
    TaskRunLineagePort,
};
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
use crate::shared::domain::{ProjectConfig, ProjectId, StageId, WorkspaceConfig};
use crate::shared::error::{AppError, AppResult};

const ACTIVE_PROJECT_FILE: &str = "active-project";
const WORKSPACE_DIR: &str = ".ralph-burning";
const PROJECTS_DIR: &str = "projects";
const PROJECT_CONFIG_FILE: &str = "project.toml";
const PROJECT_POLICY_CONFIG_FILE: &str = "config.toml";
const RUN_FILE: &str = "run.json";
const JOURNAL_FILE: &str = "journal.ndjson";
const SESSIONS_FILE: &str = "sessions.json";
const PROMPT_FILE: &str = "prompt.md";
const JOURNAL_APPEND_FAIL_AFTER_ENV: &str = "RALPH_BURNING_TEST_JOURNAL_APPEND_FAIL_AFTER";
const AMENDMENT_WRITE_FAIL_AFTER_ENV: &str = "RALPH_BURNING_TEST_AMENDMENT_WRITE_FAIL_AFTER";
const AMENDMENT_REMOVE_FAIL_AFTER_ENV: &str = "RALPH_BURNING_TEST_AMENDMENT_REMOVE_FAIL_AFTER";

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

static JOURNAL_APPEND_FAIL_COUNT: AtomicU32 = AtomicU32::new(0);
static JOURNAL_APPEND_FAIL_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static AMENDMENT_WRITE_FAIL_COUNT: AtomicU32 = AtomicU32::new(0);
static AMENDMENT_WRITE_FAIL_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();
static AMENDMENT_REMOVE_FAIL_COUNT: AtomicU32 = AtomicU32::new(0);
static AMENDMENT_REMOVE_FAIL_CONFIG: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn maybe_inject_project_failpoint(
    env_var: &str,
    project_id: &ProjectId,
    counter: &AtomicU32,
    config_state: &OnceLock<Mutex<Option<String>>>,
    error_message: &str,
) -> AppResult<()> {
    let Ok(raw_config) = env::var(env_var) else {
        clear_failpoint_state(counter, config_state);
        return Ok(());
    };

    let Some((target_project, threshold)) = parse_failpoint_config(&raw_config) else {
        return Ok(());
    };

    let state = config_state.get_or_init(|| Mutex::new(None));
    let mut current_config = state.lock().expect("failpoint config lock poisoned");
    if current_config.as_deref() != Some(raw_config.as_str()) {
        counter.store(0, Ordering::SeqCst);
        *current_config = Some(raw_config.clone());
    }
    drop(current_config);

    if target_project.is_some_and(|target| target != project_id.as_str()) {
        return Ok(());
    }

    let current = counter.fetch_add(1, Ordering::SeqCst);
    if current >= threshold {
        return Err(AppError::Io(std::io::Error::other(
            error_message.to_owned(),
        )));
    }

    Ok(())
}

fn clear_failpoint_state(counter: &AtomicU32, config_state: &OnceLock<Mutex<Option<String>>>) {
    let Some(state) = config_state.get() else {
        return;
    };
    let mut current_config = state.lock().expect("failpoint config lock poisoned");
    if current_config.take().is_some() {
        counter.store(0, Ordering::SeqCst);
    }
}

fn parse_failpoint_config(raw: &str) -> Option<(Option<&str>, u32)> {
    if let Ok(threshold) = raw.parse::<u32>() {
        return Some((None, threshold));
    }

    let (project_id, threshold) = raw.split_once(':')?;
    Some((Some(project_id), threshold.parse().ok()?))
}

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

    /// Atomically update `prompt.md`, write `prompt.original.md`, and recompute
    /// the prompt hash in `project.toml`. If any step fails, previously written
    /// files in this batch are best-effort cleaned up.
    pub fn replace_prompt_atomically(
        base_dir: &Path,
        project_id: &ProjectId,
        original_prompt: &str,
        refined_prompt: &str,
    ) -> AppResult<String> {
        let project_root = Self::project_root(base_dir, project_id);
        let prompt_path = project_root.join(PROMPT_FILE);
        let original_path = project_root.join("prompt.original.md");
        let project_toml_path = project_root.join(PROJECT_CONFIG_FILE);

        // Step 1: Write prompt.original.md
        Self::write_atomic(&original_path, original_prompt).map_err(|e| {
            AppError::PromptReplacementFailed {
                details: format!("failed to write prompt.original.md: {e}"),
            }
        })?;

        // Step 2: Replace prompt.md
        if let Err(e) = Self::write_atomic(&prompt_path, refined_prompt) {
            let _ = fs::remove_file(&original_path);
            return Err(AppError::PromptReplacementFailed {
                details: format!("failed to write prompt.md: {e}"),
            });
        }

        // Step 3: Recompute prompt hash and update project.toml
        let new_hash = Self::prompt_hash(refined_prompt);
        let project_toml_content = match fs::read_to_string(&project_toml_path) {
            Ok(content) => content,
            Err(e) => {
                // Rollback: restore original prompt.md, remove prompt.original.md
                let _ = Self::write_atomic(&prompt_path, original_prompt);
                let _ = fs::remove_file(&original_path);
                return Err(AppError::PromptReplacementFailed {
                    details: format!("failed to read project.toml for hash update: {e}"),
                });
            }
        };

        // Parse and update prompt_hash. If parse or serialize fails, rollback
        // prompt.md and prompt.original.md to preserve the atomicity invariant.
        let mut project_record: crate::contexts::project_run_record::model::ProjectRecord =
            match toml::from_str(&project_toml_content) {
                Ok(record) => record,
                Err(e) => {
                    let _ = Self::write_atomic(&prompt_path, original_prompt);
                    let _ = fs::remove_file(&original_path);
                    return Err(AppError::PromptReplacementFailed {
                        details: format!("failed to parse project.toml: {e}"),
                    });
                }
            };
        project_record.prompt_hash = new_hash.clone();
        let updated_toml = match toml::to_string_pretty(&project_record) {
            Ok(toml) => toml,
            Err(e) => {
                let _ = Self::write_atomic(&prompt_path, original_prompt);
                let _ = fs::remove_file(&original_path);
                return Err(AppError::PromptReplacementFailed {
                    details: format!("failed to serialize project.toml: {e}"),
                });
            }
        };
        if let Err(e) = Self::write_atomic(&project_toml_path, &updated_toml) {
            let _ = Self::write_atomic(&prompt_path, original_prompt);
            let _ = fs::remove_file(&original_path);
            return Err(AppError::PromptReplacementFailed {
                details: format!("failed to write project.toml: {e}"),
            });
        }

        Ok(new_hash)
    }

    /// Best-effort revert of a successful `replace_prompt_atomically` call.
    /// Restores `prompt.md` to `original_prompt`, removes `prompt.original.md`,
    /// and restores the prompt hash in `project.toml`. All steps are
    /// best-effort; individual failures are silently ignored since this runs
    /// on error-recovery paths where partial cleanup is acceptable.
    pub fn revert_prompt_replacement(
        base_dir: &Path,
        project_id: &ProjectId,
        original_prompt: &str,
    ) {
        let project_root = Self::project_root(base_dir, project_id);
        let prompt_path = project_root.join(PROMPT_FILE);
        let original_path = project_root.join("prompt.original.md");
        let project_toml_path = project_root.join(PROJECT_CONFIG_FILE);

        // Restore original prompt.md
        let _ = Self::write_atomic(&prompt_path, original_prompt);

        // Remove prompt.original.md
        let _ = fs::remove_file(&original_path);

        // Restore original hash in project.toml
        let original_hash = Self::prompt_hash(original_prompt);
        if let Ok(content) = fs::read_to_string(&project_toml_path) {
            if let Ok(mut record) = toml::from_str::<
                crate::contexts::project_run_record::model::ProjectRecord,
            >(&content)
            {
                record.prompt_hash = original_hash;
                if let Ok(updated) = toml::to_string_pretty(&record) {
                    let _ = Self::write_atomic(&project_toml_path, &updated);
                }
            }
        }
    }

    // ── Helpers for project filesystem layout ──

    pub(crate) fn workspace_root_path(base_dir: &Path) -> PathBuf {
        base_dir.join(WORKSPACE_DIR)
    }

    pub(crate) fn daemon_root(base_dir: &Path) -> PathBuf {
        Self::workspace_root_path(base_dir).join("daemon")
    }

    pub(crate) fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(project_id.as_str())
    }

    pub fn read_project_config(
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectConfig> {
        let path = Self::project_root(base_dir, project_id).join(PROJECT_POLICY_CONFIG_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => toml::from_str(&raw).map_err(|error| AppError::CorruptRecord {
                file: format!("projects/{}/config.toml", project_id),
                details: error.to_string(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(ProjectConfig::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn write_project_config(
        base_dir: &Path,
        project_id: &ProjectId,
        config: &ProjectConfig,
    ) -> AppResult<()> {
        let rendered = toml::to_string_pretty(config)?;
        Self::write_atomic(
            &Self::project_root(base_dir, project_id).join(PROJECT_POLICY_CONFIG_FILE),
            &rendered,
        )
    }

    pub(crate) fn project_policy_config_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::project_root(base_dir, project_id).join(PROJECT_POLICY_CONFIG_FILE)
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

            // config.toml
            let project_config_toml = toml::to_string_pretty(&ProjectConfig::default())
                .map_err(|e| AppError::Io(std::io::Error::other(e.to_string())))?;
            fs::write(
                staging_root.join(PROJECT_POLICY_CONFIG_FILE),
                project_config_toml,
            )?;

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
        maybe_inject_project_failpoint(
            JOURNAL_APPEND_FAIL_AFTER_ENV,
            project_id,
            &JOURNAL_APPEND_FAIL_COUNT,
            &JOURNAL_APPEND_FAIL_CONFIG,
            "injected journal append failure for testing",
        )?;
        let path = FileSystem::project_root(base_dir, project_id).join(JOURNAL_FILE);
        FileSystem::append_line(&path, line)
    }
}

/// Filesystem-backed implementation of `ArtifactStorePort`.
pub struct FsArtifactStore;

impl FsArtifactStore {
    pub fn read_payload_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
    ) -> AppResult<PayloadRecord> {
        let path = FileSystem::project_root(base_dir, project_id)
            .join("history/payloads")
            .join(format!("{payload_id}.json"));
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(AppError::PayloadNotFound {
                    payload_id: payload_id.to_owned(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
            file: format!("history/payloads/{payload_id}.json"),
            details: error.to_string(),
        })
    }

    pub fn read_artifact_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        artifact_id: &str,
    ) -> AppResult<ArtifactRecord> {
        let path = FileSystem::project_root(base_dir, project_id)
            .join("history/artifacts")
            .join(format!("{artifact_id}.json"));
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(AppError::ArtifactNotFound {
                    artifact_id: artifact_id.to_owned(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
            file: format!("history/artifacts/{artifact_id}.json"),
            details: error.to_string(),
        })
    }
}

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

    fn read_payload_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
    ) -> AppResult<PayloadRecord> {
        FsArtifactStore::read_payload_by_id(self, base_dir, project_id, payload_id)
    }

    fn read_artifact_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        artifact_id: &str,
    ) -> AppResult<ArtifactRecord> {
        FsArtifactStore::read_artifact_by_id(self, base_dir, project_id, artifact_id)
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
        // Format: "N" or "<project_id>:N" where N is the number of successful
        // writes before failure. E.g., "1" means the first write succeeds, the
        // second fails.
        maybe_inject_project_failpoint(
            AMENDMENT_WRITE_FAIL_AFTER_ENV,
            project_id,
            &AMENDMENT_WRITE_FAIL_COUNT,
            &AMENDMENT_WRITE_FAIL_CONFIG,
            "injected amendment write failure for testing",
        )?;
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
        maybe_inject_project_failpoint(
            AMENDMENT_REMOVE_FAIL_AFTER_ENV,
            project_id,
            &AMENDMENT_REMOVE_FAIL_COUNT,
            &AMENDMENT_REMOVE_FAIL_CONFIG,
            "injected amendment remove failure for testing",
        )?;
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
            if path.extension().is_none_or(|ext| ext != "json") {
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
            if path.extension().is_none_or(|ext| ext != "json") {
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
        release_writer_lock_impl(
            &Self::writer_lock_path(base_dir, project_id),
            expected_owner,
        )
    }
}

/// Shared implementation of the TOCTOU-safe writer-lock release logic.
/// Used by both `FsDaemonStore` and `FsDataDirDaemonStore` to avoid
/// duplicating the four-phase inode-verification protocol.
fn release_writer_lock_impl(
    path: &Path,
    expected_owner: &str,
) -> AppResult<crate::contexts::automation_runtime::WriterLockReleaseOutcome> {
    use crate::contexts::automation_runtime::WriterLockReleaseOutcome;
    use std::io::Read as _;

    // ── Phase 1: read-only ownership check ──────────────────────────
    // Open via fd so we can later compare inodes after the rename to
    // detect a concurrent file replacement.
    let mut file = match fs::File::open(path) {
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
    match fs::rename(path, &staging) {
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
                // fstat on the open fd failed. Attempt non-clobbering
                // restore via hard_link so we never overwrite a newly
                // acquired canonical lock.
                match fs::hard_link(&staging, path) {
                    Ok(()) => {
                        let _ = fs::remove_file(&staging);
                    }
                    Err(link_err) if link_err.kind() == std::io::ErrorKind::AlreadyExists => {
                        // Canonical lock was recreated — leave staged
                        // artifact durable and return error.
                        return Err(AppError::Io(std::io::Error::other(format!(
                            "writer lock release: verification failed ({e}) \
                                 and canonical lock was recreated; staged artifact preserved"
                        ))));
                    }
                    Err(link_err) => {
                        // Cannot restore — staged artifact remains
                        // durable for later recovery.
                        return Err(AppError::Io(std::io::Error::other(format!(
                            "writer lock release: verification failed ({e}) \
                                 and could not restore lock from staging: {link_err}"
                        ))));
                    }
                }
                return Err(e.into());
            }
        };
        let staged_meta = match fs::metadata(&staging) {
            Ok(m) => m,
            Err(e) => {
                // Cannot verify — attempt non-clobbering restore via
                // hard_link so we never overwrite a newly acquired lock.
                match fs::hard_link(&staging, path) {
                    Ok(()) => {
                        let _ = fs::remove_file(&staging);
                    }
                    Err(link_err) if link_err.kind() == std::io::ErrorKind::AlreadyExists => {
                        // Canonical lock was recreated — leave staged
                        // artifact durable and return error.
                        return Err(AppError::Io(std::io::Error::other(format!(
                            "writer lock release: staging verification failed ({e}) \
                                 and canonical lock was recreated; staged artifact preserved"
                        ))));
                    }
                    Err(link_err) => {
                        return Err(AppError::Io(std::io::Error::other(format!(
                            "writer lock release: staging verification failed ({e}) \
                                 and could not restore lock: {link_err}"
                        ))));
                    }
                }
                return Err(e.into());
            }
        };
        if fd_meta.ino() != staged_meta.ino() || fd_meta.dev() != staged_meta.dev() {
            // A different file was at the canonical path when we
            // renamed. Restore it via hard_link (which fails safely
            // if a new lock already exists at the canonical path).
            drop(file);
            match fs::hard_link(&staging, path) {
                Ok(()) => {
                    let _ = fs::remove_file(&staging);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Canonical path already has a new lock — the
                    // staging copy has been superseded; safe to
                    // delete it.
                    let _ = fs::remove_file(&staging);
                }
                Err(restore_err) => {
                    // Cannot restore to canonical path due to an
                    // unexpected I/O error. Leave the staging file
                    // intact for later recovery. Surface as an I/O
                    // error rather than masquerading as OwnerMismatch
                    // so callers know the lock is not at the canonical
                    // path and the staging artifact is still durable.
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "writer lock release: inode mismatch detected \
                             but could not restore replacement lock: {restore_err}"
                    ))));
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

// ── Data-dir daemon store (base_dir IS the daemon directory) ────────────────

/// Filesystem-backed daemon store where `base_dir` IS the daemon directory
/// directly (containing `tasks/`, `leases/`, `journal.ndjson`), unlike
/// `FsDaemonStore` which resolves through `.ralph-burning/daemon/`.
pub struct FsDataDirDaemonStore;

impl FsDataDirDaemonStore {
    fn tasks_dir(base_dir: &Path) -> PathBuf {
        base_dir.join(DAEMON_TASKS_DIR)
    }

    fn leases_dir(base_dir: &Path) -> PathBuf {
        base_dir.join(DAEMON_LEASES_DIR)
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
        base_dir.join(DAEMON_JOURNAL_FILE)
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

impl DaemonStorePort for FsDataDirDaemonStore {
    fn list_tasks(&self, base_dir: &Path) -> AppResult<Vec<DaemonTask>> {
        let dir = Self::tasks_dir(base_dir);
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut tasks = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let file = format!(
                "tasks/{}",
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
            format!("tasks/{task_id}.json"),
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
                file: format!("leases/{lease_id}.json"),
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
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let file = format!(
                "leases/{}",
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
            format!("leases/{lease_id}.json"),
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
                            file: "journal.ndjson".to_owned(),
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
        release_writer_lock_impl(
            &Self::writer_lock_path(base_dir, project_id),
            expected_owner,
        )
    }
}

// ── Repo registry filesystem store ──────────────────────────────────────────

/// Filesystem-backed implementation of `RepoRegistryPort`.
/// Persists repo registrations as JSON files under `<data-dir>/repos/<owner>/<repo>/registration.json`.
pub struct FsRepoRegistryStore;

impl FsRepoRegistryStore {
    fn registration_path(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        data_dir
            .join("repos")
            .join(owner)
            .join(repo)
            .join("registration.json")
    }
}

impl RepoRegistryPort for FsRepoRegistryStore {
    fn list_registrations(&self, data_dir: &Path) -> AppResult<Vec<RepoRegistration>> {
        let repos_dir = data_dir.join("repos");
        if !repos_dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut registrations = Vec::new();
        for owner_entry in fs::read_dir(&repos_dir)? {
            let owner_entry = owner_entry?;
            let owner_path = owner_entry.path();
            if !owner_path.is_dir() {
                continue;
            }
            for repo_entry in fs::read_dir(&owner_path)? {
                let repo_entry = repo_entry?;
                let repo_path = repo_entry.path();
                if !repo_path.is_dir() {
                    continue;
                }
                let reg_path = repo_path.join("registration.json");
                match fs::read_to_string(&reg_path) {
                    Ok(raw) => {
                        let reg: RepoRegistration =
                            serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                                file: reg_path.display().to_string(),
                                details: e.to_string(),
                            })?;
                        registrations.push(reg);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        // No registration.json in this repo directory — skip.
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
        registrations.sort_by(|a, b| a.repo_slug.cmp(&b.repo_slug));
        Ok(registrations)
    }

    fn read_registration(&self, data_dir: &Path, repo_slug: &str) -> AppResult<RepoRegistration> {
        let (owner, repo) = parse_repo_slug(repo_slug)?;
        let path = Self::registration_path(data_dir, owner, repo);
        let raw = fs::read_to_string(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                AppError::CorruptRecord {
                    file: path.display().to_string(),
                    details: "registration file not found".to_owned(),
                }
            } else {
                error.into()
            }
        })?;
        serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
            file: path.display().to_string(),
            details: e.to_string(),
        })
    }

    fn write_registration(
        &self,
        data_dir: &Path,
        registration: &RepoRegistration,
    ) -> AppResult<()> {
        let (owner, repo) = parse_repo_slug(&registration.repo_slug)?;
        let path = Self::registration_path(data_dir, owner, repo);
        let contents = serde_json::to_string_pretty(registration)?;
        FileSystem::write_atomic(&path, &contents)
    }
}

// ── Milestone filesystem layout ─────────────────────────────────────────────
//
// .ralph-burning/milestones/{id}/
//   ├─ milestone.toml        (immutable record)
//   ├─ status.json           (mutable snapshot)
//   ├─ journal.ndjson        (event log)
//   ├─ plan.json             (machine-readable plan)
//   ├─ plan.md               (human-readable plan)
//   └─ task-runs.ndjson      (bead → project lineage)

const MILESTONES_DIR: &str = "milestones";
const MILESTONE_CONFIG_FILE: &str = "milestone.toml";
const MILESTONE_STATUS_FILE: &str = "status.json";
const MILESTONE_JOURNAL_FILE: &str = "journal.ndjson";
const MILESTONE_PLAN_JSON_FILE: &str = "plan.json";
const MILESTONE_PLAN_MD_FILE: &str = "plan.md";
const MILESTONE_TASK_RUNS_FILE: &str = "task-runs.ndjson";
const MILESTONE_MUTATION_LOCK_FILE: &str = "mutation.lock";
const MILESTONE_LOCKS_DIR: &str = ".locks";

impl FileSystem {
    pub(crate) fn milestone_root(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(MILESTONES_DIR)
            .join(milestone_id.as_str())
    }

    pub(crate) fn milestone_lock_root(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(MILESTONES_DIR)
            .join(MILESTONE_LOCKS_DIR)
            .join(milestone_id.as_str())
    }
}

struct AdvisoryFileLock {
    _file: Flock<fs::File>,
}

impl AdvisoryFileLock {
    fn acquire(path: &Path) -> AppResult<Self> {
        let parent = path.parent().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path '{}' has no parent directory", path.display()),
            ))
        })?;
        fs::create_dir_all(parent)?;

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let lock = Flock::lock(file, FlockArg::LockExclusive)
            .map_err(|(_, errno)| errno_to_io_error(errno))?;
        Ok(Self { _file: lock })
    }
}

fn errno_to_io_error(errno: nix::errno::Errno) -> AppError {
    std::io::Error::from_raw_os_error(errno as i32).into()
}

pub struct FsMilestoneStore;

impl MilestoneStorePort for FsMilestoneStore {
    fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool> {
        let root = FileSystem::milestone_root(base_dir, milestone_id);
        if !root.is_dir() {
            return Ok(false);
        }
        if !root.join(MILESTONE_CONFIG_FILE).is_file() {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/milestone.toml", milestone_id),
                details: "milestone directory exists but milestone.toml is missing".to_owned(),
            });
        }
        Ok(true)
    }

    fn read_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneRecord> {
        let root = FileSystem::milestone_root(base_dir, milestone_id);
        let path = root.join(MILESTONE_CONFIG_FILE);
        let raw = fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::CorruptRecord {
                    file: format!("milestones/{}/milestone.toml", milestone_id),
                    details: "milestone.toml not found".to_owned(),
                }
            } else {
                e.into()
            }
        })?;
        toml::from_str(&raw).map_err(|e| AppError::CorruptRecord {
            file: format!("milestones/{}/milestone.toml", milestone_id),
            details: e.to_string(),
        })
    }

    fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
        let milestones_dir = FileSystem::workspace_root_path(base_dir).join(MILESTONES_DIR);
        if !milestones_dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for entry in fs::read_dir(&milestones_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with('.') {
                continue;
            }
            if let Ok(mid) = MilestoneId::new(name_str.as_ref()) {
                if entry.path().join(MILESTONE_CONFIG_FILE).is_file() {
                    ids.push(mid);
                }
            }
        }
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    fn create_milestone_atomic(
        &self,
        base_dir: &Path,
        record: &MilestoneRecord,
        snapshot: &MilestoneSnapshot,
        initial_journal_line: &str,
    ) -> AppResult<()> {
        let root = FileSystem::milestone_root(base_dir, &record.id);
        if root.exists() {
            return Err(AppError::DuplicateProject {
                project_id: record.id.to_string(),
            });
        }
        fs::create_dir_all(&root)?;

        let record_toml = toml::to_string_pretty(record)?;
        FileSystem::write_create_new(&root.join(MILESTONE_CONFIG_FILE), &record_toml)?;

        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        FileSystem::write_create_new(&root.join(MILESTONE_STATUS_FILE), &snapshot_json)?;

        FileSystem::write_create_new(&root.join(MILESTONE_JOURNAL_FILE), initial_journal_line)?;

        Ok(())
    }
}

pub struct FsMilestoneSnapshotStore;

impl FsMilestoneSnapshotStore {
    fn mutation_lock_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_lock_root(base_dir, milestone_id).join(MILESTONE_MUTATION_LOCK_FILE)
    }
}

impl MilestoneSnapshotPort for FsMilestoneSnapshotStore {
    fn read_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneSnapshot> {
        let path = FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATUS_FILE);
        let raw = fs::read_to_string(&path)?;
        serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
            file: format!("milestones/{}/status.json", milestone_id),
            details: e.to_string(),
        })
    }

    fn write_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        snapshot: &MilestoneSnapshot,
    ) -> AppResult<()> {
        let path = FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATUS_FILE);
        let json = serde_json::to_string_pretty(snapshot)?;
        FileSystem::write_atomic(&path, &json)
    }

    fn with_milestone_write_lock<T, F>(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        operation: F,
    ) -> AppResult<T>
    where
        F: FnOnce() -> AppResult<T>,
    {
        let _lock = AdvisoryFileLock::acquire(&Self::mutation_lock_path(base_dir, milestone_id))?;
        operation()
    }
}

pub struct FsMilestoneJournalStore;

impl StartJournalDetails {
    fn parse(details: &str) -> Option<Self> {
        serde_json::from_str(details).ok()
    }

    fn merge(existing: &Self, requested: &Self) -> Option<Self> {
        if existing.project_id != requested.project_id
            || FsTaskRunLineageStore::option_conflicts(
                existing.run_id.as_deref(),
                requested.run_id.as_deref(),
            )
            || FsTaskRunLineageStore::option_conflicts(
                existing.plan_hash.as_deref(),
                requested.plan_hash.as_deref(),
            )
        {
            return None;
        }

        // Explicit field-by-field merge: fill absent optional fields from
        // `requested`.  Each optional field must be listed here — adding a
        // new optional field to StartJournalDetails requires adding a merge
        // line below.  The round-trip test catches any omission.
        let mut merged = existing.clone();
        if merged.run_id.is_none() {
            merged.run_id = requested.run_id.clone();
        }
        if merged.plan_hash.is_none() {
            merged.plan_hash = requested.plan_hash.clone();
        }
        Some(merged)
    }

    fn render(&self) -> String {
        render_start_journal_details(
            &self.project_id,
            self.run_id.as_deref(),
            self.plan_hash.as_deref(),
        )
    }
}

impl CompletionJournalDetails {
    fn parse(details: &str) -> Option<Self> {
        serde_json::from_str(details).ok()
    }

    fn merge(existing: &Self, requested: &Self) -> Option<Self> {
        // Repair in place only when both journal rows refer to the same attempt.
        // Named runs use run_id; runless retries are disambiguated by started_at.
        if existing.project_id != requested.project_id
            || existing.started_at != requested.started_at
            || existing.outcome != requested.outcome
        {
            return None;
        }
        if FsTaskRunLineageStore::option_conflicts(
            existing.run_id.as_deref(),
            requested.run_id.as_deref(),
        ) || FsTaskRunLineageStore::option_conflicts(
            existing.plan_hash.as_deref(),
            requested.plan_hash.as_deref(),
        ) || FsTaskRunLineageStore::option_conflicts(
            existing.outcome_detail.as_deref(),
            requested.outcome_detail.as_deref(),
        ) {
            return None;
        }

        // Explicit field-by-field merge: fill absent optional fields from
        // `requested`.  Each optional field must be listed here — adding a
        // new optional field to CompletionJournalDetails requires adding a
        // merge line below.  The round-trip test catches any omission.
        let mut merged = existing.clone();
        if merged.run_id.is_none() {
            merged.run_id = requested.run_id.clone();
        }
        if merged.plan_hash.is_none() {
            merged.plan_hash = requested.plan_hash.clone();
        }
        if merged.outcome_detail.is_none() {
            merged.outcome_detail = requested.outcome_detail.clone();
        }
        Some(merged)
    }

    fn render(&self) -> String {
        render_completion_journal_details(
            &self.project_id,
            self.run_id.as_deref(),
            self.plan_hash.as_deref(),
            self.started_at,
            &self.outcome,
            self.outcome_detail.as_deref(),
        )
    }
}

impl FsMilestoneJournalStore {
    fn journal_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_JOURNAL_FILE)
    }

    fn journal_lock_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_lock_root(base_dir, milestone_id)
            .join(format!("{MILESTONE_JOURNAL_FILE}.lock"))
    }

    fn read_journal_from_path(
        path: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>> {
        let raw = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut events = Vec::new();
        for (i, line) in raw.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let event: MilestoneJournalEvent =
                serde_json::from_str(trimmed).map_err(|e| AppError::CorruptRecord {
                    file: format!("milestones/{}/journal.ndjson", milestone_id),
                    details: format!("line {}: {}", i + 1, e),
                })?;
            events.push(event);
        }
        Ok(events)
    }

    fn write_journal(path: &Path, events: &[MilestoneJournalEvent]) -> AppResult<()> {
        let mut content = String::new();
        for event in events {
            let line = serde_json::to_string(event)?;
            content.push_str(&line);
            content.push('\n');
        }
        FileSystem::write_atomic(path, &content)
    }

    fn is_completion_event(event: &MilestoneJournalEvent) -> bool {
        matches!(
            event.event_type,
            crate::contexts::milestone_record::model::MilestoneEventType::BeadCompleted
                | crate::contexts::milestone_record::model::MilestoneEventType::BeadFailed
                | crate::contexts::milestone_record::model::MilestoneEventType::BeadSkipped
        )
    }

    fn repairable_completion_event(
        existing: &MilestoneJournalEvent,
        requested: &MilestoneJournalEvent,
    ) -> Option<MilestoneJournalEvent> {
        if !Self::is_completion_event(existing)
            || !Self::is_completion_event(requested)
            || existing.timestamp != requested.timestamp
            || existing.event_type != requested.event_type
            || existing.bead_id != requested.bead_id
        {
            return None;
        }

        let existing_details_raw = existing.details.as_deref()?;
        let requested_details = CompletionJournalDetails::parse(requested.details.as_deref()?)?;

        let existing_details = CompletionJournalDetails::parse(existing_details_raw)?;
        let merged_details =
            CompletionJournalDetails::merge(&existing_details, &requested_details)?;

        let mut repaired = existing.clone();
        repaired.details = Some(merged_details.render());
        Some(repaired)
    }

    fn repairable_start_event(
        existing: &MilestoneJournalEvent,
        requested: &MilestoneJournalEvent,
    ) -> Option<MilestoneJournalEvent> {
        if existing.event_type != MilestoneEventType::BeadStarted
            || requested.event_type != MilestoneEventType::BeadStarted
            || existing.timestamp != requested.timestamp
            || existing.bead_id != requested.bead_id
        {
            return None;
        }

        let existing_details = StartJournalDetails::parse(existing.details.as_deref()?)?;
        let requested_details = StartJournalDetails::parse(requested.details.as_deref()?)?;
        let merged_details = StartJournalDetails::merge(&existing_details, &requested_details)?;

        let mut repaired = existing.clone();
        repaired.details = Some(merged_details.render());
        Some(repaired)
    }
}

impl MilestoneJournalPort for FsMilestoneJournalStore {
    fn read_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>> {
        Self::read_journal_from_path(&Self::journal_path(base_dir, milestone_id), milestone_id)
    }

    fn append_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        line: &str,
    ) -> AppResult<()> {
        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        FileSystem::append_line(&path, line)
    }

    fn append_event_if_missing(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool> {
        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        let mut journal = Self::read_journal_from_path(&path, milestone_id)?;
        if journal.iter().any(|existing| {
            existing.timestamp == event.timestamp
                && existing.event_type == event.event_type
                && existing.bead_id == event.bead_id
                && existing.details == event.details
        }) {
            return Ok(false);
        }

        if let Some((existing_index, repaired_event)) =
            journal.iter().enumerate().find_map(|(index, existing)| {
                Self::repairable_start_event(existing, event)
                    .or_else(|| Self::repairable_completion_event(existing, event))
                    .map(|repaired| (index, repaired))
            })
        {
            if journal[existing_index].details == repaired_event.details {
                return Ok(false);
            }

            journal[existing_index] = repaired_event;
            Self::write_journal(&path, &journal)?;
            return Ok(true);
        }

        let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
        FileSystem::append_line(&path, &line)?;
        Ok(true)
    }
}

pub struct FsTaskRunLineageStore;

impl FsTaskRunLineageStore {
    fn task_runs_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_TASK_RUNS_FILE)
    }

    fn task_runs_lock_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_lock_root(base_dir, milestone_id)
            .join(format!("{MILESTONE_TASK_RUNS_FILE}.lock"))
    }

    /// Read the snapshot's plan_hash for backfill-only auto-population.
    /// Returns `None` if no snapshot exists or no plan has been persisted.
    /// Logs a warning for unexpected errors (corrupt JSON, permission failures)
    /// so operators can detect issues that silently prevent auto-population.
    fn snapshot_plan_hash(base_dir: &Path, milestone_id: &MilestoneId) -> Option<String> {
        match FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id) {
            Ok(snapshot) => snapshot.plan_hash,
            Err(AppError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => {
                tracing::warn!(
                    milestone_id = %milestone_id,
                    error = %err,
                    "failed to read milestone snapshot for plan_hash auto-population; \
                     treating as no snapshot"
                );
                None
            }
        }
    }

    /// Backfill `plan_hash` on an existing entry from the current milestone
    /// snapshot, but only when it is provably safe — i.e., no plan evolution
    /// has occurred since the entry was created.
    ///
    /// Safety rules:
    /// - Entry must still have `plan_hash == None`.
    /// - If the entry has `snapshot_plan_hash_at_creation` (new entries), the
    ///   current snapshot must match it — otherwise the plan has evolved and
    ///   backfill is unsafe.
    /// - Legacy entries (where `snapshot_plan_hash_at_creation` is `None`)
    ///   cannot be verified, so backfill is skipped.
    ///
    /// Returns `true` if the entry was modified.
    fn safe_plan_hash_backfill(
        entry: &mut TaskRunEntry,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> bool {
        if entry.plan_hash.is_some() {
            return false;
        }
        // Terminal entries must never be auto-populated — only the caller's
        // explicit value (passed through backfill_terminal_entry) is used.
        if entry.outcome.is_terminal() {
            return false;
        }
        match &entry.snapshot_plan_hash_at_creation {
            Some(creation_hash) => {
                // Normal provenance path: verify the plan hasn't evolved.
                let current_hash = Self::snapshot_plan_hash(base_dir, milestone_id);
                if current_hash.as_deref() == Some(creation_hash.as_str()) {
                    entry.plan_hash = Some(creation_hash.clone());
                    true
                } else {
                    // Plan has evolved since entry creation → backfill unsafe.
                    false
                }
            }
            None => {
                // **Deliberate deviation from Issue 3 acceptance criteria.**
                //
                // Issue 3 requires "auto-populate plan_hash from snapshot
                // when the caller omits it."  We intentionally do NOT
                // backfill here because `snapshot_plan_hash_at_creation`
                // being `None` is ambiguous: it could mean (a) the entry
                // was created before any plan existed, or (b) a legacy
                // entry written by older code that didn't record the field.
                // We cannot distinguish the two, so backfilling would risk
                // silently relabeling a legacy entry with a newer plan
                // version.  Preferring correctness over completeness, we
                // skip rather than guess.
                false
            }
        }
    }

    fn read_task_runs_from_path(
        path: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<TaskRunEntry>> {
        let raw = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut entries = Vec::new();
        let expected_milestone_id = milestone_id.to_string();
        for (i, line) in raw.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let entry: TaskRunEntry =
                serde_json::from_str(trimmed).map_err(|e| AppError::CorruptRecord {
                    file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                    details: format!("line {}: {}", i + 1, e),
                })?;
            if entry.milestone_id != expected_milestone_id {
                return Err(AppError::CorruptRecord {
                    file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                    details: format!(
                        "line {}: task run entry milestone_id '{}' does not match milestone path '{}'",
                        i + 1,
                        entry.milestone_id,
                        milestone_id
                    ),
                });
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    fn write_task_runs(path: &Path, entries: &[TaskRunEntry]) -> AppResult<()> {
        let mut content = String::new();
        for entry in entries {
            let line = serde_json::to_string(entry)?;
            content.push_str(&line);
            content.push('\n');
        }
        FileSystem::write_atomic(path, &content)
    }

    fn is_superseded_legacy_start(entries: &[TaskRunEntry], index: usize) -> bool {
        let entry = &entries[index];
        !entry.outcome.is_terminal()
            && entries.iter().enumerate().any(|(other_index, other)| {
                other_index != index
                    && other.outcome.is_terminal()
                    && TaskRunEntry::same_attempt(entry, other)
            })
    }

    fn is_legacy_start_for(
        entries: &[TaskRunEntry],
        start_index: usize,
        terminal_index: usize,
    ) -> bool {
        start_index != terminal_index
            && !entries[start_index].outcome.is_terminal()
            && entries[terminal_index].outcome.is_terminal()
            && TaskRunEntry::same_attempt(&entries[start_index], &entries[terminal_index])
    }

    fn option_conflicts(existing: Option<&str>, requested: Option<&str>) -> bool {
        matches!((existing, requested), (Some(existing), Some(requested)) if existing != requested)
    }

    fn running_task_runs_for_bead<'a>(
        entries: &'a [TaskRunEntry],
        bead_id: &str,
    ) -> Vec<&'a TaskRunEntry> {
        entries
            .iter()
            .filter(|entry| entry.bead_id == bead_id && !entry.outcome.is_terminal())
            .collect()
    }

    fn plan_hash_conflict_details(
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        existing_plan_hash: &str,
        requested_plan_hash: &str,
    ) -> String {
        let run_suffix = run_id
            .map(|run_id| format!(" run={run_id}"))
            .unwrap_or_default();
        format!(
            "conflicting plan_hash for bead={bead_id} project={project_id}{run_suffix}: existing={existing_plan_hash} requested={requested_plan_hash}"
        )
    }

    fn backfill_terminal_entry(
        entry: &mut TaskRunEntry,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
        outcome_detail: Option<&str>,
        finished_at: DateTime<Utc>,
    ) -> bool {
        let mut changed = false;

        if let Some(run_id) = run_id {
            if entry.run_id.is_none() {
                entry.run_id = Some(run_id.to_owned());
                changed = true;
            }
        }
        if let Some(plan_hash) = plan_hash {
            if entry.plan_hash.is_none() {
                entry.plan_hash = Some(plan_hash.to_owned());
                changed = true;
            }
        }
        if let Some(outcome_detail) = outcome_detail {
            if entry.outcome_detail.is_none() {
                entry.outcome_detail = Some(outcome_detail.to_owned());
                changed = true;
            }
        }
        if entry.finished_at.is_none() {
            entry.finished_at = Some(finished_at);
            changed = true;
        }

        changed
    }

    fn backfill_running_entry(
        entry: &mut TaskRunEntry,
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
    ) -> AppResult<bool> {
        let mut changed = false;

        if let Some(run_id) = run_id {
            if entry.run_id.is_none() {
                entry.run_id = Some(run_id.to_owned());
                changed = true;
            }
        }
        if let Some(plan_hash) = plan_hash {
            match entry.plan_hash.as_deref() {
                Some(existing_plan_hash) if existing_plan_hash != plan_hash => {
                    return Err(AppError::RunStartFailed {
                        reason: Self::plan_hash_conflict_details(
                            bead_id,
                            project_id,
                            entry.run_id.as_deref().or(run_id),
                            existing_plan_hash,
                            plan_hash,
                        ),
                    });
                }
                None => {
                    entry.plan_hash = Some(plan_hash.to_owned());
                    changed = true;
                }
                _ => {}
            }
        }

        Ok(changed)
    }

    fn fail_superseded_running_attempt(
        entries: &mut [TaskRunEntry],
        prior_attempt: &TaskRunEntry,
        retry_started_at: DateTime<Utc>,
    ) -> bool {
        let detail = format!(
            "superseded by retry started at {}",
            retry_started_at.to_rfc3339()
        );
        let mut changed = false;

        for entry in entries.iter_mut().filter(|entry| {
            !entry.outcome.is_terminal() && TaskRunEntry::same_attempt(entry, prior_attempt)
        }) {
            entry.outcome = TaskRunOutcome::Failed;
            entry.outcome_detail = Some(detail.clone());
            entry.finished_at = Some(retry_started_at);
            changed = true;
        }

        changed
    }

    /// Find a unique terminal entry among `matching_indices` that can be
    /// treated as a replay match.
    ///
    /// # Contract
    ///
    /// When `requested_run_id` is `Some`, `requested_started_at` **must**
    /// also be `Some`.  The started_at guard prevents backfilling the wrong
    /// legacy terminal row when multiple runless entries exist.  Passing
    /// `(Some(run_id), None)` would bypass the guard entirely.
    fn unique_terminal_replay_match(
        entries: &[TaskRunEntry],
        matching_indices: &[usize],
        requested_run_id: Option<&str>,
        requested_started_at: Option<DateTime<Utc>>,
        milestone_id: &MilestoneId,
    ) -> AppResult<Option<usize>> {
        // Enforce the contract in all build modes: when the caller supplies a
        // run_id, started_at must also be present so the guard on runless
        // terminal candidates is never bypassed.
        if requested_run_id.is_some() && requested_started_at.is_none() {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                details: "contract violation: requested_run_id is Some but \
                          requested_started_at is None — this would bypass \
                          the started_at guard on runless terminal candidates"
                    .to_string(),
            });
        }
        let terminal_matches: Vec<usize> = matching_indices
            .iter()
            .copied()
            .filter(|candidate_index| {
                let candidate = &entries[*candidate_index];
                if !candidate.outcome.is_terminal() {
                    return false;
                }
                if let Some(run_id) = requested_run_id {
                    if candidate
                        .run_id
                        .as_deref()
                        .is_some_and(|candidate_run_id| candidate_run_id != run_id)
                    {
                        return false;
                    }
                    // When the caller supplies a run_id but the candidate is
                    // runless, require started_at to match — same guard as the
                    // open-match path — to avoid backfilling the wrong legacy
                    // terminal attempt after plan evolution.
                    if candidate.run_id.is_none() {
                        if let Some(started_at) = requested_started_at {
                            if candidate.started_at != started_at {
                                return false;
                            }
                        }
                    }
                }

                matching_indices.iter().copied().all(|other_index| {
                    other_index == *candidate_index
                        || Self::is_legacy_start_for(entries, other_index, *candidate_index)
                })
            })
            .collect();

        match terminal_matches.as_slice() {
            [index] => Ok(Some(*index)),
            _ => Ok(None),
        }
    }
}

impl TaskRunLineagePort for FsTaskRunLineageStore {
    fn read_task_runs(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<TaskRunEntry>> {
        Self::read_task_runs_from_path(&Self::task_runs_path(base_dir, milestone_id), milestone_id)
    }

    fn append_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        entry: &TaskRunEntry,
    ) -> AppResult<()> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        if entry.milestone_id != milestone_id.to_string() {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                details: format!(
                    "task run entry milestone_id '{}' does not match milestone path '{}'",
                    entry.milestone_id, milestone_id
                ),
            });
        }
        let line = serde_json::to_string(entry)?;
        FileSystem::append_line(&path, &line)
    }

    fn record_task_run_start(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        let mut entries = Self::read_task_runs_from_path(&path, milestone_id)?;
        let canonical_task_runs = collapse_task_run_attempts(entries.clone());
        let running_attempts_for_bead =
            Self::running_task_runs_for_bead(&canonical_task_runs, bead_id);

        if let Some(other_active_bead) = canonical_task_runs
            .iter()
            .filter(|entry| !entry.outcome.is_terminal())
            .map(|entry| entry.bead_id.as_str())
            .find(|active_bead_id| *active_bead_id != bead_id)
        {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': bead '{other_active_bead}' is already active"
                ),
            });
        }

        let finalized_attempts = matching_finalized_task_runs(
            &canonical_task_runs,
            bead_id,
            project_id,
            run_id,
            started_at,
        );
        match finalized_attempts.as_slice() {
            [] => {}
            [entry] => {
                let finalized_attempt = entry
                    .run_id
                    .as_deref()
                    .map(|run_id| format!("run '{run_id}'"))
                    .unwrap_or_else(|| {
                        format!("attempt started at {}", entry.started_at.to_rfc3339())
                    });
                return Err(AppError::RunStartFailed {
                    reason: format!(
                        "cannot start bead '{bead_id}': {finalized_attempt} for project '{project_id}' is already finalized"
                    ),
                });
            }
            _ => {
                return Err(AppError::RunStartFailed {
                    reason: format!(
                        "cannot start bead '{bead_id}': ambiguous finalized attempts already exist for project '{project_id}' started_at={}",
                        started_at.to_rfc3339()
                    ),
                });
            }
        }

        if let Some(existing_entry) = find_matching_running_task_run(
            &canonical_task_runs,
            bead_id,
            project_id,
            run_id,
            started_at,
        ) {
            if let Some(existing_index) = entries.iter().position(|entry| {
                !entry.outcome.is_terminal() && TaskRunEntry::same_attempt(entry, &existing_entry)
            }) {
                // Pre-merge metadata from the canonical (collapsed) entry
                // into the raw row.  In duplicate-row states the first raw
                // row may be the stale copy while the canonical entry
                // already holds the most complete metadata from all
                // duplicates.
                let pre_merge = entries[existing_index].clone();
                entries[existing_index] =
                    TaskRunEntry::merge_attempt_entries(&entries[existing_index], &existing_entry);
                let mut changed = entries[existing_index] != pre_merge;
                changed |= Self::backfill_running_entry(
                    &mut entries[existing_index],
                    bead_id,
                    project_id,
                    run_id,
                    plan_hash,
                )?;
                // Safe plan_hash backfill for existing rows: only when the
                // entry's plan_hash is still None AND we can verify no plan
                // evolution has occurred since the entry was created.
                changed |= Self::safe_plan_hash_backfill(
                    &mut entries[existing_index],
                    base_dir,
                    milestone_id,
                );
                if changed {
                    Self::write_task_runs(&path, &entries)?;
                }
                return Ok(entries[existing_index].clone());
            }

            return Ok(existing_entry);
        }

        if running_attempts_for_bead.len() > 1 {
            let run_suffix = run_id
                .map(|run_id| format!(" run '{run_id}'"))
                .unwrap_or_default();
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': ambiguous existing running attempts{run_suffix}"
                ),
            });
        }

        if let [prior_running_attempt] = running_attempts_for_bead.as_slice() {
            Self::fail_superseded_running_attempt(&mut entries, prior_running_attempt, started_at);
        }

        // Record the current snapshot hash for provenance tracking, but do
        // NOT auto-populate plan_hash from it.  The dispatch-v1/snapshot-v2
        // race makes auto-population unsafe: if persist_plan() advances the
        // snapshot between bead dispatch and record_task_run_start, the entry
        // would be stamped with the *newer* snapshot hash even though the bead
        // was dispatched under the older plan.  This causes false "conflicting
        // plan_hash" errors when the caller later supplies the correct (older)
        // hash at completion time.  Callers that know which plan version
        // dispatched the bead should supply plan_hash explicitly.
        let current_snapshot_hash = Self::snapshot_plan_hash(base_dir, milestone_id);
        let effective_plan_hash = plan_hash.map(str::to_owned);

        let entry = TaskRunEntry {
            milestone_id: milestone_id.to_string(),
            bead_id: bead_id.to_owned(),
            project_id: project_id.to_owned(),
            run_id: run_id.map(str::to_owned),
            plan_hash: effective_plan_hash,
            snapshot_plan_hash_at_creation: current_snapshot_hash,
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
        };
        entries.push(entry.clone());
        Self::write_task_runs(&path, &entries)?;
        Ok(entry)
    }

    fn update_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        let mut entries = Self::read_task_runs_from_path(&path, milestone_id)?;
        if entries.is_empty() {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                details: format!(
                    "no task runs file found when updating bead={bead_id} project={project_id}"
                ),
            });
        }

        let matching_indices: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                (entry.bead_id == bead_id && entry.project_id == project_id).then_some(index)
            })
            .collect();
        if matching_indices.is_empty() {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                details: format!("no matching task run for bead={bead_id} project={project_id}"),
            });
        }

        let mut duplicate_indices_to_remove = Vec::new();
        let target_index = if let Some(run_id) = run_id {
            let exact_matches: Vec<usize> = matching_indices
                .iter()
                .copied()
                .filter(|index| entries[*index].run_id.as_deref() == Some(run_id))
                .collect();
            match exact_matches.as_slice() {
                [index] => *index,
                [] => {
                    let open_matches: Vec<usize> = matching_indices
                        .iter()
                        .copied()
                        .filter(|index| {
                            !entries[*index].outcome.is_terminal()
                                && !Self::is_superseded_legacy_start(&entries, *index)
                        })
                        .collect();
                    match open_matches.as_slice() {
                        [] => {
                            // Pre-filter to rows matching started_at so that
                            // unrelated completed attempts for the same
                            // bead/project are excluded from the uniqueness
                            // check (they would otherwise cause
                            // is_legacy_start_for to reject valid candidates).
                            let attempt_indices: Vec<usize> = matching_indices
                                .iter()
                                .copied()
                                .filter(|index| entries[*index].started_at == started_at)
                                .collect();
                            match Self::unique_terminal_replay_match(
                                &entries,
                                &attempt_indices,
                                Some(run_id),
                                Some(started_at),
                                milestone_id,
                            )? {
                                Some(index) => index,
                                None => {
                                    return Err(AppError::CorruptRecord {
                                        file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                        details: format!(
                                            "no matching task run for bead={bead_id} project={project_id} run={run_id}"
                                        ),
                                    });
                                }
                            }
                        }
                        [index]
                            if entries[*index].run_id.is_none()
                                && entries[*index].started_at == started_at =>
                        {
                            // Accept runless match: backfill run_id (and plan_hash)
                            // so a start recorded before the controller knew the
                            // run ID can be finalized with the now-known identity.
                            // Require started_at to match to avoid backfilling the
                            // wrong legacy attempt.
                            *index
                        }
                        [open_index] if entries[*open_index].run_id.is_none() => {
                            // Open runless row exists but started_at doesn't
                            // match.  Fall through to the terminal replay
                            // matcher — a completed row with the matching
                            // started_at may exist (partial-write repair path
                            // where the lineage row was written terminal but
                            // the snapshot/journal update failed).
                            let attempt_indices: Vec<usize> = matching_indices
                                .iter()
                                .copied()
                                .filter(|index| entries[*index].started_at == started_at)
                                .collect();
                            match Self::unique_terminal_replay_match(
                                &entries,
                                &attempt_indices,
                                Some(run_id),
                                Some(started_at),
                                milestone_id,
                            )? {
                                Some(index) => index,
                                None => {
                                    return Err(AppError::CorruptRecord {
                                        file: format!(
                                            "milestones/{}/task-runs.ndjson",
                                            milestone_id
                                        ),
                                        details: format!(
                                            "no matching task run for bead={bead_id} project={project_id} run={run_id}; \
                                             open runless attempt started_at={} does not match requested started_at={started_at} \
                                             and no terminal row found for the requested started_at",
                                            entries[*open_index].started_at,
                                        ),
                                    });
                                }
                            }
                        }
                        [index] => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: format!(
                                    "no matching task run for bead={bead_id} project={project_id} run={run_id}; only run={} is still open",
                                    entries[*index].run_id.as_deref().unwrap_or("<missing>")
                                ),
                            });
                        }
                        _ => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: format!(
                                    "ambiguous task run update for bead={bead_id} project={project_id} run={run_id}"
                                ),
                            });
                        }
                    }
                }
                _ => {
                    let terminal_exact_matches: Vec<usize> = exact_matches
                        .iter()
                        .copied()
                        .filter(|index| entries[*index].outcome.is_terminal())
                        .collect();
                    match terminal_exact_matches.as_slice() {
                        [index] => {
                            duplicate_indices_to_remove.extend(
                                exact_matches
                                    .iter()
                                    .copied()
                                    .filter(|other_index| other_index != index),
                            );
                            *index
                        }
                        [] => {
                            let index = exact_matches[0];
                            duplicate_indices_to_remove
                                .extend(exact_matches.iter().copied().skip(1));
                            index
                        }
                        _ => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: format!(
                                    "ambiguous task run update for bead={bead_id} project={project_id} run={run_id}"
                                ),
                            });
                        }
                    }
                }
            }
        } else {
            let matching_attempt_indices: Vec<usize> = matching_indices
                .iter()
                .copied()
                .filter(|index| entries[*index].started_at == started_at)
                .collect();

            if matching_attempt_indices.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                    details: format!(
                        "no matching task run for bead={bead_id} project={project_id} started_at={started_at}"
                    ),
                });
            }

            let open_matches: Vec<usize> = matching_attempt_indices
                .iter()
                .copied()
                .filter(|index| {
                    !entries[*index].outcome.is_terminal()
                        && !Self::is_superseded_legacy_start(&entries, *index)
                })
                .collect();

            match open_matches.as_slice() {
                [index] => *index,
                [] => {
                    let terminal_matches: Vec<usize> = matching_attempt_indices
                        .iter()
                        .copied()
                        .filter(|index| entries[*index].outcome.is_terminal())
                        .collect();
                    match terminal_matches.as_slice() {
                        [index] => *index,
                        _ => {
                            match Self::unique_terminal_replay_match(
                                &entries,
                                &matching_attempt_indices,
                                None,
                                None,
                                milestone_id,
                            )? {
                                Some(index) => index,
                                None => {
                                    return Err(AppError::CorruptRecord {
                                        file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                        details: format!(
                                            "ambiguous task run update for bead={bead_id} project={project_id} started_at={started_at}; provide run_id to disambiguate"
                                        ),
                                    });
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(AppError::CorruptRecord {
                        file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                        details: format!(
                            "ambiguous task run update for bead={bead_id} project={project_id} started_at={started_at}; provide run_id to disambiguate"
                        ),
                    });
                }
            }
        };

        let mut should_write = !duplicate_indices_to_remove.is_empty();
        if !duplicate_indices_to_remove.is_empty() {
            let mut merged_target = entries[target_index].clone();
            for duplicate_index in duplicate_indices_to_remove.iter().copied() {
                merged_target =
                    TaskRunEntry::merge_attempt_entries(&merged_target, &entries[duplicate_index]);
            }
            entries[target_index] = merged_target;
        }

        let finalized_entry = {
            let entry = &mut entries[target_index];
            if entry.outcome.is_terminal() {
                let run_suffix = entry
                    .run_id
                    .as_deref()
                    .or(run_id)
                    .map(|run_id| format!(" run={run_id}"))
                    .unwrap_or_default();
                let incompatible_terminal_update = entry.outcome != outcome
                    || Self::option_conflicts(entry.run_id.as_deref(), run_id)
                    || Self::option_conflicts(entry.plan_hash.as_deref(), plan_hash)
                    || Self::option_conflicts(
                        entry.outcome_detail.as_deref(),
                        outcome_detail.as_deref(),
                    );
                if incompatible_terminal_update {
                    return Err(AppError::CorruptRecord {
                        file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                        details: format!(
                            "task run for bead={bead_id} project={project_id}{run_suffix} is already finalized with outcome={}",
                            entry.outcome,
                        ),
                    });
                }

                should_write |= Self::backfill_terminal_entry(
                    entry,
                    run_id,
                    plan_hash,
                    outcome_detail.as_deref(),
                    finished_at,
                );
                // Do NOT call safe_plan_hash_backfill here — terminal
                // replays must preserve the caller-supplied plan_hash
                // (including None) and never auto-populate from the
                // snapshot.
            } else {
                if let Some(run_id) = run_id {
                    entry.run_id.get_or_insert_with(|| run_id.to_owned());
                }
                if let Some(plan_hash) = plan_hash {
                    match entry.plan_hash.as_deref() {
                        Some(existing_plan_hash) if existing_plan_hash != plan_hash => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: Self::plan_hash_conflict_details(
                                    bead_id,
                                    project_id,
                                    entry.run_id.as_deref().or(run_id),
                                    existing_plan_hash,
                                    plan_hash,
                                ),
                            });
                        }
                        None => entry.plan_hash = Some(plan_hash.to_owned()),
                        _ => {}
                    }
                }
                // Safe plan_hash backfill: only when provenance confirms no
                // plan evolution since the entry was created.
                Self::safe_plan_hash_backfill(entry, base_dir, milestone_id);
                entry.outcome = outcome;
                entry.outcome_detail = outcome_detail;
                entry.finished_at = Some(finished_at);
                should_write = true;
            }

            entry.clone()
        };

        if should_write {
            let filtered_entries: Vec<TaskRunEntry> = entries
                .iter()
                .enumerate()
                .filter(|(index, _)| !duplicate_indices_to_remove.contains(index))
                .map(|(_, entry)| entry.clone())
                .collect();
            Self::write_task_runs(&path, &filtered_entries)?;
        }

        Ok(finalized_entry)
    }

    fn find_runs_for_bead(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
    ) -> AppResult<Vec<TaskRunEntry>> {
        let mut runs: Vec<TaskRunEntry> =
            collapse_task_run_attempts(self.read_task_runs(base_dir, milestone_id)?)
                .into_iter()
                .filter(|entry| entry.bead_id == bead_id)
                .collect();
        runs.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.finished_at.cmp(&right.finished_at))
        });
        Ok(runs)
    }
}

pub struct FsMilestonePlanStore;

impl MilestonePlanPort for FsMilestonePlanStore {
    fn read_plan_json(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String> {
        let path =
            FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_JSON_FILE);
        Ok(fs::read_to_string(&path)?)
    }

    fn write_plan_json(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()> {
        let path =
            FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_JSON_FILE);
        FileSystem::write_atomic(&path, content)
    }

    fn read_plan_md(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String> {
        let path = FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_MD_FILE);
        Ok(fs::read_to_string(&path)?)
    }

    fn write_plan_md(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()> {
        let path = FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_MD_FILE);
        FileSystem::write_atomic(&path, content)
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

    fn list_requirements_run_ids(&self, base_dir: &Path) -> AppResult<Vec<String>> {
        let requirements_dir = requirements_root(base_dir);
        if !requirements_dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut run_ids = Vec::new();
        for entry in fs::read_dir(&requirements_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let name = entry.file_name();
            let run_id = name.to_string_lossy();
            if run_id.starts_with('.') || !entry.path().join("run.json").is_file() {
                continue;
            }
            run_ids.push(run_id.to_string());
        }

        run_ids.sort();
        Ok(run_ids)
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
    requirements_root(base_dir).join(run_id)
}

fn requirements_root(base_dir: &Path) -> PathBuf {
    FileSystem::workspace_root_path(base_dir).join(REQUIREMENTS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::automation_runtime::{DaemonStorePort, WriterLockReleaseOutcome};
    use crate::shared::domain::ProjectId;
    use tempfile::tempdir;

    /// Verify that `render_start_journal_details` output round-trips through
    /// `StartJournalDetails` parse→render. Both paths use the same canonical
    /// struct from model.rs, so drift is structurally impossible.
    #[test]
    fn start_journal_details_round_trips_with_model_render() {
        use crate::contexts::milestone_record::model::render_start_journal_details;

        // Render via the model's canonical function
        let rendered = render_start_journal_details("proj-1", Some("run-42"), Some("hash-abc"));
        // Parse through the fs adapter's struct
        let parsed = StartJournalDetails::parse(&rendered)
            .expect("StartJournalDetails must parse model-rendered JSON");
        assert_eq!(parsed.project_id, "proj-1");
        assert_eq!(parsed.run_id.as_deref(), Some("run-42"));
        assert_eq!(parsed.plan_hash.as_deref(), Some("hash-abc"));
        // Re-render and confirm stability
        assert_eq!(parsed.render(), rendered);
    }

    /// Verify that `render_completion_journal_details` output round-trips
    /// through `CompletionJournalDetails` parse→render. Both paths use the
    /// same canonical struct from model.rs, so drift is structurally impossible.
    #[test]
    fn completion_journal_details_round_trips_with_model_render() {
        use crate::contexts::milestone_record::model::render_completion_journal_details;
        use chrono::{TimeZone, Utc};

        let ts = Utc.with_ymd_and_hms(2026, 3, 30, 12, 0, 0).unwrap();
        let rendered = render_completion_journal_details(
            "proj-2",
            Some("run-99"),
            Some("hash-xyz"),
            ts,
            "succeeded",
            Some("All checks passed"),
        );
        let parsed = CompletionJournalDetails::parse(&rendered)
            .expect("CompletionJournalDetails must parse model-rendered JSON");
        assert_eq!(parsed.project_id, "proj-2");
        assert_eq!(parsed.run_id.as_deref(), Some("run-99"));
        assert_eq!(parsed.plan_hash.as_deref(), Some("hash-xyz"));
        assert_eq!(parsed.started_at, ts);
        assert_eq!(parsed.outcome, "succeeded");
        assert_eq!(parsed.outcome_detail.as_deref(), Some("All checks passed"));
        // Re-render and confirm stability
        assert_eq!(parsed.render(), rendered);
    }

    /// Regression: merge() must fill ALL optional fields from the requested
    /// struct during journal repair. If a future field is added to the shared
    /// structs but merge doesn't copy it, the merged render will differ from
    /// the fully-populated requested render, failing this test.
    #[test]
    fn journal_merge_fills_all_optional_fields() {
        use crate::contexts::milestone_record::model::{
            render_completion_journal_details, render_start_journal_details,
        };
        use chrono::{TimeZone, Utc};

        // --- StartJournalDetails ---
        let existing_json = render_start_journal_details("proj-1", None, None);
        let requested_json =
            render_start_journal_details("proj-1", Some("run-42"), Some("hash-abc"));

        let existing =
            StartJournalDetails::parse(&existing_json).expect("existing start must parse");
        let requested =
            StartJournalDetails::parse(&requested_json).expect("requested start must parse");
        let merged = StartJournalDetails::merge(&existing, &requested)
            .expect("merge must succeed when no conflicts");

        assert_eq!(
            merged.render(),
            requested_json,
            "merged start details must match fully-populated requested"
        );

        // --- CompletionJournalDetails ---
        let ts = Utc.with_ymd_and_hms(2026, 3, 30, 12, 0, 0).unwrap();
        let existing_json =
            render_completion_journal_details("proj-2", None, None, ts, "succeeded", None);
        let requested_json = render_completion_journal_details(
            "proj-2",
            Some("run-99"),
            Some("hash-xyz"),
            ts,
            "succeeded",
            Some("All checks passed"),
        );

        let existing = CompletionJournalDetails::parse(&existing_json)
            .expect("existing completion must parse");
        let requested = CompletionJournalDetails::parse(&requested_json)
            .expect("requested completion must parse");
        let merged = CompletionJournalDetails::merge(&existing, &requested)
            .expect("merge must succeed when no conflicts");

        assert_eq!(
            merged.render(),
            requested_json,
            "merged completion details must match fully-populated requested"
        );
    }

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
        let project_id = ProjectId::new("toctou-test".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);
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
        let project_id = ProjectId::new("post-check".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let lock_path_clone = lock_path.clone();

        // Install hook: after inode verification passes, create a new
        // lock at the canonical path to simulate a new writer acquiring
        // between our final check and the staging delete.
        release_lock_testing::set_post_verify_hook(move || {
            std::fs::write(&lock_path_clone, "owner-B").expect("write new lock");
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
        let project_id = ProjectId::new("post-rename-fail".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let staging_path = lock_path.with_extension("lock.releasing.owner-A");
        let lock_path_clone = lock_path.clone();
        let staging_clone = staging_path.clone();

        // After rename-to-staging: delete the staging file and place a
        // replacement lock at the canonical path. This simulates the
        // staging file becoming unavailable while a new writer acquires.
        release_lock_testing::set_post_rename_hook(move || {
            std::fs::remove_file(&staging_clone).expect("remove staging");
            std::fs::write(&lock_path_clone, "owner-B").expect("write replacement lock");
        });

        let result = store.release_writer_lock(temp.path(), &project_id, "owner-A");

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
        let project_id = ProjectId::new("staging-race".to_owned()).expect("valid id");

        // Acquire lock as owner-A.
        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire A");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);

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
            std::fs::write(&staging_b_clone, "owner-B").expect("write B staging");
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

    #[test]
    fn release_writer_lock_rename_back_failure_surfaces_io_error() {
        // Regression: when staging verification fails AND the rename-back to
        // canonical also fails, the release must surface an explicit I/O
        // error (not silently drop the restore failure). This exercises the
        // recovery path added to satisfy the spec's failure invariants.
        //
        // Strategy: use post_rename_hook to delete the staging file AND
        // place a directory at the staging path. When staged_meta fails
        // (NotFound — the original staging is gone), the recovery
        // rename(staging_dir, canonical) fails because rename() of a
        // directory onto a non-directory path is an error (or ENOENT if
        // staging_dir was already cleaned up). Either way, the restore
        // fails and the code must surface the compound error.
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("rename-back-fail".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let lock_path_clone = lock_path.clone();

        // After rename-to-staging: delete the staging file so
        // fs::metadata(&staging) fails, then create a new file at
        // canonical. When the code tries rename(&staging, &path) to
        // restore, staging doesn't exist → rename returns ENOENT →
        // restore failure is surfaced.
        release_lock_testing::set_post_rename_hook(move || {
            let staging = lock_path_clone.with_extension("lock.releasing.owner-A");
            // Remove staging so fs::metadata fails.
            std::fs::remove_file(&staging).expect("remove staging");
            // Put a replacement at canonical so we can verify it survives.
            std::fs::write(&lock_path_clone, "owner-B").expect("write replacement");
        });

        let result = store.release_writer_lock(temp.path(), &project_id, "owner-A");

        // Must surface an error (not silently return the original error
        // while ignoring the failed restore).
        assert!(
            result.is_err(),
            "expected compound I/O error, got: {result:?}"
        );

        // The error message must mention the restore failure.
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("could not restore lock"),
            "error should mention restore failure, got: {err_msg}"
        );

        // The replacement lock at canonical must survive untouched.
        assert_eq!(
            std::fs::read_to_string(&lock_path).expect("read canonical"),
            "owner-B",
            "replacement lock at canonical path must survive"
        );

        // Cleanup
        std::fs::remove_file(&lock_path).expect("cleanup");
    }

    #[test]
    fn verification_error_recovery_does_not_clobber_reacquired_canonical() {
        // Regression for fail-closed recovery: after rename-to-staging,
        // a new writer acquires the canonical lock. If verification then
        // detects an inode mismatch, the recovery must fail with
        // AlreadyExists (not clobber the new lock via rename).
        //
        // Scenario:
        //   1. owner-A acquires lock (opens fd, reads content)
        //   2. pre-rename hook: replace canonical with owner-B (new inode)
        //   3. rename moves owner-B file to staging
        //   4. post-rename hook: owner-C creates a new canonical lock
        //   5. inode check: fd (owner-A) ≠ staging (owner-B) → mismatch
        //   6. hard_link(staging, canonical) → AlreadyExists (owner-C)
        //   7. staging deleted, canonical owner-C preserved
        let store = FsDaemonStore;
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("no-clobber".to_owned()).expect("valid id");

        store
            .acquire_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("acquire");

        let lock_path = FsDaemonStore::writer_lock_path(temp.path(), &project_id);
        let lock_path_for_pre = lock_path.clone();
        let lock_path_for_post = lock_path.clone();

        // Pre-rename hook: replace canonical with a different inode.
        release_lock_testing::set_pre_rename_hook(move || {
            std::fs::remove_file(&lock_path_for_pre).expect("remove original lock");
            std::fs::write(&lock_path_for_pre, "owner-B").expect("write replacement");
        });

        // Post-rename hook: simulate owner-C acquiring the canonical lock
        // after staging moved owner-B out of the way.
        release_lock_testing::set_post_rename_hook(move || {
            std::fs::write(&lock_path_for_post, "owner-C").expect("write new canonical lock");
        });

        let outcome = store
            .release_writer_lock(temp.path(), &project_id, "owner-A")
            .expect("release should not error");

        // Must detect inode mismatch and return OwnerMismatch.
        assert!(
            matches!(outcome, WriterLockReleaseOutcome::OwnerMismatch { .. }),
            "expected OwnerMismatch, got: {outcome:?}"
        );

        // The canonical lock must be owner-C's — never clobbered.
        assert_eq!(
            std::fs::read_to_string(&lock_path).expect("read canonical"),
            "owner-C",
            "newly acquired canonical lock must NOT be clobbered by recovery"
        );

        // Staging file must be cleaned up.
        let staging = lock_path.with_extension("lock.releasing.owner-A");
        assert!(
            !staging.exists(),
            "staging file should be removed after recovery"
        );

        // Cleanup
        std::fs::remove_file(&lock_path).expect("cleanup");
    }
}
