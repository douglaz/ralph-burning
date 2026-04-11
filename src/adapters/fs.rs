use std::cell::RefCell;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(all(unix, not(target_os = "linux")))]
use chrono::NaiveDateTime;
use chrono::{DateTime, TimeZone, Utc};
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
use crate::contexts::milestone_record::controller::{
    MilestoneControllerPort, MilestoneControllerRecord, MilestoneControllerTransitionEvent,
};
use crate::contexts::milestone_record::model::{
    collapse_task_run_attempts, find_matching_running_task_run, matching_finalized_task_runs,
    render_completion_journal_details, render_start_journal_details, CompletionJournalDetails,
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneRecord, MilestoneSnapshot,
    StartJournalDetails, TaskRunEntry, TaskRunOutcome,
};
use crate::contexts::milestone_record::service::{
    JournalWriteOp, JournalWriteOpKind, MilestoneJournalPort, MilestonePlanPort,
    MilestoneSnapshotPort, MilestoneStorePort, TaskRunLineagePort,
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
const AUDIT_WORKSPACE_DIR: &str = ".ralph-burning";
const LIVE_WORKSPACE_DIR: &str = "ralph-burning-live";
const PROJECTS_DIR: &str = "projects";
const PROJECT_CONFIG_FILE: &str = "project.toml";
const PROJECT_POLICY_CONFIG_FILE: &str = "config.toml";
const RUN_FILE: &str = "run.json";
const PID_FILE: &str = "run.pid";
const ACTIVE_BACKEND_PROCESSES_FILE: &str = "active-processes.json";
const ACTIVE_BACKEND_PROCESSES_LOCK_FILE: &str = "active-processes.lock";
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

thread_local! {
    static HELD_MILESTONE_MUTATION_LOCKS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RunPidOwner {
    #[default]
    Cli,
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPidRecord {
    pub pid: u32,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub owner: RunPidOwner,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_start_ticks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_start_marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunBackendProcessRecord {
    pub pid: u32,
    pub recorded_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_start_ticks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_start_marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct RunBackendProcessSet {
    #[serde(default)]
    processes: Vec<RunBackendProcessRecord>,
}

pub struct FileSystem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStartPrecision {
    Precise,
    #[cfg(any(test, all(unix, not(target_os = "linux"))))]
    WholeSeconds,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessStartTime {
    started_at: DateTime<Utc>,
    precision: ProcessStartPrecision,
}

impl ProcessStartTime {
    fn precise(started_at: DateTime<Utc>) -> Self {
        Self {
            started_at,
            precision: ProcessStartPrecision::Precise,
        }
    }

    #[cfg(any(test, all(unix, not(target_os = "linux"))))]
    #[allow(dead_code)]
    fn whole_seconds(started_at: DateTime<Utc>) -> Self {
        Self {
            started_at,
            precision: ProcessStartPrecision::WholeSeconds,
        }
    }

    fn matches_legacy_pid_record(self, record_started_at: DateTime<Utc>) -> bool {
        let _ = self.precision;
        self.started_at <= record_started_at
    }
}

impl FileSystem {
    fn identity_is_authoritative(
        proc_start_ticks: Option<u64>,
        proc_start_marker: Option<&str>,
    ) -> bool {
        if proc_start_ticks.is_some() {
            return true;
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            return proc_start_marker.is_some();
        }

        #[cfg(not(all(unix, not(target_os = "linux"))))]
        {
            let _ = proc_start_marker;
            false
        }
    }

    fn process_identity_is_alive(
        pid: u32,
        proc_start_ticks: Option<u64>,
        proc_start_marker: Option<&str>,
    ) -> bool {
        if !Self::is_pid_running_unchecked(pid) {
            return false;
        }

        if let Some(expected_ticks) = proc_start_ticks {
            return matches!(Self::proc_start_ticks(pid), Some(actual_ticks) if actual_ticks == expected_ticks);
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            if let (Some(expected), Some(actual)) = (proc_start_marker, Self::proc_start_marker(pid))
            {
                if actual == expected {
                    return true;
                }
                // Backward compat: old records may have been written under a
                // different locale/TZ.  Fall back to a raw `ps` call without
                // locale normalization and accept either format.
                if let Some(raw_actual) = Self::proc_start_marker_raw(pid) {
                    return raw_actual == expected;
                }
            }
            return false;
        }

        #[cfg(not(all(unix, not(target_os = "linux"))))]
        {
            let _ = proc_start_marker;
            false
        }
    }

    #[cfg(target_os = "linux")]
    fn process_started_at(pid: u32) -> Option<ProcessStartTime> {
        let start_ticks = Self::proc_start_ticks(pid)?;
        // CLK_TCK is 100 on virtually all Linux kernels (set at compile
        // time, almost never changed).  Avoid shelling out to `getconf`
        // which may not exist in slim containers.
        let ticks_per_second: u64 = 100;
        let boot_time = fs::read_to_string("/proc/stat")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("btime "))?
            .trim()
            .parse::<i64>()
            .ok()?;
        let started_at_seconds = boot_time.checked_add((start_ticks / ticks_per_second) as i64)?;
        let started_at_nanos =
            ((start_ticks % ticks_per_second) * 1_000_000_000 / ticks_per_second) as u32;
        Utc.timestamp_opt(started_at_seconds, started_at_nanos)
            .single()
            .map(ProcessStartTime::precise)
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn process_started_at(pid: u32) -> Option<ProcessStartTime> {
        let marker = Self::proc_start_marker(pid)?;
        let naive = NaiveDateTime::parse_from_str(&marker, "%a %b %e %H:%M:%S %Y").ok()?;
        Some(ProcessStartTime::whole_seconds(
            Utc.from_utc_datetime(&naive),
        ))
    }

    #[cfg(not(unix))]
    fn process_started_at(_pid: u32) -> Option<ProcessStartTime> {
        None
    }

    fn live_project_backend_processes_path(project_root: &Path) -> PathBuf {
        project_root
            .join("runtime")
            .join("backend")
            .join(ACTIVE_BACKEND_PROCESSES_FILE)
    }

    fn live_project_backend_processes_lock_path(project_root: &Path) -> PathBuf {
        project_root
            .join("runtime")
            .join("backend")
            .join(ACTIVE_BACKEND_PROCESSES_LOCK_FILE)
    }

    fn read_pid_file_from_project_root(project_root: &Path) -> AppResult<Option<RunPidRecord>> {
        let path = project_root.join(PID_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => Ok(Some(serde_json::from_str(&raw).map_err(|error| {
                AppError::CorruptRecord {
                    file: format!("{PID_FILE} ({})", project_root.display()),
                    details: error.to_string(),
                }
            })?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn read_backend_process_set_from_project_root(
        project_root: &Path,
    ) -> AppResult<RunBackendProcessSet> {
        let path = Self::live_project_backend_processes_path(project_root);
        match fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
                file: format!(
                    "runtime/backend/{ACTIVE_BACKEND_PROCESSES_FILE} ({})",
                    project_root.display()
                ),
                details: error.to_string(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(RunBackendProcessSet::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn write_backend_process_set_to_project_root(
        project_root: &Path,
        processes: &[RunBackendProcessRecord],
    ) -> AppResult<()> {
        let path = Self::live_project_backend_processes_path(project_root);
        if processes.is_empty() {
            match fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        } else {
            let mut set = RunBackendProcessSet {
                processes: processes.to_vec(),
            };
            set.processes.sort_by_key(|left| left.pid);
            Self::write_atomic(&path, &serde_json::to_string_pretty(&set)?)
        }
    }

    fn mutate_backend_process_set_in_project_root<T>(
        project_root: &Path,
        mutator: impl FnOnce(&mut Vec<RunBackendProcessRecord>) -> AppResult<T>,
    ) -> AppResult<T> {
        let _lock = AdvisoryFileLock::acquire(&Self::live_project_backend_processes_lock_path(
            project_root,
        ))?;
        let mut processes =
            Self::read_backend_process_set_from_project_root(project_root)?.processes;
        let result = mutator(&mut processes)?;
        Self::write_backend_process_set_to_project_root(project_root, &processes)?;
        Ok(result)
    }

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

    /// Compute a stable SHA-256 digest for prompt integrity tracking.
    pub fn prompt_hash(contents: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(contents.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn write_pid_file(
        base_dir: &Path,
        project_id: &ProjectId,
        owner: RunPidOwner,
        writer_owner: Option<&str>,
        run_id: Option<&str>,
        run_started_at: Option<DateTime<Utc>>,
    ) -> AppResult<RunPidRecord> {
        let project_root = Self::ensure_live_project_root(base_dir, project_id)?;
        let pid = std::process::id();
        let record = RunPidRecord {
            pid,
            started_at: Utc::now(),
            owner,
            writer_owner: writer_owner.map(str::to_owned),
            run_id: run_id.map(str::to_owned),
            run_started_at,
            proc_start_ticks: Self::proc_start_ticks(pid),
            proc_start_marker: Self::proc_start_marker(pid),
        };
        Self::write_atomic(
            &project_root.join(PID_FILE),
            &serde_json::to_string_pretty(&record)?,
        )?;
        Ok(record)
    }

    pub fn read_pid_file(
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Option<RunPidRecord>> {
        let project_root = Self::live_project_root(base_dir, project_id);
        Self::read_pid_file_from_project_root(&project_root)
    }

    pub fn register_backend_process(
        project_root: &Path,
        pid: u32,
    ) -> AppResult<RunBackendProcessRecord> {
        let pid_record = Self::read_pid_file_from_project_root(project_root)?;
        let record = RunBackendProcessRecord {
            pid,
            recorded_at: Utc::now(),
            run_id: pid_record.as_ref().and_then(|record| record.run_id.clone()),
            run_started_at: pid_record.as_ref().and_then(|record| record.run_started_at),
            proc_start_ticks: Self::proc_start_ticks(pid),
            proc_start_marker: Self::proc_start_marker(pid),
        };
        Self::mutate_backend_process_set_in_project_root(project_root, |processes| {
            processes.retain(|existing| existing.pid != pid);
            processes.push(record.clone());
            Ok(())
        })?;
        Ok(record)
    }

    pub fn remove_backend_process(project_root: &Path, pid: u32) -> AppResult<()> {
        Self::mutate_backend_process_set_in_project_root(project_root, |processes| {
            processes.retain(|existing| existing.pid != pid);
            Ok(())
        })
    }

    pub fn read_backend_processes(
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RunBackendProcessRecord>> {
        let project_root = Self::live_project_root(base_dir, project_id);
        Ok(Self::read_backend_process_set_from_project_root(&project_root)?.processes)
    }

    pub fn write_backend_processes(
        base_dir: &Path,
        project_id: &ProjectId,
        processes: &[RunBackendProcessRecord],
    ) -> AppResult<()> {
        let project_root = Self::live_project_root(base_dir, project_id);
        let updated = processes.to_vec();
        Self::mutate_backend_process_set_in_project_root(&project_root, |current| {
            *current = updated;
            Ok(())
        })
    }

    pub fn remove_backend_processes_for_attempt(
        base_dir: &Path,
        project_id: &ProjectId,
        run_id: &str,
        run_started_at: DateTime<Utc>,
    ) -> AppResult<()> {
        let project_root = Self::live_project_root(base_dir, project_id);
        Self::mutate_backend_process_set_in_project_root(&project_root, |processes| {
            processes.retain(|record| {
                !Self::backend_process_matches_attempt(record, run_id, run_started_at)
            });
            Ok(())
        })
    }

    pub fn remove_pid_file(base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let path = Self::live_project_root(base_dir, project_id).join(PID_FILE);
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn remove_pid_file_if_matches(
        base_dir: &Path,
        project_id: &ProjectId,
        expected_record: &RunPidRecord,
    ) -> AppResult<bool> {
        let Some(current_record) = Self::read_pid_file(base_dir, project_id)? else {
            return Ok(false);
        };
        if &current_record != expected_record {
            return Ok(false);
        }
        Self::remove_pid_file(base_dir, project_id)?;
        Ok(true)
    }

    pub fn pid_record_matches_attempt(
        record: &RunPidRecord,
        run_id: &str,
        run_started_at: DateTime<Utc>,
    ) -> bool {
        record.run_id.as_deref() == Some(run_id) && record.run_started_at == Some(run_started_at)
    }

    pub fn backend_process_matches_attempt(
        record: &RunBackendProcessRecord,
        run_id: &str,
        run_started_at: DateTime<Utc>,
    ) -> bool {
        record.run_id.as_deref() == Some(run_id) && record.run_started_at == Some(run_started_at)
    }

    pub fn pid_record_is_authoritative(record: &RunPidRecord) -> bool {
        Self::identity_is_authoritative(
            record.proc_start_ticks,
            record.proc_start_marker.as_deref(),
        )
    }

    pub fn backend_process_is_authoritative(record: &RunBackendProcessRecord) -> bool {
        Self::identity_is_authoritative(
            record.proc_start_ticks,
            record.proc_start_marker.as_deref(),
        )
    }

    pub fn is_pid_running_unchecked(pid: u32) -> bool {
        #[cfg(unix)]
        {
            let pid = nix::unistd::Pid::from_raw(pid as i32);
            match nix::sys::signal::kill(pid, None) {
                Ok(()) => !matches!(Self::proc_state(pid.as_raw() as u32), Some('Z')),
                Err(nix::errno::Errno::ESRCH) => false,
                Err(_) => false,
            }
        }

        #[cfg(not(unix))]
        {
            let _ = pid;
            false
        }
    }

    pub fn is_pid_alive(record: &RunPidRecord) -> bool {
        Self::process_identity_is_alive(
            record.pid,
            record.proc_start_ticks,
            record.proc_start_marker.as_deref(),
        )
    }

    pub fn pid_record_matches_live_process(record: &RunPidRecord) -> bool {
        if Self::pid_record_is_authoritative(record) {
            return Self::is_pid_alive(record);
        }

        Self::legacy_pid_record_matches_live_process(record)
    }

    pub fn legacy_pid_record_matches_live_process(record: &RunPidRecord) -> bool {
        if Self::pid_record_is_authoritative(record) {
            return Self::is_pid_alive(record);
        }
        if !Self::is_pid_running_unchecked(record.pid) {
            return false;
        }
        let Some(live_started_at) = Self::process_started_at(record.pid) else {
            return false;
        };

        // Legacy pid files without any recorded process identity can only be
        // matched by comparing the live process start against the pid-file
        // write timestamp. A newer live start implies PID reuse.
        live_started_at.matches_legacy_pid_record(record.started_at)
    }

    pub fn is_backend_process_alive(record: &RunBackendProcessRecord) -> bool {
        Self::process_identity_is_alive(
            record.pid,
            record.proc_start_ticks,
            record.proc_start_marker.as_deref(),
        )
    }

    pub fn proc_start_ticks_for_pid(pid: u32) -> Option<u64> {
        Self::proc_start_ticks(pid)
    }

    pub fn proc_start_marker_for_pid(pid: u32) -> Option<String> {
        Self::proc_start_marker(pid)
    }

    /// Legacy 64-bit prompt hash retained only for resume compatibility with
    /// snapshots and project metadata written before the SHA-256 migration.
    pub(crate) fn legacy_prompt_hash(contents: &str) -> String {
        use siphasher::sip::SipHasher13;
        use std::hash::{Hash, Hasher};

        // Use an explicit SipHash 1-3 implementation so the compatibility path
        // stays stable across Rust/std upgrades instead of inheriting
        // `DefaultHasher` algorithm changes.
        let mut hasher = SipHasher13::new();
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
        let project_root = Self::ensure_live_project_root(base_dir, project_id)?;
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

        Self::mirror_project_file(base_dir, project_id, "prompt.original.md", original_prompt);
        Self::mirror_project_file(base_dir, project_id, PROMPT_FILE, refined_prompt);
        Self::mirror_project_file(base_dir, project_id, PROJECT_CONFIG_FILE, &updated_toml);

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
        let project_root = Self::live_project_root(base_dir, project_id);
        let prompt_path = project_root.join(PROMPT_FILE);
        let original_path = project_root.join("prompt.original.md");
        let project_toml_path = project_root.join(PROJECT_CONFIG_FILE);
        let audit_original_path =
            Self::audit_project_root(base_dir, project_id).join("prompt.original.md");

        // Restore original prompt.md
        let _ = Self::write_atomic(&prompt_path, original_prompt);

        // Remove prompt.original.md
        let _ = fs::remove_file(&original_path);
        let _ = fs::remove_file(&audit_original_path);

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
                    Self::mirror_project_file(base_dir, project_id, PROJECT_CONFIG_FILE, &updated);
                }
            }
        }
        Self::mirror_project_file(base_dir, project_id, PROMPT_FILE, original_prompt);
    }

    // ── Helpers for project filesystem layout ──

    fn resolve_git_dir(base_dir: &Path) -> PathBuf {
        let marker = base_dir.join(".git");
        if marker.is_dir() {
            return marker;
        }
        if let Ok(raw) = fs::read_to_string(&marker) {
            if let Some(raw_git_dir) = raw.trim().strip_prefix("gitdir:") {
                let git_dir = PathBuf::from(raw_git_dir.trim());
                return if git_dir.is_absolute() {
                    git_dir
                } else {
                    base_dir.join(git_dir)
                };
            }
        }
        marker
    }

    pub fn audit_workspace_root_path(base_dir: &Path) -> PathBuf {
        base_dir.join(AUDIT_WORKSPACE_DIR)
    }

    pub fn live_workspace_root_path(base_dir: &Path) -> PathBuf {
        Self::resolve_git_dir(base_dir).join(LIVE_WORKSPACE_DIR)
    }

    pub fn workspace_root_path(base_dir: &Path) -> PathBuf {
        let live_root = Self::live_workspace_root_path(base_dir);
        if live_root.join("workspace.toml").is_file() {
            live_root
        } else {
            Self::audit_workspace_root_path(base_dir)
        }
    }

    pub(crate) fn daemon_root(base_dir: &Path) -> PathBuf {
        Self::workspace_root_path(base_dir).join("daemon")
    }

    pub(crate) fn live_project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::live_workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(project_id.as_str())
    }

    pub(crate) fn audit_project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::audit_workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(project_id.as_str())
    }

    pub(crate) fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        let live_root = Self::live_project_root(base_dir, project_id);
        if live_root.exists() {
            live_root
        } else {
            Self::audit_project_root(base_dir, project_id)
        }
    }

    fn live_pending_delete_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::live_workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(format!(".{}.pending-delete", project_id))
    }

    fn audit_pending_delete_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::audit_workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(format!(".{}.pending-delete", project_id))
    }

    fn pending_delete_paths(base_dir: &Path, project_id: &ProjectId) -> [PathBuf; 2] {
        [
            Self::live_pending_delete_path(base_dir, project_id),
            Self::audit_pending_delete_path(base_dir, project_id),
        ]
    }

    fn project_delete_is_pending(base_dir: &Path, project_id: &ProjectId) -> bool {
        Self::pending_delete_paths(base_dir, project_id)
            .into_iter()
            .any(|path| path.is_dir())
    }

    fn copy_tree(source: &Path, destination: &Path) -> AppResult<()> {
        if !source.exists() {
            return Ok(());
        }
        fs::create_dir_all(destination)?;
        let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, std::io::Error>>()?;
        entries.sort_by_key(|left| left.file_name());
        for entry in entries {
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(AppError::CorruptRecord {
                    file: source_path.display().to_string(),
                    details: "symlink entries are not supported while seeding the live workspace"
                        .to_owned(),
                });
            }
            if file_type.is_dir() {
                Self::copy_tree(&source_path, &destination_path)?;
            } else {
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(&source_path, &destination_path)?;
            }
        }
        Ok(())
    }

    fn ensure_live_workspace_seeded(base_dir: &Path) -> AppResult<()> {
        let live_root = Self::live_workspace_root_path(base_dir);
        if live_root.join("workspace.toml").is_file() {
            return Ok(());
        }
        let audit_root = Self::audit_workspace_root_path(base_dir);
        if audit_root.exists() {
            let sentinel_name = "workspace.toml";
            fs::create_dir_all(&live_root)?;
            let mut entries =
                fs::read_dir(&audit_root)?.collect::<Result<Vec<_>, std::io::Error>>()?;
            entries.sort_by_key(|left| left.file_name());

            let mut deferred_sentinel = None;
            for entry in entries {
                let source_path = entry.path();
                let destination_path = live_root.join(entry.file_name());
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    return Err(AppError::CorruptRecord {
                        file: source_path.display().to_string(),
                        details:
                            "symlink entries are not supported while seeding the live workspace"
                                .to_owned(),
                    });
                }

                if entry.file_name() == sentinel_name {
                    deferred_sentinel = Some(source_path);
                    continue;
                }

                if file_type.is_dir() {
                    Self::copy_tree(&source_path, &destination_path)?;
                } else {
                    fs::copy(&source_path, &destination_path)?;
                }
            }

            if let Some(source_path) = deferred_sentinel {
                fs::copy(source_path, live_root.join(sentinel_name))?;
            }
        }
        Ok(())
    }

    fn ensure_live_project_root(base_dir: &Path, project_id: &ProjectId) -> AppResult<PathBuf> {
        Self::ensure_live_workspace_seeded(base_dir)?;
        let live_root = Self::live_project_root(base_dir, project_id);
        if live_root.join(PROJECT_CONFIG_FILE).is_file() {
            return Ok(live_root);
        }
        let audit_root = Self::audit_project_root(base_dir, project_id);
        if audit_root.exists() {
            Self::copy_tree(&audit_root, &live_root)?;
        }
        Ok(live_root)
    }

    #[cfg(target_os = "linux")]
    fn proc_start_ticks(pid: u32) -> Option<u64> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, rest) = stat.rsplit_once(") ")?;
        let fields: Vec<&str> = rest.split_whitespace().collect();
        fields.get(19)?.parse().ok()
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn proc_start_ticks(_pid: u32) -> Option<u64> {
        None
    }

    #[cfg(not(unix))]
    fn proc_start_ticks(_pid: u32) -> Option<u64> {
        None
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn proc_start_marker(pid: u32) -> Option<String> {
        Self::ps_field(pid, "lstart")
    }

    /// Raw marker without `LC_ALL=C TZ=UTC` normalization — used as a
    /// backward-compatibility fallback when comparing against records
    /// written by older binaries under a different locale/timezone.
    #[cfg(all(unix, not(target_os = "linux")))]
    fn proc_start_marker_raw(pid: u32) -> Option<String> {
        let output = Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let value = String::from_utf8_lossy(&output.stdout);
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }

    #[cfg(any(target_os = "linux", not(unix)))]
    fn proc_start_marker(_pid: u32) -> Option<String> {
        None
    }

    #[cfg(any(target_os = "linux", not(unix)))]
    fn proc_start_marker_raw(_pid: u32) -> Option<String> {
        None
    }

    #[cfg(target_os = "linux")]
    fn proc_state(pid: u32) -> Option<char> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, rest) = stat.rsplit_once(") ")?;
        rest.chars().next()
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn proc_state(pid: u32) -> Option<char> {
        Self::ps_field(pid, "stat").and_then(|value| value.chars().next())
    }

    #[cfg(not(unix))]
    fn proc_state(_pid: u32) -> Option<char> {
        None
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    fn ps_field(pid: u32, field: &str) -> Option<String> {
        let output = Self::ps_command(pid, field).output().ok()?;
        if !output.status.success() {
            return None;
        }
        let value = String::from_utf8(output.stdout).ok()?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }

    #[cfg(any(test, all(unix, not(target_os = "linux"))))]
    fn ps_command(pid: u32, field: &str) -> Command {
        let mut command = Command::new("ps");
        command
            .env("LC_ALL", "C")
            .env("LANG", "C")
            .env("TZ", "UTC")
            .args(["-o", &format!("{field}="), "-p", &pid.to_string()]);
        command
    }

    pub(crate) fn mirror_project_file(
        base_dir: &Path,
        project_id: &ProjectId,
        relative_path: &str,
        contents: &str,
    ) {
        let path = Self::audit_project_root(base_dir, project_id).join(relative_path);
        let _ = Self::write_atomic(&path, contents);
    }

    fn append_project_mirror_line(
        base_dir: &Path,
        project_id: &ProjectId,
        relative_path: &str,
        line: &str,
    ) {
        let path = Self::audit_project_root(base_dir, project_id).join(relative_path);
        let _ = Self::append_line(&path, line);
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
        let project_root = Self::ensure_live_project_root(base_dir, project_id)?;
        Self::write_atomic(&project_root.join(PROJECT_POLICY_CONFIG_FILE), &rendered)?;
        Self::mirror_project_file(base_dir, project_id, PROJECT_POLICY_CONFIG_FILE, &rendered);
        Ok(())
    }

    pub(crate) fn project_policy_config_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::project_root(base_dir, project_id).join(PROJECT_POLICY_CONFIG_FILE)
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
        if FileSystem::project_delete_is_pending(base_dir, project_id) {
            return Ok(false);
        }
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
        if FileSystem::project_delete_is_pending(base_dir, project_id) {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }
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
        let mut candidate_ids = std::collections::BTreeSet::new();
        for projects_dir in [
            FileSystem::audit_workspace_root_path(base_dir).join(PROJECTS_DIR),
            FileSystem::live_workspace_root_path(base_dir).join(PROJECTS_DIR),
        ] {
            if !projects_dir.is_dir() {
                continue;
            }
            for entry in fs::read_dir(&projects_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with('.') {
                    continue;
                }
                if let Ok(pid) = ProjectId::new(name_str.as_ref()) {
                    candidate_ids.insert(pid.to_string());
                }
            }
        }
        let mut ids = Vec::new();
        for id in candidate_ids {
            let pid = ProjectId::new(id.clone()).expect("candidate ids already validated");
            if FileSystem::project_delete_is_pending(base_dir, &pid) {
                continue;
            }
            let project_root = FileSystem::project_root(base_dir, &pid);
            if !project_root.is_dir() {
                continue;
            }
            if !project_root.join(PROJECT_CONFIG_FILE).is_file() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{id}/project.toml"),
                    details: "project directory exists but canonical project.toml is missing"
                        .to_owned(),
                });
            }
            ids.push(pid);
        }
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    fn stage_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let live_project_root = FileSystem::live_project_root(base_dir, project_id);
        let audit_project_root = FileSystem::audit_project_root(base_dir, project_id);
        if !live_project_root.is_dir() && !audit_project_root.is_dir() {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }

        for pending_path in FileSystem::pending_delete_paths(base_dir, project_id) {
            if pending_path.is_dir() {
                fs::remove_dir_all(&pending_path)?;
            } else if pending_path.exists() {
                fs::remove_file(&pending_path)?;
            }
        }

        let mut renamed_paths = Vec::new();
        for (project_root, pending_path) in [
            (
                live_project_root,
                FileSystem::live_pending_delete_path(base_dir, project_id),
            ),
            (
                audit_project_root,
                FileSystem::audit_pending_delete_path(base_dir, project_id),
            ),
        ] {
            if !project_root.is_dir() {
                continue;
            }
            if let Some(parent) = pending_path.parent() {
                fs::create_dir_all(parent)?;
            }
            match fs::rename(&project_root, &pending_path) {
                Ok(()) => renamed_paths.push((project_root, pending_path)),
                Err(error) => {
                    for (original_root, staged_root) in renamed_paths.into_iter().rev() {
                        let _ = fs::rename(&staged_root, &original_root);
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }

    fn commit_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        for pending_path in FileSystem::pending_delete_paths(base_dir, project_id) {
            if pending_path.is_dir() {
                fs::remove_dir_all(&pending_path)?;
            } else if pending_path.exists() {
                fs::remove_file(&pending_path)?;
            }
        }
        Ok(())
    }

    fn rollback_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let mut restored_paths = Vec::new();
        for (pending_path, project_root) in [
            (
                FileSystem::live_pending_delete_path(base_dir, project_id),
                FileSystem::live_project_root(base_dir, project_id),
            ),
            (
                FileSystem::audit_pending_delete_path(base_dir, project_id),
                FileSystem::audit_project_root(base_dir, project_id),
            ),
        ] {
            if !pending_path.is_dir() {
                continue;
            }
            if let Some(parent) = project_root.parent() {
                fs::create_dir_all(parent)?;
            }
            match fs::rename(&pending_path, &project_root) {
                Ok(()) => restored_paths.push((pending_path, project_root)),
                Err(error) => {
                    for (staged_root, original_root) in restored_paths.into_iter().rev() {
                        let _ = fs::rename(&original_root, &staged_root);
                    }
                    return Err(error.into());
                }
            }
        }
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
        let project_root = FileSystem::live_project_root(base_dir, &record.id);

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

        let project_toml = toml::to_string_pretty(record)
            .map_err(|e| AppError::Io(std::io::Error::other(e.to_string())))?;
        let project_config_toml = toml::to_string_pretty(&ProjectConfig::default())
            .map_err(|e| AppError::Io(std::io::Error::other(e.to_string())))?;
        let run_json = serde_json::to_string_pretty(run_snapshot)?;
        let mut journal_content = initial_journal_line.to_owned();
        journal_content.push('\n');
        FileSystem::mirror_project_file(base_dir, &record.id, PROJECT_CONFIG_FILE, &project_toml);
        FileSystem::mirror_project_file(
            base_dir,
            &record.id,
            PROJECT_POLICY_CONFIG_FILE,
            &project_config_toml,
        );
        FileSystem::mirror_project_file(base_dir, &record.id, PROMPT_FILE, prompt_contents);
        FileSystem::mirror_project_file(base_dir, &record.id, RUN_FILE, &run_json);
        FileSystem::mirror_project_file(base_dir, &record.id, JOURNAL_FILE, &journal_content);

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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let path = project_root.join(JOURNAL_FILE);
        FileSystem::append_line(&path, line)?;
        FileSystem::append_project_mirror_line(base_dir, project_id, JOURNAL_FILE, line);
        Ok(())
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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let path = project_root.join(RUN_FILE);
        let contents = serde_json::to_string_pretty(snapshot)?;
        FileSystem::write_atomic(&path, &contents)?;
        FileSystem::mirror_project_file(base_dir, project_id, RUN_FILE, &contents);
        Ok(())
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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let path = project_root
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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;

        let payload_final = project_root
            .join("history/payloads")
            .join(format!("{}.json", payload.payload_id));
        let artifact_final = project_root
            .join("history/artifacts")
            .join(format!("{}.json", artifact.artifact_id));
        if let Some(parent) = payload_final.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = artifact_final.parent() {
            fs::create_dir_all(parent)?;
        }

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

        FileSystem::mirror_project_file(
            base_dir,
            project_id,
            &format!("history/artifacts/{}.json", artifact.artifact_id),
            &artifact_json,
        );

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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let dir = project_root.join("amendments");
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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let path = project_root
            .join("amendments")
            .join(format!("{}.json", amendment_id));
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn drain_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<u32> {
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let dir = project_root.join("amendments");
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
        let project_root = FileSystem::ensure_live_project_root(base_dir, project_id)?;
        let dir = project_root.join("runtime/logs");
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
//   ├─ controller.json       (controller state machine snapshot)
//   ├─ controller-journal.ndjson (controller transition log)
//   ├─ plan.json             (machine-readable plan)
//   ├─ plan.md               (human-readable plan)
//   └─ task-runs.ndjson      (bead → project lineage)

const MILESTONES_DIR: &str = "milestones";
const MILESTONE_CONFIG_FILE: &str = "milestone.toml";
const MILESTONE_STATUS_FILE: &str = "status.json";
const MILESTONE_JOURNAL_FILE: &str = "journal.ndjson";
const MILESTONE_CONTROLLER_FILE: &str = "controller.json";
const MILESTONE_CONTROLLER_JOURNAL_FILE: &str = "controller-journal.ndjson";
const MILESTONE_PLAN_JSON_FILE: &str = "plan.json";
const MILESTONE_PLAN_MD_FILE: &str = "plan.md";
const MILESTONE_PLAN_SHAPE_FILE: &str = "plan.shape.json";
const MILESTONE_TASK_RUNS_FILE: &str = "task-runs.ndjson";
const MILESTONE_PLANNED_ELSEWHERE_FILE: &str = "planned_elsewhere.ndjson";
const MILESTONE_STATE_COMMIT_FILE: &str = ".state-commit.json";
const MILESTONE_MUTATION_LOCK_FILE: &str = "mutation.lock";
const MILESTONE_LOCKS_DIR: &str = ".locks";

impl FileSystem {
    pub(crate) fn milestone_root(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        Self::audit_workspace_root_path(base_dir)
            .join(MILESTONES_DIR)
            .join(milestone_id.as_str())
    }

    pub(crate) fn milestone_lock_root(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        Self::audit_workspace_root_path(base_dir)
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

struct CurrentThreadMilestoneMutationLockGuard {
    lock_path: PathBuf,
}

impl CurrentThreadMilestoneMutationLockGuard {
    fn acquire(lock_path: &Path) -> Self {
        HELD_MILESTONE_MUTATION_LOCKS.with(|locks| {
            locks.borrow_mut().push(lock_path.to_path_buf());
        });
        Self {
            lock_path: lock_path.to_path_buf(),
        }
    }

    fn is_held(lock_path: &Path) -> bool {
        HELD_MILESTONE_MUTATION_LOCKS
            .with(|locks| locks.borrow().iter().any(|held| held == lock_path))
    }
}

impl Drop for CurrentThreadMilestoneMutationLockGuard {
    fn drop(&mut self) {
        HELD_MILESTONE_MUTATION_LOCKS.with(|locks| {
            let mut locks = locks.borrow_mut();
            if let Some(index) = locks.iter().rposition(|held| held == &self.lock_path) {
                locks.swap_remove(index);
            }
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum PendingMilestoneStateCommitRecoveryAction {
    #[default]
    Publish,
    Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingMilestoneStateCommit {
    #[serde(default)]
    recovery_action: PendingMilestoneStateCommitRecoveryAction,
    previous_snapshot: MilestoneSnapshot,
    previous_journal: Vec<MilestoneJournalEvent>,
    next_snapshot: MilestoneSnapshot,
    next_journal: Vec<MilestoneJournalEvent>,
}

impl PendingMilestoneStateCommit {
    fn target_snapshot(&self) -> &MilestoneSnapshot {
        match self.recovery_action {
            PendingMilestoneStateCommitRecoveryAction::Publish => &self.next_snapshot,
            PendingMilestoneStateCommitRecoveryAction::Rollback => &self.previous_snapshot,
        }
    }

    fn target_journal(&self) -> &[MilestoneJournalEvent] {
        match self.recovery_action {
            PendingMilestoneStateCommitRecoveryAction::Publish => &self.next_journal,
            PendingMilestoneStateCommitRecoveryAction::Rollback => &self.previous_journal,
        }
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

    fn write_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        record: &MilestoneRecord,
    ) -> AppResult<()> {
        let root = FileSystem::milestone_root(base_dir, milestone_id);
        let path = root.join(MILESTONE_CONFIG_FILE);
        let raw = toml::to_string_pretty(record)?;
        FileSystem::write_atomic(&path, &raw)?;
        Ok(())
    }

    fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
        let milestones_dir = FileSystem::audit_workspace_root_path(base_dir).join(MILESTONES_DIR);
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

    fn pending_state_commit_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATE_COMMIT_FILE)
    }

    fn read_pending_state_commit(
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Option<PendingMilestoneStateCommit>> {
        let path = Self::pending_state_commit_path(base_dir, milestone_id);
        let raw = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let pending = serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
            file: format!(
                "milestones/{}/{}",
                milestone_id, MILESTONE_STATE_COMMIT_FILE
            ),
            details: error.to_string(),
        })?;
        Ok(Some(pending))
    }

    fn read_snapshot_from_path(
        path: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneSnapshot> {
        let raw = fs::read_to_string(path)?;
        serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
            file: format!("milestones/{}/status.json", milestone_id),
            details: e.to_string(),
        })
    }

    fn apply_pending_state_commit(
        base_dir: &Path,
        milestone_id: &MilestoneId,
        pending: &PendingMilestoneStateCommit,
    ) -> AppResult<()> {
        let status_path =
            FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATUS_FILE);
        let status_json = serde_json::to_string_pretty(pending.target_snapshot())?;
        FileSystem::write_atomic(&status_path, &status_json)?;
        FsMilestoneJournalStore::write_journal(
            &FsMilestoneJournalStore::journal_path(base_dir, milestone_id),
            pending.target_journal(),
        )?;
        let _ = fs::remove_file(Self::pending_state_commit_path(base_dir, milestone_id));
        Ok(())
    }

    fn recover_pending_state_commit(base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<()> {
        if let Some(pending) = Self::read_pending_state_commit(base_dir, milestone_id)? {
            Self::apply_pending_state_commit(base_dir, milestone_id, &pending)?;
        }
        Ok(())
    }

    fn recover_pending_state_commit_for_plain_read(
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<()> {
        let pending_path = Self::pending_state_commit_path(base_dir, milestone_id);
        // This early return is only an optimization. A concurrent writer can still create the
        // sidecar after this check, and the XOR masking in `read_snapshot` / `read_journal`
        // remains the correctness guard for partially visible cross-file commits.
        if !pending_path.is_file() {
            return Ok(());
        }

        let lock_path = Self::mutation_lock_path(base_dir, milestone_id);
        if CurrentThreadMilestoneMutationLockGuard::is_held(&lock_path) {
            return Ok(());
        }

        let _lock = AdvisoryFileLock::acquire(&lock_path)?;
        let _marker = CurrentThreadMilestoneMutationLockGuard::acquire(&lock_path);
        Self::recover_pending_state_commit(base_dir, milestone_id)
    }
}

impl MilestoneSnapshotPort for FsMilestoneSnapshotStore {
    fn read_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneSnapshot> {
        Self::recover_pending_state_commit_for_plain_read(base_dir, milestone_id)?;
        let path = FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATUS_FILE);
        let snapshot = Self::read_snapshot_from_path(&path, milestone_id)?;
        let Some(pending) = Self::read_pending_state_commit(base_dir, milestone_id)? else {
            return Ok(snapshot);
        };

        let journal = FsMilestoneJournalStore::read_journal_from_path(
            &FsMilestoneJournalStore::journal_path(base_dir, milestone_id),
            milestone_id,
        )?;
        let snapshot_visible = snapshot == pending.next_snapshot;
        let journal_visible = journal == pending.next_journal;
        if snapshot_visible ^ journal_visible {
            return Ok(pending.previous_snapshot);
        }

        Ok(snapshot)
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
        let lock_path = Self::mutation_lock_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&lock_path)?;
        let _marker = CurrentThreadMilestoneMutationLockGuard::acquire(&lock_path);
        Self::recover_pending_state_commit(base_dir, milestone_id)?;
        operation()
    }
}

pub struct FsMilestoneControllerStore;

impl FsMilestoneControllerStore {
    fn controller_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_CONTROLLER_FILE)
    }

    fn controller_journal_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_CONTROLLER_JOURNAL_FILE)
    }

    fn ensure_milestone_root_exists(base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<()> {
        let root = FileSystem::milestone_root(base_dir, milestone_id);
        if root.is_dir() {
            return Ok(());
        }

        Err(AppError::CorruptRecord {
            file: format!("milestones/{}/{}", milestone_id, MILESTONE_CONTROLLER_FILE),
            details: "milestone directory does not exist".to_owned(),
        })
    }

    fn read_controller_from_path(
        path: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Option<MilestoneControllerRecord>> {
        let raw = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        let controller: MilestoneControllerRecord =
            serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
                file: format!("milestones/{}/{}", milestone_id, MILESTONE_CONTROLLER_FILE),
                details: error.to_string(),
            })?;
        if controller.schema_version
            != crate::contexts::milestone_record::controller::MILESTONE_CONTROLLER_SCHEMA_VERSION
        {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/{}", milestone_id, MILESTONE_CONTROLLER_FILE),
                details: format!(
                    "unsupported controller schema_version {} (expected {})",
                    controller.schema_version,
                    crate::contexts::milestone_record::controller::MILESTONE_CONTROLLER_SCHEMA_VERSION
                ),
            });
        }
        if controller.milestone_id != *milestone_id {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/{}", milestone_id, MILESTONE_CONTROLLER_FILE),
                details: format!(
                    "controller milestone_id '{}' does not match milestone path '{}'",
                    controller.milestone_id, milestone_id
                ),
            });
        }
        Ok(Some(controller))
    }

    fn read_controller_journal_from_path(
        path: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneControllerTransitionEvent>> {
        let raw = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        let entries: Vec<_> = raw
            .lines()
            .enumerate()
            .filter_map(|(index, line)| {
                let trimmed = line.trim();
                (!trimmed.is_empty()).then_some((index, trimmed))
            })
            .collect();

        let mut events = Vec::with_capacity(entries.len());
        for (position, (index, trimmed)) in entries.iter().enumerate() {
            let event: MilestoneControllerTransitionEvent = match serde_json::from_str(trimmed) {
                Ok(event) => event,
                Err(error) if position + 1 == entries.len() => {
                    tracing::warn!(
                        milestone_id = %milestone_id,
                        file = MILESTONE_CONTROLLER_JOURNAL_FILE,
                        line_number = *index + 1,
                        error = %error,
                        "discarding malformed trailing controller journal line"
                    );
                    break;
                }
                Err(error) => {
                    return Err(AppError::CorruptRecord {
                        file: format!(
                            "milestones/{}/{}",
                            milestone_id, MILESTONE_CONTROLLER_JOURNAL_FILE
                        ),
                        details: format!("line {}: {}", index + 1, error),
                    });
                }
            };
            events.push(event);
        }
        Ok(events)
    }
}

impl MilestoneControllerPort for FsMilestoneControllerStore {
    fn read_controller(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Option<MilestoneControllerRecord>> {
        Self::ensure_milestone_root_exists(base_dir, milestone_id)?;
        Self::read_controller_from_path(
            &Self::controller_path(base_dir, milestone_id),
            milestone_id,
        )
    }

    fn write_controller(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        controller: &MilestoneControllerRecord,
    ) -> AppResult<()> {
        Self::ensure_milestone_root_exists(base_dir, milestone_id)?;
        let raw = serde_json::to_string_pretty(controller)?;
        FileSystem::write_atomic(&Self::controller_path(base_dir, milestone_id), &raw)
    }

    fn read_transition_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneControllerTransitionEvent>> {
        Self::ensure_milestone_root_exists(base_dir, milestone_id)?;
        Self::read_controller_journal_from_path(
            &Self::controller_journal_path(base_dir, milestone_id),
            milestone_id,
        )
    }

    fn append_transition_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneControllerTransitionEvent,
    ) -> AppResult<()> {
        Self::ensure_milestone_root_exists(base_dir, milestone_id)?;
        FileSystem::append_line(
            &Self::controller_journal_path(base_dir, milestone_id),
            &event.to_ndjson_line().map_err(AppError::SerdeJson)?,
        )
    }

    fn with_controller_lock<T, F>(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        operation: F,
    ) -> AppResult<T>
    where
        F: FnOnce() -> AppResult<T>,
    {
        Self::ensure_milestone_root_exists(base_dir, milestone_id)?;
        let lock_path = FsMilestoneSnapshotStore::mutation_lock_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&lock_path)?;
        let _marker = CurrentThreadMilestoneMutationLockGuard::acquire(&lock_path);
        FsMilestoneSnapshotStore::recover_pending_state_commit(base_dir, milestone_id)?;
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
        ) || FsTaskRunLineageStore::option_conflicts(
            existing.task_id.as_deref(),
            requested.task_id.as_deref(),
        ) {
            return None;
        }

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
        if merged.task_id.is_none() {
            merged.task_id = requested.task_id.clone();
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
            self.task_id.as_deref(),
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
        _milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>> {
        let raw = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let lines: Vec<&str> = raw.lines().collect();
        let line_count = lines.len();
        let mut events = Vec::new();
        for (i, line) in lines.into_iter().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<MilestoneJournalEvent>(trimmed) {
                Ok(event) => events.push(event),
                Err(e) => {
                    let is_last_line = i + 1 == line_count;
                    if is_last_line {
                        // Tolerate a malformed trailing line — this
                        // happens when the process crashed mid-append
                        // (partial write).  The line is lost but the
                        // journal up to that point is intact.
                        tracing::warn!(
                            line = i + 1,
                            error = %e,
                            "ignoring malformed trailing journal line (likely partial write)"
                        );
                    } else {
                        // Non-trailing malformed lines indicate genuine
                        // corruption in the authoritative journal — fail
                        // loudly so the operator can investigate.
                        return Err(AppError::CorruptRecord {
                            file: path.display().to_string(),
                            details: format!("line {} is malformed: {e}", i + 1,),
                        });
                    }
                }
            }
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

    fn matches_event(existing: &MilestoneJournalEvent, requested: &MilestoneJournalEvent) -> bool {
        existing == requested
    }

    fn repairable_completion_event(
        existing: &MilestoneJournalEvent,
        requested: &MilestoneJournalEvent,
    ) -> Option<MilestoneJournalEvent> {
        if !Self::is_completion_event(existing)
            || !Self::is_completion_event(requested)
            || existing.event_type != requested.event_type
            || existing.bead_id != requested.bead_id
        {
            return None;
        }

        let existing_details = CompletionJournalDetails::parse(existing.details.as_deref()?)?;
        let requested_details = CompletionJournalDetails::parse(requested.details.as_deref()?)?;
        if existing_details.project_id != requested_details.project_id
            || existing_details.started_at != requested_details.started_at
            || FsTaskRunLineageStore::option_conflicts(
                existing_details.run_id.as_deref(),
                requested_details.run_id.as_deref(),
            )
            || FsTaskRunLineageStore::option_conflicts(
                existing_details.plan_hash.as_deref(),
                requested_details.plan_hash.as_deref(),
            )
        {
            return None;
        }

        let merged_details =
            CompletionJournalDetails::merge(&existing_details, &requested_details)?;
        let mut repaired = existing.clone();
        repaired.details = Some(merged_details.render());
        Some(repaired)
    }

    fn explicitly_repaired_completion_event(
        existing: &MilestoneJournalEvent,
        requested: &MilestoneJournalEvent,
    ) -> Option<MilestoneJournalEvent> {
        if !Self::is_completion_event(existing)
            || !Self::is_completion_event(requested)
            || existing.bead_id != requested.bead_id
        {
            return None;
        }

        let existing_details = CompletionJournalDetails::parse(existing.details.as_deref()?)?;
        let requested_details = CompletionJournalDetails::parse(requested.details.as_deref()?)?;
        if existing_details.project_id != requested_details.project_id
            || existing_details.started_at != requested_details.started_at
            || FsTaskRunLineageStore::option_conflicts(
                existing_details.run_id.as_deref(),
                requested_details.run_id.as_deref(),
            )
            || FsTaskRunLineageStore::option_conflicts(
                existing_details.plan_hash.as_deref(),
                requested_details.plan_hash.as_deref(),
            )
        {
            return None;
        }

        let mut repaired_details = requested_details.clone();
        if repaired_details.run_id.is_none() {
            repaired_details.run_id = existing_details.run_id.clone();
        }
        if repaired_details.plan_hash.is_none() {
            repaired_details.plan_hash = existing_details.plan_hash.clone();
        }
        if repaired_details.outcome == existing_details.outcome
            && repaired_details.outcome_detail.is_none()
        {
            repaired_details.outcome_detail = existing_details.outcome_detail.clone();
        }

        let mut repaired = requested.clone();
        repaired.details = Some(repaired_details.render());
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

    fn apply_append_if_missing(
        journal: &mut Vec<MilestoneJournalEvent>,
        event: &MilestoneJournalEvent,
    ) -> bool {
        if journal
            .iter()
            .any(|existing| Self::matches_event(existing, event))
        {
            return false;
        }

        if let Some((existing_index, repaired_event)) =
            journal.iter().enumerate().find_map(|(index, existing)| {
                Self::repairable_start_event(existing, event)
                    .or_else(|| Self::repairable_completion_event(existing, event))
                    .map(|repaired| (index, repaired))
            })
        {
            if journal[existing_index].details == repaired_event.details {
                return false;
            }

            journal[existing_index] = repaired_event;
            return true;
        }

        journal.push(event.clone());
        true
    }

    fn apply_repair_completion(
        journal: &mut Vec<MilestoneJournalEvent>,
        event: &MilestoneJournalEvent,
    ) -> bool {
        let exact_match_index = journal
            .iter()
            .position(|existing| Self::matches_event(existing, event));

        if let Some((existing_index, repaired_event)) =
            journal.iter().enumerate().find_map(|(index, existing)| {
                if Self::matches_event(existing, event) {
                    return None;
                }
                Self::explicitly_repaired_completion_event(existing, event)
                    .map(|repaired| (index, repaired))
            })
        {
            if exact_match_index.is_some() {
                journal.remove(existing_index);
            } else {
                journal[existing_index] = repaired_event;
            }
            return true;
        }

        if exact_match_index.is_some() {
            return false;
        }

        journal.push(event.clone());
        true
    }

    fn apply_journal_write_op(
        journal: &mut Vec<MilestoneJournalEvent>,
        op: &JournalWriteOp,
    ) -> bool {
        match op.kind {
            JournalWriteOpKind::AppendIfMissing => {
                Self::apply_append_if_missing(journal, &op.event)
            }
            JournalWriteOpKind::RepairCompletion => {
                Self::apply_repair_completion(journal, &op.event)
            }
        }
    }
}

impl MilestoneJournalPort for FsMilestoneJournalStore {
    fn read_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>> {
        FsMilestoneSnapshotStore::recover_pending_state_commit_for_plain_read(
            base_dir,
            milestone_id,
        )?;
        let journal = Self::read_journal_from_path(
            &Self::journal_path(base_dir, milestone_id),
            milestone_id,
        )?;
        let Some(pending) =
            FsMilestoneSnapshotStore::read_pending_state_commit(base_dir, milestone_id)?
        else {
            return Ok(journal);
        };

        let snapshot = FsMilestoneSnapshotStore::read_snapshot_from_path(
            &FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_STATUS_FILE),
            milestone_id,
        )?;
        let snapshot_visible = snapshot == pending.next_snapshot;
        let journal_visible = journal == pending.next_journal;
        if snapshot_visible ^ journal_visible {
            return Ok(pending.previous_journal);
        }

        Ok(journal)
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

    fn replace_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        events: &[MilestoneJournalEvent],
    ) -> AppResult<()> {
        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        Self::write_journal(&path, events)
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
        if !Self::apply_append_if_missing(&mut journal, event) {
            return Ok(false);
        }
        Self::write_journal(&path, &journal)?;
        Ok(true)
    }

    fn repair_completion_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool> {
        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        let mut journal = Self::read_journal_from_path(&path, milestone_id)?;
        if !Self::apply_repair_completion(&mut journal, event) {
            return Ok(false);
        }
        Self::write_journal(&path, &journal)?;
        Ok(true)
    }

    fn commit_journal_ops(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        ops: &[JournalWriteOp],
    ) -> AppResult<usize> {
        if ops.is_empty() {
            return Ok(0);
        }

        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        let mut journal = Self::read_journal_from_path(&path, milestone_id)?;
        let mut applied = 0;

        for op in ops {
            if Self::apply_journal_write_op(&mut journal, op) {
                applied += 1;
            }
        }

        if applied == 0 {
            return Ok(0);
        }

        Self::write_journal(&path, &journal)?;
        Ok(applied)
    }

    fn commit_snapshot_and_journal_ops<S: MilestoneSnapshotPort>(
        &self,
        snapshot_store: &S,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        previous_snapshot: &MilestoneSnapshot,
        next_snapshot: &MilestoneSnapshot,
        ops: &[JournalWriteOp],
        _error_context: &str,
    ) -> AppResult<usize> {
        let path = Self::journal_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::journal_lock_path(base_dir, milestone_id))?;
        let mut journal = Self::read_journal_from_path(&path, milestone_id)?;
        let previous_journal = journal.clone();
        let mut applied = 0;

        for op in ops {
            if Self::apply_journal_write_op(&mut journal, op) {
                applied += 1;
            }
        }

        if applied == 0 && previous_snapshot == next_snapshot {
            return Ok(0);
        }

        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Publish,
            previous_snapshot: previous_snapshot.clone(),
            previous_journal,
            next_snapshot: next_snapshot.clone(),
            next_journal: journal,
        };
        let pending_path =
            FsMilestoneSnapshotStore::pending_state_commit_path(base_dir, milestone_id);
        let pending_json = serde_json::to_string_pretty(&pending)?;
        FileSystem::write_atomic(&pending_path, &pending_json)?;
        if let Err(error) = snapshot_store.write_snapshot(base_dir, milestone_id, next_snapshot) {
            // Snapshot writes are all-or-nothing, so a failure leaves the old snapshot/journal
            // pair intact and the pending sidecar must be discarded to avoid replaying a commit
            // that never became partially visible.
            let _ = fs::remove_file(&pending_path);
            return Err(error);
        }
        if let Err(error) = Self::write_journal(&path, &pending.next_journal) {
            if let Err(rollback_error) =
                snapshot_store.write_snapshot(base_dir, milestone_id, previous_snapshot)
            {
                let rollback_pending = PendingMilestoneStateCommit {
                    recovery_action: PendingMilestoneStateCommitRecoveryAction::Rollback,
                    ..pending
                };
                let rollback_pending_json = serde_json::to_string_pretty(&rollback_pending)?;
                FileSystem::write_atomic(&pending_path, &rollback_pending_json)?;
                return Err(AppError::CorruptRecord {
                    file: format!("milestones/{}/status.json", milestone_id),
                    details: format!(
                        "{_error_context}: {error}; failed to restore the previous snapshot: {rollback_error}; a rollback recovery sidecar was retained"
                    ),
                });
            }
            let _ = fs::remove_file(&pending_path);
            return Err(error);
        }
        let _ = fs::remove_file(&pending_path);
        Ok(applied)
    }
}

/// Extract a `task_id` value from an `outcome_detail` string of the form
/// `"task_id=<value>"`, returning the `<value>` portion if it matches.
fn extract_task_id_from_outcome_detail(detail: &str) -> Option<&str> {
    detail.strip_prefix("task_id=")
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
        run_id: &str,
        existing_plan_hash: &str,
        requested_plan_hash: &str,
    ) -> String {
        format!(
            "conflicting plan_hash for bead={bead_id} project={project_id} run={run_id}: existing={existing_plan_hash} requested={requested_plan_hash}"
        )
    }

    fn backfill_terminal_entry(
        entry: &mut TaskRunEntry,
        plan_hash: Option<&str>,
        outcome_detail: Option<&str>,
        finished_at: DateTime<Utc>,
        overwrite_finished_at: bool,
    ) -> bool {
        let mut changed = false;

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
        // Extract structured task_id from outcome_detail when present.
        if entry.task_id.is_none() {
            if let Some(ref detail) = entry.outcome_detail {
                if let Some(tid) = extract_task_id_from_outcome_detail(detail) {
                    entry.task_id = Some(tid.to_owned());
                    changed = true;
                }
            }
        }
        if overwrite_finished_at {
            if entry.finished_at != Some(finished_at) {
                entry.finished_at = Some(finished_at);
                changed = true;
            }
        } else if entry.finished_at.is_none() {
            entry.finished_at = Some(finished_at);
            changed = true;
        }

        changed
    }

    #[allow(clippy::too_many_arguments)]
    fn replace_terminal_entry(
        entry: &mut TaskRunEntry,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<bool> {
        if let Some(plan_hash) = plan_hash {
            match entry.plan_hash.as_deref() {
                Some(existing_plan_hash) if existing_plan_hash != plan_hash => {
                    return Err(AppError::CorruptRecord {
                        file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                        details: Self::plan_hash_conflict_details(
                            bead_id,
                            project_id,
                            entry.run_id.as_deref().unwrap_or(run_id),
                            existing_plan_hash,
                            plan_hash,
                        ),
                    });
                }
                None => entry.plan_hash = Some(plan_hash.to_owned()),
                _ => {}
            }
        }

        let mut changed = false;
        if entry.started_at != started_at {
            // Repair paths may need to retarget a stale terminal row to the durable resumed
            // attempt when the controller reused the same run_id but milestone start sync was
            // missed before completion.
            entry.started_at = started_at;
            changed = true;
        }
        if entry.outcome != outcome {
            entry.outcome = outcome;
            changed = true;
        }
        if entry.outcome_detail != outcome_detail {
            entry.outcome_detail = outcome_detail;
            changed = true;
        }
        if entry.finished_at != Some(finished_at) {
            entry.finished_at = Some(finished_at);
            changed = true;
        }
        // Extract structured task_id from outcome_detail when present.
        if entry.task_id.is_none() {
            if let Some(ref detail) = entry.outcome_detail {
                if let Some(tid) = extract_task_id_from_outcome_detail(detail) {
                    entry.task_id = Some(tid.to_owned());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn backfill_running_entry(
        entry: &mut TaskRunEntry,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: &str,
    ) -> AppResult<bool> {
        let mut changed = false;

        match entry.plan_hash.as_deref() {
            Some(existing_plan_hash) if existing_plan_hash != plan_hash => {
                return Err(AppError::RunStartFailed {
                    reason: Self::plan_hash_conflict_details(
                        bead_id,
                        project_id,
                        entry.run_id.as_deref().unwrap_or(run_id),
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

        Ok(changed)
    }

    fn reopen_failed_exact_attempt(
        entries: &mut Vec<TaskRunEntry>,
        canonical_entry: &TaskRunEntry,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: &str,
        started_at: DateTime<Utc>,
    ) -> AppResult<Option<TaskRunEntry>> {
        if canonical_entry.outcome != TaskRunOutcome::Failed {
            return Ok(None);
        }

        let exact_match_indices: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                (entry.bead_id == bead_id
                    && entry.project_id == project_id
                    && entry.run_id.as_deref() == Some(run_id))
                .then_some(index)
            })
            .collect();
        if exact_match_indices.is_empty() {
            return Ok(None);
        }

        let target_index = exact_match_indices[0];
        let mut reopened_entry = canonical_entry.clone();
        reopened_entry.started_at = started_at;
        reopened_entry.outcome = TaskRunOutcome::Running;
        reopened_entry.outcome_detail = None;
        reopened_entry.finished_at = None;
        Self::backfill_running_entry(&mut reopened_entry, bead_id, project_id, run_id, plan_hash)?;
        entries[target_index] = reopened_entry.clone();

        if exact_match_indices.len() > 1 {
            let filtered_entries: Vec<TaskRunEntry> = entries
                .iter()
                .enumerate()
                .filter(|(index, _)| !exact_match_indices[1..].contains(index))
                .map(|(_, entry)| entry.clone())
                .collect();
            *entries = filtered_entries;
        }

        Ok(Some(reopened_entry))
    }

    /// One-time upgrade repair: mark any running entries with `run_id: None`
    /// as failed.  Prior code versions allowed starting beads without a run ID;
    /// the current write paths require `run_id: &str` at the port boundary, so
    /// these legacy entries can never be finalized normally.  Failing them on
    /// first write access prevents them from blocking new starts or producing
    /// confusing "ambiguous" errors.
    fn fail_legacy_runless_entries(entries: &mut [TaskRunEntry], now: DateTime<Utc>) -> bool {
        let mut changed = false;
        for entry in entries.iter_mut() {
            if entry.run_id.is_none() && !entry.outcome.is_terminal() {
                entry.outcome = TaskRunOutcome::Failed;
                entry.outcome_detail =
                    Some("superseded: run_id is now required (upgrade repair)".to_owned());
                entry.finished_at = Some(now);
                changed = true;
            }
        }
        changed
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
    /// treated as a replay match.  Matches by exact `run_id` only — legacy
    /// entries with `run_id: None` are never matched.
    fn unique_terminal_replay_match(
        entries: &[TaskRunEntry],
        matching_indices: &[usize],
        requested_run_id: &str,
    ) -> Option<usize> {
        let terminal_matches: Vec<usize> = matching_indices
            .iter()
            .copied()
            .filter(|candidate_index| {
                let candidate = &entries[*candidate_index];
                candidate.outcome.is_terminal()
                    && candidate.run_id.as_deref() == Some(requested_run_id)
            })
            .filter(|candidate_index| {
                matching_indices.iter().copied().all(|other_index| {
                    other_index == *candidate_index
                        || Self::is_legacy_start_for(entries, other_index, *candidate_index)
                })
            })
            .collect();

        match terminal_matches.as_slice() {
            [index] => Some(*index),
            _ => None,
        }
    }

    fn finalized_task_run_already_exists_error(
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        outcome: TaskRunOutcome,
    ) -> AppError {
        AppError::CorruptRecord {
            file: format!("milestones/{}/task-runs.ndjson", milestone_id),
            details: format!(
                "task run for bead={bead_id} project={project_id} run={run_id} is already finalized with outcome={outcome}",
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_task_run_internal(
        path: &Path,
        milestone_id: &MilestoneId,
        entries: &mut Vec<TaskRunEntry>,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
        allow_terminal_repair: bool,
    ) -> AppResult<TaskRunEntry> {
        let legacy_repaired = Self::fail_legacy_runless_entries(entries, finished_at);
        if legacy_repaired {
            Self::write_task_runs(path, entries)?;
        }
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
        let target_index = {
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
                                && !Self::is_superseded_legacy_start(entries, *index)
                        })
                        .collect();
                    match open_matches.as_slice() {
                        [] => {
                            let attempt_indices: Vec<usize> = matching_indices
                                .iter()
                                .copied()
                                .filter(|index| entries[*index].started_at == started_at)
                                .collect();
                            match Self::unique_terminal_replay_match(
                                entries,
                                &attempt_indices,
                                run_id,
                            ) {
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
                            let open_run_ids: Vec<&str> = open_matches
                                .iter()
                                .map(|idx| entries[*idx].run_id.as_deref().unwrap_or("<none>"))
                                .collect();
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: format!(
                                    "ambiguous task run update for bead={bead_id} project={project_id} run={run_id}; open runs: [{}]",
                                    open_run_ids.join(", ")
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
                let incompatible_terminal_update =
                    Self::option_conflicts(entry.run_id.as_deref(), Some(run_id))
                        || Self::option_conflicts(entry.plan_hash.as_deref(), plan_hash)
                        || entry.started_at != started_at;
                if incompatible_terminal_update {
                    if allow_terminal_repair {
                        should_write |= Self::replace_terminal_entry(
                            entry,
                            milestone_id,
                            bead_id,
                            project_id,
                            run_id,
                            plan_hash,
                            started_at,
                            outcome,
                            outcome_detail,
                            finished_at,
                        )?;
                    } else {
                        let existing_run_id = entry.run_id.as_deref().unwrap_or(run_id);
                        return Err(Self::finalized_task_run_already_exists_error(
                            milestone_id,
                            bead_id,
                            project_id,
                            existing_run_id,
                            entry.outcome,
                        ));
                    }
                } else {
                    let can_backfill_without_repair = entry.outcome == outcome
                        && (outcome_detail.is_none()
                            || entry.outcome_detail.is_none()
                            || entry.outcome_detail == outcome_detail);

                    if can_backfill_without_repair {
                        should_write |= Self::backfill_terminal_entry(
                            entry,
                            plan_hash,
                            outcome_detail.as_deref(),
                            finished_at,
                            allow_terminal_repair,
                        );
                    } else if allow_terminal_repair {
                        should_write |= Self::replace_terminal_entry(
                            entry,
                            milestone_id,
                            bead_id,
                            project_id,
                            run_id,
                            plan_hash,
                            started_at,
                            outcome,
                            outcome_detail,
                            finished_at,
                        )?;
                    } else {
                        let existing_run_id = entry.run_id.as_deref().unwrap_or(run_id);
                        return Err(Self::finalized_task_run_already_exists_error(
                            milestone_id,
                            bead_id,
                            project_id,
                            existing_run_id,
                            entry.outcome,
                        ));
                    }
                }
            } else {
                if let Some(plan_hash) = plan_hash {
                    match entry.plan_hash.as_deref() {
                        Some(existing_plan_hash) if existing_plan_hash != plan_hash => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: Self::plan_hash_conflict_details(
                                    bead_id,
                                    project_id,
                                    entry.run_id.as_deref().unwrap_or(run_id),
                                    existing_plan_hash,
                                    plan_hash,
                                ),
                            });
                        }
                        None => entry.plan_hash = Some(plan_hash.to_owned()),
                        _ => {}
                    }
                }
                entry.outcome = outcome;
                entry.outcome_detail = outcome_detail;
                entry.finished_at = Some(finished_at);
                // Extract structured task_id from the outcome_detail
                // "task_id=<value>" format set by success reconciliation.
                if entry.task_id.is_none() {
                    if let Some(ref detail) = entry.outcome_detail {
                        if let Some(tid) = extract_task_id_from_outcome_detail(detail) {
                            entry.task_id = Some(tid.to_owned());
                        }
                    }
                }
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
            Self::write_task_runs(path, &filtered_entries)?;
        }

        Ok(finalized_entry)
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
        run_id: &str,
        plan_hash: &str,
        started_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        let mut entries = Self::read_task_runs_from_path(&path, milestone_id)?;
        let legacy_repaired = Self::fail_legacy_runless_entries(&mut entries, started_at);
        if legacy_repaired {
            Self::write_task_runs(&path, &entries)?;
        }
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

        let finalized_attempts =
            matching_finalized_task_runs(&canonical_task_runs, bead_id, project_id, run_id);
        match finalized_attempts.as_slice() {
            [] => {}
            [entry] => {
                if let Some(reopened_entry) = Self::reopen_failed_exact_attempt(
                    &mut entries,
                    entry,
                    bead_id,
                    project_id,
                    run_id,
                    plan_hash,
                    started_at,
                )? {
                    Self::write_task_runs(&path, &entries)?;
                    return Ok(reopened_entry);
                }
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

        if let Some(existing_entry) =
            find_matching_running_task_run(&canonical_task_runs, bead_id, project_id, run_id)
        {
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
                if changed {
                    Self::write_task_runs(&path, &entries)?;
                }
                return Ok(entries[existing_index].clone());
            }

            return Ok(existing_entry);
        }

        if running_attempts_for_bead.len() > 1 {
            let run_suffix = format!(" run '{run_id}'");
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': ambiguous existing running attempts{run_suffix}"
                ),
            });
        }

        if let [prior_running_attempt] = running_attempts_for_bead.as_slice() {
            Self::fail_superseded_running_attempt(&mut entries, prior_running_attempt, started_at);
        }

        let entry = TaskRunEntry {
            milestone_id: milestone_id.to_string(),
            bead_id: bead_id.to_owned(),
            project_id: project_id.to_owned(),
            run_id: Some(run_id.to_owned()),
            plan_hash: Some(plan_hash.to_owned()),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
            task_id: None,
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
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        let mut entries = Self::read_task_runs_from_path(&path, milestone_id)?;
        Self::finalize_task_run_internal(
            &path,
            milestone_id,
            &mut entries,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
            false,
        )
    }

    fn repair_task_run_terminal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        let path = Self::task_runs_path(base_dir, milestone_id);
        let _lock = AdvisoryFileLock::acquire(&Self::task_runs_lock_path(base_dir, milestone_id))?;
        let mut entries = Self::read_task_runs_from_path(&path, milestone_id)?;
        Self::finalize_task_run_internal(
            &path,
            milestone_id,
            &mut entries,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
            true,
        )
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

    fn read_plan_shape(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String> {
        let path =
            FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_SHAPE_FILE);
        Ok(fs::read_to_string(&path)?)
    }

    fn write_plan_shape(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()> {
        let path =
            FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLAN_SHAPE_FILE);
        FileSystem::write_atomic(&path, content)
    }
}

// ── Planned-elsewhere mapping store ──────────────────────────────────────────

use crate::contexts::milestone_record::model::PlannedElsewhereMapping;
use crate::contexts::milestone_record::service::PlannedElsewhereMappingPort;

pub struct FsPlannedElsewhereMappingStore;

impl FsPlannedElsewhereMappingStore {
    fn mappings_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_root(base_dir, milestone_id).join(MILESTONE_PLANNED_ELSEWHERE_FILE)
    }

    fn mappings_lock_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
        FileSystem::milestone_lock_root(base_dir, milestone_id)
            .join(format!("{MILESTONE_PLANNED_ELSEWHERE_FILE}.lock"))
    }
}

impl PlannedElsewhereMappingPort for FsPlannedElsewhereMappingStore {
    fn read_mappings(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<PlannedElsewhereMapping>> {
        let path = Self::mappings_path(base_dir, milestone_id);
        let raw = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut mappings = Vec::new();
        for (i, line) in raw.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mapping: PlannedElsewhereMapping =
                serde_json::from_str(trimmed).map_err(|e| AppError::CorruptRecord {
                    file: format!(
                        "milestones/{}/{}",
                        milestone_id, MILESTONE_PLANNED_ELSEWHERE_FILE
                    ),
                    details: format!("line {}: {}", i + 1, e),
                })?;
            mappings.push(mapping);
        }
        Ok(mappings)
    }

    fn append_mapping(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        mapping: &PlannedElsewhereMapping,
    ) -> AppResult<()> {
        let path = Self::mappings_path(base_dir, milestone_id);
        let lock_path = Self::mappings_lock_path(base_dir, milestone_id);
        let line = serde_json::to_string(mapping)?;
        let _lock = AdvisoryFileLock::acquire(&lock_path)?;
        FileSystem::append_line(&path, &line)?;
        Ok(())
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
        if let Some(parent) = payload_final.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = artifact_final.parent() {
            fs::create_dir_all(parent)?;
        }

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
    FileSystem::audit_workspace_root_path(base_dir).join(REQUIREMENTS_DIR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::automation_runtime::{DaemonStorePort, WriterLockReleaseOutcome};
    use crate::shared::domain::ProjectId;
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn ensure_live_workspace_seeded_writes_workspace_toml_last() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().expect("tempdir");
        let audit_root = FileSystem::audit_workspace_root_path(temp.path());
        let live_root = FileSystem::live_workspace_root_path(temp.path());

        std::fs::create_dir_all(audit_root.join("projects")).expect("create audit projects dir");
        std::fs::write(audit_root.join("workspace.toml"), "version = 1\n")
            .expect("write audit workspace config");
        let outside_file = temp.path().join("outside.txt");
        std::fs::write(&outside_file, "secret").expect("write outside file");
        symlink(&outside_file, audit_root.join("linked.txt")).expect("create audit symlink");

        let error =
            FileSystem::ensure_live_workspace_seeded(temp.path()).expect_err("seeding must fail");
        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(
            !live_root.join("workspace.toml").exists(),
            "workspace.toml must not become the completion sentinel before the rest of the seed succeeds"
        );
    }

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
            None,
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

    #[test]
    fn prompt_hash_uses_stable_sha256_digest() {
        let prompt = "# Ralph Task Prompt\n\nStable prompt body.\n";
        assert_eq!(
            FileSystem::prompt_hash(prompt),
            "87b3874a6af501c8b259ef3b85c6e65ad843c00be92f6179864626a96846fd12"
        );
    }

    #[test]
    fn legacy_prompt_hash_matches_pre_sha256_digest() {
        let prompt = "# Prompt\n\nOriginal prompt.\n";
        assert_eq!(FileSystem::legacy_prompt_hash(prompt), "5b624debc08968f4");
    }

    #[test]
    fn pid_file_round_trips_for_live_project() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("pid-roundtrip".to_owned()).expect("project id");

        let written = FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Cli,
            Some("lease-1"),
            Some("run-1"),
            Some(Utc::now()),
        )
        .expect("write pid file");
        let read_back = FileSystem::read_pid_file(temp.path(), &project_id)
            .expect("read pid file")
            .expect("pid file present");

        assert_eq!(read_back.pid, written.pid);
        assert_eq!(read_back.started_at, written.started_at);
        assert_eq!(read_back.owner, RunPidOwner::Cli);
        assert_eq!(read_back.writer_owner.as_deref(), Some("lease-1"));
        assert_eq!(read_back.run_id.as_deref(), Some("run-1"));
        assert_eq!(read_back.run_started_at, written.run_started_at);
        assert_eq!(read_back.proc_start_marker, written.proc_start_marker);
        #[cfg(target_os = "linux")]
        assert!(FileSystem::is_pid_alive(&read_back));

        // On non-Linux Unix, marker-only records are now authoritative.
        #[cfg(all(unix, not(target_os = "linux")))]
        assert!(FileSystem::is_pid_alive(&read_back));

        #[cfg(not(unix))]
        assert!(!FileSystem::is_pid_alive(&read_back));

        FileSystem::remove_pid_file(temp.path(), &project_id).expect("remove pid file");
        assert!(FileSystem::read_pid_file(temp.path(), &project_id)
            .expect("read after remove")
            .is_none());
    }

    #[test]
    fn backend_process_tracking_round_trips_for_live_project() {
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("backend-process-roundtrip".to_owned()).expect("project id");
        let project_root = FileSystem::ensure_live_project_root(temp.path(), &project_id)
            .expect("ensure live project root");
        let run_started_at = Utc::now();
        FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Cli,
            Some("lease-1"),
            Some("run-1"),
            Some(run_started_at),
        )
        .expect("write pid file");

        let tracked = FileSystem::register_backend_process(&project_root, std::process::id())
            .expect("register backend process");
        let read_back = FileSystem::read_backend_processes(temp.path(), &project_id)
            .expect("read backend processes");

        assert_eq!(read_back, vec![tracked.clone()]);
        assert!(FileSystem::backend_process_matches_attempt(
            &tracked,
            "run-1",
            run_started_at
        ));
        #[cfg(target_os = "linux")]
        {
            assert!(FileSystem::backend_process_is_authoritative(&tracked));
            assert!(FileSystem::is_backend_process_alive(&tracked));
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            assert!(tracked.proc_start_ticks.is_none());
            assert!(tracked.proc_start_marker.is_some());
            assert!(FileSystem::backend_process_is_authoritative(&tracked));
            assert!(FileSystem::is_backend_process_alive(&tracked));
        }

        #[cfg(not(unix))]
        {
            assert!(!FileSystem::backend_process_is_authoritative(&tracked));
            assert!(!FileSystem::is_backend_process_alive(&tracked));
        }

        FileSystem::remove_backend_process(&project_root, tracked.pid)
            .expect("remove backend process");
        assert!(FileSystem::read_backend_processes(temp.path(), &project_id)
            .expect("read backend processes after remove")
            .is_empty());
    }

    #[test]
    fn backend_process_tracking_preserves_concurrent_registrations() {
        use std::sync::{Arc, Barrier};

        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("backend-process-concurrent".to_owned()).expect("project id");
        let project_root = FileSystem::ensure_live_project_root(temp.path(), &project_id)
            .expect("ensure live project root");
        FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Cli,
            Some("lease-1"),
            Some("run-1"),
            Some(Utc::now()),
        )
        .expect("write pid file");

        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for pid in 20_000..20_008u32 {
            let barrier = Arc::clone(&barrier);
            let project_root = project_root.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                FileSystem::register_backend_process(&project_root, pid)
                    .expect("register backend process");
            }));
        }

        barrier.wait();
        for thread in threads {
            thread.join().expect("join concurrent register");
        }

        let tracked = FileSystem::read_backend_processes(temp.path(), &project_id)
            .expect("read backend processes after concurrent register");
        let tracked_pids: std::collections::BTreeSet<_> =
            tracked.into_iter().map(|record| record.pid).collect();
        let expected_pids: std::collections::BTreeSet<_> = (20_000..20_008u32).collect();
        assert_eq!(tracked_pids, expected_pids);
    }

    #[test]
    fn pid_liveness_rejects_missing_process() {
        let record = RunPidRecord {
            pid: 999_999,
            started_at: Utc::now(),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(!FileSystem::is_pid_alive(&record));
    }

    #[cfg(unix)]
    #[test]
    fn pid_liveness_rejects_live_process_without_start_ticks() {
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now(),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(
            !FileSystem::is_pid_alive(&record),
            "pid records without proc_start_ticks must not be treated as authoritative"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_pid_liveness_accepts_same_live_process_when_recorded_after_start() {
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now(),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(FileSystem::legacy_pid_record_matches_live_process(&record));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn legacy_pid_liveness_rejects_live_process_started_after_record_timestamp() {
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now() - chrono::Duration::days(365),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(
            !FileSystem::legacy_pid_record_matches_live_process(&record),
            "legacy pid records must not treat a newer process as the original owner after pid reuse"
        );
    }

    #[cfg(unix)]
    #[test]
    fn pid_liveness_rejects_mismatched_start_ticks() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("pid-start-ticks".to_owned()).expect("project id");
        let mut written = FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Cli,
            None,
            None,
            None,
        )
        .expect("write pid file");
        let expected_ticks = written
            .proc_start_ticks
            .expect("unix pid records should capture proc_start_ticks");
        written.proc_start_ticks = Some(expected_ticks.saturating_add(1));

        assert!(
            !FileSystem::is_pid_alive(&written),
            "mismatched proc_start_ticks should reject reused or stale pid records"
        );
    }

    #[test]
    fn pid_record_with_only_start_marker_matches_platform_authority_rules() {
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now(),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: Some("fallback-start-marker".to_owned()),
        };

        #[cfg(all(unix, not(target_os = "linux")))]
        assert!(FileSystem::pid_record_is_authoritative(&record));

        #[cfg(not(all(unix, not(target_os = "linux"))))]
        assert!(!FileSystem::pid_record_is_authoritative(&record));
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn marker_only_pid_records_match_live_marker_on_non_linux_unix() {
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now(),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
        };

        assert!(FileSystem::pid_record_is_authoritative(&record));
        assert!(FileSystem::is_pid_alive(&record));
        assert!(FileSystem::pid_record_matches_live_process(&record));
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn legacy_pid_liveness_accepts_same_second_live_process_on_non_linux_unix() {
        let live_started_at =
            FileSystem::process_started_at(std::process::id()).expect("process start time");
        let record = RunPidRecord {
            pid: std::process::id(),
            started_at: live_started_at.started_at + chrono::Duration::milliseconds(900),
            owner: RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(
            !FileSystem::is_pid_alive(&record),
            "legacy pid records without identity fields must not be treated as authoritative"
        );
        assert!(FileSystem::legacy_pid_record_matches_live_process(&record));
    }

    #[test]
    fn coarse_process_start_precision_accepts_same_second_legacy_recovery() {
        let live_started_at = Utc
            .with_ymd_and_hms(2026, 4, 11, 12, 0, 0)
            .single()
            .expect("timestamp");
        let recorded_at = live_started_at + chrono::Duration::milliseconds(900);

        assert!(
            ProcessStartTime::whole_seconds(live_started_at).matches_legacy_pid_record(recorded_at)
        );
    }

    #[test]
    fn precise_process_start_precision_accepts_legacy_recorded_after_start() {
        let live_started_at = Utc
            .with_ymd_and_hms(2026, 4, 11, 12, 0, 0)
            .single()
            .expect("timestamp")
            + chrono::Duration::milliseconds(100);
        let recorded_at = live_started_at + chrono::Duration::milliseconds(800);

        assert!(ProcessStartTime::precise(live_started_at).matches_legacy_pid_record(recorded_at));
    }

    #[cfg(unix)]
    #[test]
    fn ps_command_forces_c_locale_and_utc() {
        let command = FileSystem::ps_command(42, "lstart");
        let args = command
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let envs = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(args, vec!["-o", "lstart=", "-p", "42"]);
        assert_eq!(envs.get("LC_ALL"), Some(&Some("C".to_owned())));
        assert_eq!(envs.get("LANG"), Some(&Some("C".to_owned())));
        assert_eq!(envs.get("TZ"), Some(&Some("UTC".to_owned())));
    }

    #[test]
    fn live_workspace_root_path_resolves_gitdir_marker_files() {
        let temp = tempdir().expect("tempdir");
        let actual_git_dir = temp.path().join(".git-worktree");
        std::fs::create_dir_all(&actual_git_dir).expect("create gitdir");
        std::fs::write(temp.path().join(".git"), "gitdir: .git-worktree\n")
            .expect("write gitdir marker");

        assert_eq!(
            FileSystem::live_workspace_root_path(temp.path()),
            actual_git_dir.join(LIVE_WORKSPACE_DIR)
        );
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
            render_completion_journal_details("proj-2", None, None, ts, "succeeded", None, None);
        let requested_json = render_completion_journal_details(
            "proj-2",
            Some("run-99"),
            Some("hash-xyz"),
            ts,
            "succeeded",
            Some("All checks passed"),
            None,
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
    fn repair_completion_event_removes_stale_row_even_when_exact_event_exists() {
        use crate::contexts::milestone_record::model::{
            render_completion_journal_details, MilestoneEventType, MilestoneId,
            MilestoneJournalEvent,
        };
        use chrono::{TimeZone, Utc};

        let store = FsMilestoneJournalStore;
        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let started_at = Utc.with_ymd_and_hms(2026, 4, 1, 10, 11, 0).unwrap();
        let stale_timestamp = Utc.with_ymd_and_hms(2026, 4, 1, 10, 12, 0).unwrap();
        let repaired_timestamp = Utc.with_ymd_and_hms(2026, 4, 1, 10, 15, 0).unwrap();
        let stale_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadFailed, stale_timestamp)
                .with_bead("ms-alpha.bead-2")
                .with_details(render_completion_journal_details(
                    "proj-1",
                    Some("run-1"),
                    Some("plan-1"),
                    started_at,
                    "failed",
                    Some("stale failure"),
                    None,
                ));
        let exact_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, repaired_timestamp)
                .with_bead("ms-alpha.bead-2")
                .with_details(render_completion_journal_details(
                    "proj-1",
                    Some("run-1"),
                    Some("plan-1"),
                    started_at,
                    "succeeded",
                    None,
                    None,
                ));
        let journal_path = FsMilestoneJournalStore::journal_path(temp.path(), &milestone_id);
        FsMilestoneJournalStore::write_journal(
            &journal_path,
            &[stale_event.clone(), exact_event.clone()],
        )
        .expect("write journal");

        let changed = store
            .repair_completion_event(temp.path(), &milestone_id, &exact_event)
            .expect("repair completion event");
        assert!(changed, "stale conflicting row should be removed");

        let repaired =
            FsMilestoneJournalStore::read_journal_from_path(&journal_path, &milestone_id)
                .expect("read repaired journal");
        assert_eq!(repaired.len(), 1);
        assert_eq!(repaired[0].timestamp, exact_event.timestamp);
        assert_eq!(repaired[0].event_type, exact_event.event_type);
        assert_eq!(repaired[0].bead_id, exact_event.bead_id);
        assert_eq!(repaired[0].details, exact_event.details);
    }

    #[test]
    fn append_event_if_missing_does_not_merge_mismatched_completion_event_types() {
        use crate::contexts::milestone_record::model::{
            render_completion_journal_details, MilestoneEventType, MilestoneId,
            MilestoneJournalEvent,
        };
        use chrono::{TimeZone, Utc};

        let store = FsMilestoneJournalStore;
        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("ms-mismatched-completion").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let started_at = Utc.with_ymd_and_hms(2026, 4, 1, 10, 11, 0).unwrap();
        let existing = MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, started_at)
            .with_bead("ms-alpha.bead-2")
            .with_details(render_completion_journal_details(
                "proj-1",
                Some("run-1"),
                Some("plan-1"),
                started_at,
                "succeeded",
                None,
                None,
            ));
        let requested = MilestoneJournalEvent::new(
            MilestoneEventType::BeadFailed,
            started_at + chrono::Duration::seconds(5),
        )
        .with_bead("ms-alpha.bead-2")
        .with_details(render_completion_journal_details(
            "proj-1",
            Some("run-1"),
            Some("plan-1"),
            started_at,
            "failed",
            Some("different outcome"),
            None,
        ));
        let journal_path = FsMilestoneJournalStore::journal_path(temp.path(), &milestone_id);
        FsMilestoneJournalStore::write_journal(&journal_path, &[existing.clone()])
            .expect("write journal");

        let changed = store
            .append_event_if_missing(temp.path(), &milestone_id, &requested)
            .expect("append event");
        assert!(
            changed,
            "mismatched completion types should append a new row"
        );

        let journal = FsMilestoneJournalStore::read_journal_from_path(&journal_path, &milestone_id)
            .expect("read journal");
        assert_eq!(journal, vec![existing, requested]);
    }

    #[test]
    fn milestone_plain_reads_recover_pending_state_commit() {
        use crate::contexts::milestone_record::model::{
            MilestoneId, MilestoneSnapshot, MilestoneStatus,
        };
        use chrono::{Duration, TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-read-test").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let current_snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 0, 0, 0).unwrap());
        let current_snapshot_json =
            serde_json::to_string_pretty(&current_snapshot).expect("serialize current snapshot");
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            current_snapshot_json,
        )
        .expect("write current snapshot");

        let current_event = MilestoneJournalEvent::new(
            MilestoneEventType::PlanDrafted,
            Utc.with_ymd_and_hms(2026, 4, 5, 0, 0, 0).unwrap(),
        );
        FsMilestoneJournalStore::write_journal(
            &milestone_root.join(MILESTONE_JOURNAL_FILE),
            &[current_event.clone()],
        )
        .expect("write current journal");

        let mut next_snapshot = current_snapshot.clone();
        next_snapshot.status = MilestoneStatus::Ready;
        next_snapshot.plan_hash = Some("plan-v1".to_owned());
        next_snapshot.plan_version = 1;
        next_snapshot.updated_at += Duration::seconds(5);
        let next_event = MilestoneJournalEvent::lifecycle_transition(
            next_snapshot.updated_at,
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            "system",
            "plan finalized and beads exported",
            serde_json::Map::new(),
        );
        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Publish,
            previous_snapshot: current_snapshot.clone(),
            previous_journal: vec![current_event.clone()],
            next_snapshot: next_snapshot.clone(),
            next_journal: vec![next_event.clone()],
        };
        let pending_json =
            serde_json::to_string_pretty(&pending).expect("serialize pending milestone state");
        std::fs::write(
            milestone_root.join(MILESTONE_STATE_COMMIT_FILE),
            pending_json,
        )
        .expect("write pending state commit");

        let snapshot = FsMilestoneSnapshotStore
            .read_snapshot(temp.path(), &milestone_id)
            .expect("recover and read durable snapshot");
        assert_eq!(snapshot, next_snapshot);

        let journal = FsMilestoneJournalStore
            .read_journal(temp.path(), &milestone_id)
            .expect("recover and read durable journal");
        assert_eq!(journal, vec![next_event]);
        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "plain reads should finalize the pending state commit"
        );
    }

    #[test]
    fn milestone_controller_lock_recovers_pending_state_commit() {
        use crate::contexts::milestone_record::controller::load_controller;
        use crate::contexts::milestone_record::model::{
            MilestoneId, MilestoneSnapshot, MilestoneStatus,
        };
        use chrono::{Duration, TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-controller-lock").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let current_snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 1, 0, 0).unwrap());
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&current_snapshot).expect("serialize current snapshot"),
        )
        .expect("write current snapshot");

        let current_event = MilestoneJournalEvent::new(
            MilestoneEventType::PlanDrafted,
            Utc.with_ymd_and_hms(2026, 4, 5, 1, 0, 0).unwrap(),
        );
        FsMilestoneJournalStore::write_journal(
            &milestone_root.join(MILESTONE_JOURNAL_FILE),
            &[current_event.clone()],
        )
        .expect("write current journal");

        let mut next_snapshot = current_snapshot.clone();
        next_snapshot.status = MilestoneStatus::Ready;
        next_snapshot.plan_hash = Some("plan-v1".to_owned());
        next_snapshot.plan_version = 1;
        next_snapshot.updated_at += Duration::seconds(5);
        let next_event = MilestoneJournalEvent::lifecycle_transition(
            next_snapshot.updated_at,
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            "system",
            "plan finalized and beads exported",
            serde_json::Map::new(),
        );
        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Publish,
            previous_snapshot: current_snapshot.clone(),
            previous_journal: vec![current_event],
            next_snapshot: next_snapshot.clone(),
            next_journal: vec![next_event.clone()],
        };
        std::fs::write(
            milestone_root.join(MILESTONE_STATE_COMMIT_FILE),
            serde_json::to_string_pretty(&pending).expect("serialize pending state"),
        )
        .expect("write pending state commit");

        let controller = load_controller(&FsMilestoneControllerStore, temp.path(), &milestone_id)
            .expect("controller load should succeed");
        assert!(controller.is_none(), "controller should remain absent");

        let snapshot = FsMilestoneSnapshotStore
            .read_snapshot(temp.path(), &milestone_id)
            .expect("recover and read durable snapshot");
        assert_eq!(snapshot, next_snapshot);

        let journal = FsMilestoneJournalStore
            .read_journal(temp.path(), &milestone_id)
            .expect("recover and read durable journal");
        assert_eq!(journal, vec![next_event]);
        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "controller lock should finalize the pending state commit"
        );
    }

    #[test]
    fn milestone_commit_snapshot_and_journal_ops_skips_noop_sidecar_when_nothing_changes() {
        use crate::contexts::milestone_record::model::{MilestoneId, MilestoneSnapshot};
        use chrono::{TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-noop-test").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 0, 30, 0).unwrap());
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&snapshot).expect("serialize snapshot"),
        )
        .expect("write snapshot");

        let existing_event =
            MilestoneJournalEvent::new(MilestoneEventType::PlanDrafted, snapshot.updated_at);
        let journal_path = milestone_root.join(MILESTONE_JOURNAL_FILE);
        FsMilestoneJournalStore::write_journal(
            &journal_path,
            std::slice::from_ref(&existing_event),
        )
        .expect("write journal");

        let applied = FsMilestoneJournalStore
            .commit_snapshot_and_journal_ops(
                &FsMilestoneSnapshotStore,
                temp.path(),
                &milestone_id,
                &snapshot,
                &snapshot,
                &[JournalWriteOp {
                    event: existing_event,
                    kind: JournalWriteOpKind::AppendIfMissing,
                }],
                "noop lifecycle commit",
            )
            .expect("commit noop snapshot/journal update");

        assert_eq!(applied, 0);
        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "noop commits should not create a pending state sidecar"
        );
    }

    #[test]
    fn milestone_write_lock_recovers_pending_state_commit() {
        use crate::contexts::milestone_record::model::{
            MilestoneId, MilestoneSnapshot, MilestoneStatus,
        };
        use chrono::{Duration, TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-recover-test").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let current_snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 1, 0, 0).unwrap());
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&current_snapshot).expect("serialize current snapshot"),
        )
        .expect("write current snapshot");
        FsMilestoneJournalStore::write_journal(
            &milestone_root.join(MILESTONE_JOURNAL_FILE),
            &[MilestoneJournalEvent::new(
                MilestoneEventType::PlanDrafted,
                current_snapshot.updated_at,
            )],
        )
        .expect("write current journal");

        let mut recovered_snapshot = current_snapshot.clone();
        recovered_snapshot.status = MilestoneStatus::Running;
        recovered_snapshot.plan_hash = Some("plan-v2".to_owned());
        recovered_snapshot.plan_version = 2;
        recovered_snapshot.active_bead = Some("bead-1".to_owned());
        recovered_snapshot.updated_at += Duration::seconds(5);
        let current_event = MilestoneJournalEvent::new(
            MilestoneEventType::PlanDrafted,
            current_snapshot.updated_at,
        );
        let recovered_event = MilestoneJournalEvent::lifecycle_transition(
            recovered_snapshot.updated_at,
            MilestoneStatus::Paused,
            MilestoneStatus::Running,
            "controller",
            "execution resumed",
            serde_json::Map::new(),
        );
        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Publish,
            previous_snapshot: current_snapshot.clone(),
            previous_journal: vec![current_event],
            next_snapshot: recovered_snapshot.clone(),
            next_journal: vec![recovered_event.clone()],
        };
        std::fs::write(
            milestone_root.join(MILESTONE_STATE_COMMIT_FILE),
            serde_json::to_string_pretty(&pending).expect("serialize pending state"),
        )
        .expect("write pending state");

        FsMilestoneSnapshotStore
            .with_milestone_write_lock(temp.path(), &milestone_id, || Ok(()))
            .expect("recover pending state under lock");

        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "recovery should clear the pending state sidecar"
        );

        let snapshot = FsMilestoneSnapshotStore
            .read_snapshot(temp.path(), &milestone_id)
            .expect("read recovered snapshot");
        assert_eq!(snapshot, recovered_snapshot);

        let journal = FsMilestoneJournalStore
            .read_journal(temp.path(), &milestone_id)
            .expect("read recovered journal");
        assert_eq!(journal, vec![recovered_event]);
    }

    #[test]
    fn milestone_plain_reads_recover_partially_visible_snapshot_write() {
        use crate::contexts::milestone_record::model::{
            MilestoneId, MilestoneSnapshot, MilestoneStatus,
        };
        use chrono::{Duration, TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-torn-read-test").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let current_snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 2, 0, 0).unwrap());
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&current_snapshot).expect("serialize current snapshot"),
        )
        .expect("write current snapshot");

        let current_event = MilestoneJournalEvent::new(
            MilestoneEventType::PlanDrafted,
            current_snapshot.updated_at,
        );
        FsMilestoneJournalStore::write_journal(
            &milestone_root.join(MILESTONE_JOURNAL_FILE),
            &[current_event.clone()],
        )
        .expect("write current journal");

        let mut next_snapshot = current_snapshot.clone();
        next_snapshot.status = MilestoneStatus::Ready;
        next_snapshot.plan_hash = Some("plan-v3".to_owned());
        next_snapshot.plan_version = 3;
        next_snapshot.updated_at += Duration::seconds(5);
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&next_snapshot).expect("serialize next snapshot"),
        )
        .expect("write partially visible snapshot");

        let next_event = MilestoneJournalEvent::lifecycle_transition(
            next_snapshot.updated_at,
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            "system",
            "plan finalized and beads exported",
            serde_json::Map::new(),
        );
        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Publish,
            previous_snapshot: current_snapshot.clone(),
            previous_journal: vec![current_event.clone()],
            next_snapshot,
            next_journal: vec![next_event],
        };
        let expected_snapshot = pending.next_snapshot.clone();
        let expected_journal = pending.next_journal.clone();
        std::fs::write(
            milestone_root.join(MILESTONE_STATE_COMMIT_FILE),
            serde_json::to_string_pretty(&pending).expect("serialize pending state"),
        )
        .expect("write pending state");

        let snapshot = FsMilestoneSnapshotStore
            .read_snapshot(temp.path(), &milestone_id)
            .expect("recover and read durable snapshot");
        assert_eq!(snapshot, expected_snapshot);

        let journal = FsMilestoneJournalStore
            .read_journal(temp.path(), &milestone_id)
            .expect("recover and read durable journal");
        assert_eq!(journal, expected_journal);
        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "plain reads should clear the pending state sidecar after repair"
        );
    }

    #[test]
    fn milestone_plain_reads_rollback_failed_pending_state_commit() {
        use crate::contexts::milestone_record::model::{
            MilestoneId, MilestoneSnapshot, MilestoneStatus,
        };
        use chrono::{Duration, TimeZone, Utc};

        let temp = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("pending-rollback-read-test").expect("milestone id");
        let milestone_root = FileSystem::milestone_root(temp.path(), &milestone_id);
        std::fs::create_dir_all(&milestone_root).expect("create milestone root");

        let previous_snapshot =
            MilestoneSnapshot::initial(Utc.with_ymd_and_hms(2026, 4, 5, 3, 0, 0).unwrap());
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&previous_snapshot).expect("serialize snapshot"),
        )
        .expect("write previous snapshot");
        let previous_event = MilestoneJournalEvent::new(
            MilestoneEventType::PlanDrafted,
            previous_snapshot.updated_at,
        );
        FsMilestoneJournalStore::write_journal(
            &milestone_root.join(MILESTONE_JOURNAL_FILE),
            &[previous_event.clone()],
        )
        .expect("write previous journal");

        let mut next_snapshot = previous_snapshot.clone();
        next_snapshot.status = MilestoneStatus::Ready;
        next_snapshot.plan_hash = Some("plan-v4".to_owned());
        next_snapshot.plan_version = 4;
        next_snapshot.updated_at += Duration::seconds(5);
        std::fs::write(
            milestone_root.join(MILESTONE_STATUS_FILE),
            serde_json::to_string_pretty(&next_snapshot).expect("serialize next snapshot"),
        )
        .expect("write partially visible snapshot");

        let next_event = MilestoneJournalEvent::lifecycle_transition(
            next_snapshot.updated_at,
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            "system",
            "plan finalized and beads exported",
            serde_json::Map::new(),
        );
        let pending = PendingMilestoneStateCommit {
            recovery_action: PendingMilestoneStateCommitRecoveryAction::Rollback,
            previous_snapshot: previous_snapshot.clone(),
            previous_journal: vec![previous_event.clone()],
            next_snapshot,
            next_journal: vec![next_event],
        };
        std::fs::write(
            milestone_root.join(MILESTONE_STATE_COMMIT_FILE),
            serde_json::to_string_pretty(&pending).expect("serialize rollback pending state"),
        )
        .expect("write rollback pending state");

        let snapshot = FsMilestoneSnapshotStore
            .read_snapshot(temp.path(), &milestone_id)
            .expect("rollback and read durable snapshot");
        assert_eq!(snapshot, previous_snapshot);

        let journal = FsMilestoneJournalStore
            .read_journal(temp.path(), &milestone_id)
            .expect("rollback and read durable journal");
        assert_eq!(journal, vec![previous_event]);
        assert!(
            !milestone_root.join(MILESTONE_STATE_COMMIT_FILE).exists(),
            "plain reads should clear rollback sidecars after restoring the previous state"
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
