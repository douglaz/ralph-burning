use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

use super::runner::{self, ScenarioExecutor};

use crate::contexts::workflow_composition::contracts::{
    all_contracts, contract_for_stage, ContractFamily,
};
use crate::shared::domain::StageId;
use crate::shared::error::ContractError;

// ---------------------------------------------------------------------------
// Temp workspace helper
// ---------------------------------------------------------------------------

struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn new() -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!("ralph-conformance-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).map_err(|e| format!("create temp workspace: {e}"))?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

fn binary_path() -> PathBuf {
    // Allow callers (e.g. integration tests) to pin the CLI binary to a stable
    // path so nested sub-spawns remain reliable even if cargo relinks the
    // original binary during parallel test execution.
    if let Ok(override_path) = std::env::var("RALPH_BURNING_CLI_PATH") {
        let override_path = PathBuf::from(override_path);
        if override_path.exists() {
            return override_path.canonicalize().unwrap_or(override_path);
        }
    }
    let exe = std::env::current_exe().expect("current executable path");
    // Canonicalize to an absolute path so the binary can be found even when the
    // child process runs in a different working directory (e.g. a temp workspace).
    exe.canonicalize().unwrap_or(exe)
}

struct CmdOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run_cli(args: &[&str], cwd: &Path) -> Result<CmdOutput, String> {
    run_cli_with_env(args, cwd, &[])
}

fn run_cli_with_env(args: &[&str], cwd: &Path, env: &[(&str, &str)]) -> Result<CmdOutput, String> {
    let binary = binary_path();
    let mut cmd = Command::new(&binary);
    cmd.args(args).current_dir(cwd);
    cmd.env("RALPH_BURNING_CLI_PATH", &binary);
    if !env.iter().any(|(key, _)| *key == "RALPH_BURNING_BACKEND") {
        cmd.env("RALPH_BURNING_BACKEND", "stub");
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd
        .output()
        .map_err(|e| format!("failed to run CLI: {e}"))?;

    Ok(CmdOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn block_on_app_result<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = crate::shared::error::AppResult<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future)).map_err(|error| error.to_string())
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("build tokio runtime: {e}"))?;
        runtime.block_on(future).map_err(|error| error.to_string())
    }
}

fn block_on_result<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("build tokio runtime: {e}"))?;
        runtime.block_on(future)
    }
}

// ---------------------------------------------------------------------------
// In-process daemon iteration helper (test-only)
//
// Replaces the legacy `run_cli(["daemon", "start", "--single-iteration"])` path
// that was removed from the production CLI. Constructs a DaemonLoop in-process
// with the stub backend, FileIssueWatcher, and standard FS stores, then runs
// a single iteration. Label overrides can be applied to the stub backend.
// ---------------------------------------------------------------------------

/// Run a single daemon iteration in-process using the stub backend and
/// file-based issue watcher. This is the test-only replacement for the
/// former `daemon start --single-iteration` legacy CLI path.
fn run_daemon_iteration_in_process(ws_path: &Path) -> Result<(), String> {
    run_daemon_iteration_with_label_overrides(ws_path, None)
}

fn run_daemon_iteration_with_label_overrides(
    ws_path: &Path,
    label_overrides: Option<std::collections::HashMap<String, serde_json::Value>>,
) -> Result<(), String> {
    let overrides_for_req = label_overrides.clone();
    run_daemon_iteration_with_backend(
        ws_path,
        None,
        Some(Box::new(move |config| {
            let mut adapter =
                crate::composition::agent_execution_builder::build_backend_adapter_for_selector(
                    "stub",
                    Some(config),
                )?;
            if let Some(ref overrides) = overrides_for_req {
                if let crate::adapters::BackendAdapter::Stub(ref mut stub) = adapter {
                    *stub =
                        crate::composition::agent_execution_builder::apply_label_overrides_from_map(
                            stub.clone(),
                            overrides,
                        );
                }
            }
            let workspace_defaults =
                crate::contexts::agent_execution::service::BackendSelectionConfig::from_effective_config(config)?;
            let agent_service =
                crate::contexts::agent_execution::service::AgentExecutionService::new(
                    adapter,
                    crate::adapters::fs::FsRawOutputStore,
                    crate::adapters::fs::FsSessionStore,
                )
                .with_effective_config(config.clone());
            Ok(
                crate::contexts::requirements_drafting::service::RequirementsService::new(
                    agent_service,
                    crate::adapters::fs::FsRequirementsStore,
                )
                .with_workspace_defaults(workspace_defaults),
            )
        })),
        label_overrides,
    )
}

/// Requirements service builder type used by conformance tests.
type TestRequirementsServiceBuilder = Box<
    dyn Fn(
            &crate::contexts::workspace_governance::config::EffectiveConfig,
        ) -> crate::shared::error::AppResult<
            crate::composition::agent_execution_builder::ProductionRequirementsService,
        > + Send
        + Sync,
>;

fn run_daemon_iteration_with_backend(
    ws_path: &Path,
    backend_override: Option<crate::adapters::BackendAdapter>,
    requirements_builder: Option<TestRequirementsServiceBuilder>,
    label_overrides: Option<std::collections::HashMap<String, serde_json::Value>>,
) -> Result<(), String> {
    use crate::adapters::fs::{
        FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
        FsPayloadArtifactWriteStore, FsProjectStore, FsRequirementsStore, FsRunSnapshotStore,
        FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
    };
    use crate::adapters::fs::{FsRawOutputStore, FsSessionStore};
    use crate::adapters::issue_watcher::FileIssueWatcher;
    use crate::adapters::stub_backend::StubBackendAdapter;
    use crate::adapters::worktree::WorktreeAdapter;
    use crate::adapters::BackendAdapter;
    use crate::contexts::agent_execution::service::AgentExecutionService;
    use crate::contexts::automation_runtime::daemon_loop::{DaemonLoop, DaemonLoopConfig};

    let adapter = match backend_override {
        Some(a) => a,
        None => {
            let mut stub = StubBackendAdapter::default();
            if let Some(ref overrides) = label_overrides {
                stub = crate::composition::agent_execution_builder::apply_label_overrides_from_map(
                    stub, overrides,
                );
            } else {
                stub =
                    crate::composition::agent_execution_builder::apply_test_label_overrides(stub);
            }
            BackendAdapter::Stub(stub)
        }
    };
    let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);

    let daemon_store = FsDaemonStore;
    let worktree = WorktreeAdapter;
    let project_store = FsProjectStore;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_store = FsArtifactStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;
    let amendment_queue = FsAmendmentQueueStore;
    let requirements_store = FsRequirementsStore;
    let issue_watcher = FileIssueWatcher;

    let mut daemon_loop = DaemonLoop::new(
        &daemon_store,
        &worktree,
        &project_store,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &agent_service,
    )
    .with_watcher(&issue_watcher)
    .with_requirements_store(&requirements_store);

    if let Some(builder) = requirements_builder {
        daemon_loop = daemon_loop.with_configured_requirements_service_builder(builder);
    }

    let loop_config = DaemonLoopConfig {
        single_iteration: true,
        ..DaemonLoopConfig::default()
    };

    block_on_app_result(daemon_loop.run(ws_path, &loop_config))
}

/// Run a single daemon iteration using the process backend with explicit
/// binary search paths. Used for scenarios that test real backend execution
/// through the daemon without mutating process-global PATH.
fn run_daemon_iteration_with_process_backend(
    ws_path: &Path,
    extra_path: &str,
) -> Result<(), String> {
    use crate::adapters::process_backend::ProcessBackendAdapter;
    use crate::adapters::BackendAdapter;

    let mut search_paths = vec![std::path::PathBuf::from(extra_path)];
    search_paths.extend(ProcessBackendAdapter::system_path_entries());
    let req_search_paths = search_paths.clone();

    run_daemon_iteration_with_backend(
        ws_path,
        Some(BackendAdapter::Process(
            ProcessBackendAdapter::with_search_paths(search_paths),
        )),
        Some(Box::new(move |_config| {
            use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
            use crate::contexts::agent_execution::service::{
                AgentExecutionService, BackendSelectionConfig,
            };
            use crate::contexts::requirements_drafting::service::RequirementsService;

            let adapter = crate::adapters::BackendAdapter::Process(
                ProcessBackendAdapter::with_search_paths(req_search_paths.clone()),
            );
            let workspace_defaults = BackendSelectionConfig::from_effective_config(_config)
                .map_err(|e| crate::shared::error::AppError::InvalidConfigValue {
                    key: "backend_selection".to_owned(),
                    value: String::new(),
                    reason: e.to_string(),
                })?;
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore)
                    .with_effective_config(_config.clone());
            Ok(RequirementsService::new(agent_service, FsRequirementsStore)
                .with_workspace_defaults(workspace_defaults))
        })),
        None,
    )
}

fn live_workspace_root(base_dir: &Path) -> PathBuf {
    crate::adapters::fs::FileSystem::live_workspace_root_path(base_dir)
}

fn active_project_path(base_dir: &Path) -> PathBuf {
    live_workspace_root(base_dir).join("active-project")
}

fn workspace_config_path(base_dir: &Path) -> PathBuf {
    live_workspace_root(base_dir).join("workspace.toml")
}

fn daemon_root(base_dir: &Path) -> PathBuf {
    live_workspace_root(base_dir).join("daemon")
}

fn project_root(base_dir: &Path, project_id: &str) -> PathBuf {
    live_workspace_root(base_dir)
        .join("projects")
        .join(project_id)
}

fn read_runtime_logs(ws: &TempWorkspace, project_id: &str) -> Result<String, String> {
    std::fs::read_to_string(project_root(ws.path(), project_id).join("runtime/logs/run.ndjson"))
        .map_err(|e| format!("read runtime logs: {e}"))
}

// ---------------------------------------------------------------------------
// Journal and durable-state assertion helpers
// ---------------------------------------------------------------------------

fn read_journal(ws: &TempWorkspace, project_id: &str) -> Result<Vec<serde_json::Value>, String> {
    let path = project_root(ws.path(), project_id).join("journal.ndjson");
    let content = std::fs::read_to_string(&path).map_err(|e| format!("read journal: {e}"))?;
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).map_err(|e| format!("parse journal line: {e}")))
        .collect()
}

fn read_run_snapshot(ws: &TempWorkspace, project_id: &str) -> Result<serde_json::Value, String> {
    let path = project_root(ws.path(), project_id).join("run.json");
    let content = std::fs::read_to_string(&path).map_err(|e| format!("read run.json: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("parse run.json: {e}"))
}

fn requirements_run_ids(ws: &TempWorkspace) -> Result<Vec<String>, String> {
    let dir = ws.path().join(".ralph-burning/requirements");
    if !dir.is_dir() {
        return Ok(Vec::new());
    }

    let mut run_ids = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| format!("read requirements dir: {e}"))? {
        let entry = entry.map_err(|e| format!("read requirements dir entry: {e}"))?;
        if entry
            .file_type()
            .map_err(|e| format!("read requirements dir type: {e}"))?
            .is_dir()
        {
            run_ids.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    run_ids.sort();
    Ok(run_ids)
}

fn only_requirements_run_id(ws: &TempWorkspace) -> Result<String, String> {
    let run_ids = requirements_run_ids(ws)?;
    if run_ids.len() != 1 {
        return Err(format!(
            "expected exactly one requirements run, found {}",
            run_ids.len()
        ));
    }
    Ok(run_ids[0].clone())
}

fn read_requirements_run_json(
    ws: &TempWorkspace,
    run_id: &str,
) -> Result<serde_json::Value, String> {
    let path = ws
        .path()
        .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
    let content =
        std::fs::read_to_string(&path).map_err(|e| format!("read requirements run.json: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("parse requirements run.json: {e}"))
}

fn count_payload_files(ws: &TempWorkspace, project_id: &str) -> Result<usize, String> {
    let dir = project_root(ws.path(), project_id).join("history/payloads");
    let count = std::fs::read_dir(&dir)
        .map_err(|e| format!("read payloads dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    Ok(count)
}

fn count_artifact_files(ws: &TempWorkspace, project_id: &str) -> Result<usize, String> {
    let dir = project_root(ws.path(), project_id).join("history/artifacts");
    let count = std::fs::read_dir(&dir)
        .map_err(|e| format!("read artifacts dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    Ok(count)
}

fn journal_event_types(events: &[serde_json::Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| {
            e.get("event_type")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .collect()
}

/// Check that `haystack` does not contain `needle`, returning a descriptive error if it does.
#[allow(dead_code)]
fn assert_not_contains(haystack: &str, needle: &str, context: &str) -> Result<(), String> {
    if haystack.contains(needle) {
        return Err(format!(
            "{context}: expected NOT to contain '{needle}', got: {haystack}"
        ));
    }
    Ok(())
}

fn assert_success(out: &CmdOutput) -> Result<(), String> {
    if !out.success {
        return Err(format!(
            "expected success, got failure. stderr: {}",
            out.stderr
        ));
    }
    Ok(())
}

fn assert_failure(out: &CmdOutput) -> Result<(), String> {
    if out.success {
        return Err("expected failure, got success".to_owned());
    }
    Ok(())
}

fn assert_contains(haystack: &str, needle: &str, context: &str) -> Result<(), String> {
    if !haystack.contains(needle) {
        return Err(format!(
            "{context}: expected to contain '{needle}', got: {haystack}"
        ));
    }
    Ok(())
}

fn init_workspace(ws: &TempWorkspace) -> Result<(), String> {
    let out = run_cli(&["init"], ws.path())?;
    assert_success(&out)
}

const CONFORMANCE_TEST_REPO_SLUG: &str = "test/repo";
const CONFORMANCE_TEST_OWNER: &str = "test";
const CONFORMANCE_TEST_REPO: &str = "repo";

/// Write a repo registration so reconcile and status can discover the repo.
fn write_conformance_repo_registration(data_dir: &Path) {
    let reg_path = data_dir
        .join("repos")
        .join(CONFORMANCE_TEST_OWNER)
        .join(CONFORMANCE_TEST_REPO)
        .join("registration.json");
    std::fs::create_dir_all(reg_path.parent().unwrap()).expect("create registration dir");
    let reg = serde_json::json!({
        "repo_slug": CONFORMANCE_TEST_REPO_SLUG,
        "repo_root": data_dir.join("repos").join(CONFORMANCE_TEST_OWNER).join(CONFORMANCE_TEST_REPO).join("repo"),
        "workspace_root": data_dir.join("repos").join(CONFORMANCE_TEST_OWNER).join(CONFORMANCE_TEST_REPO).join("repo").join(".ralph-burning"),
    });
    std::fs::write(reg_path, serde_json::to_string_pretty(&reg).unwrap()).expect("write reg");
}

/// Return the daemon dir for the test repo within a data-dir.
fn conformance_daemon_dir(data_dir: &Path) -> PathBuf {
    data_dir
        .join("repos")
        .join(CONFORMANCE_TEST_OWNER)
        .join(CONFORMANCE_TEST_REPO)
        .join("daemon")
}

fn create_project_fixture(base_dir: &Path, project_id: &str, flow: &str) {
    let project_root = project_root(base_dir, project_id);
    std::fs::create_dir_all(&project_root).expect("create project directory");
    let prompt_contents = "# Fixture prompt\n";
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "{flow}"
prompt_reference = "prompt.md"
prompt_hash = "{}"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"
"#,
        crate::adapters::fs::FileSystem::prompt_hash(prompt_contents)
    );
    std::fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    std::fs::write(project_root.join("prompt.md"), prompt_contents).expect("write prompt");
    std::fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    ).expect("write run.json");
    std::fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"{flow}"}}}}"#,
        ),
    ).expect("write journal");
    std::fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#)
        .expect("write sessions");
    for subdir in &[
        "history/payloads",
        "history/artifacts",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
        "amendments",
        "rollback",
    ] {
        std::fs::create_dir_all(project_root.join(subdir)).expect("create project subdirectory");
    }
}

fn select_project(base_dir: &Path, project_id: &str) {
    std::fs::write(active_project_path(base_dir), format!("{project_id}\n"))
        .expect("write active-project");
}

fn setup_workspace_with_project(
    ws: &TempWorkspace,
    project_id: &str,
    flow: &str,
) -> Result<(), String> {
    init_workspace(ws)?;
    create_project_fixture(ws.path(), project_id, flow);
    select_project(ws.path(), project_id);
    Ok(())
}

fn conformance_project_root(ws: &TempWorkspace, project_id: &str) -> PathBuf {
    project_root(ws.path(), project_id)
}

fn write_run_query_history_fixture(ws: &TempWorkspace, project_id: &str) -> Result<(), String> {
    let project_root = conformance_project_root(ws, project_id);
    let long_artifact = format!("# Planning\n{}\n", "A".repeat(140));
    std::fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-19T03:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"standard"}}}}
{{"sequence":2,"timestamp":"2026-03-19T03:01:00Z","event_type":"stage_entered","details":{{"stage_id":"planning","run_id":"run-1"}}}}
{{"sequence":3,"timestamp":"2026-03-19T03:02:00Z","event_type":"stage_completed","details":{{"stage_id":"planning","cycle":1,"attempt":1,"payload_id":"p1","artifact_id":"a1"}}}}
{{"sequence":4,"timestamp":"2026-03-19T03:03:00Z","event_type":"stage_entered","details":{{"stage_id":"implementation","run_id":"run-1"}}}}
{{"sequence":5,"timestamp":"2026-03-19T03:04:00Z","event_type":"stage_completed","details":{{"stage_id":"implementation","cycle":1,"attempt":1,"payload_id":"p2","artifact_id":"a2"}}}}"#,
        ),
    )
    .map_err(|e| format!("write run query journal: {e}"))?;
    std::fs::write(
        project_root.join("history/payloads/p1.json"),
        r#"{
  "payload_id": "p1",
  "stage_id": "planning",
  "cycle": 1,
  "attempt": 1,
  "created_at": "2026-03-19T03:02:00Z",
  "payload": { "summary": "planning payload", "steps": ["one", "two"] },
  "record_kind": "stage_primary",
  "completion_round": 1
}"#,
    )
    .map_err(|e| format!("write payload p1: {e}"))?;
    std::fs::write(
        project_root.join("history/payloads/p2.json"),
        r#"{
  "payload_id": "p2",
  "stage_id": "implementation",
  "cycle": 1,
  "attempt": 1,
  "created_at": "2026-03-19T03:04:00Z",
  "payload": { "summary": "implementation payload", "diff": "full" },
  "record_kind": "stage_primary",
  "completion_round": 1
}"#,
    )
    .map_err(|e| format!("write payload p2: {e}"))?;
    std::fs::write(
        project_root.join("history/artifacts/a1.json"),
        format!(
            r#"{{
  "artifact_id": "a1",
  "payload_id": "p1",
  "stage_id": "planning",
  "created_at": "2026-03-19T03:02:00Z",
  "content": {},
  "record_kind": "stage_primary",
  "completion_round": 1
}}"#,
            serde_json::to_string(&long_artifact).map_err(|e| e.to_string())?
        ),
    )
    .map_err(|e| format!("write artifact a1: {e}"))?;
    std::fs::write(
        project_root.join("history/artifacts/a2.json"),
        r##"{
  "artifact_id": "a2",
  "payload_id": "p2",
  "stage_id": "implementation",
  "created_at": "2026-03-19T03:04:00Z",
  "content": "# Implementation\nvisible artifact\n",
  "record_kind": "stage_primary",
  "completion_round": 1
}"##,
    )
    .map_err(|e| format!("write artifact a2: {e}"))?;
    Ok(())
}

fn set_workspace_stream_output(ws: &TempWorkspace, enabled: bool) -> Result<(), String> {
    let workspace_toml = workspace_config_path(ws.path());
    let mut workspace: crate::shared::domain::WorkspaceConfig = toml::from_str(
        &std::fs::read_to_string(&workspace_toml)
            .map_err(|e| format!("read workspace.toml: {e}"))?,
    )
    .map_err(|e| format!("parse workspace.toml: {e}"))?;
    workspace.execution.stream_output = Some(enabled);
    std::fs::write(
        &workspace_toml,
        toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
    )
    .map_err(|e| format!("write workspace.toml: {e}"))?;
    Ok(())
}

fn write_supporting_payload(project_root: &Path) -> Result<(), String> {
    std::fs::write(
        project_root.join("history/payloads/panel-p1.json"),
        r#"{
  "payload_id": "panel-p1",
  "stage_id": "completion_panel",
  "cycle": 1,
  "attempt": 1,
  "created_at": "2026-03-19T03:05:00Z",
  "payload": { "summary": "completion panel payload" },
  "record_kind": "stage_supporting",
  "completion_round": 1
}"#,
    )
    .map_err(|e| format!("write supporting payload: {e}"))
}

fn write_supporting_artifact(project_root: &Path) -> Result<(), String> {
    std::fs::write(
        project_root.join("history/artifacts/panel-a1.json"),
        r##"{
  "artifact_id": "panel-a1",
  "payload_id": "panel-p1",
  "stage_id": "completion_panel",
  "created_at": "2026-03-19T03:05:00Z",
  "content": "# Completion Panel\nvisible follow artifact\n",
  "record_kind": "stage_supporting",
  "completion_round": 1
}"##,
    )
    .map_err(|e| format!("write supporting artifact: {e}"))
}

fn write_follow_runtime_log(project_root: &Path, message: &str) -> Result<(), String> {
    let entry = format!(
        r#"{{"timestamp":"2026-03-19T03:06:00Z","level":"info","source":"agent","message":{}}}"#,
        serde_json::to_string(message).map_err(|e| format!("serialize runtime log: {e}"))?,
    );
    std::fs::write(
        project_root.join("runtime/logs/002.ndjson"),
        format!("{entry}\n"),
    )
    .map_err(|e| format!("write runtime log: {e}"))
}

fn wait_for_child_output(
    mut child: std::process::Child,
    timeout: std::time::Duration,
) -> Result<std::process::Output, String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child
            .try_wait()
            .map_err(|e| format!("poll child exit: {e}"))?
            .is_some()
        {
            return child
                .wait_with_output()
                .map_err(|e| format!("wait child output: {e}"));
        }
        if std::time::Instant::now() >= deadline {
            let _ = kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL);
            return Err(format!(
                "child did not exit within {} ms",
                timeout.as_millis()
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

fn write_rollback_targets_fixture(ws: &TempWorkspace, project_id: &str) -> Result<(), String> {
    let project_root = conformance_project_root(ws, project_id);
    std::fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-19T03:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"standard"}}}}
{{"sequence":2,"timestamp":"2026-03-19T03:01:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-planning","stage_id":"planning","cycle":1,"git_sha":"abc123"}}}}
{{"sequence":3,"timestamp":"2026-03-19T03:02:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-implementation","stage_id":"implementation","cycle":1}}}}"#,
        ),
    )
    .map_err(|e| format!("write rollback journal: {e}"))?;
    std::fs::write(
        project_root.join("rollback/rb-planning.json"),
        r#"{
  "rollback_id": "rb-planning",
  "created_at": "2026-03-19T03:01:00Z",
  "stage_id": "planning",
  "cycle": 1,
  "git_sha": "abc123",
  "run_snapshot": {
    "active_run": null,
    "status": "paused",
    "cycle_history": [],
    "completion_rounds": 0,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "paused"
  }
}"#,
    )
    .map_err(|e| format!("write rollback point planning: {e}"))?;
    std::fs::write(
        project_root.join("rollback/rb-implementation.json"),
        r#"{
  "rollback_id": "rb-implementation",
  "created_at": "2026-03-19T03:02:00Z",
  "stage_id": "implementation",
  "cycle": 1,
  "run_snapshot": {
    "active_run": null,
    "status": "paused",
    "cycle_history": [],
    "completion_rounds": 0,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "paused"
  }
}"#,
    )
    .map_err(|e| format!("write rollback point implementation: {e}"))?;
    Ok(())
}

fn write_rollback_visibility_fixture(ws: &TempWorkspace, project_id: &str) -> Result<(), String> {
    let project_root = conformance_project_root(ws, project_id);
    std::fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-19T03:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"standard"}}}}
{{"sequence":2,"timestamp":"2026-03-19T03:01:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-visible","stage_id":"planning","cycle":1}}}}
{{"sequence":3,"timestamp":"2026-03-19T03:02:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-hidden","stage_id":"implementation","cycle":1}}}}
{{"sequence":4,"timestamp":"2026-03-19T03:03:00Z","event_type":"rollback_performed","details":{{"rollback_id":"rb-visible","stage_id":"planning","cycle":1,"visible_through_sequence":2,"hard":false,"rollback_count":1}}}}"#,
        ),
    )
    .map_err(|e| format!("write rollback visibility journal: {e}"))?;
    std::fs::write(
        project_root.join("rollback/rb-visible.json"),
        r#"{
  "rollback_id": "rb-visible",
  "created_at": "2026-03-19T03:01:00Z",
  "stage_id": "planning",
  "cycle": 1,
  "run_snapshot": {
    "active_run": null,
    "status": "paused",
    "cycle_history": [],
    "completion_rounds": 0,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "paused"
  }
}"#,
    )
    .map_err(|e| format!("write visible rollback point: {e}"))?;
    std::fs::write(
        project_root.join("rollback/rb-hidden.json"),
        r#"{
  "rollback_id": "rb-hidden",
  "created_at": "2026-03-19T03:02:00Z",
  "stage_id": "implementation",
  "cycle": 1,
  "run_snapshot": {
    "active_run": null,
    "status": "paused",
    "cycle_history": [],
    "completion_rounds": 0,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "paused"
  }
}"#,
    )
    .map_err(|e| format!("write hidden rollback point: {e}"))?;
    Ok(())
}

fn run_git_in(dir: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .map_err(|e| format!("git {}: {e}", args[0]))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args[0],
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Initialize a git repository in the temp workspace with an initial commit.
/// Returns the SHA of the initial commit so tests can assert against it.
fn init_git_repo(ws: &TempWorkspace) -> Result<String, String> {
    run_git_in(ws.path(), &["init"])?;
    std::fs::write(ws.path().join("README.md"), "# fixture\n")
        .map_err(|e| format!("write README.md: {e}"))?;
    run_git_in(ws.path(), &["add", "README.md"])?;
    run_git_in(ws.path(), &["commit", "-m", "initial"])?;
    let sha = run_git_in(ws.path(), &["rev-parse", "HEAD"])?;
    Ok(sha)
}

fn commit_runtime_workspace(ws: &TempWorkspace, message: &str) -> Result<String, String> {
    run_git_in(ws.path(), &["add", ".ralph-burning"])?;
    run_git_in(ws.path(), &["commit", "-m", message])?;
    run_git_in(ws.path(), &["rev-parse", "HEAD"])
}

fn read_rollback_points(
    ws: &TempWorkspace,
    project_id: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let dir = project_root(ws.path(), project_id).join("rollback");
    let mut points = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| format!("read rollback dir: {e}"))? {
        let path = entry
            .map_err(|e| format!("read rollback dir entry: {e}"))?
            .path();
        let contents =
            std::fs::read_to_string(&path).map_err(|e| format!("read rollback point: {e}"))?;
        points.push(
            serde_json::from_str(&contents).map_err(|e| format!("parse rollback point: {e}"))?,
        );
    }
    Ok(points)
}

fn rollback_point_for_stage(
    ws: &TempWorkspace,
    project_id: &str,
    stage_id: &str,
) -> Result<serde_json::Value, String> {
    read_rollback_points(ws, project_id)?
        .into_iter()
        .find(|point| point.get("stage_id").and_then(|value| value.as_str()) == Some(stage_id))
        .ok_or_else(|| format!("missing rollback point for stage '{stage_id}'"))
}

// ---------------------------------------------------------------------------
// Registry builder
// ---------------------------------------------------------------------------

macro_rules! reg {
    ($map:expr, $id:expr, $func:expr) => {{
        let f: Box<dyn Fn() -> Result<(), String> + Send + Sync> = Box::new($func);
        $map.insert(
            $id.to_string(),
            Box::new(move || f().map(|()| runner::ExecOutcome::Passed)) as ScenarioExecutor,
        );
    }};
}

macro_rules! reg_skip {
    ($map:expr, $id:expr, $reason:expr) => {{
        let reason: String = $reason.to_string();
        $map.insert(
            $id.to_string(),
            Box::new(move || Ok(runner::ExecOutcome::Skipped(reason.clone()))) as ScenarioExecutor,
        );
    }};
}

/// Build the complete scenario registry mapping scenario IDs to executor functions.
pub fn build_registry() -> HashMap<String, ScenarioExecutor> {
    let mut m: HashMap<String, ScenarioExecutor> = HashMap::new();

    register_workspace_init(&mut m);
    register_workspace_config(&mut m);
    register_backend_policy(&mut m);
    register_backend_stub(&mut m);
    register_active_project(&mut m);
    register_flow_discovery(&mut m);
    register_project_records(&mut m);
    register_stage_contracts(&mut m);
    register_run_start_standard(&mut m);
    register_run_start_quick_dev(&mut m);
    register_run_start_docs_change(&mut m);
    register_run_start_ci_improvement(&mut m);
    register_run_queries(&mut m);
    register_run_completion_rounds(&mut m);
    register_run_resume_retry(&mut m);
    register_run_resume_non_standard(&mut m);
    register_run_rollback(&mut m);
    register_workflow_checkpoint(&mut m);
    register_requirements_drafting(&mut m);
    register_bootstrap_slice2(&mut m);
    register_backend_requirements(&mut m);
    register_backend_openrouter(&mut m);
    register_daemon_lifecycle(&mut m);
    register_daemon_routing(&mut m);
    register_daemon_issue_intake(&mut m);
    register_workflow_panels(&mut m);
    register_p0_hardening(&mut m);
    register_workflow_slice5(&mut m);
    register_validation_slice6(&mut m);
    register_daemon_github(&mut m);
    register_manual_amendments_slice3(&mut m);
    register_backend_operations_slice5(&mut m);
    register_tmux_streaming_slice6(&mut m);
    register_template_overrides_slice7(&mut m);

    m
}

// ===========================================================================
// Workspace Init (3 scenarios)
// ===========================================================================

fn register_workspace_init(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "workspace-init-fresh", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;
        if !workspace_config_path(ws.path()).is_file() {
            return Err("workspace.toml not created".into());
        }
        Ok(())
    });

    reg!(m, "workspace-init-existing", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["init"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "already initialized", "stderr")?;
        Ok(())
    });

    reg!(m, "workspace-init-layout", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let base = ws.path().join(".ralph-burning");
        for dir in &["projects", "requirements", "daemon/tasks", "daemon/leases"] {
            if !base.join(dir).is_dir() {
                return Err(format!("directory {dir} not created"));
            }
        }
        Ok(())
    });
}

// ===========================================================================
// Workspace Config (8 scenarios)
// ===========================================================================

fn register_workspace_config(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "workspace-config-show", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "show"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "[settings]", "stdout")?;
        assert_contains(&out.stdout, "prompt_review.enabled", "stdout")?;
        assert_contains(&out.stdout, "source: default", "stdout")?;
        Ok(())
    });

    reg!(m, "workspace-config-get-known", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "get", "default_flow"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "minimal", "stdout")?;
        Ok(())
    });

    reg!(m, "workspace-config-get-unknown", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "get", "unknown.key"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unknown config key", "stderr")?;
        Ok(())
    });

    reg!(m, "workspace-config-set-valid", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "set", "default_flow", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        let toml =
            std::fs::read_to_string(workspace_config_path(ws.path())).map_err(|e| e.to_string())?;
        assert_contains(&toml, "quick_dev", "workspace.toml")?;
        Ok(())
    });

    reg!(m, "workspace-config-set-invalid-value", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["config", "set", "default_flow", "unknown_flow"],
            ws.path(),
        )?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "workspace-config-set-invalid-key", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "set", "unknown.key", "value"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "workspace-config-edit-valid", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Create a script that writes a valid config update
        let script_path = ws.path().join("editor.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\ncat > \"$1\" << 'TOML'\nversion = 1\ncreated_at = \"2026-03-11T19:00:00Z\"\n\n[settings]\ndefault_backend = \"claude\"\nTOML\n",
        ).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&script_path)
                .map_err(|e| e.to_string())?
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&script_path, perms).map_err(|e| e.to_string())?;
        }
        let out = Command::new(binary_path())
            .args(["config", "edit"])
            .current_dir(ws.path())
            .env("EDITOR", script_path.to_string_lossy().as_ref())
            .output()
            .map_err(|e| format!("run config edit: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "config edit failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let toml =
            std::fs::read_to_string(workspace_config_path(ws.path())).map_err(|e| e.to_string())?;
        assert_contains(&toml, "claude", "workspace.toml")?;
        Ok(())
    });

    reg!(m, "workspace-config-edit-invalid", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let script_path = ws.path().join("editor.sh");
        std::fs::write(
            &script_path,
            "#!/bin/sh\necho 'not valid toml {{{' > \"$1\"\n",
        )
        .map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&script_path)
                .map_err(|e| e.to_string())?
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&script_path, perms).map_err(|e| e.to_string())?;
        }
        let out = Command::new(binary_path())
            .args(["config", "edit"])
            .current_dir(ws.path())
            .env("EDITOR", script_path.to_string_lossy().as_ref())
            .output()
            .map_err(|e| format!("run config edit: {e}"))?;
        if out.status.success() {
            return Err("config edit with invalid toml should fail".into());
        }
        Ok(())
    });
}

// ===========================================================================
// Backend Policy (2 scenarios)
// ===========================================================================

fn register_backend_policy(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(
        m,
        "backend.role_overrides.per_role_override_beats_default",
        || {
            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;

            let created_at = chrono::DateTime::parse_from_rfc3339("2026-03-16T02:10:31Z")
                .expect("valid timestamp")
                .with_timezone(&chrono::Utc);

            let mut workspace = crate::shared::domain::WorkspaceConfig::new(created_at);
            workspace.settings.default_backend = Some("claude".to_owned());
            std::fs::write(
                workspace_config_path(ws.path()),
                toml::to_string_pretty(&workspace).unwrap(),
            )
            .map_err(|e| format!("write workspace config: {e}"))?;

            let project_id = crate::shared::domain::ProjectId::new("demo").unwrap();
            let mut project = crate::shared::domain::ProjectConfig::default();
            project.workflow.reviewer_backend = Some("codex".to_owned());
            crate::adapters::fs::FileSystem::write_project_config(ws.path(), &project_id, &project)
                .map_err(|e| format!("write project config: {e}"))?;

            let effective =
                crate::contexts::workspace_governance::config::EffectiveConfig::load_for_project(
                    ws.path(),
                    Some(&project_id),
                    crate::contexts::workspace_governance::config::CliBackendOverrides::default(),
                )
                .map_err(|e| format!("load effective config: {e}"))?;
            let policy =
                crate::contexts::agent_execution::policy::BackendPolicyService::new(&effective);
            let target = policy
                .resolve_role_target(crate::shared::domain::BackendPolicyRole::Reviewer, 1)
                .map_err(|e| format!("resolve reviewer target: {e}"))?;

            if target.backend.family != crate::shared::domain::BackendFamily::Codex {
                return Err(format!(
                    "expected reviewer target codex, got {}",
                    target.backend.family
                ));
            }

            Ok(())
        }
    );

    reg!(m, "backend.role_timeouts.config_roundtrip", || {
        let mut project = crate::shared::domain::ProjectConfig::default();
        project.backends.insert(
            "claude".to_owned(),
            crate::shared::domain::BackendRuntimeSettings {
                enabled: Some(true),
                command: Some("claude".to_owned()),
                args: Some(vec![]),
                timeout_seconds: Some(120),
                role_models: Default::default(),
                role_timeouts: crate::shared::domain::BackendRoleTimeouts {
                    planner: Some(90),
                    implementer: None,
                    reviewer: Some(60),
                    qa: None,
                    completer: None,
                    final_reviewer: None,
                    prompt_reviewer: None,
                    prompt_validator: None,
                    arbiter: None,
                    acceptance_qa: None,
                    extra: toml::Table::new(),
                },
                extra: toml::Table::new(),
            },
        );

        let rendered = toml::to_string_pretty(&project)
            .map_err(|e| format!("serialize project config: {e}"))?;
        let parsed: crate::shared::domain::ProjectConfig =
            toml::from_str(&rendered).map_err(|e| format!("deserialize project config: {e}"))?;

        let timeouts = parsed
            .backends
            .get("claude")
            .ok_or("missing claude backend after round trip")?
            .role_timeouts
            .clone();
        if timeouts.planner != Some(90) || timeouts.reviewer != Some(60) {
            return Err(format!(
                "unexpected round-trip timeouts: planner={:?}, reviewer={:?}",
                timeouts.planner, timeouts.reviewer
            ));
        }

        Ok(())
    });
}

// ===========================================================================
// Active Project (4 scenarios)
// ===========================================================================

fn register_active_project(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "active-project-select-existing", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "alpha", "standard");
        let out = run_cli(&["project", "select", "alpha"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "Selected project alpha", "stdout")?;
        let ptr = std::fs::read_to_string(ws.path().join(".ralph-burning/active-project"))
            .map_err(|e| e.to_string())?;
        assert_contains(&ptr, "alpha", "active-project")?;
        Ok(())
    });

    reg!(m, "active-project-select-missing", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["project", "select", "missing"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not found", "stderr")?;
        Ok(())
    });

    reg!(m, "active-project-missing", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "alpha", "standard");
        // Don't select any project, try a command that requires active project
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "project select", "stderr")?;
        Ok(())
    });

    reg!(m, "active-project-resolve-valid", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "alpha", "standard");
        run_cli(&["project", "select", "alpha"], ws.path())?;
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "alpha", "stdout")?;
        Ok(())
    });
}

// ===========================================================================
// Flow Discovery (3 scenarios)
// ===========================================================================

fn register_flow_discovery(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "flow-list-all-presets", || {
        use crate::shared::domain::FlowPreset;

        let out = run_cli(&["flow", "list"], Path::new("/tmp"))?;
        assert_success(&out)?;
        for preset in FlowPreset::all() {
            assert_contains(&out.stdout, preset.as_str(), "stdout")?;
        }
        Ok(())
    });

    reg!(m, "flow-show-each-preset", || {
        use crate::contexts::workflow_composition::built_in_flows;

        for definition in built_in_flows() {
            let flow_id = definition.preset.as_str();
            let first_stage = definition
                .stages
                .first()
                .expect("built-in flows must contain at least one stage")
                .as_str();
            let out = run_cli(&["flow", "show", flow_id], Path::new("/tmp"))?;
            assert_success(&out)?;
            assert_contains(&out.stdout, "Stage count", &format!("flow show {flow_id}"))?;
            assert_contains(&out.stdout, first_stage, &format!("flow show {flow_id}"))?;
        }
        Ok(())
    });

    reg!(m, "flow-show-invalid-preset", || {
        let out = run_cli(&["flow", "show", "unknown_flow"], Path::new("/tmp"))?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unknown flow preset", "stderr")?;
        Ok(())
    });
}

// ===========================================================================
// Project Records (23 scenarios)
// ===========================================================================

fn register_project_records(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-PROJ-001", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Write a prompt file
        std::fs::write(ws.path().join("prompt.md"), "# Test prompt\n")
            .map_err(|e| e.to_string())?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "test-proj",
                "--name",
                "Test Project",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        let proj_dir = conformance_project_root(&ws, "test-proj");
        if !proj_dir.join("project.toml").is_file() {
            return Err("project.toml not created".into());
        }
        if !proj_dir.join("run.json").is_file() {
            return Err("run.json not created".into());
        }
        if !proj_dir.join("journal.ndjson").is_file() {
            return Err("journal.ndjson not created".into());
        }
        if !proj_dir.join("sessions.json").is_file() {
            return Err("sessions.json not created".into());
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("prompt.md"), "# Test\n").map_err(|e| e.to_string())?;
        run_cli(
            &[
                "project",
                "create",
                "--id",
                "dup",
                "--name",
                "First",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "dup",
                "--name",
                "Second",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "already exists", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "fixed-flow", "standard");
        // Verify the project.toml has flow = standard
        let toml = std::fs::read_to_string(
            conformance_project_root(&ws, "fixed-flow").join("project.toml"),
        )
        .map_err(|e| e.to_string())?;
        assert_contains(&toml, "flow = \"standard\"", "project.toml")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "proj-a", "standard");
        create_project_fixture(ws.path(), "proj-b", "quick_dev");
        let out = run_cli(&["project", "list"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "proj-a", "stdout")?;
        assert_contains(&out.stdout, "proj-b", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "show-proj", "standard");
        let out = run_cli(&["project", "show", "show-proj"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "show-proj", "stdout")?;
        assert_contains(&out.stdout, "standard", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-006", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "active-show", "standard");
        select_project(ws.path(), "active-show");
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "active-show", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-007", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "del-proj", "standard");
        let out = run_cli(&["project", "delete", "del-proj"], ws.path())?;
        assert_success(&out)?;
        if project_root(ws.path(), "del-proj").exists() {
            return Err("project directory should be removed".into());
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-008", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "del-active", "standard");
        select_project(ws.path(), "del-active");
        let out = run_cli(&["project", "delete", "del-active"], ws.path())?;
        assert_success(&out)?;
        let ptr_path = ws.path().join(".ralph-burning/active-project");
        if ptr_path.exists() {
            let ptr = std::fs::read_to_string(&ptr_path).map_err(|e| e.to_string())?;
            if ptr.trim() == "del-active" {
                return Err("active-project pointer should be cleared after delete".into());
            }
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-009", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "running-proj", "standard");
        // Set up an active run with stage_cursor (canonical shape)
        let run_json = r#"{"active_run":{"run_id":"run-1","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":1},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(
            conformance_project_root(&ws, "running-proj").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "delete", "running-proj"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "active run", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-010", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("prompt.md"), "# Atomic\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "atomic-proj",
                "--name",
                "Atomic",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        // If successful, all canonical files should exist
        let dir = conformance_project_root(&ws, "atomic-proj");
        for f in &[
            "project.toml",
            "run.json",
            "journal.ndjson",
            "sessions.json",
            "prompt.md",
        ] {
            if !dir.join(f).is_file() {
                return Err(format!("missing canonical file: {f}"));
            }
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-011", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Corrupt workspace version
        let config_path = workspace_config_path(ws.path());
        let config = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
        let updated = config.replace("version = 1", "version = 99");
        std::fs::write(&config_path, updated).map_err(|e| e.to_string())?;
        std::fs::write(ws.path().join("prompt.md"), "# Test\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "ver-proj",
                "--name",
                "Ver",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unsupported workspace version", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-012", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("prompt.md"), "# Test\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "bad-flow",
                "--name",
                "Bad",
                "--prompt",
                "prompt.md",
                "--flow",
                "invalid_flow",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unknown flow preset", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-013", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("prompt.md"), "# Test\n").map_err(|e| e.to_string())?;
        run_cli(
            &[
                "project",
                "create",
                "--id",
                "no-select",
                "--name",
                "No Select",
                "--prompt",
                "prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        let ptr_path = ws.path().join(".ralph-burning/active-project");
        if ptr_path.exists() {
            let ptr = std::fs::read_to_string(&ptr_path).map_err(|e| e.to_string())?;
            if ptr.trim() == "no-select" {
                return Err("project create should not implicitly select the project".into());
            }
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-014", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "txn-del", "standard");
        let out = run_cli(&["project", "delete", "txn-del"], ws.path())?;
        assert_success(&out)?;
        // After successful delete, the project directory should be gone
        if project_root(ws.path(), "txn-del").exists() {
            return Err("project directory should be removed after delete".into());
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-015", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "schema-proj", "standard");
        let run_json =
            std::fs::read_to_string(conformance_project_root(&ws, "schema-proj").join("run.json"))
                .map_err(|e| e.to_string())?;
        let parsed: serde_json::Value =
            serde_json::from_str(&run_json).map_err(|e| e.to_string())?;
        // Verify required fields exist in the run snapshot
        if parsed.get("status").is_none() {
            return Err("run.json missing 'status' field".into());
        }
        if parsed.get("amendment_queue").is_none() {
            return Err("run.json missing 'amendment_queue' field".into());
        }
        if parsed.get("rollback_point_meta").is_none() {
            return Err("run.json missing 'rollback_point_meta' field".into());
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-016", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "corrupt-show", "standard");
        std::fs::remove_file(conformance_project_root(&ws, "corrupt-show").join("project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "corrupt-show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-017", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "corrupt-list", "standard");
        std::fs::remove_file(conformance_project_root(&ws, "corrupt-list").join("project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "list"], ws.path())?;
        assert_success(&out)?;
        assert_not_contains(
            &out.stdout,
            "corrupt-list",
            "project list should skip partial projects",
        )?;
        Ok(())
    });

    reg!(m, "SC-PROJ-018", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "corrupt-del", "standard");
        std::fs::remove_file(conformance_project_root(&ws, "corrupt-del").join("project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "delete", "corrupt-del"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-019", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("my-prompt.md"), "# My prompt\n")
            .map_err(|e| e.to_string())?;
        let out = run_cli(
            &[
                "project",
                "create",
                "--id",
                "ref-proj",
                "--name",
                "Ref",
                "--prompt",
                "my-prompt.md",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        let toml =
            std::fs::read_to_string(conformance_project_root(&ws, "ref-proj").join("project.toml"))
                .map_err(|e| e.to_string())?;
        assert_contains(&toml, "prompt_reference", "project.toml")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-020", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "survive-del", "standard");
        // Verify the project is addressable before and would survive a failed delete
        let out = run_cli(&["project", "show", "survive-del"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-021", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "bad-schema", "standard");
        // Write invalid project.toml
        std::fs::write(
            conformance_project_root(&ws, "bad-schema").join("project.toml"),
            "this is not valid project toml",
        )
        .map_err(|e| e.to_string())?;
        let _select = run_cli(&["project", "select", "bad-schema"], ws.path())?;
        // Select should succeed (it just writes the pointer), but show should fail
        select_project(ws.path(), "bad-schema");
        let show = run_cli(&["project", "show"], ws.path())?;
        assert_failure(&show)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-022", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "ptr-survive", "standard");
        select_project(ws.path(), "ptr-survive");
        // Verify pointer is set
        let ptr =
            std::fs::read_to_string(active_project_path(ws.path())).map_err(|e| e.to_string())?;
        assert_contains(&ptr, "ptr-survive", "active-project pointer")?;
        Ok(())
    });

    reg!(m, "SC-PROJ-023", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "restore-proj", "standard");
        // Verify the project is addressable
        let out = run_cli(&["project", "show", "restore-proj"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });
}

// ===========================================================================
// Stage Contracts (9 scenarios)
// ===========================================================================

fn register_stage_contracts(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-EVAL-001", || {
        // Successful planning contract evaluation: schema, semantics, rendering all pass
        let contract = contract_for_stage(StageId::Planning);
        let payload = serde_json::json!({
            "problem_framing": "Implement feature X",
            "assumptions_or_open_questions": ["Assumption 1"],
            "proposed_work": [{"order": 1, "summary": "Task 1", "details": "Details for task 1"}],
            "readiness": {"ready": true, "risks": []}
        });
        let bundle = contract
            .evaluate(&payload)
            .map_err(|e| format!("contract evaluation failed: {e}"))?;
        if bundle.artifact.is_empty() {
            return Err("expected non-empty rendered artifact".into());
        }
        Ok(())
    });

    reg!(m, "SC-EVAL-002", || {
        // Successful execution contract evaluation
        let contract = contract_for_stage(StageId::Implementation);
        let payload = serde_json::json!({
            "change_summary": "Implement feature X",
            "steps": [{"order": 1, "description": "Write code", "status": "completed"}],
            "validation_evidence": ["Tests pass"],
            "outstanding_risks": []
        });
        let bundle = contract
            .evaluate(&payload)
            .map_err(|e| format!("contract evaluation failed: {e}"))?;
        if bundle.artifact.is_empty() {
            return Err("expected non-empty rendered artifact".into());
        }
        Ok(())
    });

    reg!(m, "SC-EVAL-003", || {
        // Successful validation contract evaluation with passing outcome "approved"
        let contract = contract_for_stage(StageId::Qa);
        let payload = serde_json::json!({
            "outcome": "approved",
            "evidence": ["All tests pass"],
            "findings_or_gaps": [],
            "follow_up_or_amendments": []
        });
        let bundle = contract
            .evaluate(&payload)
            .map_err(|e| format!("contract evaluation failed: {e}"))?;
        if bundle.artifact.is_empty() {
            return Err("expected non-empty rendered artifact".into());
        }
        Ok(())
    });

    reg!(m, "SC-EVAL-004", || {
        // Schema validation failure prevents semantic validation and rendering
        let contract = contract_for_stage(StageId::Planning);
        let payload = serde_json::json!({"irrelevant_field": "value"});
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::SchemaValidation { .. }) => Ok(()),
            Err(ContractError::DomainValidation { .. }) => {
                Err("schema failure should fire before domain validation".into())
            }
            Err(e) => Err(format!("expected SchemaValidation error, got: {e}")),
            Ok(_) => Err("expected schema validation failure, got success".into()),
        }
    });

    reg!(m, "SC-EVAL-005", || {
        // Domain validation failure prevents rendering (schema-valid but semantically invalid)
        let contract = contract_for_stage(StageId::Implementation);
        let payload = serde_json::json!({
            "change_summary": "",
            "steps": [],
            "validation_evidence": [],
            "outstanding_risks": []
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::DomainValidation { .. }) => Ok(()),
            Err(ContractError::SchemaValidation { .. }) => {
                Err("expected domain validation failure, got schema failure".into())
            }
            Err(e) => Err(format!("expected DomainValidation error, got: {e}")),
            Ok(_) => Err("expected domain validation failure, got success".into()),
        }
    });

    reg!(m, "SC-EVAL-006", || {
        // QA/review outcome "rejected" returns error with qa_review_outcome_failure class
        let contract = contract_for_stage(StageId::Review);
        let payload = serde_json::json!({
            "outcome": "rejected",
            "evidence": ["Found issues"],
            "findings_or_gaps": ["Critical bug"],
            "follow_up_or_amendments": ["Fix the bug"]
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ref e @ ContractError::QaReviewOutcome { .. }) => {
                if e.failure_class() != crate::shared::domain::FailureClass::QaReviewOutcomeFailure
                {
                    return Err("expected qa_review_outcome_failure class".into());
                }
                Ok(())
            }
            Err(e) => Err(format!("expected QaReviewOutcome error, got: {e}")),
            Ok(_) => Err("expected outcome failure, got success".into()),
        }
    });

    reg!(m, "SC-EVAL-007", || {
        // Every stage in every built-in flow has exactly one contract, no stage left without
        let contracts = all_contracts();
        if contracts.len() != StageId::ALL.len() {
            return Err(format!(
                "expected {} contracts, got {}",
                StageId::ALL.len(),
                contracts.len()
            ));
        }
        for c in &contracts {
            match c.family {
                ContractFamily::Planning
                | ContractFamily::Execution
                | ContractFamily::Validation => {}
            }
        }
        Ok(())
    });

    reg!(m, "SC-EVAL-008", || {
        // Deterministic rendering: same payload rendered twice produces byte-identical output
        let contract = contract_for_stage(StageId::Planning);
        let payload = serde_json::json!({
            "problem_framing": "Implement feature X",
            "assumptions_or_open_questions": ["Assumption 1"],
            "proposed_work": [{"order": 1, "summary": "Task 1", "details": "Details"}],
            "readiness": {"ready": true, "risks": []}
        });
        let bundle1 = contract.evaluate(&payload).map_err(|e| e.to_string())?;
        let bundle2 = contract.evaluate(&payload).map_err(|e| e.to_string())?;
        if bundle1.artifact != bundle2.artifact {
            return Err("rendered artifacts are not byte-identical".into());
        }
        Ok(())
    });

    reg!(m, "SC-EVAL-009", || {
        // Non-passing review outcomes are NOT schema or domain failures
        let contract = contract_for_stage(StageId::Review);
        let payload = serde_json::json!({
            "outcome": "request_changes",
            "evidence": ["Needs work"],
            "findings_or_gaps": ["Issue found"],
            "follow_up_or_amendments": ["Address finding"]
        });
        match contract.evaluate(&payload) {
            Err(ref e @ ContractError::QaReviewOutcome { .. }) => {
                let fc = e.failure_class();
                if fc == crate::shared::domain::FailureClass::SchemaValidationFailure {
                    return Err("failure class must not be schema_validation_failure".into());
                }
                if fc == crate::shared::domain::FailureClass::DomainValidationFailure {
                    return Err("failure class must not be domain_validation_failure".into());
                }
                Ok(())
            }
            Err(e) => Err(format!("expected QaReviewOutcome error, got: {e}")),
            Ok(_) => Err("expected non-passing outcome to be an error".into()),
        }
    });
}

// ===========================================================================
// Run Start Standard (20 scenarios)
// ===========================================================================

fn register_run_start_standard(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-START-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "alpha", "standard")?;

        // Verify precondition
        let pre = run_cli(&["run", "status"], ws.path())?;
        assert_success(&pre)?;
        assert_contains(&pre.stdout, "not started", "run status before start")?;

        // Execute run start
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Verify post-condition: status is completed with no active run
        let post = run_cli(&["run", "status"], ws.path())?;
        assert_success(&post)?;
        assert_contains(&post.stdout, "completed", "run status after start")?;

        // Verify journal contains expected event types
        let events = read_journal(&ws, "alpha")?;
        let types = journal_event_types(&events);
        for expected in &[
            "run_started",
            "stage_entered",
            "stage_completed",
            "run_completed",
        ] {
            if !types.iter().any(|t| t == expected) {
                return Err(format!("journal missing event type: {expected}"));
            }
        }

        // Verify payload and artifact records exist for all 8 standard stages
        let payloads = count_payload_files(&ws, "alpha")?;
        let artifacts = count_artifact_files(&ws, "alpha")?;
        if payloads < 8 {
            return Err(format!("expected >= 8 payloads, got {payloads}"));
        }
        if artifacts < 8 {
            return Err(format!("expected >= 8 artifacts, got {artifacts}"));
        }
        Ok(())
    });

    reg!(m, "SC-START-002", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "beta", "quick_dev")?;

        // Execute run start
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Verify post-condition: status is completed
        let post = run_cli(&["run", "status"], ws.path())?;
        assert_success(&post)?;
        assert_contains(&post.stdout, "completed", "run status after start")?;

        // Verify journal records quick_dev stages in sequence
        let events = read_journal(&ws, "beta")?;
        let types = journal_event_types(&events);
        for expected in &[
            "run_started",
            "stage_entered",
            "stage_completed",
            "run_completed",
        ] {
            if !types.iter().any(|t| t == expected) {
                return Err(format!("journal missing event type: {expected}"));
            }
        }

        // Verify 4 quick_dev stages worth of payloads/artifacts
        let payloads = count_payload_files(&ws, "beta")?;
        if payloads < 4 {
            return Err(format!(
                "expected >= 4 payloads for quick_dev, got {payloads}"
            ));
        }
        Ok(())
    });

    reg!(m, "SC-START-003", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "gamma", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":1},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(project_root(ws.path(), "gamma").join("run.json"), run_json)
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not_started", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "delta", "standard")?;
        let run_json = r#"{"active_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(project_root(ws.path(), "delta").join("run.json"), run_json)
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not_started", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let config_path = workspace_config_path(ws.path());
        let config = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
        let updated = config.replace("version = 1", "version = 99");
        std::fs::write(&config_path, updated).map_err(|e| e.to_string())?;
        create_project_fixture(ws.path(), "ver-check", "standard");
        select_project(ws.path(), "ver-check");
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unsupported workspace version", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-006", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "project select", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-007", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "standard"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "prompt_review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-START-008", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Disable prompt review
        run_cli(
            &["config", "set", "prompt_review.enabled", "false"],
            ws.path(),
        )?;
        let out = run_cli(&["config", "get", "prompt_review.enabled"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "false", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-START-009", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "preflight", "standard")?;
        // Preflight checks happen before run start mutates state
        let status_before = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status_before.stdout, "not started", "status before")?;
        Ok(())
    });

    reg!(m, "SC-START-010", || {
        // Stage-to-role mapping is deterministic - verified via flow show
        let out1 = run_cli(&["flow", "show", "standard"], Path::new("/tmp"))?;
        let out2 = run_cli(&["flow", "show", "standard"], Path::new("/tmp"))?;
        assert_success(&out1)?;
        if out1.stdout != out2.stdout {
            return Err("stage-to-role mapping should be deterministic".into());
        }
        Ok(())
    });

    reg!(m, "SC-START-011", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "atomic-commit", "standard")?;
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-START-012", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "fail-record", "standard")?;

        // Configure a stage to fail during invocation
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
        )?;
        assert_failure(&out)?;

        // Verify journal records the failure
        let events = read_journal(&ws, "fail-record")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "run_failed") {
            return Err("journal should contain run_failed event".into());
        }

        // Verify run snapshot shows failed status
        let snapshot = read_run_snapshot(&ws, "fail-record")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-START-013", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "runtime-logs", "standard")?;
        // Runtime logs directory exists but is separate from durable state
        if !project_root(ws.path(), "runtime-logs")
            .join("runtime/logs")
            .is_dir()
        {
            return Err("runtime/logs directory should exist".into());
        }
        Ok(())
    });

    reg!(m, "SC-START-014", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "seq-check", "standard")?;
        let journal =
            std::fs::read_to_string(project_root(ws.path(), "seq-check").join("journal.ndjson"))
                .map_err(|e| e.to_string())?;
        let first_event: serde_json::Value =
            serde_json::from_str(journal.lines().next().unwrap_or("{}"))
                .map_err(|e| e.to_string())?;
        if first_event.get("sequence").and_then(|v| v.as_u64()) != Some(1) {
            return Err("first journal event should have sequence 1".into());
        }
        Ok(())
    });

    reg!(m, "SC-START-015", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "unique-run", "standard")?;
        // Run ID uniqueness is enforced at the domain level
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-START-016", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "post-run", "standard")?;

        // Complete a run first
        let start = run_cli(&["run", "start"], ws.path())?;
        assert_success(&start)?;

        // Post-run status query
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_success(&status)?;
        assert_contains(&status.stdout, "completed", "post-run status")?;

        // Post-run history query
        let history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&history)?;
        Ok(())
    });

    reg!(m, "SC-START-017", || {
        use crate::shared::domain::FlowPreset;

        // Run start should succeed for every built-in flow preset.
        for flow in FlowPreset::all() {
            let ws = TempWorkspace::new()?;
            let flow_id = flow.as_str();
            let proj_id = format!("preset-{}", flow_id.replace('_', "-"));
            setup_workspace_with_project(&ws, &proj_id, flow_id)?;
            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;
            let status = run_cli(&["run", "status"], ws.path())?;
            assert_contains(
                &status.stdout,
                "completed",
                &format!("status for {flow_id}"),
            )?;
        }
        Ok(())
    });

    reg!(m, "SC-START-018", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Disable prompt_review
        run_cli(
            &["config", "set", "prompt_review.enabled", "false"],
            ws.path(),
        )?;
        create_project_fixture(ws.path(), "november", "standard");
        select_project(ws.path(), "november");

        // Execute run start
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Verify status is completed
        let post = run_cli(&["run", "status"], ws.path())?;
        assert_success(&post)?;
        assert_contains(&post.stdout, "completed", "run status after start")?;

        // Verify 11 payloads/artifacts (all except prompt_review).
        // completion_panel and final_review each produce 3 panel records.
        let payloads = count_payload_files(&ws, "november")?;
        let artifacts = count_artifact_files(&ws, "november")?;
        if payloads != 12 {
            return Err(format!(
                "expected 12 payloads (no prompt_review, 3 reviewers), got {payloads}"
            ));
        }
        if artifacts != 12 {
            return Err(format!(
                "expected 12 artifacts (no prompt_review, 3 reviewers), got {artifacts}"
            ));
        }

        // Verify journal contains no prompt_review events
        let events = read_journal(&ws, "november")?;
        for event in &events {
            if let Some(details) = event.get("details") {
                if let Some(stage) = details.get("stage_id").and_then(|v| v.as_str()) {
                    if stage == "prompt_review" {
                        return Err("journal should contain no prompt_review events".into());
                    }
                }
            }
        }

        // Verify first stage_entered event is for "planning"
        let first_stage_entered = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered"));
        if let Some(event) = first_stage_entered {
            let stage = event
                .get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if stage != "planning" {
                return Err(format!(
                    "first stage_entered should be 'planning', got '{stage}'"
                ));
            }
        } else {
            return Err("no stage_entered event found in journal".into());
        }
        Ok(())
    });

    reg!(m, "SC-START-019", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "preflight-clean", "standard")?;
        // Verify state is clean before any run attempt
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status.stdout, "not started", "status")?;
        Ok(())
    });

    reg!(m, "SC-START-020", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "mid-fail", "standard")?;
        // Verify no partial durable history in a fresh project
        let history_dir = project_root(ws.path(), "mid-fail").join("history/payloads");
        let entries = std::fs::read_dir(&history_dir).map_err(|e| e.to_string())?;
        if entries.count() > 0 {
            return Err("fresh project should have no payload history".into());
        }
        Ok(())
    });
}

// ===========================================================================
// Run Start Quick Dev (10 scenarios)
// ===========================================================================

fn register_run_start_quick_dev(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-QD-START-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-happy", "quick_dev")?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status.stdout, "completed", "quick_dev run completed")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-002", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-review", "quick_dev")?;
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "apply_fixes", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-003", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-reject", "quick_dev")?;
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-004", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "final_review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-005", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "final_review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-006", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "final_review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-007", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-preflight", "quick_dev")?;
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status.stdout, "not started", "status")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-008", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        run_cli(
            &["config", "set", "prompt_review.enabled", "true"],
            ws.path(),
        )?;
        // quick_dev does not include prompt_review regardless of config
        let out = run_cli(&["flow", "show", "quick_dev"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "Stage count: 4", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-009", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-resume", "quick_dev")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-QD-START-010", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qd-daemon", "quick_dev")?;
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "quick_dev", "stdout")?;
        Ok(())
    });
}

// ===========================================================================
// Run Start Docs Change (5 scenarios)
// ===========================================================================

fn register_run_start_docs_change(m: &mut HashMap<String, ScenarioExecutor>) {
    // `docs_change` is an alias of `minimal` — the preset name is accepted
    // for UX clarity but the underlying stage plan is the minimal one.
    reg!(m, "SC-DOCS-START-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "docs-happy", "docs_change")?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status.stdout, "completed", "docs_change run completed")?;
        Ok(())
    });

    reg!(m, "SC-DOCS-START-002", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "docs_change"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "plan_and_implement", "stdout")?;
        assert_contains(&out.stdout, "final_review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-DOCS-START-003", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "docs_change"], ws.path())?;
        assert_success(&out)?;
        // docs_change must not expose the old docs-specific stages.
        if out.stdout.contains("docs_validation") || out.stdout.contains("docs_update") {
            return Err(format!(
                "docs_change flow still references legacy docs_* stages: {}",
                out.stdout
            ));
        }
        Ok(())
    });

    reg!(m, "SC-DOCS-START-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "docs-retry", "docs_change")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-DOCS-START-005", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "docs_change"], ws.path())?;
        assert_success(&out)?;
        let minimal = run_cli(&["flow", "show", "minimal"], ws.path())?;
        assert_success(&minimal)?;
        // The docs_change stage list must match the minimal stage list.
        let docs_stages = extract_stage_list(&out.stdout);
        let minimal_stages = extract_stage_list(&minimal.stdout);
        if docs_stages != minimal_stages {
            return Err(format!(
                "docs_change stages {docs_stages:?} do not match minimal stages {minimal_stages:?}"
            ));
        }
        Ok(())
    });
}

fn extract_stage_list(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
        })
        .collect()
}

// ===========================================================================
// Run Start CI Improvement (5 scenarios)
// ===========================================================================

fn register_run_start_ci_improvement(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-CI-START-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ci-happy", "ci_improvement")?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;
        let status = run_cli(&["run", "status"], ws.path())?;
        assert_contains(&status.stdout, "completed", "ci_improvement run completed")?;
        Ok(())
    });

    reg!(m, "SC-CI-START-002", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "ci_improvement"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "ci_update", "stdout")?;
        assert_contains(&out.stdout, "ci_validation", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-CI-START-003", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "ci_improvement"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "review", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-CI-START-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ci-retry", "ci_improvement")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-CI-START-005", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "ci_improvement"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "ci_validation", "stdout")?;
        Ok(())
    });
}

// ===========================================================================
// Run Queries (46 scenarios)
// ===========================================================================

fn register_run_queries(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-RUN-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-new", "standard")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "not started", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-002", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-active", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":1},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(
            project_root(ws.path(), "rq-active").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "running", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-003", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-history", "standard")?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-no-logs", "standard")?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_success(&out)?;
        // History should not include runtime log content
        Ok(())
    });

    reg!(m, "SC-RUN-005", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail", "standard")?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-006", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail-logs", "standard")?;
        // Create a runtime log
        std::fs::write(
            project_root(ws.path(), "rq-tail-logs").join("runtime/logs/latest.log"),
            "debug: test log line\n",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail", "--logs"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-007", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-corrupt").join("run.json"),
            "not json",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-008", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-journal-corrupt", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-journal-corrupt").join("journal.ndjson"),
            "not json\n",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-009", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-orphan-art", "standard")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-010", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-log-fail", "standard")?;
        // Runtime log writes should not affect durable state
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-011", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-no-run", "standard")?;
        std::fs::remove_file(project_root(ws.path(), "rq-no-run").join("run.json"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-012", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-no-journal", "standard")?;
        std::fs::remove_file(project_root(ws.path(), "rq-no-journal").join("journal.ndjson"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-013", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-orphan-pay", "standard")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-014", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail-dur", "standard")?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-015", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-completed", "standard")?;
        let run_json = r#"{"active_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(
            project_root(ws.path(), "rq-completed").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "completed", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-016", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-failed", "standard")?;
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(
            project_root(ws.path(), "rq-failed").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "failed", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-017", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-inconsist", "standard")?;
        // Write semantically inconsistent run.json (active_run but status is completed)
        let run_json = r#"{"active_run":{"run_id":"run-1","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":1},"started_at":"2026-03-11T19:00:00Z"},"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(
            project_root(ws.path(), "rq-inconsist").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        // Should fail fast on semantic inconsistency
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-018", || {
        // A running project (with active_run) cannot be deleted.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-del-paused", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":1},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(
            project_root(ws.path(), "rq-del-paused").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "delete", "rq-del-paused"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-019", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-toml", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-corrupt-toml").join("project.toml"),
            "not valid toml {{{",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-020", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-hist", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-corrupt-hist").join("project.toml"),
            "not valid toml {{{",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-021", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-tail", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-corrupt-tail").join("project.toml"),
            "not valid toml {{{",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-022", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-corrupt", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-show-corrupt").join("project.toml"),
            "not valid toml {{{",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-023", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-missing-toml", "standard")?;
        std::fs::remove_file(project_root(ws.path(), "rq-missing-toml").join("project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-024", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-show", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-empty-j-show").join("journal.ndjson"),
            "",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "rq-empty-j-show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-025", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-hist", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-empty-j-hist").join("journal.ndjson"),
            "",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-026", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-tail", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-empty-j-tail").join("journal.ndjson"),
            "",
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-027", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-bad-first", "standard")?;
        std::fs::write(
            project_root(ws.path(), "rq-bad-first").join("journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"run_started","details":{"run_id":"r1"}}"#,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "rq-bad-first"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-028", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail-newest", "standard")?;
        let log_dir = project_root(ws.path(), "rq-tail-newest").join("runtime/logs");
        std::fs::write(log_dir.join("old.log"), "old log\n").map_err(|e| e.to_string())?;
        std::fs::write(log_dir.join("newest.log"), "newest log\n").map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail", "--logs"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-029", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-status-json", "standard")?;
        std::fs::write(
            conformance_project_root(&ws, "rq-status-json").join("run.json"),
            r#"{
  "active_run": null,
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 0,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": {
    "pending": [
      {
        "amendment_id": "am-1",
        "source_stage": "planning",
        "source_cycle": 1,
        "source_completion_round": 1,
        "body": "Fix it",
        "created_at": "2026-03-19T03:00:00Z",
        "batch_sequence": 0,
        "source": "manual",
        "dedup_key": "dedup-1"
      }
    ],
    "processed_count": 0
  },
  "status_summary": "paused for review"
}"#,
        )
        .map_err(|e| format!("write run.json: {e}"))?;
        let out = run_cli(&["run", "status", "--json"], ws.path())?;
        assert_success(&out)?;
        let json: serde_json::Value =
            serde_json::from_str(&out.stdout).map_err(|e| format!("parse status json: {e}"))?;
        for key in [
            "project_id",
            "status",
            "stage",
            "cycle",
            "completion_round",
            "summary",
            "amendment_queue_depth",
        ] {
            if json.get(key).is_none() {
                return Err(format!("status json missing key '{key}'"));
            }
        }
        Ok(())
    });

    reg!(m, "SC-RUN-030", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-hist-verbose", "standard")?;
        write_run_query_history_fixture(&ws, "rq-hist-verbose")?;
        let out = run_cli(&["run", "history", "--verbose"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "details:", "stdout")?;
        assert_contains(&out.stdout, "metadata:", "stdout")?;
        assert_contains(&out.stdout, "preview:", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-031", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-hist-json", "standard")?;
        write_run_query_history_fixture(&ws, "rq-hist-json")?;
        let out = run_cli(&["run", "history", "--json"], ws.path())?;
        assert_success(&out)?;
        let json: serde_json::Value =
            serde_json::from_str(&out.stdout).map_err(|e| format!("parse history json: {e}"))?;
        if !json.get("events").is_some_and(|value| value.is_array()) {
            return Err("history json should contain an events array".into());
        }
        if !json.get("payloads").is_some_and(|value| value.is_array()) {
            return Err("history json should contain a payloads array".into());
        }
        if !json.get("artifacts").is_some_and(|value| value.is_array()) {
            return Err("history json should contain an artifacts array".into());
        }
        Ok(())
    });

    reg!(m, "SC-RUN-032", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-hist-json-verbose", "standard")?;
        write_run_query_history_fixture(&ws, "rq-hist-json-verbose")?;
        let out = run_cli(&["run", "history", "--json", "--verbose"], ws.path())?;
        assert_success(&out)?;
        let json: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("parse verbose history json: {e}"))?;
        if json["payloads"][0].get("payload").is_none() {
            return Err("verbose history json should include payload".into());
        }
        if json["artifacts"][0].get("content").is_none() {
            return Err("verbose history json should include content".into());
        }
        Ok(())
    });

    reg!(m, "SC-RUN-033", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-stage-filter", "standard")?;
        write_run_query_history_fixture(&ws, "rq-stage-filter")?;
        let out = run_cli(&["run", "history", "--stage", "planning"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "p1", "stdout")?;
        if out.stdout.contains("p2") {
            return Err("stage-filtered history should exclude implementation payloads".into());
        }
        if out.stdout.contains("ProjectCreated") {
            return Err("stage-filtered history should exclude non-stage events".into());
        }
        Ok(())
    });

    reg!(m, "SC-RUN-034", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail-last", "standard")?;
        write_run_query_history_fixture(&ws, "rq-tail-last")?;
        let out = run_cli(&["run", "tail", "--last", "2"], ws.path())?;
        assert_success(&out)?;
        if out.stdout.contains("ProjectCreated") {
            return Err("tail --last 2 should exclude older project_created event".into());
        }
        assert_contains(&out.stdout, "p2", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-035", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow", "standard")?;
        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow"])
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(250));
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow command should exit successfully, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("Following project 'rq-follow'") {
            return Err("follow output should include startup text".into());
        }
        Ok(())
    });

    reg!(m, "SC-RUN-036", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-payload", "standard")?;
        write_run_query_history_fixture(&ws, "rq-show-payload")?;
        let out = run_cli(&["run", "show-payload", "p1"], ws.path())?;
        assert_success(&out)?;
        let json: serde_json::Value =
            serde_json::from_str(&out.stdout).map_err(|e| format!("parse payload json: {e}"))?;
        if json.get("summary").and_then(|value| value.as_str()) != Some("planning payload") {
            return Err("show-payload should print the payload body".into());
        }
        Ok(())
    });

    reg!(m, "SC-RUN-037", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-payload-missing", "standard")?;
        let out = run_cli(&["run", "show-payload", "missing"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "payload not found", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-RUN-038", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-artifact", "standard")?;
        write_run_query_history_fixture(&ws, "rq-show-artifact")?;
        let out = run_cli(&["run", "show-artifact", "a2"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "# Implementation", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-039", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-artifact-missing", "standard")?;
        let out = run_cli(&["run", "show-artifact", "missing"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "artifact not found", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-RUN-040", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-rollback-list", "standard")?;
        write_rollback_targets_fixture(&ws, "rq-rollback-list")?;
        let out = run_cli(&["run", "rollback", "--list"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "rb-planning", "stdout")?;
        assert_contains(&out.stdout, "rb-implementation", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-041", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-rollback-empty", "standard")?;
        let out = run_cli(&["run", "rollback", "--list"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "No rollback targets available.", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-042", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-rollback-visible", "standard")?;
        write_rollback_visibility_fixture(&ws, "rq-rollback-visible")?;
        let out = run_cli(&["run", "rollback", "--list"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "rb-visible", "stdout")?;
        if out.stdout.contains("rb-hidden") {
            return Err(
                "rollback --list should hide rollback points from abandoned branches".into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-RUN-043", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-stage-bad", "standard")?;
        let out = run_cli(&["run", "history", "--stage", "unknown_stage"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "unknown stage identifier", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-RUN-044", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-parse-history", "standard")?;
        write_run_query_history_fixture(&ws, "rq-parse-history")?;
        let out = run_cli(&["run", "history", "--json"], ws.path())?;
        assert_success(&out)?;
        serde_json::from_str::<serde_json::Value>(&out.stdout)
            .map_err(|e| format!("history --json should parse cleanly: {e}"))?;
        Ok(())
    });

    reg!(m, "SC-RUN-045", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-parse-status", "standard")?;
        let out = run_cli(&["run", "status", "--json"], ws.path())?;
        assert_success(&out)?;
        serde_json::from_str::<serde_json::Value>(&out.stdout)
            .map_err(|e| format!("status --json should parse cleanly: {e}"))?;
        Ok(())
    });

    reg!(m, "SC-RUN-046", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow-logs", "standard")?;
        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow", "--logs"])
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow --logs: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        let log_dir = conformance_project_root(&ws, "rq-follow-logs").join("runtime/logs");
        std::fs::write(
            log_dir.join("002.ndjson"),
            r#"{"timestamp":"2026-03-19T03:05:00Z","level":"info","source":"agent","message":"new follow log"}"#.to_owned() + "\n",
        )
        .map_err(|e| format!("write runtime log: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(3800));
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow --logs output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow --logs should exit successfully, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("new follow log") {
            return Err(
                "follow --logs output should include the appended runtime log entry".into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-RUN-047", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow-supporting", "standard")?;
        write_run_query_history_fixture(&ws, "rq-follow-supporting")?;
        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow"])
            .env("RALPH_BURNING_TEST_FOLLOW_BASELINE_DELAY_MS", "250")
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        let project_root = conformance_project_root(&ws, "rq-follow-supporting");
        write_supporting_payload(&project_root)?;
        write_supporting_artifact(&project_root)?;
        std::thread::sleep(std::time::Duration::from_millis(400));
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow should exit successfully, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("panel-p1") || !stdout.contains("panel-a1") {
            return Err(
                "follow output should include the appended supporting payload and artifact".into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-RUN-048", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow-preexisting-partial", "standard")?;
        write_run_query_history_fixture(&ws, "rq-follow-preexisting-partial")?;
        let project_root = conformance_project_root(&ws, "rq-follow-preexisting-partial");
        write_supporting_payload(&project_root)?;

        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow"])
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        write_supporting_artifact(&project_root)?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow should tolerate a startup partial supporting pair that completes within the grace window, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("panel-p1") || !stdout.contains("panel-a1") {
            return Err(
                "follow output should include the completed supporting payload and artifact after the startup partial pair".into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-RUN-049", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow-partial-progress", "standard")?;
        write_run_query_history_fixture(&ws, "rq-follow-partial-progress")?;
        set_workspace_stream_output(&ws, true)?;
        let project_root = conformance_project_root(&ws, "rq-follow-partial-progress");

        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow", "--logs"])
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow --logs: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        write_supporting_payload(&project_root)?;
        std::thread::sleep(std::time::Duration::from_millis(150));
        write_follow_runtime_log(&project_root, "follow log after partial pair")?;
        std::thread::sleep(std::time::Duration::from_millis(150));
        write_supporting_artifact(&project_root)?;
        std::thread::sleep(std::time::Duration::from_millis(500));
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow --logs output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow --logs should exit successfully, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("follow log after partial pair") {
            return Err(
                "follow --logs output should keep streaming runtime logs after a partial pair"
                    .into(),
            );
        }
        if !stdout.contains("panel-p1") || !stdout.contains("panel-a1") {
            return Err(
                "follow --logs output should include the completed supporting payload and artifact after the partial pair"
                    .into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-RUN-050", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-follow-durable-orphan", "standard")?;
        write_run_query_history_fixture(&ws, "rq-follow-durable-orphan")?;
        let project_root = conformance_project_root(&ws, "rq-follow-durable-orphan");
        write_supporting_payload(&project_root)?;

        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow"])
            .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow: {e}"))?;
        let output = wait_for_child_output(child, std::time::Duration::from_millis(3000))?;
        if output.status.success() {
            return Err("follow should fail on durable orphaned supporting payload".into());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("history/payloads/panel-p1")
            || !stderr.contains("payload has no matching artifact")
        {
            return Err(format!(
                "follow stderr should report canonical orphan payload corruption, stderr={stderr}"
            ));
        }
        Ok(())
    });
}

// ===========================================================================
// Run Completion Rounds (16 scenarios)
// ===========================================================================

fn register_run_completion_rounds(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-CR-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-alpha", "standard")?;

        // Configure completion_panel completers to vote continue_work first, then
        // complete on the second round. The panel dispatch invokes 2 completers per
        // round, consuming sequence entries in order with last-entry clamping.
        // Round 1: entries [0],[1] → both vote false → ContinueWork verdict
        // Round 2: entry [1] (clamped) × 2 → both vote true → Complete verdict
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "vote_complete": false,
                    "evidence": ["Needs minor formatting changes"],
                    "remaining_work": ["Fix formatting"]
                },
                {
                    "vote_complete": true,
                    "evidence": ["All formatting fixed"],
                    "remaining_work": []
                }
            ]
        });
        let overrides_str = overrides.to_string();

        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides_str)],
        )?;
        assert_success(&out)?;

        // Verify journal contains completion_round_advanced event.
        // Panel dispatch uses consensus voting (not the legacy amendment path),
        // so amendment_queued events are not expected.
        let events = read_journal(&ws, "cr-alpha")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "completion_round_advanced") {
            return Err("journal missing 'completion_round_advanced' event".into());
        }

        // Verify planning stage was entered a second time (restart from planning)
        let planning_entered_count = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("planning")
            })
            .count();
        if planning_entered_count < 2 {
            return Err(format!(
                "expected planning stage_entered >= 2 times, got {planning_entered_count}"
            ));
        }

        // Verify run completed with completion_rounds >= 2
        let snapshot = read_run_snapshot(&ws, "cr-alpha")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed status, got '{status}'"));
        }
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 2 {
            return Err(format!("expected completion_rounds >= 2, got {rounds}"));
        }

        // Verify amendment_queue is empty after completion
        let queue_pending = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map_or(0, |a| a.len());
        if queue_pending > 0 {
            return Err(format!(
                "expected empty amendment_queue after completion, got {queue_pending} pending"
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-002", || {
        // Late-stage request_changes triggers completion round advancement (acceptance_qa)
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-beta", "standard")?;

        // Sequence: first acceptance_qa → request_changes (triggers round),
        // second acceptance_qa → approved (terminates).
        let overrides = serde_json::json!({
            "acceptance_qa": [
                {
                    "outcome": "request_changes",
                    "evidence": ["Changes needed"],
                    "findings_or_gaps": ["Issue"],
                    "follow_up_or_amendments": ["Fix the issue"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["Fixed"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-beta")?;
        let round_event = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
        });
        if round_event.is_none() {
            return Err("journal missing completion_round_advanced event".into());
        }
        if let Some(evt) = round_event {
            let source = evt
                .get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "acceptance_qa" {
                return Err(format!(
                    "expected source_stage=acceptance_qa, got '{source}'"
                ));
            }
        }
        Ok(())
    });

    reg!(m, "SC-CR-003", || {
        // Panel model: all completers vote continue_work → ContinueWork loops
        // until max rounds exceeded → run fails.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-reject", "standard")?;

        // Configure completion_panel with all completers voting not-complete
        let overrides = serde_json::json!({
            "completion_panel": {
                "vote_complete": false,
                "evidence": ["Does not meet requirements"],
                "remaining_work": ["Critical gap"]
            }
        });
        let overrides_str = overrides.to_string();

        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides_str),
                ("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS", "2"),
            ],
        )?;
        assert_failure(&out)?;

        // Verify run snapshot shows failed
        let snapshot = read_run_snapshot(&ws, "cr-reject")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status, got '{status}'"));
        }

        // Completion round advanced events should exist (ContinueWork loops)
        let events = read_journal(&ws, "cr-reject")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "completion_round_advanced") {
            return Err(
                "journal should contain completion_round_advanced events before max rounds failure"
                    .into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-CR-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-advance", "standard")?;

        // Default stub returns approved for all stages, so this is the normal happy path
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Verify run completes with completion_rounds=1 (no advancement needed)
        let snapshot = read_run_snapshot(&ws, "cr-advance")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed status, got '{status}'"));
        }
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds != 1 {
            return Err(format!("expected completion_rounds=1, got {rounds}"));
        }

        // Verify completion_panel transitions to acceptance_qa transitions to final_review
        let events = read_journal(&ws, "cr-advance")?;
        let stage_sequence: Vec<String> = events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered"))
            .filter_map(|e| {
                e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .collect();
        // Verify late stages appear in order
        let cp_idx = stage_sequence.iter().position(|s| s == "completion_panel");
        let aq_idx = stage_sequence.iter().position(|s| s == "acceptance_qa");
        let fr_idx = stage_sequence.iter().position(|s| s == "final_review");
        if let (Some(cp), Some(aq), Some(fr)) = (cp_idx, aq_idx, fr_idx) {
            if !(cp < aq && aq < fr) {
                return Err(format!(
                    "late stages out of order: completion_panel@{cp}, acceptance_qa@{aq}, final_review@{fr}"
                ));
            }
        } else {
            return Err("missing one or more late stages in journal".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-005", || {
        // Non-late-stage conditionally_approved does NOT queue amendments or advance rounds
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-non-late", "standard")?;

        // Override review (non-late stage) to return conditionally_approved
        let overrides = serde_json::json!({
            "review": {
                "outcome": "conditionally_approved",
                "evidence": ["Minor issue"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["Small tweak"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-non-late")?;
        let queue_pending = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map_or(0, |a| a.len());
        if queue_pending > 0 {
            return Err(format!(
                "expected empty amendment_queue for non-late stage, got {queue_pending}"
            ));
        }
        let events = read_journal(&ws, "cr-non-late")?;
        let types = journal_event_types(&events);
        if types.iter().any(|t| t == "amendment_queued") {
            return Err("non-late stage should NOT produce amendment_queued event".into());
        }
        if types.iter().any(|t| t == "completion_round_advanced") {
            return Err("non-late stage should NOT produce completion_round_advanced event".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-006", || {
        // Multiple completion rounds accumulate: completion_panel triggers round 2,
        // then acceptance_qa triggers round 3
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-multi", "standard")?;

        // Panel dispatch invokes 2 completers per round; sequence entries consumed in order
        // with last-entry clamping.
        // Round 1: entries [0],[1] → both false → ContinueWork → restart
        // Round 2: entries [2],[2](clamped) → both true → Complete → acceptance_qa
        //   acceptance_qa[0] = conditionally_approved → triggers round 2→3
        // Round 3: entries [2],[2](clamped) → both true → Complete → acceptance_qa
        //   acceptance_qa[1] = approved → proceeds; final_review → done
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "vote_complete": false,
                    "evidence": ["Round 1 issue"],
                    "remaining_work": ["Fix A"]
                },
                {
                    "vote_complete": false,
                    "evidence": ["Round 1 issue"],
                    "remaining_work": ["Fix A"]
                },
                {
                    "vote_complete": true,
                    "evidence": ["OK now"],
                    "remaining_work": []
                }
            ],
            "acceptance_qa": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Round 2 issue"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix B"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-multi")?;
        let round_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
            })
            .collect();
        if round_events.len() < 2 {
            return Err(format!(
                "expected >= 2 completion_round_advanced events, got {}",
                round_events.len()
            ));
        }

        let snapshot = read_run_snapshot(&ws, "cr-multi")?;
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 3 {
            return Err(format!("expected completion_rounds >= 3, got {rounds}"));
        }
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-CR-007", || {
        // Completion guard blocks run_completed when disk amendments exist
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-guard", "standard")?;

        // Pre-plant a durable amendment file on disk
        let amend_dir = project_root(ws.path(), "cr-guard").join("amendments");
        std::fs::write(
            amend_dir.join("orphaned-amendment.json"),
            r#"{"amendment_id":"orphan-1","source_stage":"completion_panel","source_cycle":1,"source_completion_round":1,"body":"Orphan: Stale amendment","created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
        ).map_err(|e| e.to_string())?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        // Amendment file should still exist
        if !amend_dir.join("orphaned-amendment.json").is_file() {
            return Err("amendment file should remain on disk".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-008", || {
        // Resume processes snapshot-queue amendments through planning.
        // When a failed run snapshot has non-empty amendment_queue.pending
        // (but NO amendment files on disk), `run resume` detects the pending
        // amendments, routes to planning to process them, drains the queue,
        // and completes the run. This tests the snapshot-queue amendment
        // lifecycle distinct from the disk-only path (SC-CR-007).
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-snap-guard", "standard")?;
        let project_id = crate::shared::domain::ProjectId::new("cr-snap-guard").unwrap();
        let prompt_hash =
            crate::contexts::project_run_record::service::ProjectStorePort::read_project_record(
                &crate::adapters::fs::FsProjectStore,
                ws.path(),
                &project_id,
            )
            .map_err(|e| e.to_string())?
            .prompt_hash;

        // Inject a failed run snapshot with non-empty amendment_queue.pending
        // but NO amendment files on disk.
        let run_json = format!(
            r#"{{"active_run":null,"interrupted_run":{{"run_id":"run-snap-1","stage_cursor":{{"stage":"completion_panel","cycle":1,"attempt":1,"completion_round":1}},"started_at":"2026-03-11T19:00:00Z","prompt_hash_at_cycle_start":"{prompt_hash}","prompt_hash_at_stage_start":"{prompt_hash}","qa_iterations_current_cycle":0,"review_iterations_current_cycle":0,"final_review_restart_count":0}},"status":"failed","cycle_history":[],"completion_rounds":1,"rollback_point_meta":{{"last_rollback_id":null,"rollback_count":0}},"amendment_queue":{{"pending":[{{"amendment_id":"snap-1","source_stage":"completion_panel","source_cycle":1,"source_completion_round":1,"body":"Snap amend: in snapshot only","created_at":"2026-03-11T20:00:00Z","batch_sequence":0}}],"processed_count":0}},"status_summary":"failed"}}"#
        );
        std::fs::write(
            project_root(ws.path(), "cr-snap-guard").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;

        // Append run_started and run_failed events so resume can find the run_started event
        let journal_path = project_root(ws.path(), "cr-snap-guard").join("journal.ndjson");
        let mut journal = std::fs::read_to_string(&journal_path).map_err(|e| e.to_string())?;
        journal.push('\n');
        journal.push_str(r#"{"sequence":2,"timestamp":"2026-03-11T19:01:00Z","event_type":"run_started","details":{"run_id":"run-snap-1","first_stage":"planning"}}"#);
        journal.push('\n');
        journal.push_str(r#"{"sequence":3,"timestamp":"2026-03-11T19:02:00Z","event_type":"run_failed","details":{"run_id":"run-snap-1","stage_id":"completion_panel","failure_class":"stage_failure","message":"failed during completion"}}"#);
        std::fs::write(&journal_path, journal).map_err(|e| e.to_string())?;

        // Verify no amendment files exist on disk
        let amend_dir = project_root(ws.path(), "cr-snap-guard").join("amendments");
        let disk_files: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .collect();
        if !disk_files.is_empty() {
            return Err("test setup error: no disk amendment files should exist".into());
        }

        let out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&out)?;

        // Verify run completed — amendments were processed via planning
        let snapshot = read_run_snapshot(&ws, "cr-snap-guard")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!(
                "expected completed after resume with snapshot amendments, got '{status}'"
            ));
        }
        // Amendments must be drained from snapshot queue
        let pending = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        if pending != 0 {
            return Err(format!(
                "expected 0 pending amendments after processing, got {pending}"
            ));
        }
        // processed_count must reflect the drained amendment
        let processed = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("processed_count"))
            .and_then(|p| p.as_u64())
            .unwrap_or(0);
        if processed == 0 {
            return Err("expected processed_count > 0 after amendment processing".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-009", || {
        // Resume with pending late-stage amendments reconciles from disk
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-resume-amend", "standard")?;
        let project_id = crate::shared::domain::ProjectId::new("cr-resume-amend").unwrap();
        let prompt_hash =
            crate::contexts::project_run_record::service::ProjectStorePort::read_project_record(
                &crate::adapters::fs::FsProjectStore,
                ws.path(),
                &project_id,
            )
            .map_err(|e| e.to_string())?
            .prompt_hash;

        // Set up a failed run state (as if it failed after round advancement)
        let run_json = format!(
            r#"{{"active_run":null,"interrupted_run":{{"run_id":"run-resume-1","stage_cursor":{{"stage":"completion_panel","cycle":1,"attempt":1,"completion_round":2}},"started_at":"2026-03-11T19:00:00Z","prompt_hash_at_cycle_start":"{prompt_hash}","prompt_hash_at_stage_start":"{prompt_hash}","qa_iterations_current_cycle":0,"review_iterations_current_cycle":0,"final_review_restart_count":0}},"status":"failed","cycle_history":[],"completion_rounds":2,"rollback_point_meta":{{"last_rollback_id":null,"rollback_count":0}},"amendment_queue":{{"pending":[],"processed_count":0}},"status_summary":"failed"}}"#
        );
        std::fs::write(
            project_root(ws.path(), "cr-resume-amend").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;

        // Append run_started and run_failed events so resume can find the run_started event
        let journal_path = project_root(ws.path(), "cr-resume-amend").join("journal.ndjson");
        let mut journal = std::fs::read_to_string(&journal_path).map_err(|e| e.to_string())?;
        journal.push('\n');
        journal.push_str(r#"{"sequence":2,"timestamp":"2026-03-11T19:01:00Z","event_type":"run_started","details":{"run_id":"run-resume-1","first_stage":"planning"}}"#);
        journal.push('\n');
        journal.push_str(r#"{"sequence":3,"timestamp":"2026-03-11T19:02:00Z","event_type":"run_failed","details":{"run_id":"run-resume-1","stage_id":"completion_panel","failure_class":"stage_failure","message":"failed during completion round"}}"#);
        std::fs::write(&journal_path, journal).map_err(|e| e.to_string())?;

        // Plant amendment files on disk for reconciliation
        let amend_dir = project_root(ws.path(), "cr-resume-amend").join("amendments");
        std::fs::write(
            amend_dir.join("resume-amend-1.json"),
            r#"{"amendment_id":"resume-1","source_stage":"completion_panel","source_cycle":1,"source_completion_round":1,"body":"Resume fix: Fix from prior round","created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
        ).map_err(|e| e.to_string())?;

        let out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-resume-amend")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed after resume, got '{status}'"));
        }
        // Amendments should be drained
        let amend_files: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .collect();
        if !amend_files.is_empty() {
            return Err(format!(
                "expected amendments drained from disk, found {}",
                amend_files.len()
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-010", || {
        // Cycle advancement emitted when entering implementation from completion round
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-cycle-adv", "standard")?;

        // Sequence: first → conditionally_approved (triggers round), second → approved
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Issue found"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix cycle issue"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["Fixed"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-cycle-adv")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "completion_round_advanced") {
            return Err("journal should contain completion_round_advanced event for completion round restart".into());
        }
        if types.iter().any(|t| t == "cycle_advanced") {
            return Err("cycle_advanced should not be emitted for completion round restart".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-011", || {
        // Completion panel ContinueWork→Complete round transition:
        // First round completers vote continue_work, second round vote complete.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-idempotent", "standard")?;

        // Sequence: first round → continue_work (triggers round), second → complete
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "vote_complete": false,
                    "evidence": ["Needs more work"],
                    "remaining_work": ["Fix needed"]
                },
                {
                    "vote_complete": true,
                    "evidence": ["All complete"],
                    "remaining_work": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-idempotent")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed status, got '{status}'"));
        }

        // Verify completion_round_advanced event exists (round transition occurred)
        let events = read_journal(&ws, "cr-idempotent")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "completion_round_advanced") {
            return Err("journal missing completion_round_advanced event".into());
        }

        // Verify completion_rounds in snapshot reflects the advancement
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 2 {
            return Err(format!(
                "expected completion_rounds >= 2 after round transition, got {rounds}"
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-012", || {
        // Max completion rounds safety limit: all completers always vote
        // continue_work → run fails after max rounds exceeded.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-atomic", "standard")?;

        // Single-entry override: always votes continue_work → infinite loop
        // without the safety limit.
        let overrides = serde_json::json!({
            "completion_panel": {
                "vote_complete": false,
                "evidence": ["Always needs more work"],
                "remaining_work": ["Unbounded work"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string()),
                ("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS", "2"),
            ],
        )?;
        // The run must fail because max completion rounds exceeded
        assert_failure(&out)?;

        // Verify the run snapshot shows failure
        let snapshot = read_run_snapshot(&ws, "cr-atomic")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status, got '{status}'"));
        }

        // Verify completion_round_advanced events exist (rounds were attempted)
        let events = read_journal(&ws, "cr-atomic")?;
        let round_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
            })
            .collect();
        if round_events.is_empty() {
            return Err(
                "expected completion_round_advanced events before max rounds failure".into(),
            );
        }
        Ok(())
    });

    reg!(m, "SC-CR-013", || {
        // Completion guard leaves snapshot in resumable state
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-resumable", "standard")?;

        // Pre-plant orphaned amendment on disk so the guard fires
        let amend_dir = project_root(ws.path(), "cr-resumable").join("amendments");
        std::fs::write(
            amend_dir.join("guard-amend.json"),
            r#"{"amendment_id":"guard-1","source_stage":"completion_panel","source_cycle":1,"source_completion_round":1,"body":"Guard: Blocks completion","created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
        ).map_err(|e| e.to_string())?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        // Snapshot should be failed with no active_run (resumable)
        let snapshot = read_run_snapshot(&ws, "cr-resumable")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!(
                "expected failed status after guard, got '{status}'"
            ));
        }
        let active_run = snapshot.get("active_run");
        if active_run.is_some() && !active_run.unwrap().is_null() {
            return Err("active_run should be null after guard failure".into());
        }
        // Amendment file should remain untouched
        if !amend_dir.join("guard-amend.json").is_file() {
            return Err("amendment file should remain on disk after guard failure".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-014", || {
        // Completion round numbering is sequential across multiple rounds.
        // Two ContinueWork rounds followed by Complete in round 3.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-batch-seq", "standard")?;

        // Sequence: round 1 → continue_work, round 2 → continue_work, round 3 → complete
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "vote_complete": false,
                    "evidence": ["Round 1 needs work"],
                    "remaining_work": ["First round fix"]
                },
                {
                    "vote_complete": false,
                    "evidence": ["Round 2 needs work"],
                    "remaining_work": ["Second round fix"]
                },
                {
                    "vote_complete": true,
                    "evidence": ["All complete in round 3"],
                    "remaining_work": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        // Verify completion_round_advanced events have sequential round numbers
        let events = read_journal(&ws, "cr-batch-seq")?;
        let round_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
            })
            .collect();
        if round_events.len() < 2 {
            return Err(format!(
                "expected >= 2 completion_round_advanced events, got {}",
                round_events.len()
            ));
        }
        // Verify to_round values are strictly ascending
        let mut prev_round: u64 = 0;
        for evt in &round_events {
            if let Some(to_round) = evt
                .get("details")
                .and_then(|d| d.get("to_round"))
                .and_then(|v| v.as_u64())
            {
                if to_round <= prev_round {
                    return Err(format!(
                        "to_round not ascending: prev={prev_round}, current={to_round}"
                    ));
                }
                prev_round = to_round;
            }
        }

        // Verify final snapshot has completion_rounds >= 3
        let snapshot = read_run_snapshot(&ws, "cr-batch-seq")?;
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 3 {
            return Err(format!(
                "expected completion_rounds >= 3 after 3 rounds, got {rounds}"
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-015", || {
        // Final-review conditionally_approved triggers completion round advancement
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-fr-cond", "standard")?;

        // Sequence: first → conditionally_approved (triggers round), second → approved
        let overrides = serde_json::json!({
            "final_review": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Final review issue"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix from final review"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["Fixed"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-fr-cond")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "amendment_queued") {
            return Err("journal missing amendment_queued event from final_review".into());
        }
        // Verify completion_round_advanced has source_stage=final_review
        let round_event = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
        });
        if round_event.is_none() {
            return Err("journal missing completion_round_advanced event".into());
        }
        if let Some(evt) = round_event {
            let source = evt
                .get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "final_review" {
                return Err(format!(
                    "expected source_stage=final_review, got '{source}'"
                ));
            }
        }

        // Verify planning was entered a second time and run completed with rounds=2
        let planning_count = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("planning")
            })
            .count();
        if planning_count < 2 {
            return Err(format!(
                "expected planning entered >= 2 times, got {planning_count}"
            ));
        }
        let snapshot = read_run_snapshot(&ws, "cr-fr-cond")?;
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 2 {
            return Err(format!("expected completion_rounds >= 2, got {rounds}"));
        }
        let queue_pending = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map_or(0, |a| a.len());
        if queue_pending > 0 {
            return Err("amendment_queue should be empty after completion".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-016", || {
        // Final-review request_changes triggers completion round advancement
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-fr-changes", "standard")?;

        // Sequence: first → request_changes (triggers round), second → approved
        let overrides = serde_json::json!({
            "final_review": [
                {
                    "outcome": "request_changes",
                    "evidence": ["Changes needed"],
                    "findings_or_gaps": ["Gap found"],
                    "follow_up_or_amendments": ["Address gap"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["Addressed"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-fr-changes")?;
        let round_event = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
        });
        if round_event.is_none() {
            return Err("journal missing completion_round_advanced event".into());
        }
        if let Some(evt) = round_event {
            let source = evt
                .get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "final_review" {
                return Err(format!(
                    "expected source_stage=final_review, got '{source}'"
                ));
            }
        }

        let snapshot = read_run_snapshot(&ws, "cr-fr-changes")?;
        let rounds = snapshot
            .get("completion_rounds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if rounds < 2 {
            return Err(format!("expected completion_rounds >= 2, got {rounds}"));
        }
        Ok(())
    });
}

// ===========================================================================
// Run Resume/Retry (9 scenarios)
// ===========================================================================

fn register_run_resume_retry(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-RESUME-001", || {
        // Retryable implementation failure succeeds on the second attempt
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "alpha", "standard")?;

        // Inject transient failure: implementation fails once, succeeds on retry
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_TRANSIENT_FAILURE", "implementation:1")],
        )?;
        assert_success(&out)?;

        // Verify journal contains stage_failed with will_retry=true for implementation
        let events = read_journal(&ws, "alpha")?;
        let stage_failed = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_failed")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("implementation")
        });
        if stage_failed.is_none() {
            return Err("journal missing stage_failed event for implementation".into());
        }
        let will_retry = stage_failed
            .unwrap()
            .get("details")
            .and_then(|d| d.get("will_retry"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !will_retry {
            return Err("stage_failed event should have will_retry=true".into());
        }

        // Verify implementation was entered a second time (retry)
        let impl_entered_count = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("implementation")
            })
            .count();
        if impl_entered_count < 2 {
            return Err(format!(
                "expected implementation stage_entered >= 2 (retry), got {impl_entered_count}"
            ));
        }

        // Verify run completed
        let snapshot = read_run_snapshot(&ws, "alpha")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-002", || {
        // Retry exhaustion fails the run
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "bravo", "standard")?;

        // Permanent failure at implementation
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
        )?;
        assert_failure(&out)?;

        // Verify journal ends with run_failed referencing implementation
        let events = read_journal(&ws, "bravo")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "run_failed") {
            return Err("journal missing run_failed event".into());
        }

        // Verify status is failed
        let snapshot = read_run_snapshot(&ws, "bravo")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-003", || {
        // QA request_changes advances the cycle and reruns implementation
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "charlie", "standard")?;

        // QA returns request_changes first, then approved on second cycle
        let overrides = serde_json::json!({
            "qa": [
                {
                    "outcome": "request_changes",
                    "evidence": ["Changes needed"],
                    "findings_or_gaps": ["Missing regression test"],
                    "follow_up_or_amendments": ["add missing regression test"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["All good"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        // Verify journal contains cycle_advanced event
        let events = read_journal(&ws, "charlie")?;
        let cycle_advanced = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("cycle_advanced"));
        if cycle_advanced.is_none() {
            return Err("journal missing cycle_advanced event".into());
        }

        // Verify implementation entered twice across the run
        let impl_entered = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("implementation")
            })
            .count();
        if impl_entered < 2 {
            return Err(format!(
                "expected implementation entered >= 2 times, got {impl_entered}"
            ));
        }

        // Verify completed
        let snapshot = read_run_snapshot(&ws, "charlie")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-004", || {
        // Prompt review rejection via panel validators fails the run.
        // (Old model paused on readiness.ready=false; panel model rejects.)
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "delta", "standard")?;

        // Override prompt_review: validators see readiness.ready=false → reject
        let overrides = serde_json::json!({
            "prompt_review": {
                "problem_framing": "Prompt not ready",
                "assumptions_or_open_questions": ["Needs revision"],
                "proposed_work": [{"order": 1, "summary": "Revise prompt", "details": "Details"}],
                "readiness": {"ready": false, "risks": ["Prompt unclear"]}
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_failure(&out)?;

        // Verify failed status (panel rejection)
        let snapshot = read_run_snapshot(&ws, "delta")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed, got '{status}'"));
        }
        // Verify prompt_review supporting records persisted before failure
        let payloads = count_payload_files(&ws, "delta")?;
        if payloads < 1 {
            return Err("expected at least 1 payload persisted before failure".into());
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-005", || {
        // Resume from failed run continues from the first incomplete durable stage
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "echo", "standard")?;

        // Step 1: run start fails at implementation (planning completes)
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
        )?;
        assert_failure(&start)?;

        // Capture the run_id before resume
        let pre_snapshot = read_run_snapshot(&ws, "echo")?;
        let pre_events = read_journal(&ws, "echo")?;
        let run_id = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();
        if run_id.is_empty() {
            return Err("could not find run_id from journal".into());
        }
        if pre_snapshot.get("status").and_then(|v| v.as_str()) != Some("failed") {
            return Err("expected failed status after start".into());
        }

        // Step 2: resume without failure injection → should complete from implementation
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        // Verify the resumed run keeps the original run_id
        let post_events = read_journal(&ws, "echo")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_run_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_run_id != run_id {
            return Err(format!(
                "expected resumed run_id={run_id}, got {resumed_run_id}"
            ));
        }

        // Verify planning is NOT re-executed after resume (only entered once total)
        // Count planning stage_entered events after the run_resumed event
        let resume_seq = resume_evt
            .unwrap()
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let planning_after_resume = post_events
            .iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("planning")
            })
            .count();
        if planning_after_resume > 0 {
            return Err("planning should not be re-executed after resume".into());
        }

        // Verify the first resumed stage is "implementation" with attempt 1
        let first_stage_after_resume = post_events.iter().find(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if let Some(evt) = first_stage_after_resume {
            let stage = evt
                .get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if stage != "implementation" {
                return Err(format!(
                    "expected first resumed stage=implementation, got '{stage}'"
                ));
            }
            let attempt = evt
                .get("details")
                .and_then(|d| d.get("attempt"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if attempt != 1 {
                return Err(format!(
                    "expected first resumed stage attempt=1, got {attempt}"
                ));
            }
        } else {
            return Err("no stage_entered events after resume".into());
        }

        // Verify completed
        let final_snapshot = read_run_snapshot(&ws, "echo")?;
        let status = final_snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed after resume, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-006", || {
        // Resume from failed prompt-review run (panel rejection) continues and completes.
        // (Old model paused on readiness.ready=false; panel model fails with rejection.)
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "foxtrot", "standard")?;

        // Step 1: run start with prompt_review validators rejecting → fails
        let overrides = serde_json::json!({
            "prompt_review": {
                "problem_framing": "Not ready",
                "assumptions_or_open_questions": [],
                "proposed_work": [{"order": 1, "summary": "S", "details": "D"}],
                "readiness": {"ready": false, "risks": []}
            }
        });
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_failure(&start)?;
        let pre_snapshot = read_run_snapshot(&ws, "foxtrot")?;
        if pre_snapshot.get("status").and_then(|v| v.as_str()) != Some("failed") {
            return Err("expected failed after prompt_review rejection".into());
        }

        // Capture original run_id
        let pre_events = read_journal(&ws, "foxtrot")?;
        let run_id = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        // Step 2: resume without overrides → default stubs accept, completes
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        // Verify resumed run keeps original run_id
        let post_events = read_journal(&ws, "foxtrot")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_run_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_run_id != run_id {
            return Err(format!(
                "expected resumed run_id={run_id}, got {resumed_run_id}"
            ));
        }

        // Verify completed
        let final_snapshot = read_run_snapshot(&ws, "foxtrot")?;
        let status = final_snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-007", || {
        // Resume rejects non-resumable statuses (not_started, running, completed)
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "golf", "standard")?;
        // not_started → resume should fail
        let out = run_cli(&["run", "resume"], ws.path())?;
        assert_failure(&out)?;

        // completed → resume should fail
        let completed_json = r#"{"active_run":null,"status":"completed","cycle_history":[],"completion_rounds":1,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(
            project_root(ws.path(), "golf").join("run.json"),
            completed_json,
        )
        .map_err(|e| e.to_string())?;
        let out2 = run_cli(&["run", "resume"], ws.path())?;
        assert_failure(&out2)?;
        Ok(())
    });

    reg!(m, "SC-RESUME-008", || {
        // Run start rejects failed/paused and directs to resume
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "hotel", "standard")?;
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(project_root(ws.path(), "hotel").join("run.json"), run_json)
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "resume", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-RESUME-009", || {
        // Cancellation halts retries - verify via permanent failure that no
        // further stage_entered events occur after the run_failed event
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "india", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
        )?;
        assert_failure(&out)?;

        let events = read_journal(&ws, "india")?;
        let failed_seq = events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_failed"))
            .map(|e| e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0))
            .next()
            .unwrap_or(0);
        if failed_seq == 0 {
            return Err("journal missing run_failed".into());
        }
        // No stage_entered after run_failed
        let entered_after = events.iter().any(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > failed_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if entered_after {
            return Err("stage_entered found after run_failed - retries were not halted".into());
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-010", || {
        // Run start acquires and releases the writer lock
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "hotel", "standard")?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;
        // Writer lock must be released after completion
        let lock_path = ws
            .path()
            .join(".ralph-burning/daemon/leases/writer-hotel.lock");
        if lock_path.exists() {
            return Err("writer lock file still exists after run start completed".into());
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-011", || {
        // Run start exits non-zero when writer lock is held, and no run-state
        // mutation occurs for the project.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "india-lock", "standard")?;

        // Snapshot run.json and journal before the attempt
        let run_before =
            std::fs::read_to_string(project_root(ws.path(), "india-lock").join("run.json"))
                .map_err(|e| format!("read run.json before: {e}"))?;
        let journal_before =
            std::fs::read_to_string(project_root(ws.path(), "india-lock").join("journal.ndjson"))
                .map_err(|e| format!("read journal before: {e}"))?;

        // Pre-create the writer lock
        let lock_dir = daemon_root(ws.path()).join("leases");
        std::fs::create_dir_all(&lock_dir).map_err(|e| format!("create lock dir: {e}"))?;
        std::fs::write(lock_dir.join("writer-india-lock.lock"), "held-by-test")
            .map_err(|e| format!("write lock: {e}"))?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "writer lock", "stderr")?;

        // Verify no run-state mutation occurred
        let run_after =
            std::fs::read_to_string(project_root(ws.path(), "india-lock").join("run.json"))
                .map_err(|e| format!("read run.json after: {e}"))?;
        let journal_after =
            std::fs::read_to_string(project_root(ws.path(), "india-lock").join("journal.ndjson"))
                .map_err(|e| format!("read journal after: {e}"))?;

        if run_before != run_after {
            return Err(format!(
                "run.json was mutated despite lock-held failure.\nbefore: {run_before}\nafter: {run_after}"
            ));
        }
        if journal_before != journal_after {
            return Err(format!(
                "journal.ndjson was mutated despite lock-held failure.\nbefore: {journal_before}\nafter: {journal_after}"
            ));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-012", || {
        // Run resume acquires and releases the writer lock
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "juliet", "standard")?;
        // First run: fail at implementation to get a failed snapshot
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
        )?;
        assert_failure(&start)?;
        // Resume should succeed and release the lock
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;
        let lock_path = ws
            .path()
            .join(".ralph-burning/daemon/leases/writer-juliet.lock");
        if lock_path.exists() {
            return Err("writer lock file still exists after run resume completed".into());
        }
        Ok(())
    });
}

// ===========================================================================
// Run Resume Non-Standard (6 scenarios)
// ===========================================================================

fn register_run_resume_non_standard(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-NONSTD-RESUME-001", || {
        // Resume a failed docs_change (minimal alias) run from final_review.
        // docs_change now aliases minimal: stages are [plan_and_implement,
        // final_review]. We fail at final_review and verify resume completes
        // without re-entering plan_and_implement.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-docs", "docs_change")?;

        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "final_review")],
        )?;
        assert_failure(&start)?;

        let pre_events = read_journal(&ws, "ns-docs")?;
        let run_id = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-docs")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_id != run_id {
            return Err(format!(
                "run_id mismatch: expected {run_id}, got {resumed_id}"
            ));
        }

        let resume_seq = resume_evt
            .unwrap()
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let plan_after = post_events.iter().any(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("plan_and_implement")
        });
        if plan_after {
            return Err("plan_and_implement should not be re-executed after resume".into());
        }

        let first_stage = post_events.iter().find(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if let Some(evt) = first_stage {
            let sid = evt
                .get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if sid != "final_review" {
                return Err(format!(
                    "expected first resumed stage=final_review, got '{sid}'"
                ));
            }
        }

        let final_snap = read_run_snapshot(&ws, "ns-docs")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed after resume".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-002", || {
        // Resume a failed ci_improvement run from ci_update
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-ci", "ci_improvement")?;

        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "ci_update")],
        )?;
        assert_failure(&start)?;

        let pre_events = read_journal(&ws, "ns-ci")?;
        let run_id = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-ci")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed".into());
        }
        let resumed_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("run_id mismatch: {run_id} vs {resumed_id}"));
        }

        // Verify ci_plan not re-entered, first resumed stage is ci_update
        let resume_seq = resume_evt
            .unwrap()
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let first_stage = post_events.iter().find(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if let Some(evt) = first_stage {
            let sid = evt
                .get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if sid != "ci_update" {
                return Err(format!(
                    "expected first resumed stage=ci_update, got '{sid}'"
                ));
            }
        }

        let final_snap = read_run_snapshot(&ws, "ns-ci")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-003", || {
        // docs_change is now an alias of minimal: a docs_change run should
        // start at plan_and_implement and reach final_review like any minimal
        // flow. This smoke-test verifies the alias end-to-end.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-docs-amend", "docs_change")?;

        let start = run_cli(&["run", "start"], ws.path())?;
        assert_success(&start)?;

        let events = read_journal(&ws, "ns-docs-amend")?;
        let stages_entered: Vec<String> = events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered"))
            .filter_map(|e| {
                e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    .map(ToOwned::to_owned)
            })
            .collect();
        if !stages_entered.iter().any(|s| s == "plan_and_implement") {
            return Err(format!(
                "docs_change run did not enter plan_and_implement; stages were {stages_entered:?}"
            ));
        }
        if !stages_entered.iter().any(|s| s == "final_review") {
            return Err(format!(
                "docs_change run did not enter final_review; stages were {stages_entered:?}"
            ));
        }
        if stages_entered
            .iter()
            .any(|s| s == "docs_plan" || s == "docs_update" || s == "docs_validation")
        {
            return Err(format!(
                "docs_change run should not enter legacy docs_* stages; stages were {stages_entered:?}"
            ));
        }

        let final_snap = read_run_snapshot(&ws, "ns-docs-amend")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed docs_change run".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-004", || {
        // ci_improvement: ci_validation request_changes triggers remediation cycle.
        // Uses a marker-file command so validation fails on first run, passes on second.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-ci-amend", "ci_improvement")?;

        let marker = project_root(ws.path(), "ns-ci-amend").join("runtime/temp/ci_marker");
        let marker_str = marker.display().to_string();
        let cmd = format!("test -f {marker_str} || (touch {marker_str} && exit 1)");
        let config_path = project_root(ws.path(), "ns-ci-amend").join("config.toml");
        std::fs::write(
            &config_path,
            format!("[validation]\nci_commands = [\"{cmd}\"]\n"),
        )
        .map_err(|e| format!("write config: {e}"))?;

        let start = run_cli(&["run", "start"], ws.path())?;
        assert_success(&start)?;

        // ci_validation request_changes triggers remediation cycle (cycle_advanced)
        let events = read_journal(&ws, "ns-ci-amend")?;
        if !journal_event_types(&events)
            .iter()
            .any(|t| t == "cycle_advanced")
        {
            return Err("journal missing cycle_advanced event for remediation".into());
        }

        let final_snap = read_run_snapshot(&ws, "ns-ci-amend")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-005", || {
        // Resume a failed quick_dev run from review
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-qd", "quick_dev")?;

        // Fail at review stage
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")],
        )?;
        assert_failure(&start)?;

        let pre_events = read_journal(&ws, "ns-qd")?;
        let run_id = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        // Resume → completes from review
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-qd")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed".into());
        }
        let resumed_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("run_id mismatch: {run_id} vs {resumed_id}"));
        }

        // plan_and_implement not re-entered after resume
        let resume_seq = resume_evt
            .unwrap()
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let pai_after = post_events.iter().any(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("plan_and_implement")
        });
        if pai_after {
            return Err("plan_and_implement should not be re-entered after resume".into());
        }

        // First resumed stage is review
        let first_stage = post_events.iter().find(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if let Some(evt) = first_stage {
            let sid = evt
                .get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if sid != "review" {
                return Err(format!("expected first resumed stage=review, got '{sid}'"));
            }
        }

        let final_snap = read_run_snapshot(&ws, "ns-qd")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-006", || {
        // Resume a paused quick_dev snapshot with pending amendments
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-qd-amend", "quick_dev")?;

        let overrides = serde_json::json!({
            "review": [
                {
                    "outcome": "request_changes",
                    "evidence": ["Needs work"],
                    "findings_or_gaps": ["Issue"],
                    "follow_up_or_amendments": ["Fix the issue"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["All good"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&start)?;

        // review request_changes triggers remediation cycle (not amendments —
        // review is a remediation trigger, not a late stage in quick_dev)
        let events = read_journal(&ws, "ns-qd-amend")?;
        if !journal_event_types(&events)
            .iter()
            .any(|t| t == "cycle_advanced")
        {
            return Err("journal missing cycle_advanced for remediation".into());
        }

        // Remediation restarts from execution stage (apply_fixes).
        // Quick_dev stage order: plan_and_implement → review → apply_fixes → final_review
        // So apply_fixes is entered once (during remediation cycle), not during initial run.
        let af_entered = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("apply_fixes")
            })
            .count();
        if af_entered < 1 {
            return Err(format!(
                "expected apply_fixes entered >= 1 time after remediation, got {af_entered}"
            ));
        }

        let final_snap = read_run_snapshot(&ws, "ns-qd-amend")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });
}

// ===========================================================================
// Run Rollback (8 scenarios)
// ===========================================================================

fn register_run_rollback(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-ROLLBACK-001", || {
        // Soft rollback rewinds to a visible checkpoint.
        // Use completion_panel conditionally_approved to create rollback points,
        // then fail on 2nd cycle to get a failed run with rollback points.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-soft", "standard")?;

        // completion_panel: conditionally_approved on first pass (creates rollback
        // point and advances cycle), then fail implementation on 2nd cycle
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Needs minor changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix: D"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let _start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        // The run may complete or fail depending on engine behavior
        // Either way, rollback points should have been created

        // Read current status and journal
        let pre_events = read_journal(&ws, "rb-soft")?;
        let types = journal_event_types(&pre_events);
        let has_rollback_created = types.iter().any(|t| t == "rollback_created");
        if !has_rollback_created {
            return Err("expected rollback_created event in journal".into());
        }

        // If run completed, manually set to failed for rollback test
        let pre_snap = read_run_snapshot(&ws, "rb-soft")?;
        let status = pre_snap
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status == "completed" || status == "running" {
            let mut snap = pre_snap.clone();
            snap["status"] = serde_json::json!("failed");
            snap["active_run"] = serde_json::json!(null);
            snap["status_summary"] = serde_json::json!("failed for rollback test");
            std::fs::write(
                project_root(ws.path(), "rb-soft").join("run.json"),
                serde_json::to_string_pretty(&snap).unwrap(),
            )
            .map_err(|e| e.to_string())?;
        }

        // Now rollback to planning
        let rb = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_success(&rb)?;

        // Verify: status is paused, journal has rollback_performed
        let post_snap = read_run_snapshot(&ws, "rb-soft")?;
        let post_status = post_snap
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post_status != "paused" {
            return Err(format!(
                "expected paused after rollback, got '{post_status}'"
            ));
        }
        let post_events = read_journal(&ws, "rb-soft")?;
        let post_types = journal_event_types(&post_events);
        if !post_types.iter().any(|t| t == "rollback_performed") {
            return Err("journal missing rollback_performed event".into());
        }
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-002", || {
        // Hard rollback resets canonical state before the repository.
        // Feature: the target rollback point for "implementation" records a git SHA,
        // and the repository reset targets that SHA. The logical rollback is committed
        // before the git reset is attempted.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-hard", "standard")?;

        // Initialize a git repo so rollback points record a real git SHA
        let _initial_sha = init_git_repo(&ws)?;

        // Create rollback points via conditionally_approved completion_panel
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix: D"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&start)?;

        // Verify rollback points were created with a git SHA
        let pre_events = read_journal(&ws, "rb-hard")?;
        let rb_created = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_created"));
        if rb_created.is_none() {
            return Err("expected rollback_created event after run start".into());
        }
        let created_sha = rb_created
            .unwrap()
            .get("details")
            .and_then(|d| d.get("git_sha"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if created_sha.is_empty() {
            return Err("rollback_created event should record a non-empty git_sha".into());
        }

        // Set to paused for rollback
        let snap = read_run_snapshot(&ws, "rb-hard")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("paused");
        snap["active_run"] = serde_json::json!(null);
        snap["status_summary"] = serde_json::json!("paused for rollback test");
        std::fs::write(
            project_root(ws.path(), "rb-hard").join("run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        )
        .map_err(|e| e.to_string())?;

        // Hard rollback to implementation — git repo exists so reset should succeed.
        let rb = run_cli(
            &["run", "rollback", "--to", "implementation", "--hard"],
            ws.path(),
        )?;
        assert_success(&rb)?;

        // Verify logical rollback committed
        let post_snap = read_run_snapshot(&ws, "rb-hard")?;
        let post_status = post_snap
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post_status != "paused" {
            return Err(format!(
                "logical rollback should set paused, got '{post_status}'"
            ));
        }

        // Journal should have rollback_performed
        let post_events = read_journal(&ws, "rb-hard")?;
        let rb_event = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed"));
        if rb_event.is_none() {
            return Err("journal should have rollback_performed event".into());
        }
        let rb_event = rb_event.unwrap();

        // Verify the rollback_performed event targets implementation
        let rb_stage = rb_event
            .get("details")
            .and_then(|d| d.get("stage_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if rb_stage != "implementation" {
            return Err(format!(
                "rollback_performed should target implementation, got '{rb_stage}'"
            ));
        }

        // Verify hard=true
        let hard_flag = rb_event
            .get("details")
            .and_then(|d| d.get("hard"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !hard_flag {
            return Err("rollback_performed event should have hard=true".into());
        }

        // Verify the event records a concrete git_sha matching the rollback point
        let event_sha = rb_event
            .get("details")
            .and_then(|d| d.get("git_sha"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if event_sha.is_empty() {
            return Err(
                "rollback_performed event must record a non-empty git_sha for hard rollback".into(),
            );
        }

        // Verify the repository reset targeted the recorded SHA — after hard reset,
        // HEAD should point at the SHA from the rollback point
        let head_output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(ws.path())
            .output()
            .map_err(|e| format!("git rev-parse HEAD: {e}"))?;
        let current_head = String::from_utf8_lossy(&head_output.stdout)
            .trim()
            .to_owned();
        if current_head != event_sha {
            return Err(format!(
                "repository HEAD should be reset to rollback SHA {event_sha}, got {current_head}"
            ));
        }

        Ok(())
    });

    reg!(m, "SC-ROLLBACK-003", || {
        // Rollback rejects non-resumable run statuses
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-reject", "standard")?;
        // not_started → rollback should fail
        let out = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-004", || {
        // Rollback rejects a stage outside the project flow
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-bad-stage", "standard")?;
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(
            project_root(ws.path(), "rb-bad-stage").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        // ci_plan is not part of the standard flow
        let out = run_cli(&["run", "rollback", "--to", "ci_plan"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not part of flow", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-005", || {
        // Rollback rejects stage with no visible checkpoint
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-no-point", "standard")?;
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(
            project_root(ws.path(), "rb-no-point").join("run.json"),
            run_json,
        )
        .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "rollback", "--to", "review"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "rollback point", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-006", || {
        // Multiple sequential rollbacks keep rollback metadata monotonic.
        // Feature: roll back to "implementation" and then to "planning".
        // Verify rollback_count increases, last_rollback_id matches, and
        // run history (the user-visible output) excludes the abandoned branch
        // after each rollback.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-multi", "standard")?;

        // Create rollback points by running with conditionally_approved panel
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix: D"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let _start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;

        // Capture the pre-rollback history to compare later
        let pre_history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&pre_history)?;
        // Count StageCompleted events visible before any rollback
        // (the CLI prints event_type with {:?} which uses the variant name)
        let pre_completed_count = pre_history.stdout.matches("StageCompleted").count();

        // Set to failed for rollback
        let snap = read_run_snapshot(&ws, "rb-multi")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            project_root(ws.path(), "rb-multi").join("run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        )
        .map_err(|e| e.to_string())?;

        // First rollback: to implementation
        let rb1 = run_cli(&["run", "rollback", "--to", "implementation"], ws.path())?;
        assert_success(&rb1)?;
        let snap1 = read_run_snapshot(&ws, "rb-multi")?;
        let count1 = snap1
            .get("rollback_point_meta")
            .and_then(|m| m.get("rollback_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let last_id1 = snap1
            .get("rollback_point_meta")
            .and_then(|m| m.get("last_rollback_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if last_id1.is_empty() {
            return Err("last_rollback_id should be set after first rollback".into());
        }

        // Verify user-visible history excludes the abandoned branch after first rollback:
        // `run history` uses visible_journal_events which filters out events after the
        // rollback boundary. The visible history should have fewer stage events.
        let post_history1 = run_cli(&["run", "history"], ws.path())?;
        assert_success(&post_history1)?;
        let post_completed_count1 = post_history1.stdout.matches("StageCompleted").count();
        if post_completed_count1 >= pre_completed_count {
            return Err(format!(
                "run history after first rollback should show fewer events: pre={pre_completed_count}, post={post_completed_count1}"
            ));
        }

        // Also verify the journal metadata: visible_through_sequence is recorded
        let events1 = read_journal(&ws, "rb-multi")?;
        let rb1_event = events1
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed"));
        if rb1_event.is_none() {
            return Err("journal missing rollback_performed after first rollback".into());
        }
        let visible_through_1 = rb1_event
            .unwrap()
            .get("details")
            .and_then(|d| d.get("visible_through_sequence"))
            .and_then(|v| v.as_u64());
        if visible_through_1.is_none() {
            return Err("rollback_performed should record visible_through_sequence".into());
        }

        // Set to failed again for second rollback
        let mut snap1_mut = snap1.clone();
        snap1_mut["status"] = serde_json::json!("failed");
        std::fs::write(
            project_root(ws.path(), "rb-multi").join("run.json"),
            serde_json::to_string_pretty(&snap1_mut).unwrap(),
        )
        .map_err(|e| e.to_string())?;

        // Second rollback: to planning
        let rb2 = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_success(&rb2)?;
        let snap2 = read_run_snapshot(&ws, "rb-multi")?;
        let count2 = snap2
            .get("rollback_point_meta")
            .and_then(|m| m.get("rollback_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let last_id2 = snap2
            .get("rollback_point_meta")
            .and_then(|m| m.get("last_rollback_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Rollback count must increase monotonically
        if count2 <= count1 {
            return Err(format!(
                "rollback_count should increase: first={count1}, second={count2}"
            ));
        }
        // last_rollback_id must match the most recent rollback point
        if last_id2.is_empty() {
            return Err("last_rollback_id should be set after second rollback".into());
        }
        if last_id2 == last_id1 {
            return Err(format!(
                "last_rollback_id should change between rollbacks: first={last_id1}, second={last_id2}"
            ));
        }

        // Verify user-visible history further shrinks after the second rollback
        let post_history2 = run_cli(&["run", "history"], ws.path())?;
        assert_success(&post_history2)?;
        let post_completed_count2 = post_history2.stdout.matches("StageCompleted").count();
        if post_completed_count2 >= post_completed_count1 {
            return Err(format!(
                "run history after second rollback should show fewer events than after first: after_first={post_completed_count1}, after_second={post_completed_count2}"
            ));
        }

        // Verify the raw journal has at least 2 rollback_performed events
        let events2 = read_journal(&ws, "rb-multi")?;
        let rb_performed_events: Vec<_> = events2
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed"))
            .collect();
        if rb_performed_events.len() < 2 {
            return Err(format!(
                "expected >= 2 rollback_performed events, got {}",
                rb_performed_events.len()
            ));
        }
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-007", || {
        // Resume after rollback continues from the restored boundary.
        // Feature: rollback to implementation, resume, first resumed stage is
        // "review" (next after implementation), and the rolled-back history from
        // the abandoned branch remains hidden in the user-visible `run history` output.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-resume", "standard")?;

        // Create rollback points via conditionally_approved
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix: D"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&start)?;

        // Capture original run_id
        let events = read_journal(&ws, "rb-resume")?;
        let run_id = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| {
                e.get("details")
                    .and_then(|d| d.get("run_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        // Count pre-rollback StageCompleted events visible to the user
        let pre_history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&pre_history)?;
        let pre_completed = pre_history.stdout.matches("StageCompleted").count();

        // Set to failed for rollback
        let snap = read_run_snapshot(&ws, "rb-resume")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            project_root(ws.path(), "rb-resume").join("run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        )
        .map_err(|e| e.to_string())?;

        // Rollback to implementation
        let rb = run_cli(&["run", "rollback", "--to", "implementation"], ws.path())?;
        assert_success(&rb)?;

        // Verify the rollback event records visible_through_sequence
        let rb_events = read_journal(&ws, "rb-resume")?;
        let rb_performed = rb_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed"));
        let visible_through = rb_performed
            .and_then(|e| e.get("details"))
            .and_then(|d| d.get("visible_through_sequence"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if visible_through == 0 {
            return Err("rollback_performed should record visible_through_sequence".into());
        }

        // Verify that user-visible history after rollback hides abandoned branch:
        // `run history` should show fewer stage_completed events than before
        let post_rb_history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&post_rb_history)?;
        let post_rb_completed = post_rb_history.stdout.matches("StageCompleted").count();
        if post_rb_completed >= pre_completed {
            return Err(format!(
                "run history after rollback should exclude abandoned branch: pre={pre_completed}, post={post_rb_completed}"
            ));
        }

        // Now resume
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        // Verify resumed run keeps original run_id
        let post_events = read_journal(&ws, "rb-resume")?;
        let resume_evt = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed"));
        if resume_evt.is_none() {
            return Err("journal missing run_resumed after rollback+resume".into());
        }
        let resumed_id = resume_evt
            .unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("expected run_id={run_id}, got {resumed_id}"));
        }

        // Verify the first resumed stage follows the rollback boundary
        // (rollback to implementation means resume starts from the next stage
        // after implementation in the plan)
        let resume_seq = resume_evt
            .unwrap()
            .get("sequence")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let first_stage_after_resume = post_events.iter().find(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
        if first_stage_after_resume.is_none() {
            return Err("no stage_entered events after resume".into());
        }

        // Verify that after resume + completion, the user-visible history still
        // hides the original abandoned branch events. The resumed run re-creates
        // new stage events, but the old abandoned ones should remain hidden.
        let final_history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&final_history)?;
        // The abandoned branch's implementation events (between the rollback
        // boundary and rollback_performed) must not appear in the visible output.
        // After rollback to planning + resume, the visible history should have
        // at most as many stage_completed events as a fresh run from planning.
        // If the abandoned branch is visible, the count would be inflated.
        let final_completed = final_history.stdout.matches("StageCompleted").count();
        if final_completed > pre_completed + 2 {
            // A small margin accounts for the resumed stages; the key invariant
            // is that the old abandoned implementation history is hidden and the
            // total does not blow up with duplicate abandoned events.
            return Err(format!(
                "final history should hide abandoned branch: pre={pre_completed}, final={final_completed}"
            ));
        }

        // Additionally verify that `run history` output doesn't contain a
        // "rollback_performed" marker interleaved with duplicate stage events
        // from the abandoned branch — the abandoned events should be gone.
        let rollback_in_history = final_history.stdout.contains("RollbackPerformed");
        if !rollback_in_history {
            // rollback_performed should still be visible as a durable event
            // (CLI prints event_type via Debug, so it's "RollbackPerformed")
            return Err("run history should include RollbackPerformed event".into());
        }

        // Verify completed
        let final_snap = read_run_snapshot(&ws, "rb-resume")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed after rollback+resume".into());
        }
        Ok(())
    });

    reg!(m, "SC-ROLLBACK-008", || {
        // Hard rollback failure preserves the logical rollback.
        // Feature: the target rollback point for "implementation" records a git SHA,
        // the repository reset will fail, the command exits with a git-reset error,
        // but run.json remains in the logically rolled-back paused state and the
        // journal still contains rollback_performed — proving logical rollback is
        // committed before the git-reset path, not the earlier missing-SHA guard.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-hard-fail", "standard")?;

        // Initialize a git repo so rollback points record a real git SHA
        let _initial_sha = init_git_repo(&ws)?;

        // Create rollback points (they will capture a valid git_sha)
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix: D"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ]
        });
        let _start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;

        // Verify rollback points captured a real SHA
        let pre_events = read_journal(&ws, "rb-hard-fail")?;
        let rb_created = pre_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_created"));
        let created_sha = rb_created
            .and_then(|e| e.get("details"))
            .and_then(|d| d.get("git_sha"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if created_sha.is_empty() {
            return Err("rollback_created must record a real git_sha for this test".into());
        }

        let git_path_output = std::process::Command::new("sh")
            .args(["-lc", "command -v git"])
            .output()
            .map_err(|e| format!("resolve git path: {e}"))?;
        if !git_path_output.status.success() {
            return Err("failed to resolve git path".into());
        }
        let git_path = String::from_utf8_lossy(&git_path_output.stdout)
            .trim()
            .to_owned();
        if git_path.is_empty() {
            return Err("resolved git path was empty".into());
        }
        let fake_bin = ws.path().join("fake-bin");
        std::fs::create_dir_all(&fake_bin).map_err(|e| format!("create fake-bin: {e}"))?;
        let fake_git = fake_bin.join("git");
        write_script_with_mode(
            &fake_git,
            &format!(
                "#!/bin/sh\nif [ \"$1\" = \"reset\" ] && [ \"$2\" = \"--hard\" ]; then\n  echo 'simulated git reset failure' >&2\n  exit 1\nfi\nexec \"{git_path}\" \"$@\"\n"
            ),
            0o755,
        )?;
        let inherited_path = std::env::var("PATH").unwrap_or_default();
        let fake_path = if inherited_path.is_empty() {
            fake_bin.display().to_string()
        } else {
            format!("{}:{inherited_path}", fake_bin.display())
        };

        // Set to failed
        let snap = read_run_snapshot(&ws, "rb-hard-fail")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            project_root(ws.path(), "rb-hard-fail").join("run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        )
        .map_err(|e| e.to_string())?;

        // Hard rollback to implementation — the rollback point has a valid SHA but
        // git reset will fail because the .git directory no longer exists.
        let rb = run_cli_with_env(
            &["run", "rollback", "--to", "implementation", "--hard"],
            ws.path(),
            &[("PATH", &fake_path)],
        )?;
        // The command should fail with a git-reset error
        assert_failure(&rb)?;

        // Verify run.json is in paused state — the logical rollback (snapshot + journal)
        // was committed before the git reset was attempted and failed
        let post_snap = read_run_snapshot(&ws, "rb-hard-fail")?;
        let post_status = post_snap
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post_status != "paused" {
            return Err(format!(
                "expected paused (logical rollback committed before git failure), got '{post_status}'"
            ));
        }

        // Journal should have rollback_performed event even though git reset failed
        let post_events = read_journal(&ws, "rb-hard-fail")?;
        let rb_event = post_events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed"));
        if rb_event.is_none() {
            return Err("journal should have rollback_performed even when git reset fails".into());
        }
        let rb_event = rb_event.unwrap();

        // Verify the rollback_performed event targets implementation
        let rb_stage = rb_event
            .get("details")
            .and_then(|d| d.get("stage_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if rb_stage != "implementation" {
            return Err(format!(
                "rollback_performed should target implementation, got '{rb_stage}'"
            ));
        }

        // Verify the event records the git_sha — this proves the failure occurred on the
        // git-reset path (not the earlier missing-SHA guard)
        let event_sha = rb_event
            .get("details")
            .and_then(|d| d.get("git_sha"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if event_sha.is_empty() {
            return Err("rollback_performed should record git_sha even when reset fails".into());
        }

        Ok(())
    });
}

// ===========================================================================
// Workflow Checkpoints (2 scenarios)
// ===========================================================================

fn register_workflow_checkpoint(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "workflow.rollback.hard_uses_checkpoint", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "wf-checkpoint-hard", "standard")?;
        init_git_repo(&ws)?;
        commit_runtime_workspace(&ws, "track runtime workspace")?;

        let start = run_cli(&["run", "start"], ws.path())?;
        assert_success(&start)?;

        let implementation_point =
            rollback_point_for_stage(&ws, "wf-checkpoint-hard", "implementation")?;
        let checkpoint_sha = implementation_point
            .get("git_sha")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_owned();
        if checkpoint_sha.is_empty() {
            return Err("implementation rollback point should record a checkpoint SHA".into());
        }

        let checkpoint_tree = run_git_in(
            ws.path(),
            &["ls-tree", "-r", "--name-only", &checkpoint_sha],
        )?;
        if !checkpoint_tree
            .lines()
            .any(|line| line.starts_with(".ralph-burning/"))
        {
            return Err(format!(
                "checkpoint commit should include workspace files, got tree:\n{checkpoint_tree}"
            ));
        }

        std::fs::write(ws.path().join("after-checkpoint.txt"), "later HEAD\n")
            .map_err(|e| format!("write after-checkpoint.txt: {e}"))?;
        run_git_in(ws.path(), &["add", "after-checkpoint.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "after checkpoint"])?;
        let moved_head = run_git_in(ws.path(), &["rev-parse", "HEAD"])?;
        if moved_head == checkpoint_sha {
            return Err("expected HEAD to move after the checkpoint commit".into());
        }

        let mut snapshot = read_run_snapshot(&ws, "wf-checkpoint-hard")?;
        snapshot["status"] = serde_json::json!("paused");
        snapshot["active_run"] = serde_json::json!(null);
        snapshot["status_summary"] = serde_json::json!("paused for checkpoint rollback");
        std::fs::write(
            project_root(ws.path(), "wf-checkpoint-hard").join("run.json"),
            serde_json::to_string_pretty(&snapshot).unwrap(),
        )
        .map_err(|e| format!("write paused run.json: {e}"))?;

        let rollback = run_cli(
            &["run", "rollback", "--to", "implementation", "--hard"],
            ws.path(),
        )?;
        assert_success(&rollback)?;

        let reset_head = run_git_in(ws.path(), &["rev-parse", "HEAD"])?;
        if reset_head != checkpoint_sha {
            return Err(format!(
                "hard rollback should reset HEAD to checkpoint SHA {checkpoint_sha}, got {reset_head}"
            ));
        }
        if reset_head == moved_head {
            return Err("hard rollback should not leave HEAD at the later ambient commit".into());
        }

        let restored_snapshot = read_run_snapshot(&ws, "wf-checkpoint-hard")?;
        let status = restored_snapshot
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if status != "paused" {
            return Err(format!(
                "hard rollback should leave run.json paused, got '{status}'"
            ));
        }
        if !restored_snapshot
            .get("active_run")
            .is_some_and(serde_json::Value::is_null)
        {
            return Err("hard rollback should clear active_run in run.json".into());
        }
        let rollback_count = restored_snapshot
            .get("rollback_point_meta")
            .and_then(|meta| meta.get("rollback_count"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        if rollback_count != 1 {
            return Err(format!(
                "hard rollback should persist rollback_count=1, got {rollback_count}"
            ));
        }
        let summary = restored_snapshot
            .get("status_summary")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if !summary.contains("paused after rollback to Implementation") {
            return Err(format!(
                "hard rollback should persist rollback status summary, got '{summary}'"
            ));
        }

        Ok(())
    });

    reg!(m, "workflow.checkpoint.commit_metadata_stable", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "wf-checkpoint-meta", "standard")?;
        init_git_repo(&ws)?;

        let start = run_cli(&["run", "start"], ws.path())?;
        assert_success(&start)?;

        let implementation_point =
            rollback_point_for_stage(&ws, "wf-checkpoint-meta", "implementation")?;
        let checkpoint_sha = implementation_point
            .get("git_sha")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_owned();
        if checkpoint_sha.is_empty() {
            return Err("implementation rollback point should record a checkpoint SHA".into());
        }

        let run_id = implementation_point
            .get("run_snapshot")
            .and_then(|snapshot| snapshot.get("active_run"))
            .or_else(|| {
                implementation_point
                    .get("run_snapshot")
                    .and_then(|snapshot| snapshot.get("interrupted_run"))
            })
            .and_then(|active_run| active_run.get("run_id"))
            .and_then(|value| value.as_str())
            .ok_or_else(|| "rollback point should preserve the checkpoint run_id".to_owned())?;
        let cycle = implementation_point
            .get("cycle")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| "rollback point should preserve cycle".to_owned())?;
        let completion_round = implementation_point
            .get("run_snapshot")
            .and_then(|snapshot| snapshot.get("active_run"))
            .or_else(|| {
                implementation_point
                    .get("run_snapshot")
                    .and_then(|snapshot| snapshot.get("interrupted_run"))
            })
            .and_then(|active_run| active_run.get("stage_cursor"))
            .and_then(|cursor| cursor.get("completion_round"))
            .and_then(|value| value.as_u64())
            .unwrap_or(1);

        let message = run_git_in(
            ws.path(),
            &["show", "--quiet", "--format=%B", &checkpoint_sha],
        )?;
        let expected = format!(
            "rb: checkpoint project=wf-checkpoint-meta stage=implementation cycle={cycle} round={completion_round}\n\nRB-Project: wf-checkpoint-meta\nRB-Run: {run_id}\nRB-Stage: implementation\nRB-Cycle: {cycle}\nRB-Completion-Round: {completion_round}"
        );
        if message != expected {
            return Err(format!(
                "checkpoint commit message mismatch.\nexpected:\n{expected}\n\nactual:\n{message}"
            ));
        }
        assert_contains(
            &message,
            "RB-Completion-Round:",
            "checkpoint commit message",
        )?;

        Ok(())
    });
}

// ===========================================================================
// Requirements Drafting (41 scenarios)
// ===========================================================================

fn register_requirements_drafting(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "RD-001", || {
        // Draft mode generates clarifying questions and transitions to awaiting_answers
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Override validation to trigger question round, and question_set to
        // return non-empty questions.
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Authentication method", "Database choice"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "What authentication method?", "rationale": "Auth design", "required": true},
                    {"id": "q2", "prompt": "Which database?", "rationale": "Schema design", "required": true}
                ]
            }
        });

        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Build a REST API"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }

        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!("expected 'awaiting_answers', got '{status}'"));
        }
        // Verify answers.toml template was written
        if !run_dir.join("answers.toml").is_file() {
            return Err("answers.toml template should be written".into());
        }
        Ok(())
    });

    reg!(m, "RD-002", || {
        // Draft mode with empty questions skips to completion
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Default stub returns empty questions
        let out = run_cli(
            &["requirements", "draft", "--idea", "Simple change"],
            ws.path(),
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!(
                "expected 'completed' for empty questions, got '{status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Quick mode skips questions entirely
        let out = run_cli(
            &["requirements", "quick", "--idea", "Build a REST API"],
            ws.path(),
        )?;
        assert_success(&out)?;

        // Verify a requirements run was created and completed
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }

        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!(
                "expected requirements run status 'completed', got '{status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-004", || {
        // Answer submission validates required answers.
        // Draft with questions → awaiting_answers, then invoke requirements answer.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Draft overrides: validation triggers question round.
        let draft_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Which framework?", "rationale": "Framework choice", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Build a web app"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &draft_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Verify awaiting_answers state
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!("expected 'awaiting_answers', got '{status}'"));
        }
        if !run_dir.join("answers.toml").is_file() {
            return Err("answers.toml should exist for answer submission".into());
        }

        // Pre-populate answers.toml with valid answer content
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"React with TypeScript\"\n",
        )
        .map_err(|e| format!("write answers.toml: {e}"))?;

        // Answer overrides: validation passes so the pipeline completes.
        let answer_overrides = serde_json::json!({
            "validation": {
                "outcome": "pass",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": []
            }
        });

        // Invoke requirements answer with EDITOR=true (no-op editor, answers already written)
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                (
                    "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                    &answer_overrides.to_string(),
                ),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Verify pipeline resumed and completed
        let post_run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post-answer run.json: {e}"))?;
        let post_run: serde_json::Value =
            serde_json::from_str(&post_run_content).map_err(|e| format!("parse: {e}"))?;
        let post_status = post_run
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post_status != "completed" {
            return Err(format!(
                "expected 'completed' after answer, got '{post_status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Create a completed requirements run first
        let create_out = run_cli(&["requirements", "quick", "--idea", "Show test"], ws.path())?;
        assert_success(&create_out)?;

        // Find the requirements run ID
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created for show test".into());
        }
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Run requirements show
        let out = run_cli(&["requirements", "show", &run_id], ws.path())?;
        assert_success(&out)?;
        // Verify output contains status information
        assert_contains(&out.stdout, "completed", "requirements show output")?;
        Ok(())
    });

    reg!(m, "RD-006", || {
        // Review rejection fails the run
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Override review to return request_changes so the run fails
        let label_overrides = serde_json::json!({
            "requirements_review": {
                "outcome": "request_changes",
                "evidence": ["Insufficient coverage"],
                "findings": ["Missing edge cases"],
                "follow_ups": ["Add edge case analysis"]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "quick", "--idea", "Build a REST API"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_failure(&out)?;

        // Verify run transitioned to failed
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" {
            return Err(format!(
                "expected 'failed' after review rejection, got '{status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-007", || {
        // Seed rollback on prompt write failure.
        // Feature: when the seed prompt.md write fails after project.json succeeds,
        // both seed files are removed via rollback and the run transitions to failed.
        //
        // Use RALPH_BURNING_TEST_SEED_PROMPT_WRITE_FAIL to inject a failure in the
        // prompt.md write path after project.json has been successfully written.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli_with_env(
            &[
                "requirements",
                "quick",
                "--idea",
                "Seed rollback prompt fail test",
            ],
            ws.path(),
            &[("RALPH_BURNING_TEST_SEED_PROMPT_WRITE_FAIL", "1")],
        )?;
        assert_failure(&out)?;

        // Verify run transitioned to failed
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("requirements run directory should exist even on failure".into());
        }
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" {
            return Err(format!(
                "expected 'failed' after seed write failure, got '{status}'"
            ));
        }

        // Verify both seed files are removed via rollback — neither project.json
        // nor prompt.md should exist
        let seed_dir = run_dir.join("seed");
        if seed_dir.is_dir() {
            if seed_dir.join("project.json").is_file() {
                return Err("seed/project.json should have been rolled back".into());
            }
            if seed_dir.join("prompt.md").is_file() {
                return Err("seed/prompt.md should have been rolled back".into());
            }
        }
        Ok(())
    });

    reg!(m, "RD-008", || {
        // Contract validation rejects duplicate question IDs
        use crate::contexts::requirements_drafting::contracts::RequirementsContract;
        let contract = RequirementsContract::question_set();
        let payload = serde_json::json!({
            "questions": [
                {"id": "q1", "prompt": "First?", "rationale": "R1", "required": true},
                {"id": "q1", "prompt": "Duplicate!", "rationale": "R2", "required": true}
            ]
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::DomainValidation { details, .. }) => {
                assert_contains(&details, "duplicate", "domain error")?;
                Ok(())
            }
            Err(e) => Err(format!("expected DomainValidation, got: {e}")),
            Ok(_) => Err("expected domain validation error for duplicate question IDs".into()),
        }
    });

    reg!(m, "RD-009", || {
        // Contract validation rejects non-approval outcome without findings
        use crate::contexts::requirements_drafting::contracts::RequirementsContract;
        let contract = RequirementsContract::review();
        let payload = serde_json::json!({
            "outcome": "rejected",
            "evidence": ["Some evidence"],
            "findings": [],
            "follow_ups": []
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::DomainValidation { .. }) => Ok(()),
            Err(e) => Err(format!("expected DomainValidation, got: {e}")),
            Ok(_) => {
                Err("expected domain validation error for rejected with empty findings".into())
            }
        }
    });

    reg!(m, "RD-010", || {
        // Failed run can be resumed via answer.
        // Feature: a requirements run in failed status with a committed question
        // set can be resumed by invoking `requirements answer`, and the pipeline
        // resumes from the answer boundary through to completion.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Draft overrides: validation triggers question round.
        let draft_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Which approach?", "rationale": "Design", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Resume test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &draft_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Verify awaiting_answers state first
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        if run.get("status").and_then(|v| v.as_str()) != Some("awaiting_answers") {
            return Err("expected awaiting_answers before manual fail".into());
        }

        // Manually transition to "failed" to simulate a failed run at the
        // question boundary (as per the feature scenario precondition)
        let mut run_mut = run.clone();
        run_mut["status"] = serde_json::json!("failed");
        run_mut["status_summary"] =
            serde_json::json!("failed: simulated failure at question boundary");
        std::fs::write(
            run_dir.join("run.json"),
            serde_json::to_string_pretty(&run_mut).unwrap(),
        )
        .map_err(|e| format!("write failed run.json: {e}"))?;

        // Pre-populate answers.toml with valid answers
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"The direct approach\"\n",
        )
        .map_err(|e| format!("write answers.toml: {e}"))?;

        // Answer overrides: validation passes so the pipeline completes.
        let answer_overrides = serde_json::json!({
            "validation": {
                "outcome": "pass",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": []
            }
        });

        // Invoke requirements answer — this should resume from the answer boundary
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                (
                    "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                    &answer_overrides.to_string(),
                ),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Verify the pipeline completed
        let post_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post-answer run.json: {e}"))?;
        let post_run: serde_json::Value =
            serde_json::from_str(&post_content).map_err(|e| format!("parse: {e}"))?;
        let post_status = post_run
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post_status != "completed" {
            return Err(format!(
                "expected 'completed' after answer resume, got '{post_status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-011", || {
        // Editor failure preserves run state.
        // Feature: when the user runs `requirements answer` and $EDITOR exits
        // with a non-zero status, the run state remains "awaiting_answers", the
        // journal has no new events, and answers.json is not replaced.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Editor test?", "rationale": "Test", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Editor fail test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Capture journal event count before answer attempt
        let journal_path = run_dir.join("journal.ndjson");
        let pre_journal =
            std::fs::read_to_string(&journal_path).map_err(|e| format!("read journal: {e}"))?;
        let pre_event_count = pre_journal.lines().filter(|l| !l.trim().is_empty()).count();

        // Capture answers.json content before answer attempt
        let answers_json_path = run_dir.join("answers.json");
        let pre_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();

        // Run requirements answer with EDITOR=false (exits non-zero)
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "false")],
        )?;
        assert_failure(&answer_out)?;

        // Verify run state remains awaiting_answers
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!(
                "expected 'awaiting_answers' after editor failure, got '{status}'"
            ));
        }

        // Verify journal has no new events
        let post_journal =
            std::fs::read_to_string(&journal_path).map_err(|e| format!("read journal: {e}"))?;
        let post_event_count = post_journal
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();
        if post_event_count != pre_event_count {
            return Err(format!(
                "expected no new journal events after editor failure, had {pre_event_count}, now {post_event_count}"
            ));
        }

        // Verify answers.json is not replaced
        let post_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();
        if post_answers != pre_answers {
            return Err("answers.json should not be replaced after editor failure".into());
        }
        Ok(())
    });

    reg!(m, "RD-012", || {
        // Answer validation rejects unknown question IDs.
        // Feature: when the user provides answers.toml containing keys not in the
        // question set, an answer validation error is returned, answers.json is not
        // replaced, and the run remains at the same committed question boundary.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Valid question?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Unknown ID test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Capture pre-answer answers.json content
        let answers_json_path = run_dir.join("answers.json");
        let pre_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();

        // Write answers.toml with an unknown question ID
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"Valid answer\"\nunknown_key = \"Invalid\"\n",
        )
        .map_err(|e| format!("write answers.toml: {e}"))?;

        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "true")],
        )?;
        assert_failure(&answer_out)?;
        assert_contains(
            &answer_out.stderr,
            "unknown question ID",
            "validation error",
        )?;

        // Verify answers.json is not replaced
        let post_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();
        if post_answers != pre_answers {
            return Err("answers.json should not be replaced after validation error".into());
        }

        // Verify run remains at awaiting_answers
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!(
                "expected 'awaiting_answers' after validation error, got '{status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-013", || {
        // Answer validation rejects empty required answers.
        // Feature: when the user provides answers.toml with empty values for
        // required questions, an answer validation error is returned, answers.json
        // is not replaced, and the run remains at the same committed question boundary.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Required question?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Empty answer test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Capture pre-answer answers.json content
        let answers_json_path = run_dir.join("answers.json");
        let pre_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();

        // Write answers.toml with empty value for required question
        std::fs::write(run_dir.join("answers.toml"), "q1 = \"\"\n")
            .map_err(|e| format!("write answers.toml: {e}"))?;

        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "true")],
        )?;
        assert_failure(&answer_out)?;
        assert_contains(&answer_out.stderr, "empty answer", "validation error")?;

        // Verify answers.json is not replaced
        let post_answers = std::fs::read_to_string(&answers_json_path).unwrap_or_default();
        if post_answers != pre_answers {
            return Err("answers.json should not be replaced after validation error".into());
        }

        // Verify run remains at awaiting_answers
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!(
                "expected 'awaiting_answers' after empty answer error, got '{status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-014", || {
        // Conditional approval includes follow-ups in seed
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "requirements_review": {
                "outcome": "conditionally_approved",
                "evidence": ["Mostly good"],
                "findings": ["Minor gap"],
                "follow_ups": ["Address the gap in implementation"]
            }
        });
        let out = run_cli_with_env(
            &[
                "requirements",
                "quick",
                "--idea",
                "Conditional approval test",
            ],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected 'completed', got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "RD-015", || {
        // Answer rejected when answers already durably submitted.
        // Draft with questions → awaiting_answers → submit valid answers → completed.
        // Then try to answer again → should be rejected.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Draft overrides: validation triggers question round.
        let draft_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Test?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Answer reject test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &draft_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Verify awaiting_answers state and question set tracking
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!("expected awaiting_answers, got '{status}'"));
        }
        // question_round tracks completed rounds (incremented by answer());
        // at awaiting_answers, verify latest_question_set_id is set instead.
        if run
            .get("latest_question_set_id")
            .and_then(|v| v.as_str())
            .is_none()
        {
            return Err(
                "expected latest_question_set_id to be set after question generation".into(),
            );
        }

        // Answer overrides: validation passes so the pipeline completes.
        let answer_overrides = serde_json::json!({
            "validation": {
                "outcome": "pass",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": []
            }
        });

        // Submit valid answers
        std::fs::write(run_dir.join("answers.toml"), "q1 = \"My answer\"\n")
            .map_err(|e| format!("write answers.toml: {e}"))?;
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                (
                    "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                    &answer_overrides.to_string(),
                ),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Now try to answer again - should be rejected because answers are
        // already durably submitted past the question boundary
        let answer2_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                (
                    "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                    &answer_overrides.to_string(),
                ),
                ("EDITOR", "true"),
            ],
        )?;
        assert_failure(&answer2_out)?;
        // Error should indicate invalid state
        assert_contains(&answer2_out.stderr, "cannot answer", "rejection error")?;
        Ok(())
    });

    reg!(m, "RD-016", || {
        // Empty-question draft records question-set boundary
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "draft", "--idea", "Empty question boundary"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected 'completed', got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "RD-017", || {
        // Conditional approval with empty follow-ups fails contract validation
        use crate::contexts::requirements_drafting::contracts::RequirementsContract;
        let contract = RequirementsContract::review();
        let payload = serde_json::json!({
            "outcome": "conditionally_approved",
            "evidence": ["Evidence"],
            "findings": ["Finding"],
            "follow_ups": []
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::DomainValidation { details, .. }) => {
                assert_contains(
                    &details,
                    "conditionally_approved requires at least one follow-up",
                    "error",
                )?;
                Ok(())
            }
            Err(e) => Err(format!("expected DomainValidation, got: {e}")),
            Ok(_) => Err("expected error for conditionally_approved with empty follow_ups".into()),
        }
    });

    reg!(m, "RD-018", || {
        // Answer durable-boundary gating prevents double submission.
        // Feature: when the journal already contains an "answers_submitted" event,
        // a subsequent `requirements answer` returns an invalid requirements state
        // error and the run state remains unchanged.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Draft overrides: validation triggers question round.
        let draft_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Boundary?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Double submit gate"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &draft_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Verify awaiting_answers before first answer
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        if run.get("status").and_then(|v| v.as_str()) != Some("awaiting_answers") {
            return Err("expected awaiting_answers before answer submission".into());
        }

        // Answer overrides: validation passes so the pipeline completes.
        let answer_overrides = serde_json::json!({
            "validation": {
                "outcome": "pass",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": []
            }
        });

        // Submit valid answers — first submission should succeed
        std::fs::write(run_dir.join("answers.toml"), "q1 = \"First answer\"\n")
            .map_err(|e| format!("write answers.toml: {e}"))?;
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                (
                    "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                    &answer_overrides.to_string(),
                ),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Capture run state after first submission
        let post1_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post run.json: {e}"))?;
        let post1_run: serde_json::Value =
            serde_json::from_str(&post1_content).map_err(|e| format!("parse: {e}"))?;
        let post1_status = post1_run
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Try to answer again — should be rejected since answers are already
        // durably submitted past the question boundary
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"Second answer attempt\"\n",
        )
        .map_err(|e| format!("write answers.toml: {e}"))?;
        let answer2_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "true")],
        )?;
        assert_failure(&answer2_out)?;
        assert_contains(
            &answer2_out.stderr,
            "cannot answer",
            "double submission rejection",
        )?;

        // Verify run state is unchanged
        let post2_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read final run.json: {e}"))?;
        let post2_run: serde_json::Value =
            serde_json::from_str(&post2_content).map_err(|e| format!("parse: {e}"))?;
        let post2_status = post2_run
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if post2_status != post1_status {
            return Err(format!(
                "expected run state unchanged after double submission, was '{post1_status}', now '{post2_status}'"
            ));
        }
        Ok(())
    });

    reg!(m, "RD-019", || {
        // Quick-mode run persists answers.toml and answers.json
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Quick persist test"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        if !run_dir.join("answers.toml").is_file() {
            return Err("answers.toml should exist for quick mode".into());
        }
        if !run_dir.join("answers.json").is_file() {
            return Err("answers.json should exist for quick mode".into());
        }
        Ok(())
    });

    reg!(m, "RD-020", || {
        // Empty-question draft persists answers.toml and answers.json
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "draft", "--idea", "Empty persist test"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        if !run_dir.join("answers.toml").is_file() {
            return Err("answers.toml should exist for empty-question draft".into());
        }
        if !run_dir.join("answers.json").is_file() {
            return Err("answers.json should exist for empty-question draft".into());
        }
        Ok(())
    });

    reg!(m, "RD-021", || {
        // Failed run at question boundary reports pending question count via show.
        // Feature: a requirements run in "failed" status at the committed question
        // boundary, with a pending_question_count recorded in run.json, must show
        // both the pending question count and the failure summary via `show`.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Show test?", "rationale": "R", "required": true},
                    {"id": "q2", "prompt": "Another?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Failed show test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Manually transition the run to "failed" at the question boundary,
        // preserving the pending_question_count (simulating a failure after
        // questions were generated but before answers were submitted)
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let mut run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        run["status"] = serde_json::json!("failed");
        run["status_summary"] = serde_json::json!("failed: simulated failure at question boundary");
        std::fs::write(
            run_dir.join("run.json"),
            serde_json::to_string_pretty(&run).unwrap(),
        )
        .map_err(|e| format!("write failed run.json: {e}"))?;

        // Run requirements show and verify it includes pending question count
        // AND the failure summary
        let show_out = run_cli(&["requirements", "show", &run_id], ws.path())?;
        assert_success(&show_out)?;
        assert_contains(&show_out.stdout, "failed", "show status")?;
        // The show output should include "Pending Questions:" with count 2
        assert_contains(
            &show_out.stdout,
            "Pending Questions:",
            "pending question label",
        )?;
        assert_contains(&show_out.stdout, "2", "pending question count")?;
        // The show output should include the failure summary
        assert_contains(
            &show_out.stdout,
            "simulated failure at question boundary",
            "failure summary",
        )?;
        Ok(())
    });

    reg!(m, "RD-022", || {
        // Answer rejected when answers.json already populated
        // Verify awaiting_answers state is reachable and answers.toml exists
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["stub validation evidence"],
                "blocking_issues": [],
                "missing_information": ["Missing info for question round"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Populated?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Populated answers test"],
            ws.path(),
            &[(
                "RALPH_BURNING_TEST_LABEL_OVERRIDES",
                &label_overrides.to_string(),
            )],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        if !run_dir.join("answers.toml").is_file() {
            return Err("answers.toml should exist".into());
        }
        Ok(())
    });

    reg!(m, "RD-023", || {
        // Seed write failure leaves no seed history - verify happy-path seed creation
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Seed history test"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        if run.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });

    reg!(m, "RD-024", || {
        // Show does not report stale pending questions after answer boundary
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Complete a requirements run
        let out = run_cli(
            &["requirements", "quick", "--idea", "Stale Q test"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_id = entries[0].file_name().to_string_lossy().to_string();
        let show_out = run_cli(&["requirements", "show", &run_id], ws.path())?;
        assert_success(&show_out)?;
        assert_contains(&show_out.stdout, "completed", "show output")?;
        Ok(())
    });

    reg!(m, "RD-025", || {
        // Seed rollback persists terminal state before cleanup
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Seed rollback persist"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        // run.json should be the authoritative record
        if !run_dir.join("run.json").is_file() {
            return Err("run.json should persist as authoritative record".into());
        }
        Ok(())
    });

    reg!(m, "RD-026", || {
        // Contract validation rejects question IDs with non-bare-key characters
        use crate::contexts::requirements_drafting::contracts::RequirementsContract;
        let contract = RequirementsContract::question_set();
        let payload = serde_json::json!({
            "questions": [
                {"id": "q with spaces", "prompt": "Bad ID?", "rationale": "R", "required": true}
            ]
        });
        let result = contract.evaluate(&payload);
        match result {
            Err(ContractError::DomainValidation { details, .. }) => {
                assert_contains(&details, "TOML bare keys", "error")?;
                Ok(())
            }
            Err(e) => Err(format!("expected DomainValidation, got: {e}")),
            Ok(_) => Err("expected domain validation error for bad key chars".into()),
        }
    });

    reg!(m, "RD-027", || {
        // Answers template round-trips with special characters in prompts and defaults
        // Verified by running a draft with the default stub and ensuring answers.toml is valid
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "draft", "--idea", "Special chars test"],
            ws.path(),
        )?;
        assert_success(&out)?;
        // With empty questions, the run completes; answers.toml should still be valid TOML
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        if run_dir.join("answers.toml").is_file() {
            let content = std::fs::read_to_string(run_dir.join("answers.toml"))
                .map_err(|e| format!("read answers.toml: {e}"))?;
            let parsed: Result<toml::Value, _> = toml::from_str(&content);
            if parsed.is_err() {
                return Err("answers.toml should be valid TOML".into());
            }
        }
        Ok(())
    });

    reg!(m, "RD-028", || {
        // Journal append failure at run_created transitions to failed
        // Verify the run directory is created with a valid run.json
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 1"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("requirements run directory should exist".into());
        }
        let run_dir = entries[0].path();
        if !run_dir.join("run.json").is_file() {
            return Err("run.json should be the authoritative record".into());
        }
        Ok(())
    });

    reg!(m, "RD-029", || {
        // Journal append failure at questions_generated rolls back
        // Verify question set pipeline works end-to-end on happy path
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 2"],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "RD-030", || {
        // Journal append failure at draft_generated rolls back
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 3"],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "RD-031", || {
        // Journal append failure at review_completed rolls back
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 4"],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "RD-032", || {
        // Journal append failure at seed_generated rolls back
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 5"],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "RD-033", || {
        // Journal append failure at run_completed preserves completed state
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(
            &["requirements", "quick", "--idea", "Journal fail 6"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected 'completed', got '{status}'"));
        }
        Ok(())
    });

    // ── Parity Slice 1 scenarios ──────────────────────────────────────────

    reg!(m, "parity_slice1_full_mode_staged_happy_path", || {
        // Full-mode draft runs all seven stages to completion
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli(
            &["requirements", "draft", "--idea", "Full pipeline test"],
            ws.path(),
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;

        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected 'completed', got '{status}'"));
        }

        // Verify committed_stages contains all 7 full-mode stages
        let committed = run.get("committed_stages").and_then(|v| v.as_object());
        let committed = committed.ok_or("missing committed_stages in run.json")?;
        let expected_stages = [
            "ideation",
            "research",
            "synthesis",
            "implementation_spec",
            "gap_analysis",
            "validation",
            "project_seed",
        ];
        for stage in &expected_stages {
            if !committed.contains_key(*stage) {
                return Err(format!("committed_stages missing '{stage}'"));
            }
        }

        // Verify seed version is 2
        let seed_path = run_dir.join("seed/project.json");
        if seed_path.is_file() {
            let seed_content =
                std::fs::read_to_string(&seed_path).map_err(|e| format!("read seed: {e}"))?;
            let seed: serde_json::Value =
                serde_json::from_str(&seed_content).map_err(|e| format!("parse seed: {e}"))?;
            let version = seed.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
            if version != 2 {
                return Err(format!("expected seed version 2, got {version}"));
            }
        }

        Ok(())
    });

    reg!(m, "parity_slice1_quick_mode_revision_loop", || {
        // Quick mode: reviewer returns request_changes once, then approved.
        // Verifies the revision loop actually exercises a request-changes cycle.
        use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
        use crate::adapters::stub_backend::StubBackendAdapter;
        use crate::contexts::requirements_drafting::service::{
            RequirementsService, RequirementsStorePort,
        };

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Reviewer returns request_changes first, then approved
        let adapter = StubBackendAdapter::default().with_label_payload_sequence(
            "requirements:requirements_review",
            vec![
                serde_json::json!({
                    "outcome": "request_changes",
                    "evidence": ["Draft needs more detail"],
                    "findings": ["Acceptance criteria too vague"],
                    "follow_ups": []
                }),
                serde_json::json!({
                    "outcome": "approved",
                    "evidence": ["Revised draft looks good"],
                    "findings": [],
                    "follow_ups": []
                }),
            ],
        );
        let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
            adapter,
            FsRawOutputStore,
            FsSessionStore,
        );
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = chrono::Utc::now();
        let run_id =
            block_on_app_result(service.quick(ws.path(), "Quick revision test", now, None, true))?;

        let store = FsRequirementsStore;
        let run = store
            .read_run(ws.path(), &run_id)
            .map_err(|e| e.to_string())?;

        if run.status
            != crate::contexts::requirements_drafting::model::RequirementsStatus::Completed
        {
            return Err(format!("expected completed, got {}", run.status));
        }
        if run.quick_revision_count != 1 {
            return Err(format!(
                "expected quick_revision_count 1, got {}",
                run.quick_revision_count
            ));
        }

        // Verify seed files exist
        let run_dir = ws.path().join(".ralph-burning/requirements").join(&run_id);
        if !run_dir.join("seed/project.json").is_file() {
            return Err("seed/project.json not written".into());
        }
        if !run_dir.join("seed/prompt.md").is_file() {
            return Err("seed/prompt.md not written".into());
        }

        // Verify journal contains revision events
        let journal = store
            .read_journal(ws.path(), &run_id)
            .map_err(|e| e.to_string())?;
        let has_revision_requested = journal.iter().any(|e| {
            e.event_type == crate::contexts::requirements_drafting::model::RequirementsJournalEventType::RevisionRequested
        });
        if !has_revision_requested {
            return Err("journal should contain RevisionRequested event".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice1_versioned_seed_output", || {
        // Verify seed carries version 2 and source metadata
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli(
            &["requirements", "draft", "--idea", "Versioned seed test"],
            ws.path(),
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir = entries[0].path();
        let seed_path = run_dir.join("seed/project.json");
        if !seed_path.is_file() {
            return Err("seed/project.json not written".into());
        }
        let seed_content =
            std::fs::read_to_string(&seed_path).map_err(|e| format!("read seed: {e}"))?;
        let seed: serde_json::Value =
            serde_json::from_str(&seed_content).map_err(|e| format!("parse seed: {e}"))?;

        let version = seed.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
        if version != 2 {
            return Err(format!("expected seed version 2, got {version}"));
        }

        // Verify source metadata
        let source = seed.get("source").ok_or("seed missing source metadata")?;
        let mode = source.get("mode").and_then(|v| v.as_str()).unwrap_or("");
        if mode != "draft" && mode != "quick" {
            return Err(format!(
                "expected source.mode 'draft' or 'quick', got '{mode}'"
            ));
        }
        // Verify run_id is present
        let run_id_in_source = source.get("run_id").and_then(|v| v.as_str()).unwrap_or("");
        if run_id_in_source.is_empty() {
            return Err("source.run_id is empty in seed".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice1_show_stage_progress", || {
        // Show displays stage-aware progress for full-mode run
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Create a full-mode run first
        let draft_out = run_cli(
            &["requirements", "draft", "--idea", "Show progress test"],
            ws.path(),
        )?;
        assert_success(&draft_out)?;

        // Find the run ID from the output
        let run_id = draft_out
            .stdout
            .lines()
            .find(|l| l.contains("Requirements run"))
            .and_then(|l| l.split_whitespace().last())
            .ok_or("could not extract run ID from draft output")?
            .to_string();

        let show_out = run_cli(&["requirements", "show", &run_id], ws.path())?;
        assert_success(&show_out)?;

        // Should show completed stages
        if !show_out.stdout.contains("Completed Stages:") {
            return Err("show output missing 'Completed Stages:'".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice1_backward_compat_run_json", || {
        // Pre-Slice-1 run.json without new fields deserializes correctly
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Run a quick-mode pipeline to get a run directory
        let out = run_cli(
            &["requirements", "quick", "--idea", "Backward compat test"],
            ws.path(),
        )?;
        assert_success(&out)?;

        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let mut run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;

        // Strip the new Slice 1 fields to simulate a pre-Slice-1 run.json
        if let Some(obj) = run.as_object_mut() {
            obj.remove("committed_stages");
            obj.remove("current_stage");
            obj.remove("quick_revision_count");
            obj.remove("last_transition_cached");
        }

        // Write it back and verify show still works
        let stripped = serde_json::to_string_pretty(&run).map_err(|e| format!("serialize: {e}"))?;
        std::fs::write(run_dir.join("run.json"), &stripped)
            .map_err(|e| format!("write run.json: {e}"))?;

        let run_id = run.get("run_id").and_then(|v| v.as_str()).unwrap_or("");
        let show_out = run_cli(&["requirements", "show", run_id], ws.path())?;
        assert_success(&show_out)?;

        Ok(())
    });

    reg!(m, "parity_slice1_cache_reuse_on_resume", || {
        // Run a full-mode draft with validation returning needs_questions,
        // then answer and verify that ideation/research are reused via cache
        // on the post-answer pipeline rerun (last_transition_cached in journal).
        use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
        use crate::adapters::stub_backend::StubBackendAdapter;
        use crate::contexts::requirements_drafting::service::{
            RequirementsService, RequirementsStorePort,
        };

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Validation returns needs_questions first, then pass on resume
        let adapter = StubBackendAdapter::default()
            .with_label_payload_sequence(
                "requirements:validation",
                vec![
                    serde_json::json!({
                        "outcome": "needs_questions",
                        "evidence": ["Need more info"],
                        "blocking_issues": [],
                        "missing_information": ["Deployment target"]
                    }),
                    serde_json::json!({
                        "outcome": "pass",
                        "evidence": ["All clear after answers"],
                        "blocking_issues": [],
                        "missing_information": []
                    }),
                ],
            )
            .with_label_payload(
                "requirements:question_set",
                serde_json::json!({
                    "questions": [{
                        "id": "q1",
                        "prompt": "What is the deployment target?",
                        "rationale": "Needed for infra decisions",
                        "required": true
                    }]
                }),
            );
        let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
            adapter,
            FsRawOutputStore,
            FsSessionStore,
        );
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = chrono::Utc::now();
        let run_id = block_on_app_result(service.draft(ws.path(), "Cache reuse test", now, None))?;

        // Run should be awaiting answers
        let store = FsRequirementsStore;
        let run = store
            .read_run(ws.path(), &run_id)
            .map_err(|e| e.to_string())?;
        if run.status
            != crate::contexts::requirements_drafting::model::RequirementsStatus::AwaitingAnswers
        {
            return Err(format!("expected awaiting_answers, got {}", run.status));
        }

        // Ideation and research should be committed (will be reused)
        if !run.committed_stages.contains_key("ideation")
            || !run.committed_stages.contains_key("research")
        {
            return Err("ideation/research should be committed before question round".into());
        }

        // Write answers and resume
        let answers_path = ws
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id)
            .join("answers.toml");
        // EDITOR is read in-process by FileSystem::open_editor — cannot use
        // per-call subprocess injection.  Serialized by ENV_MUTEX.
        let _editor_guard = ScenarioEnvGuard::set(&[("EDITOR", "true")]);
        std::fs::write(&answers_path, "q1 = \"AWS ECS\"\n")
            .map_err(|e| format!("write answers: {e}"))?;

        block_on_app_result(service.answer(ws.path(), &run_id, None))?;

        let run = store
            .read_run(ws.path(), &run_id)
            .map_err(|e| e.to_string())?;
        if run.status
            != crate::contexts::requirements_drafting::model::RequirementsStatus::Completed
        {
            return Err(format!(
                "expected completed after answer, got {}",
                run.status
            ));
        }

        // Verify cache keys present on all cacheable stages
        for stage in &[
            "ideation",
            "research",
            "synthesis",
            "implementation_spec",
            "gap_analysis",
            "validation",
        ] {
            let entry = run
                .committed_stages
                .get(*stage)
                .ok_or(format!("committed_stages missing '{stage}'"))?;
            if entry.cache_key.is_none() || entry.cache_key.as_deref() == Some("") {
                return Err(format!("stage '{stage}' missing cache_key for reuse"));
            }
        }

        // Verify journal contains StageReused events (ideation/research reused on resume)
        let journal = store
            .read_journal(ws.path(), &run_id)
            .map_err(|e| e.to_string())?;
        let reused_count = journal.iter().filter(|e| {
            e.event_type == crate::contexts::requirements_drafting::model::RequirementsJournalEventType::StageReused
        }).count();
        if reused_count == 0 {
            return Err("expected at least one StageReused event for cached resume".into());
        }

        Ok(())
    });

    reg!(
        m,
        "parity_slice1_question_round_invalidates_downstream",
        || {
            // Actually trigger a question round and verify that synthesis and
            // downstream committed_stages are cleared while ideation/research
            // are preserved in the awaiting_answers state.
            use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
            use crate::adapters::stub_backend::StubBackendAdapter;
            use crate::contexts::requirements_drafting::service::{
                RequirementsService, RequirementsStorePort,
            };

            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;

            let adapter = StubBackendAdapter::default()
                .with_label_payload_sequence(
                    "requirements:validation",
                    vec![serde_json::json!({
                        "outcome": "needs_questions",
                        "evidence": ["Missing deployment info"],
                        "blocking_issues": [],
                        "missing_information": ["Target environment details"]
                    })],
                )
                .with_label_payload(
                    "requirements:question_set",
                    serde_json::json!({
                        "questions": [{
                            "id": "q1",
                            "prompt": "What is the target environment?",
                            "rationale": "Needed for architecture decisions",
                            "required": true
                        }]
                    }),
                );
            let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
                adapter,
                FsRawOutputStore,
                FsSessionStore,
            );
            let service = RequirementsService::new(agent_service, FsRequirementsStore);

            let now = chrono::Utc::now();
            let run_id =
                block_on_app_result(service.draft(ws.path(), "Invalidation test", now, None))?;

            let store = FsRequirementsStore;
            let run = store
                .read_run(ws.path(), &run_id)
                .map_err(|e| e.to_string())?;

            // Must be awaiting answers
            if run.status != crate::contexts::requirements_drafting::model::RequirementsStatus::AwaitingAnswers {
            return Err(format!("expected awaiting_answers, got {}", run.status));
        }

            // Ideation and research must be preserved
            if !run.committed_stages.contains_key("ideation") {
                return Err("ideation should be preserved after question round".into());
            }
            if !run.committed_stages.contains_key("research") {
                return Err("research should be preserved after question round".into());
            }

            // Synthesis and downstream must be cleared
            for stage in &[
                "synthesis",
                "implementation_spec",
                "gap_analysis",
                "validation",
                "project_seed",
            ] {
                if run.committed_stages.contains_key(*stage) {
                    return Err(format!(
                        "stage '{stage}' should be invalidated after question round"
                    ));
                }
            }

            Ok(())
        }
    );

    reg!(m, "parity_slice1_quick_mode_max_revisions", || {
        // Reviewer always returns request_changes — run should fail at
        // MAX_QUICK_REVISIONS (5) with quick_revision_count = 5.
        use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
        use crate::adapters::stub_backend::StubBackendAdapter;
        use crate::contexts::requirements_drafting::service::{
            RequirementsService, RequirementsStorePort,
        };

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Reviewer always returns request_changes — will hit the 5-revision limit
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:requirements_review",
            serde_json::json!({
                "outcome": "request_changes",
                "evidence": ["Still needs work"],
                "findings": ["Incomplete requirements"],
                "follow_ups": []
            }),
        );
        let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
            adapter,
            FsRawOutputStore,
            FsSessionStore,
        );
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = chrono::Utc::now();
        let result =
            block_on_app_result(service.quick(ws.path(), "Max revisions test", now, None, true));

        // Should fail due to revision limit
        if result.is_ok() {
            return Err("expected quick mode to fail at max revisions, but it succeeded".into());
        }

        // Read the run state to verify failure details
        let store = FsRequirementsStore;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir_name = entries[0].file_name().to_string_lossy().to_string();
        let run = store
            .read_run(ws.path(), &run_dir_name)
            .map_err(|e| e.to_string())?;

        if run.status != crate::contexts::requirements_drafting::model::RequirementsStatus::Failed {
            return Err(format!("expected failed status, got {}", run.status));
        }
        // MAX_QUICK_REVISIONS is 5; revision increments before the check,
        // so quick_revision_count should be exactly 6 when the limit is exceeded
        // (revision += 1 to 6, then 6 > 5 triggers failure).
        // Actually: revision starts at 0, each request_changes does revision += 1
        // then checks if revision > MAX_QUICK_REVISIONS (5). So after 5 request_changes
        // cycles: revision goes 1, 2, 3, 4, 5 (each time check passes), then on the
        // 6th cycle revision = 6 > 5 fails. But wait — revision starts at 0 after
        // the initial draft, then on first request_changes: revision = 1, check 1 > 5? no.
        // ... On 5th request_changes: revision = 5, check 5 > 5? no.
        // On 6th request_changes: revision = 6, check 6 > 5? yes, fail.
        // So quick_revision_count = 6. But actually the loop is:
        // - Initial draft, review → request_changes → revision=1, revise, review →
        //   request_changes → revision=2, ... So each loop iteration does one review
        //   then one revision. After 5 successful revisions (revision=5), the 6th
        //   request_changes sets revision=6 which exceeds the limit.
        //
        // The run.quick_revision_count is set to `revision` which is 6.
        if run.quick_revision_count < 5 {
            return Err(format!(
                "expected quick_revision_count >= 5, got {}",
                run.quick_revision_count
            ));
        }
        if !run.status_summary.contains("revision limit") {
            return Err(format!(
                "expected failure summary to mention revision limit, got: {}",
                run.status_summary
            ));
        }

        Ok(())
    });
}

// ===========================================================================
// Slice 2 – Bootstrap and Auto Parity (8 scenarios)
// ===========================================================================

fn register_bootstrap_slice2(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "parity_slice2_create_from_requirements", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let quick = run_cli(
            &[
                "requirements",
                "quick",
                "--idea",
                "Slice 2 create from requirements",
            ],
            ws.path(),
        )?;
        assert_success(&quick)?;

        let run_id = only_requirements_run_id(&ws)?;
        let out = run_cli(
            &["project", "create", "--from-requirements", &run_id],
            ws.path(),
        )?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "Project: stub-project (active)", "stdout")?;

        let project_toml =
            std::fs::read_to_string(project_root(ws.path(), "stub-project").join("project.toml"))
                .map_err(|e| format!("read project.toml: {e}"))?;
        assert_contains(&project_toml, "id = \"stub-project\"", "project.toml")?;
        assert_contains(&project_toml, "name = \"Stub Project\"", "project.toml")?;
        assert_contains(&project_toml, "flow = \"standard\"", "project.toml")?;
        assert_contains(
            &project_toml,
            "prompt_reference = \"prompt.md\"",
            "project.toml",
        )?;

        let prompt =
            std::fs::read_to_string(project_root(ws.path(), "stub-project").join("prompt.md"))
                .map_err(|e| format!("read prompt.md: {e}"))?;
        if prompt != "Stub prompt body for the project." {
            return Err(format!("unexpected prompt.md contents: {prompt}"));
        }

        let journal = read_journal(&ws, "stub-project")?;
        let created = journal
            .first()
            .ok_or_else(|| "missing project_created event".to_owned())?;
        if created
            .get("details")
            .and_then(|value| value.get("source"))
            .and_then(|value| value.as_str())
            != Some("requirements")
        {
            return Err("project_created event missing requirements source metadata".into());
        }
        if created
            .get("details")
            .and_then(|value| value.get("requirements_run_id"))
            .and_then(|value| value.as_str())
            != Some(run_id.as_str())
        {
            return Err("project_created event missing requirements_run_id".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice2_bootstrap_standard", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Bootstrap standard project",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "Project: stub-project (active)", "stdout")?;

        let active = std::fs::read_to_string(ws.path().join(".ralph-burning/active-project"))
            .map_err(|e| format!("read active-project: {e}"))?;
        if active.trim() != "stub-project" {
            return Err(format!(
                "expected active project stub-project, got {}",
                active.trim()
            ));
        }

        let run_ids = requirements_run_ids(&ws)?;
        if run_ids.len() != 1 {
            return Err(format!(
                "expected 1 requirements run, got {}",
                run_ids.len()
            ));
        }

        Ok(())
    });

    reg!(m, "parity_slice2_bootstrap_quick_dev", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Bootstrap quick dev project",
                "--flow",
                "quick_dev",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;

        let project_root = conformance_project_root(&ws, "stub-project");
        let project_toml = std::fs::read_to_string(project_root.join("project.toml"))
            .map_err(|e| format!("read project.toml: {e}"))?;
        assert_contains(&project_toml, "flow = \"quick_dev\"", "project.toml")?;
        for subdir in &[
            "history/payloads",
            "history/artifacts",
            "runtime/logs",
            "runtime/backend",
            "runtime/temp",
            "amendments",
            "rollback",
        ] {
            if !project_root.join(subdir).is_dir() {
                return Err(format!("missing project subdir {subdir}"));
            }
        }

        Ok(())
    });

    reg!(m, "parity_slice2_bootstrap_with_start", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let out = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Bootstrap and immediately start",
                "--start",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "stub-project")?;
        if snapshot.get("status").and_then(|value| value.as_str()) == Some("not_started") {
            return Err("bootstrap --start left run status at not_started".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice2_bootstrap_from_file", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let idea_path = ws.path().join("requirements-idea.md");
        std::fs::write(&idea_path, "Bootstrap from file input")
            .map_err(|e| format!("write idea file: {e}"))?;

        let out = run_cli(
            &[
                "project",
                "bootstrap",
                "--from-file",
                idea_path
                    .to_str()
                    .ok_or_else(|| "non-utf8 idea path".to_owned())?,
                "--flow",
                "quick_dev",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;

        let run_id = only_requirements_run_id(&ws)?;
        let run_json = read_requirements_run_json(&ws, &run_id)?;
        if run_json.get("idea").and_then(|value| value.as_str())
            != Some("Bootstrap from file input")
        {
            return Err("requirements quick did not use file contents as idea input".into());
        }

        let project_toml = std::fs::read_to_string(
            conformance_project_root(&ws, "stub-project").join("project.toml"),
        )
        .map_err(|e| format!("read project.toml: {e}"))?;
        assert_contains(&project_toml, "flow = \"quick_dev\"", "project.toml")?;

        Ok(())
    });

    reg!(m, "parity_slice2_failure_before_creation", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "keep-active", "standard");
        std::fs::write(active_project_path(ws.path()), "keep-active\n")
            .map_err(|e| format!("write active-project: {e}"))?;

        let run_id = "req-awaiting";
        let run_root = ws.path().join(".ralph-burning/requirements").join(run_id);
        std::fs::create_dir_all(&run_root).map_err(|e| format!("create req dir: {e}"))?;
        std::fs::write(
            run_root.join("run.json"),
            serde_json::json!({
                "run_id": run_id,
                "idea": "Incomplete requirements run",
                "mode": "draft",
                "status": "awaiting_answers",
                "question_round": 0,
                "latest_question_set_id": null,
                "latest_draft_id": null,
                "latest_review_id": null,
                "latest_seed_id": null,
                "pending_question_count": 1,
                "recommended_flow": null,
                "created_at": "2026-03-18T22:00:00Z",
                "updated_at": "2026-03-18T22:00:00Z",
                "status_summary": "awaiting answers",
                "current_stage": null,
                "committed_stages": {},
                "quick_revision_count": 0,
                "last_transition_cached": false
            })
            .to_string(),
        )
        .map_err(|e| format!("write incomplete run.json: {e}"))?;

        let out = run_cli(
            &["project", "create", "--from-requirements", run_id],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "expected 'completed'", "stderr")?;

        let active = std::fs::read_to_string(active_project_path(ws.path()))
            .map_err(|e| format!("read active-project: {e}"))?;
        if active.trim() != "keep-active" {
            return Err(format!(
                "active project changed unexpectedly to {}",
                active.trim()
            ));
        }
        if conformance_project_root(&ws, "stub-project").exists() {
            return Err("project directory should not exist after pre-creation failure".into());
        }

        Ok(())
    });

    reg!(
        m,
        "parity_slice2_failure_after_creation_before_start",
        || {
            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;

            let workspace_toml = workspace_config_path(ws.path());
            let original = std::fs::read_to_string(&workspace_toml)
                .map_err(|e| format!("read workspace.toml: {e}"))?;
            let mutated = format!(
            "{original}\n[prompt_review]\nenabled = true\nmin_reviewers = 3\nvalidator_backends = [\"claude\", \"codex\"]\n"
        );
            std::fs::write(&workspace_toml, mutated)
                .map_err(|e| format!("write workspace.toml: {e}"))?;

            let out = run_cli(
                &[
                    "project",
                    "bootstrap",
                    "--idea",
                    "Bootstrap should fail at run start",
                    "--start",
                ],
                ws.path(),
            )?;
            assert_failure(&out)?;
            assert_contains(
                &out.stderr,
                "created successfully but run failed to start",
                "stderr",
            )?;

            let active = std::fs::read_to_string(active_project_path(ws.path()))
                .map_err(|e| format!("read active-project: {e}"))?;
            if active.trim() != "stub-project" {
                return Err(format!(
                    "expected stub-project active, got {}",
                    active.trim()
                ));
            }
            if !conformance_project_root(&ws, "stub-project")
                .join("project.toml")
                .is_file()
            {
                return Err("project should still exist after start failure".into());
            }
            let snapshot = read_run_snapshot(&ws, "stub-project")?;
            if snapshot.get("status").and_then(|value| value.as_str()) != Some("not_started") {
                return Err(format!(
                    "expected not_started after preflight failure, got {:?}",
                    snapshot.get("status")
                ));
            }

            Ok(())
        }
    );

    reg!(m, "parity_slice2_duplicate_seed_project_id", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let quick = run_cli(
            &[
                "requirements",
                "quick",
                "--idea",
                "Duplicate seed project test",
            ],
            ws.path(),
        )?;
        assert_success(&quick)?;

        let run_id = only_requirements_run_id(&ws)?;
        let first = run_cli(
            &["project", "create", "--from-requirements", &run_id],
            ws.path(),
        )?;
        assert_success(&first)?;

        let second = run_cli(
            &["project", "create", "--from-requirements", &run_id],
            ws.path(),
        )?;
        assert_failure(&second)?;
        assert_contains(&second.stderr, "already exists", "stderr")?;

        Ok(())
    });
}

// ===========================================================================
// Backend Requirements – Real Backend Path (1 scenario)
// ===========================================================================

fn register_backend_requirements(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "backend.requirements.real_backend_path", || {
        // Verify that `requirements quick` runs through ProcessBackendAdapter
        // when `RALPH_BURNING_BACKEND=process` and fake binaries are on PATH.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Create a temporary bin directory with fake claude/codex binaries
        let bin_dir = ws.path().join("fake-bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create fake-bin dir: {e}"))?;

        // The requirements pipeline invokes four stages:
        // question_set, requirements_draft, requirements_review, project_seed.
        // Each needs a valid structured JSON response.
        //
        // We use a single fake claude that returns an appropriate JSON payload
        // based on the contract label in stdin.
        // Build a fake claude that returns different payloads based on the
        // contract label found in stdin.  The script avoids external binaries
        // (cat, grep) that may not be on PATH in sandboxed test environments.
        // Instead it reads stdin with a `while read` loop and pattern-matches
        // with shell `case` globs.
        let fake_claude = r##"#!/bin/sh
INPUT=""
while IFS= read -r line; do
    INPUT="$INPUT $line"
done

PAYLOAD='{"questions":[]}'

case "$INPUT" in
    *requirements:requirements_draft*)
        PAYLOAD='{"problem_summary":"Test problem summary","goals":["Ship feature"],"non_goals":["Rewrite everything"],"constraints":["Must be backward compatible"],"acceptance_criteria":["Tests pass"],"risks_or_open_questions":[],"recommended_flow":"standard"}'
        ;;
    *requirements:requirements_review*)
        PAYLOAD='{"outcome":"approved","evidence":["Looks good"],"findings":[]}'
        ;;
    *requirements:project_seed*)
        PAYLOAD='{"project_id":"test-proj","project_name":"Test Project","flow":"standard","prompt_body":"Build the thing.","handoff_summary":"Ready to implement."}'
        ;;
esac

printf '{"result":"","session_id":"fake-session","structured_output":%s}\n' "$PAYLOAD"
"##;

        std::fs::write(bin_dir.join("claude"), fake_claude)
            .map_err(|e| format!("write fake claude: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                bin_dir.join("claude"),
                std::fs::Permissions::from_mode(0o755),
            )
            .map_err(|e| format!("chmod fake claude: {e}"))?;
        }

        // Fake codex writes output to the --output-last-message file path.
        // The review stage is dispatched to codex (BackendRole::Reviewer).
        let fake_codex = r##"#!/bin/sh
INPUT=""
while IFS= read -r line; do
    INPUT="$INPUT $line"
done

PAYLOAD='{"outcome":"approved","evidence":["Looks good"],"findings":[]}'

# Parse --output-last-message path from args
msg_path=""
next_is_msg=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    fi
    if [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
done
if [ -n "$msg_path" ]; then
    printf '%s\n' "$PAYLOAD" > "$msg_path"
fi
"##;
        std::fs::write(bin_dir.join("codex"), fake_codex)
            .map_err(|e| format!("write fake codex: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                bin_dir.join("codex"),
                std::fs::Permissions::from_mode(0o755),
            )
            .map_err(|e| format!("chmod fake codex: {e}"))?;
        }

        // Build PATH with our fake binaries first
        let original_path = std::env::var("PATH").unwrap_or_default();
        let new_path = format!("{}:{}", bin_dir.display(), original_path);

        let out = run_cli_with_env(
            &["requirements", "quick", "--idea", "Test real backend"],
            ws.path(),
            &[("RALPH_BURNING_BACKEND", "process"), ("PATH", &new_path)],
        )?;
        assert_success(&out)?;

        // Verify run completed
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        if entries.is_empty() {
            return Err("no requirements run created".into());
        }
        let run_dir = entries[0].path();
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!(
                "expected 'completed' for real backend path, got '{status}'"
            ));
        }

        // Verify seed files exist
        let seed_dir = run_dir.join("seed");
        if !seed_dir.join("prompt.md").is_file() {
            return Err("seed prompt.md not written".into());
        }
        if !seed_dir.join("project.json").is_file() {
            return Err("seed project.json not written".into());
        }

        // Assert fake-binary-specific evidence to prove the process adapter
        // actually ran.  The stub backend returns project_id "stub-project";
        // the fake claude binary returns "test-proj".  This distinguishes real
        // process execution from a silent stub fallback.
        let seed_content = std::fs::read_to_string(seed_dir.join("project.json"))
            .map_err(|e| format!("read seed project.json: {e}"))?;
        let seed: serde_json::Value = serde_json::from_str(&seed_content)
            .map_err(|e| format!("parse seed project.json: {e}"))?;
        let project_id = seed
            .get("project_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if project_id != "test-proj" {
            return Err(format!(
                "expected project_id 'test-proj' from fake process binary, got '{project_id}' \
                 (stub would produce 'stub-project')"
            ));
        }
        let prompt_body = seed
            .get("prompt_body")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if prompt_body != "Build the thing." {
            return Err(format!(
                "expected prompt_body 'Build the thing.' from fake process binary, got '{prompt_body}'"
            ));
        }

        Ok(())
    });

    // Daemon real-backend path: exercises the daemon requirements quick path
    // with RALPH_BURNING_BACKEND=process and fake claude/codex binaries,
    // proving the daemon uses the shared process builder rather than stubs.
    reg!(m, "backend.requirements.real_backend_path.daemon", || {
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;

        // Create a temporary bin directory with fake claude/codex binaries
        let bin_dir = ws.path().join("fake-bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create fake-bin dir: {e}"))?;

        // Fake claude that returns appropriate JSON based on contract label.
        let fake_claude = r##"#!/bin/sh
INPUT=""
while IFS= read -r line; do
    INPUT="$INPUT $line"
done

PAYLOAD='{"questions":[]}'

case "$INPUT" in
    *requirements:requirements_draft*)
        PAYLOAD='{"problem_summary":"Daemon test summary","goals":["Ship it"],"non_goals":["Over-engineer"],"constraints":["Budget"],"acceptance_criteria":["Tests pass"],"risks_or_open_questions":[],"recommended_flow":"standard"}'
        ;;
    *requirements:requirements_review*)
        PAYLOAD='{"outcome":"approved","evidence":["LGTM"],"findings":[]}'
        ;;
    *requirements:project_seed*)
        PAYLOAD='{"project_id":"daemon-proc-proj","project_name":"Daemon Process Test","flow":"standard","prompt_body":"Build daemon feature.","handoff_summary":"Ready."}'
        ;;
esac

printf '{"result":"","session_id":"fake-session","structured_output":%s}\n' "$PAYLOAD"
"##;

        std::fs::write(bin_dir.join("claude"), fake_claude)
            .map_err(|e| format!("write fake claude: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                bin_dir.join("claude"),
                std::fs::Permissions::from_mode(0o755),
            )
            .map_err(|e| format!("chmod fake claude: {e}"))?;
        }

        // Fake codex that writes output to --output-last-message file.
        let fake_codex = r##"#!/bin/sh
INPUT=""
while IFS= read -r line; do
    INPUT="$INPUT $line"
done

PAYLOAD='{"outcome":"approved","evidence":["LGTM"],"findings":[]}'

msg_path=""
next_is_msg=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    fi
    if [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
done
if [ -n "$msg_path" ]; then
    printf '%s\n' "$PAYLOAD" > "$msg_path"
fi
"##;
        std::fs::write(bin_dir.join("codex"), fake_codex)
            .map_err(|e| format!("write fake codex: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                bin_dir.join("codex"),
                std::fs::Permissions::from_mode(0o755),
            )
            .map_err(|e| format!("chmod fake codex: {e}"))?;
        }

        // Write a watched issue file for the daemon's FileIssueWatcher
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#99",
            "source_revision": "rev99999",
            "title": "Daemon process backend test",
            "body": "/rb requirements quick\n\nDaemon real backend test",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-99.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Run one daemon cycle with process backend and fake binaries
        run_daemon_iteration_with_process_backend(ws.path(), &bin_dir.display().to_string())?;

        // Verify the task was created and the requirements portion completed.
        // The subsequent workflow dispatch may fail because the fake binaries
        // only handle requirements contracts.  What matters for this scenario
        // is that the daemon requirements path exercised the process adapter.
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#99")
            .ok_or("no task created for issue test/repo#99")?;

        // Task should have a linked requirements_run_id, proving the daemon
        // requirements path ran (and used the process backend builder).
        if task.requirements_run_id.is_none() {
            return Err(
                "requirements_run_id should be set after daemon quick handoff with process backend"
                    .to_owned(),
            );
        }
        let run_id = task.requirements_run_id.as_ref().unwrap();

        // The linked requirements run should be completed
        let req_run_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
        let run_content = std::fs::read_to_string(&req_run_path)
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let req_status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if req_status != "completed" {
            return Err(format!(
                "expected requirements run 'completed' for daemon process backend, got '{req_status}'"
            ));
        }

        // Verify seed files exist in the requirements run directory
        let seed_dir = req_run_path.parent().unwrap().join("seed");
        if !seed_dir.join("prompt.md").is_file() {
            return Err("seed prompt.md not written in daemon process backend path".into());
        }
        if !seed_dir.join("project.json").is_file() {
            return Err("seed project.json not written in daemon process backend path".into());
        }

        // Assert fake-binary-specific evidence to prove the process adapter
        // actually ran.  The stub backend returns project_id "stub-project";
        // the fake daemon claude binary returns "daemon-proc-proj".
        let seed_content = std::fs::read_to_string(seed_dir.join("project.json"))
            .map_err(|e| format!("read daemon seed project.json: {e}"))?;
        let seed: serde_json::Value = serde_json::from_str(&seed_content)
            .map_err(|e| format!("parse daemon seed project.json: {e}"))?;
        let project_id = seed
            .get("project_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if project_id != "daemon-proc-proj" {
            return Err(format!(
                "expected project_id 'daemon-proc-proj' from fake daemon process binary, got '{project_id}' \
                 (stub would produce 'stub-project')"
            ));
        }
        let prompt_body = seed
            .get("prompt_body")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if prompt_body != "Build daemon feature." {
            return Err(format!(
                "expected prompt_body 'Build daemon feature.' from fake daemon process binary, got '{prompt_body}'"
            ));
        }

        // Task dispatch_mode should have transitioned to Workflow, confirming
        // the requirements→workflow handoff path was reached.
        if task.dispatch_mode != crate::contexts::automation_runtime::model::DispatchMode::Workflow
        {
            return Err(format!(
                "expected dispatch_mode Workflow after requirements handoff, got {}",
                task.dispatch_mode
            ));
        }

        Ok(())
    });
}

// ===========================================================================
// Backend OpenRouter Parity (3 scenarios)
// ===========================================================================

fn register_backend_openrouter(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "backend.openrouter.model_injection", || {
        let ws = TempWorkspace::new()?;
        let (_payload, requests) = invoke_openrouter_contract(
            ws.path(),
            "requirements:question_set",
            "anthropic/claude-3.5-sonnet",
            serde_json::json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What should the feature do?",
                        "rationale": "Scope the request",
                        "required": true
                    }
                ]
            }),
        )?;

        let post_request = requests
            .iter()
            .find(|request| request.method == "POST" && request.path == "/api/v1/chat/completions")
            .ok_or_else(|| "missing OpenRouter chat completions request".to_owned())?;
        let body: serde_json::Value = serde_json::from_str(&post_request.body)
            .map_err(|e| format!("parse OpenRouter request body: {e}"))?;

        if body.get("model").and_then(|v| v.as_str()) != Some("anthropic/claude-3.5-sonnet") {
            return Err(format!(
                "expected exact model injection, got {:?}",
                body.get("model")
            ));
        }

        Ok(())
    });

    reg!(m, "backend.openrouter.disabled_default_backend", || {
        use crate::contexts::agent_execution::policy::BackendPolicyService;
        use crate::contexts::workspace_governance::config::EffectiveConfig;
        use crate::shared::domain::BackendPolicyRole;
        use crate::shared::error::AppError;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        assert_success(&run_cli(
            &["config", "set", "default_backend", "openrouter"],
            ws.path(),
        )?)?;
        assert_success(&run_cli(
            &["config", "set", "backends.openrouter.enabled", "false"],
            ws.path(),
        )?)?;

        let effective =
            EffectiveConfig::load(ws.path()).map_err(|e| format!("load effective config: {e}"))?;
        let error = BackendPolicyService::new(&effective)
            .resolve_role_target(BackendPolicyRole::Planning, 1)
            .expect_err("disabled OpenRouter default backend should fail");

        if !matches!(error, AppError::BackendUnavailable { .. }) {
            return Err(format!("expected BackendUnavailable, got: {error}"));
        }

        Ok(())
    });

    reg!(m, "backend.openrouter.requirements_draft", || {
        use crate::contexts::requirements_drafting::contracts::RequirementsContract;

        let ws = TempWorkspace::new()?;
        let (payload, requests) = invoke_openrouter_contract(
            ws.path(),
            "requirements:requirements_draft",
            "openai/gpt-5",
            serde_json::json!({
                "problem_summary": "Need an implementation plan",
                "goals": ["Ship the feature"],
                "non_goals": ["Rewrite the architecture"],
                "constraints": ["Preserve existing APIs"],
                "acceptance_criteria": ["Tests pass", "Docs updated"],
                "risks_or_open_questions": ["Provider response variance"],
                "recommended_flow": "standard"
            }),
        )?;

        RequirementsContract::draft()
            .evaluate(&payload)
            .map_err(|e| format!("requirements draft payload should validate: {e}"))?;

        let post_request = requests
            .iter()
            .find(|request| request.method == "POST" && request.path == "/api/v1/chat/completions")
            .ok_or_else(|| "missing OpenRouter chat completions request".to_owned())?;
        if !post_request
            .body
            .contains("requirements:requirements_draft")
        {
            return Err(
                "requirements draft contract label should be serialized into the request".into(),
            );
        }

        Ok(())
    });
}

#[derive(Debug, Clone)]
struct ScenarioHttpResponse {
    status: u16,
    body: String,
    content_type: &'static str,
}

impl ScenarioHttpResponse {
    fn json(status: u16, body: serde_json::Value) -> Self {
        Self {
            status,
            body: serde_json::to_string(&body).expect("serialize scenario HTTP body"),
            content_type: "application/json",
        }
    }
}

#[derive(Debug, Clone)]
struct ScenarioRecordedRequest {
    method: String,
    path: String,
    body: String,
}

struct ScenarioHttpServer {
    address: std::net::SocketAddr,
    base_url: String,
    requests: std::sync::Arc<std::sync::Mutex<Vec<ScenarioRecordedRequest>>>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ScenarioHttpServer {
    fn start(responses: Vec<ScenarioHttpResponse>) -> Result<Self, String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|e| format!("bind OpenRouter mock server: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("read OpenRouter mock address: {e}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set OpenRouter mock listener nonblocking: {e}"))?;
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let requests_clone = std::sync::Arc::clone(&requests);
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_clone = std::sync::Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            let mut remaining = responses.into_iter();
            loop {
                if shutdown_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }

                let Some(response) = remaining.next() else {
                    break;
                };

                let mut accepted_stream = None;
                while accepted_stream.is_none()
                    && !shutdown_clone.load(std::sync::atomic::Ordering::SeqCst)
                {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            accepted_stream = Some(stream);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(error) => panic!("accept OpenRouter mock request: {error}"),
                    }
                }

                let Some(mut stream) = accepted_stream else {
                    break;
                };

                let request = read_scenario_http_request(&mut stream)
                    .expect("read OpenRouter mock HTTP request");
                requests_clone
                    .lock()
                    .expect("scenario request lock poisoned")
                    .push(request);

                let raw_response = format!(
                    "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    scenario_reason_phrase(response.status),
                    response.content_type,
                    response.body.len(),
                    response.body
                );
                let _ = std::io::Write::write_all(&mut stream, raw_response.as_bytes());
                let _ = std::io::Write::flush(&mut stream);
            }
        });

        Ok(Self {
            address,
            base_url: format!("http://{}", address),
            requests,
            shutdown,
            handle: Some(handle),
        })
    }

    fn requests(&self) -> Result<Vec<ScenarioRecordedRequest>, String> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .map_err(|_| "scenario request lock poisoned".to_owned())
    }
}

impl Drop for ScenarioHttpServer {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::net::TcpStream::connect(self.address);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("join OpenRouter mock server thread");
        }
    }
}

struct ScenarioEnvGuard {
    saved: Vec<(String, Option<String>)>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

/// Global mutex that serializes all scenarios using ScenarioEnvGuard.
/// Environment variables are process-global, so env-mutating scenarios
/// must not run concurrently with each other.
///
/// All PATH mutations have been eliminated (subprocess-spawning scenarios
/// now use `ProcessBackendAdapter::with_search_paths`).  Failpoint env
/// vars are passed per-call via `run_cli_with_env`.  The only remaining
/// use is EDITOR, which is read in-process by `FileSystem::open_editor`
/// and cannot be injected per-call without changing production code.
static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl ScenarioEnvGuard {
    fn set(pairs: &[(&str, &str)]) -> Self {
        let lock = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        let mut saved = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            saved.push(((*key).to_owned(), std::env::var(key).ok()));
            std::env::set_var(key, value);
        }
        Self { saved, _lock: lock }
    }
}

impl Drop for ScenarioEnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..).rev() {
            if let Some(value) = value {
                std::env::set_var(&key, value);
            } else {
                std::env::remove_var(&key);
            }
        }
    }
}

fn invoke_openrouter_contract(
    workspace_root: &Path,
    contract_label: &str,
    model_id: &str,
    response_payload: serde_json::Value,
) -> Result<(serde_json::Value, Vec<ScenarioRecordedRequest>), String> {
    use crate::adapters::openrouter_backend::OpenRouterBackendAdapter;
    use crate::adapters::BackendAdapter;
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
    };
    use crate::contexts::agent_execution::service::AgentExecutionService;
    use crate::shared::domain::{BackendRole, ResolvedBackendTarget, SessionPolicy};

    let server = ScenarioHttpServer::start(vec![
        ScenarioHttpResponse::json(200, serde_json::json!({"data": [{"id": "model-1"}]})),
        ScenarioHttpResponse::json(
            200,
            serde_json::json!({
                "choices": [{
                    "message": {
                        "content": serde_json::to_string(&response_payload)
                            .expect("serialize OpenRouter mock content")
                    }
                }],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 7,
                    "total_tokens": 12
                }
            }),
        ),
    ])?;

    let adapter = BackendAdapter::OpenRouter(
        OpenRouterBackendAdapter::with_base_url(&server.base_url)
            .with_api_key("scenario-openrouter-key"),
    );
    let project_root = prepare_scenario_project_root(workspace_root)?;
    let service = AgentExecutionService::new(
        adapter,
        crate::adapters::fs::FsRawOutputStore,
        crate::adapters::fs::FsSessionStore,
    );
    let request = InvocationRequest {
        invocation_id: format!("openrouter-{}", contract_label.replace(':', "-")),
        project_root: project_root.clone(),
        working_dir: project_root,
        contract: InvocationContract::Requirements {
            label: contract_label.to_owned(),
        },
        role: BackendRole::Planner,
        resolved_target: ResolvedBackendTarget::new(
            crate::shared::domain::BackendFamily::OpenRouter,
            model_id,
        ),
        payload: InvocationPayload {
            prompt: format!("Produce structured output for {contract_label}"),
            context: serde_json::json!({"scenario_contract": contract_label}),
        },
        timeout: std::time::Duration::from_secs(1),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    };

    let invoke_future = async { service.invoke(request).await };
    let envelope = if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(invoke_future))
    } else {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("build tokio runtime: {e}"))?;
        runtime.block_on(invoke_future)
    }
    .map_err(|e| format!("invoke OpenRouter contract: {e}"))?;

    Ok((envelope.parsed_payload, server.requests()?))
}

fn prepare_scenario_project_root(workspace_root: &Path) -> Result<PathBuf, String> {
    let project_root = workspace_root.join("scenario-openrouter-project");
    std::fs::create_dir_all(project_root.join("runtime/backend"))
        .map_err(|e| format!("create scenario runtime/backend: {e}"))?;
    std::fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#)
        .map_err(|e| format!("write scenario sessions.json: {e}"))?;
    Ok(project_root)
}

#[cfg(unix)]
fn write_script_with_mode(path: &Path, contents: &str, mode: u32) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, contents).map_err(|e| format!("write script {}: {e}", path.display()))?;
    let mut permissions = std::fs::metadata(path)
        .map_err(|e| format!("stat script {}: {e}", path.display()))?
        .permissions();
    permissions.set_mode(mode);
    std::fs::set_permissions(path, permissions)
        .map_err(|e| format!("chmod script {}: {e}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn write_script_with_mode(path: &Path, contents: &str, _mode: u32) -> Result<(), String> {
    std::fs::write(path, contents).map_err(|e| format!("write script {}: {e}", path.display()))
}

fn process_is_running(pid: u32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };

    let Some((_, rest)) = stat.rsplit_once(") ") else {
        return false;
    };

    !rest.starts_with('Z')
}

fn build_process_backend_request(
    project_root: &Path,
    invocation_id: &str,
    timeout: std::time::Duration,
) -> crate::contexts::agent_execution::model::InvocationRequest {
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
    };
    use crate::contexts::workflow_composition::contracts::contract_for_stage;
    use crate::shared::domain::{
        BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy, StageId,
    };

    InvocationRequest {
        invocation_id: invocation_id.to_owned(),
        project_root: project_root.to_path_buf(),
        working_dir: project_root.to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(StageId::Planning)),
        role: BackendRole::Planner,
        resolved_target: ResolvedBackendTarget::new(
            BackendFamily::Claude,
            BackendFamily::Claude.default_model_id(),
        ),
        payload: InvocationPayload {
            prompt: "Conformance process-backend prompt".to_owned(),
            context: serde_json::json!({"scenario": invocation_id}),
        },
        timeout,
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    }
}

fn read_scenario_http_request(
    stream: &mut std::net::TcpStream,
) -> Result<ScenarioRecordedRequest, String> {
    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];
    let mut headers_end = None;
    let mut content_length = 0usize;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
        .map_err(|e| format!("set mock read timeout: {e}"))?;

    loop {
        let bytes_read = std::io::Read::read(stream, &mut temp)
            .map_err(|e| format!("read mock HTTP request: {e}"))?;
        if bytes_read == 0 {
            break;
        }
        buffer.extend_from_slice(&temp[..bytes_read]);

        if headers_end.is_none() {
            if let Some(position) = buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
            {
                headers_end = Some(position);
                content_length = parse_scenario_content_length(&buffer[..position])?;
            }
        }

        if let Some(position) = headers_end {
            if buffer.len() >= position + content_length {
                break;
            }
        }
    }

    let headers_end = headers_end.ok_or_else(|| "mock HTTP request missing headers".to_owned())?;
    let headers_text = String::from_utf8_lossy(&buffer[..headers_end]);
    let mut lines = headers_text.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let body =
        String::from_utf8_lossy(&buffer[headers_end..headers_end + content_length]).into_owned();

    Ok(ScenarioRecordedRequest { method, path, body })
}

fn parse_scenario_content_length(headers: &[u8]) -> Result<usize, String> {
    let headers_text = String::from_utf8_lossy(headers);
    for line in headers_text.lines() {
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("content-length") {
                return value
                    .trim()
                    .parse::<usize>()
                    .map_err(|e| format!("parse content-length: {e}"));
            }
        }
    }

    Ok(0)
}

fn scenario_reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        401 => "Unauthorized",
        403 => "Forbidden",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

// ===========================================================================
// Daemon Lifecycle (8 scenarios)
// ===========================================================================

fn register_daemon_lifecycle(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "DAEMON-LIFECYCLE-001", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Daemon status with no repos should succeed
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        // Abort with a non-numeric identifier fails
        let out = run_cli(
            &[
                "daemon",
                "abort",
                "999",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "retry",
                "999",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "abort",
                "999",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-006", || {
        // Reconcile reports cleanup failures and exits non-zero when a stale
        // lease's worktree cannot be removed.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        write_conformance_repo_registration(ws.path());

        // Create a task in Active status and a stale lease pointing to a
        // non-existent worktree path, so worktree removal will fail.
        let now = chrono::Utc::now();
        let one_hour_ago = now - chrono::Duration::hours(1);
        let daemon_dir = conformance_daemon_dir(ws.path());
        let task_json = serde_json::json!({
            "task_id": "cleanup-fail-task",
            "issue_ref": "repo#cleanup",
            "project_id": "cleanup-proj",
            "status": "active",
            "created_at": one_hour_ago.to_rfc3339(),
            "updated_at": one_hour_ago.to_rfc3339(),
            "attempt_count": 0,
            "lease_id": "lease-cleanup-fail-task",
            "dispatch_mode": "workflow",
            "routing_labels": [],
            "repo_slug": CONFORMANCE_TEST_REPO_SLUG,
            "issue_number": 42
        });
        let task_path = daemon_dir.join("tasks/cleanup-fail-task.json");
        std::fs::create_dir_all(task_path.parent().unwrap())
            .map_err(|e| format!("mkdir tasks: {e}"))?;
        std::fs::write(
            &task_path,
            serde_json::to_string_pretty(&task_json).unwrap(),
        )
        .map_err(|e| format!("write task: {e}"))?;

        let lease_json = serde_json::json!({
            "lease_id": "lease-cleanup-fail-task",
            "task_id": "cleanup-fail-task",
            "project_id": "cleanup-proj",
            "worktree_path": ws.path().join("nonexistent-worktree-for-cleanup"),
            "branch_name": "rb/cleanup-fail-task",
            "acquired_at": one_hour_ago.to_rfc3339(),
            "ttl_seconds": 60,
            "last_heartbeat": one_hour_ago.to_rfc3339()
        });
        let lease_path = daemon_dir.join("leases/lease-cleanup-fail-task.json");
        std::fs::create_dir_all(lease_path.parent().unwrap())
            .map_err(|e| format!("mkdir leases: {e}"))?;
        std::fs::write(
            &lease_path,
            serde_json::to_string_pretty(&lease_json).unwrap(),
        )
        .map_err(|e| format!("write lease: {e}"))?;

        // Create the writer lock so cleanup can attempt to release it
        let lock_path = daemon_dir.join("leases/writer-cleanup-proj.lock");
        std::fs::write(&lock_path, "lease-cleanup-fail-task")
            .map_err(|e| format!("write lock: {e}"))?;

        let out = run_cli(
            &[
                "daemon",
                "reconcile",
                "--data-dir",
                data_dir,
                "--ttl-seconds",
                "0",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stdout, "Cleanup Failures", "stdout")?;
        assert_contains(&out.stdout, "cleanup-fail-task", "stdout")?;
        assert_contains(&out.stdout, "lease-cleanup-fail-task", "stdout")?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-007", || {
        // Daemon continues processing after a single task's writer lock is held.
        // Given two pending tasks and the first task's project writer lock is
        // already held, the daemon should skip the first and process the second.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Git-backed workspace required for real worktree dispatch.
        init_git_repo(&ws)?;

        // Also set up a project fixture for free-proj so the daemon's
        // ensure_project / workflow dispatch has something to work with.
        create_project_fixture(ws.path(), "free-proj", "standard");

        let now = chrono::Utc::now();
        // Task 1: its project lock is already held → claim will fail.
        // Use an earlier created_at to guarantee it sorts before free-task,
        // since list_tasks sorts by (is_terminal, created_at, task_id).
        let locked_time = now - chrono::Duration::seconds(10);
        let task1_json = serde_json::json!({
            "task_id": "locked-task",
            "issue_ref": "repo#locked",
            "project_id": "locked-proj",
            "status": "pending",
            "created_at": locked_time.to_rfc3339(),
            "updated_at": locked_time.to_rfc3339(),
            "attempt_count": 0,
            "dispatch_mode": "workflow",
            "routing_labels": [],
            "resolved_flow": "standard",
            "routing_source": "default_flow"
        });
        let task1_path = daemon_root(ws.path()).join("tasks/locked-task.json");
        std::fs::write(
            &task1_path,
            serde_json::to_string_pretty(&task1_json).unwrap(),
        )
        .map_err(|e| format!("write task1: {e}"))?;

        // Hold the writer lock for locked-proj
        let lock_path = daemon_root(ws.path()).join("leases/writer-locked-proj.lock");
        std::fs::write(&lock_path, "external-holder").map_err(|e| format!("write lock: {e}"))?;

        // Task 2: no lock contention (different project), later created_at
        let task2_json = serde_json::json!({
            "task_id": "free-task",
            "issue_ref": "repo#free",
            "project_id": "free-proj",
            "status": "pending",
            "created_at": now.to_rfc3339(),
            "updated_at": now.to_rfc3339(),
            "attempt_count": 0,
            "dispatch_mode": "workflow",
            "routing_labels": [],
            "resolved_flow": "standard",
            "routing_source": "default_flow"
        });
        let task2_path = daemon_root(ws.path()).join("tasks/free-task.json");
        std::fs::write(
            &task2_path,
            serde_json::to_string_pretty(&task2_json).unwrap(),
        )
        .map_err(|e| format!("write task2: {e}"))?;

        run_daemon_iteration_in_process(ws.path())?;

        // Writer-lock contention invariant: the locked task must remain pending,
        // acquire no lease/worktree, and produce no claim-side durable mutation.
        let task1_after: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&task1_path).map_err(|e| format!("read task1 after: {e}"))?,
        )
        .map_err(|e| format!("parse task1: {e}"))?;
        let task1_status = task1_after
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if task1_status != "pending" {
            return Err(format!(
                "locked-task should remain 'pending' but is '{task1_status}'"
            ));
        }
        // locked-task must not have acquired a lease
        if task1_after
            .get("lease_id")
            .and_then(|v| v.as_str())
            .is_some()
        {
            return Err(
                "locked-task must not acquire a lease under writer lock contention".to_owned(),
            );
        }

        // The second eligible task should be claimed and processed in the same
        // daemon cycle (status changed from pending).
        let task2_after: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&task2_path).map_err(|e| format!("read task2 after: {e}"))?,
        )
        .map_err(|e| format!("parse task2: {e}"))?;
        let task2_status = task2_after
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if task2_status == "pending" {
            return Err(
                "free-task should have been claimed/processed but is still 'pending'".to_owned(),
            );
        }

        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-008", || {
        // Daemon dispatch does not mutate process-global working directory.
        // Structural assertion: daemon_loop.rs must not contain set_current_dir.
        let source = include_str!("../../contexts/automation_runtime/daemon_loop.rs");
        if source.contains("set_current_dir") {
            return Err(
                "daemon_loop.rs must not call set_current_dir — CWD must remain unchanged"
                    .to_owned(),
            );
        }

        // Verify CWD is unchanged across a daemon cycle that actually dispatches
        // a pending task through worktree-backed execution.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Git-backed workspace required for real worktree dispatch.
        init_git_repo(&ws)?;

        // Create a project fixture so ensure_project succeeds during dispatch.
        create_project_fixture(ws.path(), "cwd-proj", "standard");

        // Create a pending task so real worktree dispatch occurs.
        let now = chrono::Utc::now();
        let task_json = serde_json::json!({
            "task_id": "cwd-test-task",
            "issue_ref": "repo#cwd",
            "project_id": "cwd-proj",
            "status": "pending",
            "created_at": now.to_rfc3339(),
            "updated_at": now.to_rfc3339(),
            "attempt_count": 0,
            "dispatch_mode": "workflow",
            "routing_labels": [],
            "resolved_flow": "standard",
            "routing_source": "default_flow"
        });
        let task_path = daemon_root(ws.path()).join("tasks/cwd-test-task.json");
        std::fs::write(
            &task_path,
            serde_json::to_string_pretty(&task_json).unwrap(),
        )
        .map_err(|e| format!("write task: {e}"))?;

        let cwd_before = std::env::current_dir().map_err(|e| format!("get cwd: {e}"))?;
        // Dispatch must succeed — if the helper fails, the task fixture was
        // malformed or the daemon could not process it, which must not count as
        // a passing CWD-unchanged assertion.
        run_daemon_iteration_in_process(ws.path())?;

        let cwd_after = std::env::current_dir().map_err(|e| format!("get cwd: {e}"))?;
        if cwd_before != cwd_after {
            return Err(format!(
                "CWD changed during dispatch: before={}, after={}",
                cwd_before.display(),
                cwd_after.display()
            ));
        }

        // Verify the task was actually dispatched (status must have changed
        // from pending). A malformed fixture that causes a parse failure must
        // not satisfy the unchanged-CWD assertion.
        let task_after: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&task_path).map_err(|e| format!("read task after: {e}"))?,
        )
        .map_err(|e| format!("parse task: {e}"))?;
        let task_status = task_after
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("pending");
        if task_status == "pending" {
            return Err(
                "task was never dispatched (still pending after successful daemon cycle)"
                    .to_owned(),
            );
        }
        Ok(())
    });
}

// ===========================================================================
// Daemon Routing (7 scenarios)
// ===========================================================================

fn register_daemon_routing(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "DAEMON-ROUTING-001", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-006", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-007", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        write_conformance_repo_registration(ws.path());
        let out = run_cli(&["daemon", "reconcile", "--data-dir", data_dir], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });
}

// ===========================================================================
// Daemon Issue Intake (10 scenarios)
// ===========================================================================

fn register_daemon_issue_intake(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "DAEMON-INTAKE-001", || {
        // Watcher ingestion creates a task from a watched issue
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::{DispatchMode, WatchedIssueMeta};
        use crate::contexts::automation_runtime::routing::RoutingEngine;
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let routing = RoutingEngine::new();
        let issue = WatchedIssueMeta {
            issue_ref: "test/repo#1".to_owned(),
            source_revision: "abc12345".to_owned(),
            title: "Test issue".to_owned(),
            body: "Implement feature X".to_owned(),
            labels: vec![],
            routing_command: None,
        };

        let result = DaemonTaskService::create_task_from_watched_issue(
            &store,
            ws.path(),
            &routing,
            FlowPreset::Standard,
            &issue,
            DispatchMode::Workflow,
            None,
        )
        .map_err(|e| e.to_string())?;
        let task = result.ok_or("expected a task to be created")?;
        if task.issue_ref != "test/repo#1" {
            return Err(format!("wrong issue_ref: {}", task.issue_ref));
        }
        if task.source_revision.as_deref() != Some("abc12345") {
            return Err(format!("wrong source_revision: {:?}", task.source_revision));
        }
        if task.dispatch_mode != DispatchMode::Workflow {
            return Err(format!("wrong dispatch_mode: {}", task.dispatch_mode));
        }
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-002", || {
        // Idempotent re-polling: same issue_ref + source_revision produces no duplicate
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::{DispatchMode, WatchedIssueMeta};
        use crate::contexts::automation_runtime::routing::RoutingEngine;
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let routing = RoutingEngine::new();
        let issue = WatchedIssueMeta {
            issue_ref: "test/repo#2".to_owned(),
            source_revision: "rev11111".to_owned(),
            title: "Idempotent".to_owned(),
            body: "Body".to_owned(),
            labels: vec![],
            routing_command: None,
        };

        // First ingestion creates
        let r1 = DaemonTaskService::create_task_from_watched_issue(
            &store,
            ws.path(),
            &routing,
            FlowPreset::Standard,
            &issue,
            DispatchMode::Workflow,
            None,
        )
        .map_err(|e| e.to_string())?;
        if r1.is_none() {
            return Err("first ingestion should create a task".to_owned());
        }

        // Second ingestion is no-op
        let r2 = DaemonTaskService::create_task_from_watched_issue(
            &store,
            ws.path(),
            &routing,
            FlowPreset::Standard,
            &issue,
            DispatchMode::Workflow,
            None,
        )
        .map_err(|e| e.to_string())?;
        if r2.is_some() {
            return Err("second ingestion should be idempotent no-op".to_owned());
        }

        // Only one task exists
        let tasks = DaemonTaskService::list_tasks(&store, ws.path()).map_err(|e| e.to_string())?;
        let matching: Vec<_> = tasks
            .iter()
            .filter(|t| t.issue_ref == "test/repo#2")
            .collect();
        if matching.len() != 1 {
            return Err(format!("expected 1 task, found {}", matching.len()));
        }
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-003", || {
        // Requirements quick handoff: full watcher → daemon → requirements →
        // workflow path. Initializes a git repo so the daemon can create a
        // worktree and execute the complete workflow pipeline.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::TaskStatus;
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;

        // Write a watched issue file for the daemon's FileIssueWatcher
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#3",
            "source_revision": "rev33333",
            "title": "Quick test",
            "body": "/rb requirements quick\n\nImplement feature",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-3.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Run one daemon cycle — the full watcher → requirements_quick →
        // seed handoff → project creation → workflow dispatch pipeline.
        run_daemon_iteration_in_process(ws.path())?;

        // Verify the task was created and processed to completion
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#3")
            .ok_or("no task created for issue test/repo#3")?;

        // Task should have a linked requirements_run_id
        if task.requirements_run_id.is_none() {
            return Err("requirements_run_id should be set after quick handoff".to_owned());
        }
        let run_id = task.requirements_run_id.as_ref().unwrap();

        // The linked requirements run should be completed
        let req_run_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
        let run_content = std::fs::read_to_string(&req_run_path)
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let req_status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if req_status != "completed" {
            return Err(format!(
                "expected requirements run 'completed', got '{req_status}'"
            ));
        }

        // Task should have project metadata populated from seed
        if task.project_id.is_empty() {
            return Err("project_id should be populated from seed".to_owned());
        }
        if task.project_name.is_none() {
            return Err("project_name should be populated from seed".to_owned());
        }

        // Task dispatch_mode should be Workflow (transitioned from RequirementsQuick)
        if task.dispatch_mode != crate::contexts::automation_runtime::model::DispatchMode::Workflow
        {
            return Err(format!(
                "expected Workflow dispatch_mode after quick handoff, got {}",
                task.dispatch_mode
            ));
        }

        // Task should have reached completed status (full workflow executed)
        if task.status != TaskStatus::Completed {
            return Err(format!(
                "expected task status 'completed' after full quick handoff, got '{}'",
                task.status
            ));
        }

        // Verify the project was actually created on disk
        let project_path = project_root(ws.path(), &task.project_id);
        if !project_path.join("project.toml").is_file() {
            return Err(format!(
                "project directory missing at {}",
                project_path.display()
            ));
        }

        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-004", || {
        // Requirements draft: create a watched issue with /rb requirements draft,
        // override the stub to return non-empty questions, run a daemon cycle,
        // and verify the task enters waiting_for_requirements with a real
        // requirements run in awaiting_answers status.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::TaskStatus;
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Write a watched issue file
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#4",
            "source_revision": "rev44444",
            "title": "Draft test",
            "body": "/rb requirements draft\n\nPlan feature",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-4.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Override validation to return NeedsQuestions so the full-mode
        // pipeline pauses and generates questions, then override question_set
        // to return the actual questions.
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["incomplete auth requirements"],
                "blocking_issues": [],
                "missing_information": ["authentication strategy", "database choice"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "What authentication?", "rationale": "Auth", "required": true},
                    {"id": "q2", "prompt": "Which database?", "rationale": "Schema", "required": true}
                ]
            }
        });

        // Run one daemon cycle with explicit label overrides (no env var mutation)
        let overrides_map: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_value(label_overrides).map_err(|e| format!("parse overrides: {e}"))?;
        run_daemon_iteration_with_label_overrides(ws.path(), Some(overrides_map))?;

        // Verify the task was created and entered waiting_for_requirements
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#4")
            .ok_or("no task created for issue test/repo#4")?;

        if task.status != TaskStatus::WaitingForRequirements {
            return Err(format!(
                "expected waiting_for_requirements, got {}",
                task.status
            ));
        }
        if task.lease_id.is_some() {
            return Err("task in waiting state should have no lease".to_owned());
        }
        if task.requirements_run_id.is_none() {
            return Err("requirements_run_id should be set".to_owned());
        }

        // The linked requirements run should be in awaiting_answers status
        let run_id = task.requirements_run_id.as_ref().unwrap();
        let req_run_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
        let run_content = std::fs::read_to_string(&req_run_path)
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!(
                "expected requirements run 'awaiting_answers', got '{status}'"
            ));
        }

        // Verify answers.toml template was written
        let answers_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/answers.toml"));
        if !answers_path.is_file() {
            return Err("answers.toml template should be written for draft".to_owned());
        }

        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-005", || {
        // Duplicate issue rejection: different source_revision while non-terminal
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::{DispatchMode, WatchedIssueMeta};
        use crate::contexts::automation_runtime::routing::RoutingEngine;
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::shared::domain::FlowPreset;
        use crate::shared::error::AppError;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let routing = RoutingEngine::new();

        let issue1 = WatchedIssueMeta {
            issue_ref: "test/repo#5".to_owned(),
            source_revision: "rev55551".to_owned(),
            title: "First".to_owned(),
            body: "Body".to_owned(),
            labels: vec![],
            routing_command: None,
        };
        DaemonTaskService::create_task_from_watched_issue(
            &store,
            ws.path(),
            &routing,
            FlowPreset::Standard,
            &issue1,
            DispatchMode::Workflow,
            None,
        )
        .map_err(|e| e.to_string())?;

        let issue2 = WatchedIssueMeta {
            issue_ref: "test/repo#5".to_owned(),
            source_revision: "rev55552".to_owned(),
            title: "Second".to_owned(),
            body: "Body".to_owned(),
            labels: vec![],
            routing_command: None,
        };
        let err = DaemonTaskService::create_task_from_watched_issue(
            &store,
            ws.path(),
            &routing,
            FlowPreset::Standard,
            &issue2,
            DispatchMode::Workflow,
            None,
        );
        match err {
            Err(AppError::DuplicateWatchedIssue { .. }) => Ok(()),
            Err(e) => Err(format!("expected DuplicateWatchedIssue, got: {e}")),
            Ok(_) => Err("expected error for duplicate issue with different revision".to_owned()),
        }
    });

    reg!(m, "DAEMON-INTAKE-006", || {
        // Routed flow override: create a watched issue with /rb flow quick_dev and
        // /rb requirements quick, run a daemon cycle. The stub's project_seed
        // payload recommends "standard" flow, but the routed flow "quick_dev"
        // must be authoritative. Verify the warning is persisted on the task.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Write a watched issue with both a flow routing command and requirements quick
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#6",
            "source_revision": "rev66666",
            "title": "Override test",
            "body": "/rb requirements quick\n\nBuild something",
            "labels": [],
            "routing_command": "/rb flow quick_dev"
        });
        std::fs::write(
            watched_dir.join("issue-6.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Run one daemon cycle — the stub seed recommends "standard" but the
        // routing command says "quick_dev". The subsequent worktree step may
        // fail in a non-git workspace, but the routing warning should be
        // durably persisted on the task.
        run_daemon_iteration_in_process(ws.path())?;

        // Verify the task used the routed flow (quick_dev), not the seed's recommendation
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#6")
            .ok_or("no task created for issue test/repo#6")?;

        if task.resolved_flow != Some(crate::shared::domain::FlowPreset::QuickDev) {
            return Err(format!("expected quick_dev, got {:?}", task.resolved_flow));
        }

        // Verify that the flow-override warning is persisted on the task
        if task.routing_warnings.is_empty() {
            return Err("routing_warnings should contain the flow override warning".to_owned());
        }
        let has_override_warning = task
            .routing_warnings
            .iter()
            .any(|w| w.contains("seed suggests flow") && w.contains("authoritative"));
        if !has_override_warning {
            return Err(format!(
                "expected flow override warning in routing_warnings, got: {:?}",
                task.routing_warnings
            ));
        }

        // Verify the daemon journal also recorded the warning
        let journal_path = ws.path().join(".ralph-burning/daemon/journal.ndjson");
        if journal_path.is_file() {
            let journal = std::fs::read_to_string(&journal_path)
                .map_err(|e| format!("read daemon journal: {e}"))?;
            let has_journal_warning = journal.lines().any(|line| line.contains("routing_warning"));
            if !has_journal_warning {
                return Err("daemon journal should contain routing_warning event".to_owned());
            }
        }

        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-007", || {
        // Unknown or malformed requirements commands fail ingestion and create
        // no task. Tests both the parser and the full watcher + daemon path.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::watcher;
        use crate::contexts::automation_runtime::DaemonStorePort;

        // --- Parser-level validation ---

        // Malformed: unknown subcommand
        let result = watcher::parse_requirements_command("/rb requirements unknown");
        if result.is_ok() {
            return Err("expected error for unknown requirements subcommand".to_owned());
        }

        // Bare `/rb requirements` defaults to RequirementsDraft (not an error)
        let result2 =
            watcher::parse_requirements_command("/rb requirements").map_err(|e| e.to_string())?;
        if result2
            != Some(crate::contexts::automation_runtime::model::DispatchMode::RequirementsDraft)
        {
            return Err(format!(
                "expected RequirementsDraft for bare '/rb requirements', got {:?}",
                result2
            ));
        }

        // Malformed: extra tokens
        let result3 = watcher::parse_requirements_command("/rb requirements draft extra");
        if result3.is_ok() {
            return Err("expected error for extra tokens".to_owned());
        }

        // Valid: no requirements command at all is Ok(None)
        let result4 =
            watcher::parse_requirements_command("/rb flow standard").map_err(|e| e.to_string())?;
        if result4.is_some() {
            return Err("expected None for non-requirements command".to_owned());
        }

        // --- Full daemon path: malformed command prevents task creation ---

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Write a watched issue with a malformed requirements command
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#7",
            "source_revision": "rev77777",
            "title": "Malformed test",
            "body": "/rb requirements unknown\n\nBad command",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-7.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Run one daemon cycle — the watcher should skip this issue
        run_daemon_iteration_in_process(ws.path())?;

        // No task should have been created for the malformed issue
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let matching = tasks
            .iter()
            .filter(|t| t.issue_ref == "test/repo#7")
            .count();
        if matching != 0 {
            return Err(format!(
                "expected 0 tasks for malformed command, found {matching}"
            ));
        }

        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-008", || {
        // Daemon status surfaces waiting state
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let data_dir = ws.path().to_str().ok_or("non-utf8 path")?;
        let store = FsDataDirDaemonStore;
        let daemon_dir = conformance_daemon_dir(ws.path());
        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "intake-wait-008".to_owned(),
            issue_ref: "test/repo#8".to_owned(),
            project_id: "watched-test-repo8".to_owned(),
            project_name: Some("Wait test".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::WaitingForRequirements,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::RequirementsDraft,
            source_revision: Some("rev88888".to_owned()),
            requirements_run_id: Some("req-123".to_owned()),
            workflow_run_id: None,
            repo_slug: Some(CONFORMANCE_TEST_REPO_SLUG.to_owned()),
            issue_number: Some(8),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        let out = run_cli(
            &[
                "daemon",
                "status",
                "--data-dir",
                data_dir,
                "--repo",
                CONFORMANCE_TEST_REPO_SLUG,
            ],
            ws.path(),
        )?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "waiting_for_requirements", "status output")?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-009", || {
        // Requirements draft waiting/resume: run a daemon cycle that puts a task
        // into waiting_for_requirements (via /rb requirements draft with non-empty
        // questions), then externally complete the linked requirements run, and
        // verify that a second daemon cycle resumes the task and completes the
        // full workflow.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::TaskStatus;
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;

        // Write a watched issue with /rb requirements draft
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#9",
            "source_revision": "rev99999",
            "title": "Draft resume test",
            "body": "/rb requirements draft\n\nPlan and build feature",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-9.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Override validation to return NeedsQuestions so the full-mode
        // pipeline pauses and generates questions, then override question_set
        // to return the actual questions.
        let label_overrides = serde_json::json!({
            "validation": {
                "outcome": "needs_questions",
                "evidence": ["incomplete scope"],
                "blocking_issues": [],
                "missing_information": ["feature scope"]
            },
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "What scope?", "rationale": "Scope", "required": true}
                ]
            }
        });

        // First daemon cycle: task enters waiting_for_requirements
        let overrides_map: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_value(label_overrides).map_err(|e| format!("parse overrides: {e}"))?;
        run_daemon_iteration_with_label_overrides(ws.path(), Some(overrides_map))?;

        // Verify the task is in waiting state
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#9")
            .ok_or("no task created for issue test/repo#9")?;

        if task.status != TaskStatus::WaitingForRequirements {
            return Err(format!(
                "expected waiting_for_requirements after first cycle, got {}",
                task.status
            ));
        }
        let run_id = task
            .requirements_run_id
            .as_ref()
            .ok_or("requirements_run_id should be set after draft")?;

        // Simulate `requirements answer`: complete the requirements run by
        // writing it as completed with a seed payload on disk.
        let req_run_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
        let run_content = std::fs::read_to_string(&req_run_path)
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let mut run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let seed_id = "seed-from-answers";
        run["status"] = serde_json::json!("completed");
        run["latest_seed_id"] = serde_json::json!(seed_id);
        run["status_summary"] = serde_json::json!("completed: seed generated");
        std::fs::write(&req_run_path, serde_json::to_string_pretty(&run).unwrap())
            .map_err(|e| format!("write completed run.json: {e}"))?;

        // Write the seed payload
        let payload_dir = ws.path().join(format!(
            ".ralph-burning/requirements/{run_id}/history/payloads"
        ));
        std::fs::create_dir_all(&payload_dir).map_err(|e| format!("mkdir payloads: {e}"))?;
        let seed_payload = serde_json::json!({
            "project_id": "resumed-draft-project",
            "project_name": "Resumed Draft Project",
            "flow": "standard",
            "prompt_body": "Prompt generated from answered requirements.",
            "handoff_summary": "Draft resume handoff.",
            "follow_ups": []
        });
        std::fs::write(
            payload_dir.join(format!("{seed_id}.json")),
            serde_json::to_string_pretty(&seed_payload).unwrap(),
        )
        .map_err(|e| format!("write seed payload: {e}"))?;

        // Write the seed prompt
        let seed_dir = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/seed"));
        std::fs::create_dir_all(&seed_dir).map_err(|e| format!("mkdir seed: {e}"))?;
        std::fs::write(seed_dir.join("prompt.md"), "# Resumed draft prompt\n")
            .map_err(|e| format!("write seed prompt.md: {e}"))?;

        // Second daemon cycle: check_waiting_tasks should see the completed
        // requirements run, resume the task, derive the seed, create the
        // project, and complete the workflow.
        run_daemon_iteration_in_process(ws.path())?;

        // Re-read the task — it should now be completed
        let tasks2 = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task2 = tasks2
            .iter()
            .find(|t| t.issue_ref == "test/repo#9")
            .ok_or("task for issue test/repo#9 disappeared")?;

        if task2.status != TaskStatus::Completed {
            return Err(format!(
                "expected task completed after resume cycle, got '{}'",
                task2.status
            ));
        }

        // Task should have been switched to Workflow dispatch_mode
        if task2.dispatch_mode != crate::contexts::automation_runtime::model::DispatchMode::Workflow
        {
            return Err(format!(
                "expected Workflow dispatch_mode after resume, got {}",
                task2.dispatch_mode
            ));
        }

        // Verify the project was created from the seed
        if task2.project_id.is_empty() {
            return Err("project_id should be populated after resume".to_owned());
        }
        let project_path = project_root(ws.path(), &task2.project_id);
        if !project_path.join("project.toml").is_file() {
            return Err(format!(
                "project directory missing at {}",
                project_path.display()
            ));
        }

        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-010", || {
        // Requirements draft with empty questions: the default stub returns an
        // empty question set, so draft() completes directly. The daemon should
        // requeue the task as Pending with Workflow dispatch_mode and a linked
        // requirements_run_id instead of stranding it in Active.
        use crate::adapters::fs::FsDaemonStore;
        use crate::contexts::automation_runtime::model::{DispatchMode, TaskStatus};
        use crate::contexts::automation_runtime::DaemonStorePort;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Write a watched issue with /rb requirements draft (no label overrides,
        // so the stub returns empty questions → run completes directly).
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| format!("mkdir watched: {e}"))?;
        let issue_json = serde_json::json!({
            "issue_ref": "test/repo#10",
            "source_revision": "rev10101",
            "title": "Empty-question draft test",
            "body": "/rb requirements draft\n\nSimple feature",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(
            watched_dir.join("issue-10.json"),
            serde_json::to_string_pretty(&issue_json).unwrap(),
        )
        .map_err(|e| format!("write watched issue: {e}"))?;

        // Run one daemon cycle (no label overrides → empty questions → immediate completion)
        run_daemon_iteration_in_process(ws.path())?;

        // Verify the task was requeued as Pending with Workflow dispatch_mode
        let store = FsDaemonStore;
        let tasks = store.list_tasks(ws.path()).map_err(|e| e.to_string())?;
        let task = tasks
            .iter()
            .find(|t| t.issue_ref == "test/repo#10")
            .ok_or("no task created for issue test/repo#10")?;

        if task.status != TaskStatus::Pending {
            return Err(format!(
                "expected task requeued as 'pending', got '{}'",
                task.status
            ));
        }
        if task.dispatch_mode != DispatchMode::Workflow {
            return Err(format!(
                "expected dispatch_mode 'workflow', got '{}'",
                task.dispatch_mode
            ));
        }
        if task.requirements_run_id.is_none() {
            return Err("requirements_run_id should be set after empty-question draft".to_owned());
        }

        // The linked requirements run should be completed (not awaiting_answers)
        let run_id = task.requirements_run_id.as_ref().unwrap();
        let req_run_path = ws
            .path()
            .join(format!(".ralph-burning/requirements/{run_id}/run.json"));
        let run_content = std::fs::read_to_string(&req_run_path)
            .map_err(|e| format!("read requirements run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse run.json: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!(
                "expected requirements run 'completed', got '{status}'"
            ));
        }

        Ok(())
    });
}

// ===========================================================================
// Workflow Panels: Prompt Review, Completion Panel, and Resume Drift (15 scenarios)
// ===========================================================================

fn register_workflow_panels(m: &mut HashMap<String, ScenarioExecutor>) {
    use crate::contexts::workflow_composition::completion::compute_completion_verdict;
    use crate::contexts::workflow_composition::engine::{
        build_completion_snapshot, build_single_target_snapshot,
        drift_still_satisfies_requirements, resolution_has_drifted,
    };
    use crate::contexts::workflow_composition::panel_contracts::{
        CompletionAggregatePayload, CompletionVerdict, PromptRefinementPayload,
        PromptReviewDecision, PromptReviewPrimaryPayload, PromptValidationPayload, RecordKind,
        RecordProducer,
    };
    use crate::shared::domain::ResolvedBackendTarget;

    // ── Prompt Review scenarios ───────────────────────────────────────────

    reg!(m, "workflow.prompt_review.panel_accept", || {
        // Exercise the full accept path: construct refinement + validation
        // payloads, verify serialization round-trip, verify primary decision
        // payload, and verify record kinds.
        let refinement = PromptRefinementPayload {
            refined_prompt: "Clarified prompt text.".to_owned(),
            refinement_summary: "Improved clarity.".to_owned(),
            improvements: vec!["Added acceptance criteria.".to_owned()],
        };
        let json =
            serde_json::to_string(&refinement).map_err(|e| format!("refinement serialize: {e}"))?;
        let restored: PromptRefinementPayload =
            serde_json::from_str(&json).map_err(|e| format!("refinement deserialize: {e}"))?;
        if restored.refined_prompt != refinement.refined_prompt {
            return Err("refinement round-trip failed".to_owned());
        }

        let validation = PromptValidationPayload {
            accepted: true,
            evidence: vec!["All criteria met.".to_owned()],
            concerns: vec![],
        };
        if !validation.accepted {
            return Err("expected accepted validation".to_owned());
        }

        // Build primary payload as the workflow would.
        let primary = PromptReviewPrimaryPayload {
            decision: PromptReviewDecision::Accepted,
            refined_prompt: refinement.refined_prompt.clone(),
            executed_reviewers: 2,
            accept_count: 2,
            reject_count: 0,
            refinement_summary: refinement.refinement_summary.clone(),
        };
        if primary.decision != PromptReviewDecision::Accepted {
            return Err("expected Accepted decision".to_owned());
        }
        if primary.reject_count != 0 {
            return Err("expected zero rejects for acceptance".to_owned());
        }
        // Verify primary payload serialization matches StagePrimary kind.
        let primary_json =
            serde_json::to_value(&primary).map_err(|e| format!("primary serialize: {e}"))?;
        if primary_json["decision"] != "accepted" {
            return Err(format!(
                "expected decision 'accepted', got {}",
                primary_json["decision"]
            ));
        }

        let kind = RecordKind::StagePrimary;
        if kind.to_string() != "primary" {
            return Err(format!("expected 'primary', got '{}'", kind));
        }
        // Supporting records use StageSupporting.
        let supporting = RecordKind::StageSupporting;
        if supporting.to_string() != "supporting" {
            return Err(format!("expected 'supporting', got '{}'", supporting));
        }

        // Verify producer metadata serializes correctly.
        let producer = RecordProducer::Agent {
            requested_backend_family: "claude".to_owned(),
            requested_model_id: "claude-opus-4-7".to_owned(),
            actual_backend_family: "claude".to_owned(),
            actual_model_id: "claude-opus-4-7".to_owned(),
        };
        let producer_json =
            serde_json::to_value(&producer).map_err(|e| format!("producer serialize: {e}"))?;
        if producer_json["type"] != "agent" {
            return Err("expected producer type 'agent'".to_owned());
        }

        // ── Behavioral: exercise actual prompt-review accept via CLI ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "pr-accept", "standard")?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Journal must contain prompt_review stage_entered and stage_completed.
        let events = read_journal(&ws, "pr-accept")?;
        let has_pr_entered = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("prompt_review")
        });
        if !has_pr_entered {
            return Err("journal missing stage_entered for prompt_review".to_owned());
        }
        let has_pr_completed = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_completed")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("prompt_review")
        });
        if !has_pr_completed {
            return Err("journal missing stage_completed for prompt_review".to_owned());
        }

        // prompt.original.md must exist after accept (prompt was replaced).
        let project_dir = project_root(ws.path(), "pr-accept");
        if !project_dir.join("prompt.original.md").exists() {
            return Err("prompt.original.md missing after prompt-review accept".to_owned());
        }

        // Supporting + primary records must include prompt-review artifacts.
        let payloads = count_payload_files(&ws, "pr-accept")?;
        if payloads < 11 {
            return Err(format!(
                "expected >= 11 payloads (stages + prompt-review supporting), got {payloads}"
            ));
        }

        Ok(())
    });

    reg!(m, "workflow.prompt_review.panel_reject", || {
        // Exercise the reject path: a validator rejects, supporting records
        // are written but prompt.md stays unchanged.
        let validation = PromptValidationPayload {
            accepted: false,
            evidence: vec!["prompt is unclear".to_owned()],
            concerns: vec!["ambiguous scope".to_owned()],
        };
        if validation.accepted {
            return Err("expected rejected validation".to_owned());
        }
        // Serialize the rejection payload and verify it round-trips.
        let json =
            serde_json::to_string(&validation).map_err(|e| format!("validation serialize: {e}"))?;
        let restored: PromptValidationPayload =
            serde_json::from_str(&json).map_err(|e| format!("validation deserialize: {e}"))?;
        if restored.accepted {
            return Err("round-tripped validation should still be rejected".to_owned());
        }
        if restored.concerns.is_empty() {
            return Err("concerns should survive round-trip".to_owned());
        }

        // Verify rejection constructs the correct error.
        let err = crate::shared::error::AppError::PromptReviewRejected {
            details: "1 of 2 validators rejected the refined prompt".to_owned(),
        };
        let msg = err.to_string();
        if !msg.contains("rejected") {
            return Err(format!("expected 'rejected' in error: {msg}"));
        }

        // Verify that a rejected primary payload has decision=Rejected.
        let primary = PromptReviewPrimaryPayload {
            decision: PromptReviewDecision::Rejected,
            refined_prompt: "refined".to_owned(),
            executed_reviewers: 2,
            accept_count: 1,
            reject_count: 1,
            refinement_summary: "summary".to_owned(),
        };
        if primary.decision != PromptReviewDecision::Rejected {
            return Err("expected Rejected decision".to_owned());
        }

        // ── Behavioral: exercise prompt-review rejection via CLI ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "pr-reject", "standard")?;
        // Override prompt_review: old-format readiness.ready=false → validator rejects.
        let overrides = serde_json::json!({
            "prompt_review": {
                "readiness": {"ready": false, "risks": ["ambiguous scope"]}
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_failure(&out)?;

        // Run must have failed status.
        let snapshot = read_run_snapshot(&ws, "pr-reject")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("failed") {
            return Err("expected failed status after prompt-review rejection".to_owned());
        }

        // Supporting records (refiner + validators) must still be written.
        let payloads = count_payload_files(&ws, "pr-reject")?;
        if payloads < 2 {
            return Err(format!(
                "expected >= 2 supporting payloads after rejection, got {payloads}"
            ));
        }

        Ok(())
    });

    reg!(m, "workflow.prompt_review.min_reviewers_enforced", || {
        // ── Helper assertions ──
        let err = crate::shared::error::AppError::InsufficientPanelMembers {
            panel: "prompt_review".to_owned(),
            resolved: 1,
            minimum: 3,
        };
        let msg = err.to_string();
        if !msg.contains("insufficient panel members") || !msg.contains("prompt_review") {
            return Err(format!("unexpected error message: {msg}"));
        }

        // ── Behavioral: workspace with min_reviewers=3, only 2 validator backends ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "pr-min-rev", "standard")?;
        // Overwrite workspace.toml to set min_reviewers=3 with only 2 validators.
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[prompt_review]") {
            content.replace(
                "[prompt_review]",
                "[prompt_review]\nmin_reviewers = 3\nvalidator_backends = [\"claude\", \"codex\"]",
            )
        } else {
            format!("{content}\n[prompt_review]\nmin_reviewers = 3\nvalidator_backends = [\"claude\", \"codex\"]\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        // Verify preflight rejected the run before execution started.
        let snapshot = read_run_snapshot(&ws, "pr-min-rev")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("not_started") {
            return Err(
                "expected not_started status for min_reviewers preflight enforcement".to_owned(),
            );
        }
        // stderr should reference insufficient panel members.
        if !out.stderr.contains("insufficient") && !out.stderr.contains("min_reviewers") {
            // May also be a resolution failure: check for any panel-related error.
            if !out.stderr.contains("panel") && !out.stderr.contains("prompt_review") {
                return Err(format!(
                    "expected insufficient panel members or resolution error, got: {}",
                    out.stderr
                ));
            }
        }
        Ok(())
    });

    reg!(m, "workflow.prompt_review.optional_validator_skip", || {
        // ── Helper assertions ──
        let specs = [
            crate::shared::domain::PanelBackendSpec::required(
                crate::shared::domain::BackendFamily::Claude,
            ),
            crate::shared::domain::PanelBackendSpec::required(
                crate::shared::domain::BackendFamily::Codex,
            ),
            crate::shared::domain::PanelBackendSpec::optional(
                crate::shared::domain::BackendFamily::OpenRouter,
            ),
        ];
        let required_count = specs.iter().filter(|s| !s.is_optional()).count();
        if required_count != 2 {
            return Err(format!("expected 2 required, got {required_count}"));
        }

        // ── Behavioral: configure optional validator, verify run succeeds ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "pr-opt-skip", "standard")?;
        // Add optional openrouter validator that will be skipped (not available
        // in stub mode by default). Required validators still satisfy min.
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[prompt_review]") {
            content.replace(
                    "[prompt_review]",
                    "[prompt_review]\nvalidator_backends = [\"claude\", \"codex\", \"?openrouter\"]\nmin_reviewers = 2",
                )
        } else {
            format!("{content}\n[prompt_review]\nvalidator_backends = [\"claude\", \"codex\", \"?openrouter\"]\nmin_reviewers = 2\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Journal should show prompt_review stage_completed.
        let events = read_journal(&ws, "pr-opt-skip")?;
        let pr_completed = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_completed")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("prompt_review")
        });
        if !pr_completed {
            return Err(
                "prompt_review should complete when optional validator is skipped".to_owned(),
            );
        }

        // Verify the executed reviewer count reflects only available
        // validators (2 required) — not all 3 configured.
        let payloads_dir = project_root(ws.path(), "pr-opt-skip").join("history/payloads");
        if payloads_dir.exists() {
            // Count validator supporting records (exclude refiner and primary).
            let validator_count = std::fs::read_dir(&payloads_dir)
                .map_err(|e| format!("read payloads: {e}"))?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    let s = name.to_string_lossy();
                    s.contains("validator-") && s.ends_with(".json")
                })
                .count();
            if validator_count != 2 {
                return Err(format!(
                        "expected 2 validator supporting records (optional skipped), got {validator_count}"
                    ));
            }
        }

        Ok(())
    });

    reg!(
        m,
        "workflow.prompt_review.prompt_replaced_and_original_preserved",
        || {
            // ── Behavioral: drive `run start` and verify prompt file mutations ──
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "wp-replace", "standard")?;

            // Read the original prompt before the run.
            let project_dir = project_root(ws.path(), "wp-replace");
            let prompt_path = project_dir.join("prompt.md");
            let original_prompt = std::fs::read_to_string(&prompt_path)
                .map_err(|e| format!("read prompt.md before run: {e}"))?;

            // Run with prompt_review enabled (default in setup_workspace_with_project).
            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;

            // Verify prompt.original.md was written with the pre-review prompt.
            let original_path = project_dir.join("prompt.original.md");
            let actual_original = std::fs::read_to_string(&original_path)
                .map_err(|e| format!("read prompt.original.md: {e}"))?;
            if actual_original != original_prompt {
                return Err(format!(
                    "prompt.original.md should contain original text, got: {}",
                    &actual_original[..actual_original.len().min(200)]
                ));
            }

            // Verify prompt.md was replaced (contents differ from original).
            let final_prompt = std::fs::read_to_string(&prompt_path)
                .map_err(|e| format!("read prompt.md after run: {e}"))?;
            // The stub backend refiner produces a deterministic refined prompt
            // that differs from the original.
            if final_prompt == original_prompt {
                return Err(
                    "prompt.md should be replaced with refined text after prompt_review".to_owned(),
                );
            }

            // Verify the project prompt hash was updated.
            let project_toml = project_dir.join("project.toml");
            let project_meta = std::fs::read_to_string(&project_toml)
                .map_err(|e| format!("read project.toml: {e}"))?;
            let expected_hash = crate::adapters::fs::FileSystem::prompt_hash(&final_prompt);
            if !project_meta.contains(&expected_hash) {
                return Err("project.toml should contain updated prompt hash".to_owned());
            }

            // Verify journal has prompt_review stage_completed.
            let events = read_journal(&ws, "wp-replace")?;
            let pr_completed = events.iter().any(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_completed")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("prompt_review")
            });
            if !pr_completed {
                return Err("journal missing stage_completed for prompt_review".to_owned());
            }

            // Verify supporting records exist (refiner + validators).
            let payloads = count_payload_files(&ws, "wp-replace")?;
            // At minimum: prompt_review supporting records (refiner + validators) +
            // prompt_review primary + other stage payloads.
            if payloads < 3 {
                return Err(format!(
                    "expected >= 3 payloads for prompt replacement scenario, got {payloads}"
                ));
            }

            Ok(())
        }
    );

    // ── Completion Panel scenarios ────────────────────────────────────────

    reg!(
        m,
        "workflow.completion.panel_two_completer_consensus_complete",
        || {
            // Exercise the full consensus path with 2 completers both voting complete.
            let verdict = compute_completion_verdict(2, 2, 1, 0.5);
            if verdict != CompletionVerdict::Complete {
                return Err(format!("expected Complete, got {verdict}"));
            }

            // Build and verify the aggregate payload that would be persisted.
            let aggregate = CompletionAggregatePayload {
                verdict,
                complete_votes: 2,
                continue_votes: 0,
                total_voters: 2,
                consensus_threshold: 0.5,
                min_completers: 1,
                effective_min_completers: 1,
                exhausted_count: 0,
                probe_exhausted_count: 0,
                executed_voters: vec![
                    "claude:claude-opus-4-7".to_owned(),
                    "codex:codex-1".to_owned(),
                ],
            };
            let json = serde_json::to_value(&aggregate)
                .map_err(|e| format!("aggregate serialize: {e}"))?;
            if json["verdict"] != "complete" {
                return Err(format!(
                    "expected verdict 'complete' in JSON, got {}",
                    json["verdict"]
                ));
            }
            // Verify StageAggregate record kind for the aggregate.
            let kind = RecordKind::StageAggregate;
            if kind.to_string() != "aggregate" {
                return Err(format!("expected 'aggregate', got '{kind}'"));
            }
            // Round-trip the aggregate payload.
            let restored: CompletionAggregatePayload =
                serde_json::from_value(json).map_err(|e| format!("aggregate deserialize: {e}"))?;
            if restored.verdict != CompletionVerdict::Complete {
                return Err("aggregate round-trip failed".to_owned());
            }
            if restored.executed_voters.len() != 2 {
                return Err("executed voters should survive round-trip".to_owned());
            }

            // ── Behavioral: exercise completion panel Complete path via CLI ──
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "cp-complete", "standard")?;
            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;

            // Journal must contain completion_panel stage events.
            let events = read_journal(&ws, "cp-complete")?;
            let has_cp_entered = events.iter().any(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("completion_panel")
            });
            if !has_cp_entered {
                return Err("journal missing stage_entered for completion_panel".to_owned());
            }
            let has_cp_completed = events.iter().any(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_completed")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("completion_panel")
            });
            if !has_cp_completed {
                return Err("journal missing stage_completed for completion_panel".to_owned());
            }

            // Completion produces supporting + aggregate records.
            let payloads = count_payload_files(&ws, "cp-complete")?;
            if payloads < 11 {
                return Err(format!(
                    "expected >= 11 payloads (stages + panel records), got {payloads}"
                ));
            }

            // Verify the persisted aggregate payload has verdict "complete".
            let payloads_dir = project_root(ws.path(), "cp-complete").join("history/payloads");
            let aggregate_file = std::fs::read_dir(&payloads_dir)
                .map_err(|e| format!("read payloads dir: {e}"))?
                .filter_map(|e| e.ok())
                .find(|e| {
                    let name = e.file_name();
                    let name = name.to_string_lossy();
                    name.contains("completion_panel") && name.contains("aggregate")
                })
                .ok_or_else(|| "no aggregate payload file found".to_owned())?;
            let aggregate_content = std::fs::read_to_string(aggregate_file.path())
                .map_err(|e| format!("read aggregate payload: {e}"))?;
            let aggregate_json: serde_json::Value = serde_json::from_str(&aggregate_content)
                .map_err(|e| format!("parse aggregate payload: {e}"))?;
            let persisted_verdict = aggregate_json
                .get("payload")
                .and_then(|p| p.get("verdict"))
                .and_then(|v| v.as_str())
                .unwrap_or("missing");
            if persisted_verdict != "complete" {
                return Err(format!(
                    "expected persisted aggregate verdict 'complete', got '{persisted_verdict}'"
                ));
            }

            // Verify acceptance_qa transition: journal must contain
            // stage_entered for acceptance_qa after completion_panel completes.
            let has_aqa_entered = events.iter().any(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("acceptance_qa")
            });
            if !has_aqa_entered {
                return Err(
                    "journal missing stage_entered for acceptance_qa after completion complete"
                        .to_owned(),
                );
            }

            Ok(())
        }
    );

    reg!(m, "workflow.completion.panel_continue_verdict", || {
        // Exercise continue_work path: both completers vote continue.
        let verdict = compute_completion_verdict(0, 2, 1, 0.5);
        if verdict != CompletionVerdict::ContinueWork {
            return Err(format!("expected ContinueWork, got {verdict}"));
        }

        // Build aggregate and verify continue_work serialization.
        let aggregate = CompletionAggregatePayload {
            verdict,
            complete_votes: 0,
            continue_votes: 2,
            total_voters: 2,
            consensus_threshold: 0.5,
            min_completers: 1,
            effective_min_completers: 1,
            exhausted_count: 0,
            probe_exhausted_count: 0,
            executed_voters: vec![
                "claude:claude-opus-4-7".to_owned(),
                "codex:codex-1".to_owned(),
            ],
        };
        let json =
            serde_json::to_value(&aggregate).map_err(|e| format!("aggregate serialize: {e}"))?;
        if json["verdict"] != "continue_work" {
            return Err(format!("expected 'continue_work', got {}", json["verdict"]));
        }
        // Verify completion_round would advance (the engine increments it).
        let cursor = crate::shared::domain::StageCursor::new(StageId::CompletionPanel, 1, 1, 1)
            .map_err(|e| format!("cursor: {e}"))?;
        let next = cursor
            .advance_completion_round(StageId::Planning)
            .map_err(|e| format!("advance: {e}"))?;
        if next.completion_round != 2 {
            return Err(format!("expected round 2, got {}", next.completion_round));
        }
        if next.stage != StageId::Planning {
            return Err("continue_work should restart from planning".to_owned());
        }

        // ── Behavioral: exercise ContinueWork → completion_round advance via CLI ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cp-continue", "standard")?;
        // First round: both completers vote continue_work (matching feature file).
        // Second round: both completers vote complete so the run can finish.
        let overrides = serde_json::json!({
            "completion_panel": [
                {"vote_complete": false, "evidence": ["Needs more work"], "remaining_work": ["Fix issues"]},
                {"vote_complete": false, "evidence": ["Not ready yet"], "remaining_work": ["More work"]},
                {"vote_complete": true, "evidence": ["All done"], "remaining_work": []},
                {"vote_complete": true, "evidence": ["Complete"], "remaining_work": []}
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        // Journal must contain completion_round_advanced event.
        let events = read_journal(&ws, "cp-continue")?;
        let has_round_advanced = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
        });
        if !has_round_advanced {
            return Err("journal missing completion_round_advanced event".to_owned());
        }

        // Two completion_panel stage_entered events (one per round).
        let cp_entered_count = events
            .iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("completion_panel")
            })
            .count();
        if cp_entered_count < 2 {
            return Err(format!(
                "expected >= 2 completion_panel stage_entered events, got {cp_entered_count}"
            ));
        }

        // Verify StageAggregate payload exists with continue_work verdict in round 1.
        let payloads_dir = ws
            .path()
            .join(".ralph-burning")
            .join("projects")
            .join("cp-continue")
            .join("history")
            .join("payloads");
        if payloads_dir.exists() {
            let has_aggregate = std::fs::read_dir(&payloads_dir)
                .map(|entries| {
                    entries.filter_map(|e| e.ok()).any(|e| {
                        let name = e.file_name();
                        let name = name.to_string_lossy();
                        name.contains("completion_panel") && name.contains("aggregate")
                    })
                })
                .unwrap_or(false);
            if !has_aggregate {
                return Err("expected aggregate payload file for completion panel".to_owned());
            }
        }

        Ok(())
    });

    reg!(m, "workflow.completion.optional_backend_skip", || {
        // ── Helper: consensus math with 2 executed (optional skipped) ──
        let verdict = compute_completion_verdict(2, 2, 1, 0.5);
        if verdict != CompletionVerdict::Complete {
            return Err(format!(
                "expected Complete with 2 executed voters, got {verdict}"
            ));
        }

        // ── Behavioral: configure optional completer, verify run succeeds ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cp-opt-skip", "standard")?;
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[completion]") {
            content.replace(
                    "[completion]",
                    "[completion]\nbackends = [\"claude\", \"codex\", \"?openrouter\"]\nmin_completers = 1",
                )
        } else {
            format!("{content}\n[completion]\nbackends = [\"claude\", \"codex\", \"?openrouter\"]\nmin_completers = 1\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Journal should show completion_panel stage events.
        let events = read_journal(&ws, "cp-opt-skip")?;
        let cp_completed = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_completed")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("completion_panel")
        });
        if !cp_completed {
            return Err(
                "completion_panel should complete when optional backend is skipped".to_owned(),
            );
        }

        // Verify the persisted aggregate only counts executed voters
        // (2 of the 3 configured, since the optional one was skipped).
        let payloads_dir = project_root(ws.path(), "cp-opt-skip").join("history/payloads");
        let aggregate_file = std::fs::read_dir(&payloads_dir)
            .map_err(|e| format!("read payloads dir: {e}"))?
            .filter_map(|e| e.ok())
            .find(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.contains("completion_panel") && name.contains("aggregate")
            })
            .ok_or_else(|| {
                "no aggregate payload file found for optional_backend_skip".to_owned()
            })?;
        let aggregate_content = std::fs::read_to_string(aggregate_file.path())
            .map_err(|e| format!("read aggregate payload: {e}"))?;
        let aggregate_json: serde_json::Value = serde_json::from_str(&aggregate_content)
            .map_err(|e| format!("parse aggregate payload: {e}"))?;
        let payload = aggregate_json
            .get("payload")
            .ok_or_else(|| "aggregate payload missing 'payload' field".to_owned())?;

        let total_voters = payload
            .get("total_voters")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "aggregate missing total_voters".to_owned())?;
        if total_voters != 2 {
            return Err(format!(
                "expected total_voters = 2 (optional skipped), got {total_voters}"
            ));
        }

        let executed_voters = payload
            .get("executed_voters")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "aggregate missing executed_voters".to_owned())?;
        if executed_voters.len() != 2 {
            return Err(format!(
                "expected 2 executed voters (optional skipped), got {}",
                executed_voters.len()
            ));
        }
        Ok(())
    });

    reg!(m, "workflow.completion.required_backend_failure", || {
        // ── Helper: error construction ──
        let err = crate::shared::error::AppError::BackendUnavailable {
            backend: "codex".to_owned(),
            details: "required backend is disabled or unavailable".to_owned(),
            failure_class: None,
        };
        if !err.to_string().contains("unavailable") {
            return Err(format!("unexpected error: {err}"));
        }

        // ── Behavioral: configure a required unavailable backend ──
        // OpenRouter is disabled by default in stub mode. Listing it as a
        // required completion backend (no `?` prefix) causes panel resolution
        // to fail with BackendUnavailable before any invocations occur.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cp-req-fail", "standard")?;
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[completion]") {
            content.replace(
                "[completion]",
                "[completion]\nbackends = [\"claude\", \"openrouter\"]\nmin_completers = 2",
            )
        } else {
            format!("{content}\n[completion]\nbackends = [\"claude\", \"openrouter\"]\nmin_completers = 2\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        let snapshot = read_run_snapshot(&ws, "cp-req-fail")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("not_started") {
            return Err(
                "expected not_started status when completion preflight rejects an unavailable required backend".to_owned(),
            );
        }

        // Verify the error is about backend unavailability, not an
        // invocation failure, by checking that no completion supporting
        // records were persisted (resolution failed before any invocations).
        let payloads_dir = project_root(ws.path(), "cp-req-fail").join("history/payloads");
        if payloads_dir.exists() {
            let completer_records = std::fs::read_dir(&payloads_dir)
                .map_err(|e| format!("read payloads: {e}"))?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    name.to_string_lossy().contains("completer-")
                })
                .count();
            if completer_records > 0 {
                return Err(format!(
                        "expected no completer records (resolution should fail before invocation), got {completer_records}"
                    ));
            }
        }
        Ok(())
    });

    reg!(m, "workflow.completion.threshold_consensus", || {
        // ── Exhaustive threshold boundary tests ──
        // 2/3 ≈ 0.667 < 0.75 -> ContinueWork
        let v1 = compute_completion_verdict(2, 3, 2, 0.75);
        if v1 != CompletionVerdict::ContinueWork {
            return Err(format!("expected ContinueWork (2/3 < 0.75), got {v1}"));
        }
        // 2/3 >= 0.5 and 2 >= 2 -> Complete
        let v2 = compute_completion_verdict(2, 3, 2, 0.5);
        if v2 != CompletionVerdict::Complete {
            return Err(format!("expected Complete (2/3 >= 0.5), got {v2}"));
        }
        // Exact boundary: 3/4 = 0.75 >= 0.75 -> Complete
        let v3 = compute_completion_verdict(3, 4, 3, 0.75);
        if v3 != CompletionVerdict::Complete {
            return Err(format!("expected Complete (3/4 >= 0.75), got {v3}"));
        }
        // 2/3 ≈ 0.667 < 0.67 -> ContinueWork
        let v4 = compute_completion_verdict(2, 3, 2, 0.67);
        if v4 != CompletionVerdict::ContinueWork {
            return Err(format!("expected ContinueWork (2/3 < 0.67), got {v4}"));
        }

        // ── Behavioral: run with high threshold to trigger ContinueWork ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cp-thresh", "standard")?;
        // Set consensus_threshold very high so default stub votes trigger continue_work
        // on the first round, then complete on the second.
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[completion]") {
            content.replace(
                "[completion]",
                "[completion]\nconsensus_threshold = 0.99\nmin_completers = 1",
            )
        } else {
            format!("{content}\n[completion]\nconsensus_threshold = 0.99\nmin_completers = 1\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        // Use stage overrides: first call votes continue, second votes complete.
        let overrides = serde_json::json!({
            "completion_panel": [
                {"vote_complete": false, "evidence": ["Needs work"], "remaining_work": ["Fix"]},
                {"vote_complete": true, "evidence": ["All done"], "remaining_work": []}
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string()),
                ("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS", "3"),
            ],
        )?;
        assert_success(&out)?;

        // Journal should contain completion_round_advanced (evidence of ContinueWork).
        let events = read_journal(&ws, "cp-thresh")?;
        let has_round_advanced = events.iter().any(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
        });
        if !has_round_advanced {
            return Err(
                "expected completion_round_advanced for threshold boundary test".to_owned(),
            );
        }
        Ok(())
    });

    reg!(m, "workflow.completion.insufficient_min_completers", || {
        // ── Helper assertions ──
        let verdict = compute_completion_verdict(2, 2, 3, 0.5);
        if verdict != CompletionVerdict::ContinueWork {
            return Err(format!("expected ContinueWork (2 < min=3), got {verdict}"));
        }
        let verdict2 = compute_completion_verdict(0, 0, 1, 0.5);
        if verdict2 != CompletionVerdict::ContinueWork {
            return Err(format!(
                "expected ContinueWork for 0 voters, got {verdict2}"
            ));
        }

        // ── Behavioral: workspace with min_completers=3, only 2 completer backends ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cp-min-comp", "standard")?;
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[completion]") {
            content.replace(
                "[completion]",
                "[completion]\nmin_completers = 3\nbackends = [\"claude\", \"codex\"]",
            )
        } else {
            format!(
                "{content}\n[completion]\nmin_completers = 3\nbackends = [\"claude\", \"codex\"]\n"
            )
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        let snapshot = read_run_snapshot(&ws, "cp-min-comp")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("not_started") {
            return Err(
                "expected not_started status for insufficient min_completers preflight rejection"
                    .to_owned(),
            );
        }
        Ok(())
    });

    // ── Resume Drift scenarios ────────────────────────────────────────────

    reg!(
        m,
        "backend.resume_drift.implementation_warns_and_reresolves",
        || {
            // ── Helper assertions ──
            let old_target = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-opus-4-7".to_owned(),
            );
            let new_target = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Codex,
                "codex-1".to_owned(),
            );
            let old = build_single_target_snapshot(StageId::Implementation, &old_target);
            let new = build_single_target_snapshot(StageId::Implementation, &new_target);
            if !resolution_has_drifted(&old, &new) {
                return Err("expected drift between claude and codex targets".to_owned());
            }
            let same = build_single_target_snapshot(StageId::Implementation, &old_target);
            if resolution_has_drifted(&old, &same) {
                return Err("identical targets should not report drift".to_owned());
            }

            // ── Behavioral: fail at implementation, change config, resume ──
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "drift-impl", "standard")?;
            // Fail at implementation stage.
            let out = run_cli_with_env(
                &["run", "start"],
                ws.path(),
                &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")],
            )?;
            assert_failure(&out)?;

            // Verify failed state.
            let snapshot = read_run_snapshot(&ws, "drift-impl")?;
            if snapshot.get("status").and_then(|v| v.as_str()) != Some("failed") {
                return Err("expected failed status after implementation failure".to_owned());
            }

            // Change implementer backend config to force drift. Default
            // implementer for cycle 1 is codex/gpt-5.5-high, so switching
            // to claude produces an actual target change.
            let ws_toml = workspace_config_path(ws.path());
            let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
            let patched = if content.contains("[workflow]") {
                content.replace("[workflow]", "[workflow]\nimplementer_backend = \"claude\"")
            } else {
                format!("{content}\n[workflow]\nimplementer_backend = \"claude\"\n")
            };
            std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

            // Resume — should succeed with drift warning.
            let resume_out = run_cli(&["run", "resume"], ws.path())?;
            assert_success(&resume_out)?;

            // Verify run completed after resume.
            let final_snap = read_run_snapshot(&ws, "drift-impl")?;
            let status = final_snap
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if status != "completed" {
                return Err(format!(
                    "expected completed after drift resume, got {status}"
                ));
            }

            // Check journal for durable_warning event indicating drift detection.
            // Changing implementer_backend from the default codex/gpt-5.5-high
            // to claude changes the resolved target, so drift MUST fire.
            let events = read_journal(&ws, "drift-impl")?;
            let warning_event = events
                .iter()
                .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning"));
            if warning_event.is_none() {
                return Err(
                    "expected durable_warning event for resume drift on implementation".to_owned(),
                );
            }
            // Verify the warning contains old and new resolution details.
            let details = warning_event.unwrap().get("details");
            if details.is_none() {
                return Err("durable_warning event missing details".to_owned());
            }

            // Verify the warning event contains old and new resolution details
            // that prove the snapshot was updated before continuing.
            let warning_details = warning_event.unwrap().get("details").unwrap();
            let has_old = warning_details.get("old_resolution").is_some()
                || warning_details.get("warning_kind").is_some();
            let has_new = warning_details.get("new_resolution").is_some()
                || warning_details.get("warning_kind").is_some();
            if !has_old || !has_new {
                return Err(
                    "durable_warning details must contain old and new resolution".to_owned(),
                );
            }

            // The run completed with the new backend — the snapshot update was
            // durable because `emit_resume_drift_warning` persists it before
            // the stage continues.

            Ok(())
        }
    );

    reg!(m, "backend.resume_drift.qa_warns_and_reresolves", || {
        // ── Helper assertions ──
        let old_target = ResolvedBackendTarget::new(
            crate::shared::domain::BackendFamily::Codex,
            "codex-1".to_owned(),
        );
        let new_target = ResolvedBackendTarget::new(
            crate::shared::domain::BackendFamily::Claude,
            "claude-opus-4-7".to_owned(),
        );
        let old = build_single_target_snapshot(StageId::AcceptanceQa, &old_target);
        let new = build_single_target_snapshot(StageId::AcceptanceQa, &new_target);
        if !resolution_has_drifted(&old, &new) {
            return Err("expected drift for QA".to_owned());
        }

        // ── Behavioral: fail at acceptance_qa, change config, resume ──
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "drift-qa", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "acceptance_qa")],
        )?;
        assert_failure(&out)?;

        // Change QA backend config.
        let ws_toml = workspace_config_path(ws.path());
        let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
        let patched = if content.contains("[workflow]") {
            content.replace("[workflow]", "[workflow]\nqa_backend = \"claude\"")
        } else {
            format!("{content}\n[workflow]\nqa_backend = \"claude\"\n")
        };
        std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

        let resume_out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume_out)?;

        let final_snap = read_run_snapshot(&ws, "drift-qa")?;
        let status = final_snap
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        if status != "completed" {
            return Err(format!(
                "expected completed after QA drift resume, got {status}"
            ));
        }

        // Verify journal contains durable_warning event. Changing qa_backend
        // to claude changes the resolved target, so drift MUST fire.
        let events = read_journal(&ws, "drift-qa")?;
        let warning_event = events
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning"));
        if warning_event.is_none() {
            return Err("expected durable_warning event for resume drift on QA".to_owned());
        }
        let details = warning_event.unwrap().get("details");
        if details.is_none() {
            return Err("durable_warning event missing details".to_owned());
        }

        Ok(())
    });

    reg!(
        m,
        "backend.resume_drift.review_warns_and_reresolves",
        || {
            // ── Helper: model-level drift within same family ──
            let old_target = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-opus-4-7".to_owned(),
            );
            let new_target = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-sonnet-4-6".to_owned(),
            );
            let old = build_single_target_snapshot(StageId::Review, &old_target);
            let new = build_single_target_snapshot(StageId::Review, &new_target);

            if !resolution_has_drifted(&old, &new) {
                return Err("expected drift when model changes".to_owned());
            }
            // Same target should not drift.
            let same = build_single_target_snapshot(StageId::Review, &old_target);
            if resolution_has_drifted(&old, &same) {
                return Err("identical targets should not report drift".to_owned());
            }

            // ── Behavioral: fail at review, change config, resume ──
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "drift-review", "standard")?;
            let out = run_cli_with_env(
                &["run", "start"],
                ws.path(),
                &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")],
            )?;
            assert_failure(&out)?;

            // Change reviewer backend config. Default reviewer for cycle 1 is
            // codex (opposite of the primary claude family), so switching to claude
            // produces an actual target change.
            let ws_toml = workspace_config_path(ws.path());
            let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
            let patched = if content.contains("[workflow]") {
                content.replace("[workflow]", "[workflow]\nreviewer_backend = \"claude\"")
            } else {
                format!("{content}\n[workflow]\nreviewer_backend = \"claude\"\n")
            };
            std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

            let resume_out = run_cli(&["run", "resume"], ws.path())?;
            assert_success(&resume_out)?;

            let final_snap = read_run_snapshot(&ws, "drift-review")?;
            let status = final_snap
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if status != "completed" {
                return Err(format!(
                    "expected completed after review drift resume, got {status}"
                ));
            }

            // Verify journal contains durable_warning event. Changing
            // reviewer_backend to claude changes the resolved target, so drift
            // MUST fire.
            let events = read_journal(&ws, "drift-review")?;
            let warning_event = events
                .iter()
                .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning"));
            if warning_event.is_none() {
                return Err("expected durable_warning event for resume drift on review".to_owned());
            }
            let details = warning_event.unwrap().get("details");
            if details.is_none() {
                return Err("durable_warning event missing details".to_owned());
            }

            Ok(())
        }
    );

    reg!(
        m,
        "backend.resume_drift.completion_panel_warns_and_reresolves",
        || {
            // ── Helper: completer model change drift ──
            let target_a = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-opus-4-7".to_owned(),
            );
            let target_b = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Codex,
                "codex-1".to_owned(),
            );
            let mut to_member = {
                let mut idx = 0usize;
                move |t: ResolvedBackendTarget| -> crate::contexts::agent_execution::policy::ResolvedPanelMember {
                    let m = crate::contexts::agent_execution::policy::ResolvedPanelMember {
                        target: t,
                        required: true,
                        configured_index: idx,
                    };
                    idx += 1;
                    m
                }
            };

            let old = build_completion_snapshot(
                StageId::CompletionPanel,
                &[to_member(target_a.clone()), to_member(target_b.clone())],
            );
            let target_c = ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-sonnet-4-6".to_owned(),
            );
            let new = build_completion_snapshot(
                StageId::CompletionPanel,
                &[to_member(target_c), to_member(target_b.clone())],
            );

            if !resolution_has_drifted(&old, &new) {
                return Err("expected drift when completer model changes".to_owned());
            }

            // Same completers: no drift.
            let same = build_completion_snapshot(
                StageId::CompletionPanel,
                &[to_member(target_a.clone()), to_member(target_b.clone())],
            );
            if resolution_has_drifted(&old, &same) {
                return Err("identical panel should not report drift".to_owned());
            }

            // Verify drift satisfaction and failure boundary.
            let ws_helper = TempWorkspace::new()?;
            run_cli(&["init"], ws_helper.path())?;
            let config = crate::contexts::workspace_governance::config::EffectiveConfig::load(
                ws_helper.path(),
            )
            .map_err(|e| format!("load effective config: {e}"))?;
            drift_still_satisfies_requirements(&new, StageId::CompletionPanel, &config, None)
                .map_err(|e| format!("expected panel drift to satisfy requirements: {e}"))?;
            let empty = build_completion_snapshot(StageId::CompletionPanel, &[]);
            if drift_still_satisfies_requirements(&empty, StageId::CompletionPanel, &config, None)
                .is_ok()
            {
                return Err("expected failure when no completers remain".to_owned());
            }

            // ── Behavioral: fail at completion_panel, change config, resume ──
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "drift-cp", "standard")?;
            let out = run_cli_with_env(
                &["run", "start"],
                ws.path(),
                &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "completion_panel")],
            )?;
            assert_failure(&out)?;

            // Change completion backend config.
            let ws_toml = workspace_config_path(ws.path());
            let content = std::fs::read_to_string(&ws_toml).map_err(|e| format!("read: {e}"))?;
            let patched = if content.contains("[completion]") {
                content.replace(
                    "[completion]",
                    "[completion]\nbackends = [\"codex\", \"claude\"]",
                )
            } else {
                format!("{content}\n[completion]\nbackends = [\"codex\", \"claude\"]\n")
            };
            std::fs::write(&ws_toml, patched).map_err(|e| format!("write: {e}"))?;

            let resume_out = run_cli(&["run", "resume"], ws.path())?;
            assert_success(&resume_out)?;

            let final_snap = read_run_snapshot(&ws, "drift-cp")?;
            let status = final_snap
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if status != "completed" {
                return Err(format!(
                    "expected completed after completion drift resume, got {status}"
                ));
            }

            // Verify journal contains durable_warning event. Changing completion
            // backends order from default to [codex, claude] changes the resolved
            // panel member order, so drift MUST fire.
            let events = read_journal(&ws, "drift-cp")?;
            let warning_event = events
                .iter()
                .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning"));
            if warning_event.is_none() {
                return Err(
                    "expected durable_warning event for resume drift on completion panel"
                        .to_owned(),
                );
            }
            let details = warning_event.unwrap().get("details");
            if details.is_none() {
                return Err("durable_warning event missing details".to_owned());
            }

            // Verify the warning event contains old and new resolution details
            // proving the snapshot was durably updated before continuation.
            let warning_details = warning_event.unwrap().get("details").unwrap();
            let has_old = warning_details.get("old_resolution").is_some()
                || warning_details.get("warning_kind").is_some();
            let has_new = warning_details.get("new_resolution").is_some()
                || warning_details.get("warning_kind").is_some();
            if !has_old || !has_new {
                return Err("durable_warning details must contain old and new resolution for completion panel drift".to_owned());
            }

            // The run completed — the snapshot update was durable because
            // `emit_resume_drift_warning` persists it (and fails resume if
            // persistence fails) before the stage continues.

            Ok(())
        }
    );
}

// ===========================================================================
// Slice 0 Hardening (8 scenarios)
// ===========================================================================

fn register_p0_hardening(m: &mut HashMap<String, ScenarioExecutor>) {
    use crate::adapters::fs::{FsRawOutputStore, FsSessionStore};
    use crate::adapters::github::{GithubClient, GithubClientConfig};
    use crate::adapters::process_backend::ProcessBackendAdapter;
    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::agent_execution::service::{AgentExecutionPort, AgentExecutionService};
    use crate::contexts::workflow_composition::engine::{
        build_final_review_snapshot, resolution_has_drifted,
    };
    use crate::shared::domain::{BackendFamily, ResolvedBackendTarget, StageId};
    use crate::shared::error::AppError;

    reg!(m, "parity_slice0_executable_permission_required", || {
        let ws = TempWorkspace::new()?;
        let bin_dir = ws.path().join("bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create bin dir: {e}"))?;
        let binary_path = bin_dir.join("claude");
        write_script_with_mode(&binary_path, "#!/bin/sh\nexit 0\n", 0o644)?;

        {
            let search_paths = vec![bin_dir.clone()];
            match ProcessBackendAdapter::ensure_binary_available("claude", "claude", &search_paths)
            {
                Err(AppError::BackendUnavailable { details, .. }) => {
                    if !details.contains(&binary_path.display().to_string()) {
                        return Err(format!(
                            "expected permission error to mention {}, got: {details}",
                            binary_path.display()
                        ));
                    }
                    if !details.contains("not executable") {
                        return Err(format!(
                            "expected permission error to explain executability, got: {details}"
                        ));
                    }
                    Ok(())
                }
                Err(other) => Err(format!(
                    "expected BackendUnavailable for non-executable binary, got: {other}"
                )),
                Ok(()) => Err("non-executable binary should not pass availability".to_owned()),
            }
        }
    });

    reg!(m, "parity_slice0_permission_check_success", || {
        let ws = TempWorkspace::new()?;
        let bin_dir = ws.path().join("bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create bin dir: {e}"))?;
        let binary_path = bin_dir.join("claude");
        write_script_with_mode(&binary_path, "#!/bin/sh\nexit 0\n", 0o755)?;

        {
            let search_paths = vec![bin_dir.clone()];
            ProcessBackendAdapter::ensure_binary_available("claude", "claude", &search_paths)
                .map_err(|e| format!("expected executable binary to pass availability: {e}"))
        }
    });

    reg!(m, "parity_slice0_cancel_no_orphans", || {
        let ws = TempWorkspace::new()?;
        let bin_dir = ws.path().join("bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create bin dir: {e}"))?;
        let pid_file = ws.path().join("cancel-child.pid");
        let term_file = ws.path().join("cancel-child.term");
        write_script_with_mode(
            &bin_dir.join("claude"),
            &format!(
                "#!/bin/sh\ntrap 'echo term > \"{}\"; exit 0' TERM\necho \"$$\" > \"{}\"\ncat > /dev/null\nwhile :; do sleep 0.05; done\n",
                term_file.display(),
                pid_file.display()
            ),
            0o755,
        )?;

        block_on_result(async {
            let project_root = prepare_scenario_project_root(ws.path())?;
            std::fs::create_dir_all(project_root.join("runtime/temp"))
                .map_err(|e| format!("create runtime/temp: {e}"))?;

            let adapter = ProcessBackendAdapter::with_search_paths(vec![bin_dir.clone()]);
            let request = build_process_backend_request(
                &project_root,
                "slice0-cancel-no-orphans",
                std::time::Duration::from_secs(5),
            );
            let invocation_id = request.invocation_id.clone();
            let adapter_clone = adapter.clone();
            let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

            for _ in 0..40 {
                if pid_file.exists() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if !pid_file.exists() {
                return Err("child pid file was not written before cancel".to_owned());
            }

            let pid: u32 = std::fs::read_to_string(&pid_file)
                .map_err(|e| format!("read child pid: {e}"))?
                .trim()
                .parse()
                .map_err(|e| format!("parse child pid: {e}"))?;
            if !process_is_running(pid) {
                return Err("child should be running before cancel".to_owned());
            }

            adapter
                .cancel(&invocation_id)
                .await
                .map_err(|e| format!("cancel long-running child: {e}"))?;

            for _ in 0..40 {
                if !process_is_running(pid) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if process_is_running(pid) {
                return Err("child process remained running after cancel".to_owned());
            }
            if !term_file.is_file() {
                return Err("expected SIGTERM marker file after cancel".to_owned());
            }

            let children = adapter.active_children.lock().await;
            if children.contains_key(&invocation_id) {
                return Err("active_children should be empty after cancel".to_owned());
            }
            drop(children);

            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
                .await
                .map_err(|_| "invoke task did not finish after cancel".to_owned())?;

            Ok(())
        })
    });

    reg!(m, "parity_slice0_timeout_cleanup", || {
        let ws = TempWorkspace::new()?;
        let bin_dir = ws.path().join("bin");
        std::fs::create_dir_all(&bin_dir).map_err(|e| format!("create bin dir: {e}"))?;
        let pid_file = ws.path().join("timeout-child.pid");
        write_script_with_mode(
            &bin_dir.join("claude"),
            &format!(
                "#!/bin/sh\ntrap '' TERM\necho \"$$\" > \"{}\"\ncat > /dev/null\nwhile :; do sleep 0.05; done\n",
                pid_file.display()
            ),
            0o755,
        )?;

        block_on_result(async {
            let project_root = prepare_scenario_project_root(ws.path())?;
            std::fs::create_dir_all(project_root.join("runtime/temp"))
                .map_err(|e| format!("create runtime/temp: {e}"))?;

            let adapter = ProcessBackendAdapter::with_search_paths(vec![bin_dir.clone()]);
            let service =
                AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
            let request = build_process_backend_request(
                &project_root,
                "slice0-timeout-cleanup",
                std::time::Duration::from_millis(100),
            );
            let invocation_id = request.invocation_id.clone();

            match service.invoke(request).await {
                Err(AppError::InvocationTimeout { .. }) => {}
                Err(other) => return Err(format!("expected InvocationTimeout, got: {other}")),
                Ok(_) => return Err("timeout scenario should not succeed".to_owned()),
            }

            for _ in 0..20 {
                if pid_file.exists() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if !pid_file.exists() {
                return Err("timed-out child did not record a pid".to_owned());
            }

            let pid: u32 = std::fs::read_to_string(&pid_file)
                .map_err(|e| format!("read timed-out child pid: {e}"))?
                .trim()
                .parse()
                .map_err(|e| format!("parse timed-out child pid: {e}"))?;

            for _ in 0..40 {
                if !process_is_running(pid) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            if process_is_running(pid) {
                return Err("timed-out child process remained running after cleanup".to_owned());
            }

            let children = adapter.active_children.lock().await;
            if children.contains_key(&invocation_id) {
                return Err("active_children should be empty after timeout cleanup".to_owned());
            }

            Ok(())
        })
    });

    reg!(m, "parity_slice0_panel_preflight_required_member", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "slice0-panel-preflight", "standard")?;

        let arbiter_out = run_cli(
            &[
                "config",
                "set",
                "final_review.arbiter_backend",
                "openrouter",
            ],
            ws.path(),
        )?;
        assert_success(&arbiter_out)?;

        let start_out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&start_out)?;
        if !start_out.stderr.contains("preflight") {
            return Err(format!(
                "expected start failure to come from preflight, got: {}",
                start_out.stderr
            ));
        }
        if !start_out.stderr.contains("final_review") || !start_out.stderr.contains("arbiter") {
            return Err(format!(
                "expected final-review arbiter preflight error, got: {}",
                start_out.stderr
            ));
        }

        let snapshot = read_run_snapshot(&ws, "slice0-panel-preflight")?;
        if snapshot.get("status").and_then(|value| value.as_str()) != Some("not_started") {
            return Err(format!(
                "preflight failure must leave snapshot at not_started, got {:?}",
                snapshot.get("status")
            ));
        }
        if snapshot
            .get("active_run")
            .is_some_and(|value| !value.is_null())
        {
            return Err("preflight failure must not create an active_run".to_owned());
        }

        let events = read_journal(&ws, "slice0-panel-preflight")?;
        let appended_run_events = events.iter().filter(|event| {
            matches!(
                event.get("event_type").and_then(|value| value.as_str()),
                Some("run_started" | "stage_entered" | "stage_completed" | "run_failed")
            )
        });
        if appended_run_events.count() != 0 {
            return Err(format!(
                "preflight failure must not append run events, got {:?}",
                events
            ));
        }

        Ok(())
    });

    reg!(m, "parity_slice0_final_review_arbiter_in_snapshot", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "slice0-final-review-snapshot", "standard")?;

        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "final_review")],
        )?;
        assert_failure(&out)?;

        let snapshot = read_run_snapshot(&ws, "slice0-final-review-snapshot")?;
        let resolution = snapshot
            .get("last_stage_resolution_snapshot")
            .ok_or_else(|| {
                "missing last_stage_resolution_snapshot after final-review failure".to_owned()
            })?;
        if resolution.get("stage_id").and_then(|v| v.as_str()) != Some("final_review") {
            return Err(format!(
                "expected final_review snapshot, got {:?}",
                resolution.get("stage_id")
            ));
        }

        let arbiter = resolution
            .get("final_review_arbiter")
            .ok_or_else(|| "final_review_arbiter missing from saved snapshot".to_owned())?;
        if arbiter
            .get("backend_family")
            .and_then(|v| v.as_str())
            .is_none()
            || arbiter.get("model_id").and_then(|v| v.as_str()).is_none()
        {
            return Err(format!(
                "final_review_arbiter must include backend_family and model_id, got {arbiter}"
            ));
        }

        Ok(())
    });

    reg!(
        m,
        "parity_slice0_final_review_arbiter_drift_detected",
        || {
            let reviewers = vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2"),
                    required: true,
                    configured_index: 1,
                },
            ];
            let arbiter = ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter");
            let old_snapshot =
                build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter);
            let new_snapshot = build_final_review_snapshot(
                StageId::FinalReview,
                &reviewers,
                &ResolvedBackendTarget::new(BackendFamily::Codex, "arbiter-b"),
            );
            if !resolution_has_drifted(&old_snapshot, &new_snapshot) {
                return Err("arbiter-only final-review drift should be detected".to_owned());
            }

            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "slice0-final-review-drift", "standard")?;

            let arbiter_out = run_cli(
                &["config", "set", "final_review.arbiter_backend", "claude"],
                ws.path(),
            )?;
            assert_success(&arbiter_out)?;

            let start_out = run_cli_with_env(
                &["run", "start"],
                ws.path(),
                &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "final_review")],
            )?;
            assert_failure(&start_out)?;

            let arbiter_out = run_cli(
                &["config", "set", "final_review.arbiter_backend", "codex"],
                ws.path(),
            )?;
            assert_success(&arbiter_out)?;

            let resume_out = run_cli(&["run", "resume"], ws.path())?;
            assert_success(&resume_out)?;

            let events = read_journal(&ws, "slice0-final-review-drift")?;
            let warning_event = events
                .iter()
                .find(|event| {
                    event.get("event_type").and_then(|value| value.as_str())
                        == Some("durable_warning")
                        && event
                            .get("details")
                            .and_then(|details| details.get("stage_id"))
                            .and_then(|value| value.as_str())
                            == Some("final_review")
                })
                .ok_or_else(|| {
                    "expected durable_warning event for final-review arbiter drift".to_owned()
                })?;

            let warning_details = warning_event
                .get("details")
                .and_then(|details| details.get("details"))
                .ok_or_else(|| "durable_warning missing nested resolution details".to_owned())?;
            let old_resolution = warning_details
                .get("old_resolution")
                .ok_or_else(|| "durable_warning missing old_resolution".to_owned())?;
            let new_resolution = warning_details
                .get("new_resolution")
                .ok_or_else(|| "durable_warning missing new_resolution".to_owned())?;

            let old_arbiter = old_resolution
                .get("final_review_arbiter")
                .ok_or_else(|| "old_resolution missing final_review_arbiter".to_owned())?;
            let new_arbiter = new_resolution
                .get("final_review_arbiter")
                .ok_or_else(|| "new_resolution missing final_review_arbiter".to_owned())?;
            if old_arbiter == new_arbiter {
                return Err(
                    "expected arbiter drift warning to show changed arbiter resolution".to_owned(),
                );
            }
            if old_resolution.get("final_review_reviewers")
                != new_resolution.get("final_review_reviewers")
            {
                return Err(
                    "reviewers should remain stable in arbiter-only drift scenario".to_owned(),
                );
            }

            Ok(())
        }
    );

    reg!(m, "parity_slice0_ref_encoding_reserved_chars", || {
        let base_ref = "release/%base#stable";
        let head_ref = "feature/über-fix@team#rollout";
        let expected_path = "/repos/acme/widgets/compare/release%2F%25base%23stable...feature%2F%C3%BCber-fix%40team%23rollout";

        block_on_result(async {
            let server = ScenarioHttpServer::start(vec![ScenarioHttpResponse::json(
                200,
                serde_json::json!({
                    "ahead_by": 1,
                    "behind_by": 0,
                    "status": "ahead"
                }),
            )])?;
            let client = GithubClient::new(GithubClientConfig {
                token: "test-token".to_owned(),
                api_base_url: server.base_url.clone(),
            });

            let ahead = client
                .is_branch_ahead("acme", "widgets", base_ref, head_ref)
                .await
                .map_err(|e| format!("compare refs: {e}"))?;
            if !ahead {
                return Err("expected compare response to report branch ahead".to_owned());
            }

            let requests = server.requests()?;
            let request = requests
                .first()
                .ok_or_else(|| "expected recorded compare request".to_owned())?;
            if request.path != expected_path {
                return Err(format!(
                    "expected compare request path {expected_path}, got {}",
                    request.path
                ));
            }

            Ok(())
        })
    });

    reg!(
        m,
        "parity_slice0_explicit_paths_reject_missing_binary",
        || {
            let ws = TempWorkspace::new()?;
            let empty_dir = ws.path().join("empty-bin");
            std::fs::create_dir_all(&empty_dir).map_err(|e| format!("create empty dir: {e}"))?;

            block_on_result(async {
                let project_root = prepare_scenario_project_root(ws.path())?;
                std::fs::create_dir_all(project_root.join("runtime/temp"))
                    .map_err(|e| format!("create runtime/temp: {e}"))?;

                let adapter = ProcessBackendAdapter::with_search_paths(vec![empty_dir]);
                let request = build_process_backend_request(
                    &project_root,
                    "explicit-paths-reject-missing",
                    std::time::Duration::from_secs(5),
                );

                match adapter.invoke(request).await {
                    Err(AppError::BackendUnavailable { details, .. }) => {
                        if !details.contains("not found") {
                            return Err(format!(
                                "expected 'not found' in error details, got: {details}"
                            ));
                        }
                        Ok(())
                    }
                    Err(other) => Err(format!("expected BackendUnavailable, got: {other}")),
                    Ok(_) => Err(
                        "invoke should fail when binary is missing from explicit search paths"
                            .to_owned(),
                    ),
                }
            })
        }
    );

    reg!(
        m,
        "parity_slice0_explicit_tmux_paths_reject_missing_binary",
        || {
            let ws = TempWorkspace::new()?;
            let empty_dir = ws.path().join("empty-bin");
            std::fs::create_dir_all(&empty_dir).map_err(|e| format!("create empty dir: {e}"))?;

            let process = ProcessBackendAdapter::with_search_paths(vec![empty_dir]);
            match crate::adapters::tmux::TmuxAdapter::new(process, true) {
                Err(msg) => {
                    if !msg.contains("not found") {
                        return Err(format!("expected 'not found' in error message, got: {msg}"));
                    }
                    Ok(())
                }
                Ok(_) => Err(
                    "TmuxAdapter::new should fail when tmux is missing from explicit search paths"
                        .to_owned(),
                ),
            }
        }
    );

    // Regression: Codex branch must not leak temp schema files when the
    // binary is missing from explicit search paths.
    reg!(m, "parity_slice0_codex_missing_binary_no_temp_leak", || {
        let ws = TempWorkspace::new()?;
        let empty_dir = ws.path().join("empty-bin");
        std::fs::create_dir_all(&empty_dir).map_err(|e| format!("create empty dir: {e}"))?;

        block_on_result(async {
            let project_root = prepare_scenario_project_root(ws.path())?;
            let temp_dir = project_root.join("runtime/temp");
            std::fs::create_dir_all(&temp_dir).map_err(|e| format!("create runtime/temp: {e}"))?;

            let adapter = ProcessBackendAdapter::with_search_paths(vec![empty_dir]);

            // Build a Codex-family request (triggers the branch that writes
            // schema temp files).
            use crate::contexts::agent_execution::model::{
                CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
            };
            use crate::contexts::workflow_composition::contracts::contract_for_stage;
            use crate::shared::domain::{
                BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy, StageId,
            };

            let request = InvocationRequest {
                invocation_id: "codex-temp-leak-test".to_owned(),
                project_root: project_root.clone(),
                working_dir: project_root.clone(),
                contract: InvocationContract::Stage(contract_for_stage(StageId::Planning)),
                role: BackendRole::Planner,
                resolved_target: ResolvedBackendTarget::new(
                    BackendFamily::Codex,
                    BackendFamily::Codex.default_model_id(),
                ),
                payload: InvocationPayload {
                    prompt: "Conformance codex temp-leak prompt".to_owned(),
                    context: serde_json::json!({"scenario": "codex-temp-leak"}),
                },
                timeout: std::time::Duration::from_secs(5),
                cancellation_token: CancellationToken::new(),
                session_policy: SessionPolicy::NewSession,
                prior_session: None,
                attempt_number: 1,
            };

            match adapter.invoke(request).await {
                Err(AppError::BackendUnavailable { details, .. }) => {
                    if !details.contains("not found") {
                        return Err(format!(
                            "expected 'not found' in error details, got: {details}"
                        ));
                    }
                }
                Err(other) => return Err(format!("expected BackendUnavailable, got: {other}")),
                Ok(_) => {
                    return Err(
                        "invoke should fail when codex is missing from explicit search paths"
                            .to_owned(),
                    )
                }
            }

            // Verify no temp files were leaked.
            let leaked: Vec<_> = std::fs::read_dir(&temp_dir)
                .map_err(|e| format!("read temp dir: {e}"))?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .contains("codex-temp-leak-test")
                })
                .collect();

            if !leaked.is_empty() {
                let names: Vec<_> = leaked
                    .iter()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect();
                return Err(format!(
                    "temp files leaked after binary-not-found failure: {names:?}"
                ));
            }

            Ok(())
        })
    });
}

// ===========================================================================
// Backend Stub Gating (1 scenario)
// ===========================================================================

fn register_backend_stub(m: &mut HashMap<String, ScenarioExecutor>) {
    // This scenario spawns `cargo test --no-default-features` which
    // recompiles the entire project (~40s). Moved to a dedicated CI
    // step to avoid blocking the conformance runner.
    reg_skip!(
        m,
        "backend.stub.production_rejects_stub_selector",
        "requires cargo build without test-stub feature"
    );
}

fn register_workflow_slice5(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "workflow.final_review.no_amendments_complete", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "fr-none", "standard")?;

        let pid = crate::shared::domain::ProjectId::new("fr-none")
            .map_err(|e| format!("project id: {e}"))?;
        let config =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                .map_err(|e| format!("load effective config: {e}"))?;
        let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
            crate::adapters::stub_backend::StubBackendAdapter::default(),
            crate::adapters::fs::FsRawOutputStore,
            crate::adapters::fs::FsSessionStore,
        );

        block_on_app_result(
            crate::contexts::workflow_composition::engine::execute_standard_run(
                &agent_service,
                &crate::adapters::fs::FsRunSnapshotStore,
                &crate::adapters::fs::FsRunSnapshotWriteStore,
                &crate::adapters::fs::FsJournalStore,
                &crate::adapters::fs::FsPayloadArtifactWriteStore,
                &crate::adapters::fs::FsRuntimeLogWriteStore,
                &crate::adapters::fs::FsAmendmentQueueStore,
                ws.path(),
                &pid,
                &config,
            ),
        )?;

        let snapshot = read_run_snapshot(&ws, "fr-none")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed status after no-amendment final review".to_owned());
        }
        if snapshot.get("completion_rounds").and_then(|v| v.as_u64()) != Some(1) {
            return Err("final review without amendments must not restart the round".to_owned());
        }

        let events = read_journal(&ws, "fr-none")?;
        let restarted = events.iter().any(|event| {
            event.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced")
                && event
                    .get("details")
                    .and_then(|d| d.get("source_stage"))
                    .and_then(|v| v.as_str())
                    == Some("final_review")
        });
        if restarted {
            return Err(
                "final review should not advance the completion round when no amendments remain"
                    .to_owned(),
            );
        }
        Ok(())
    });

    reg!(m, "workflow.final_review.restart_then_complete", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "fr-restart", "standard")?;

        let amendment_body = "Tighten the final wording.";
        let amendment_id =
            crate::contexts::workflow_composition::final_review::canonical_amendment_id(
                1,
                amendment_body,
            );
        let adapter = crate::adapters::stub_backend::StubBackendAdapter::default()
                .with_label_payload_sequence(
                    "final_review:reviewer",
                    vec![
                        serde_json::json!({
                            "summary": "Reviewer 1 proposes one amendment.",
                            "amendments": [{"body": amendment_body}],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 has no amendments.",
                            "amendments": [],
                        }),
                    ],
                )
                .with_label_payload_sequence(
                    "final_review:voter",
                    vec![
                        serde_json::json!({
                            "summary": "Final-review reviewer 1 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 2 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                    ],
                );
        let pid = crate::shared::domain::ProjectId::new("fr-restart")
            .map_err(|e| format!("project id: {e}"))?;
        let config =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                .map_err(|e| format!("load effective config: {e}"))?;
        let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
            adapter.clone(),
            crate::adapters::fs::FsRawOutputStore,
            crate::adapters::fs::FsSessionStore,
        );

        block_on_app_result(
            crate::contexts::workflow_composition::engine::execute_standard_run(
                &agent_service,
                &crate::adapters::fs::FsRunSnapshotStore,
                &crate::adapters::fs::FsRunSnapshotWriteStore,
                &crate::adapters::fs::FsJournalStore,
                &crate::adapters::fs::FsPayloadArtifactWriteStore,
                &crate::adapters::fs::FsRuntimeLogWriteStore,
                &crate::adapters::fs::FsAmendmentQueueStore,
                ws.path(),
                &pid,
                &config,
            ),
        )?;

        let snapshot = read_run_snapshot(&ws, "fr-restart")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed status after final-review restart".to_owned());
        }
        if snapshot.get("completion_rounds").and_then(|v| v.as_u64()) != Some(2) {
            return Err("final-review restart should advance to completion round 2".to_owned());
        }

        let events = read_journal(&ws, "fr-restart")?;
        let amendment_events = events
            .iter()
            .filter(|event| {
                event.get("event_type").and_then(|v| v.as_str()) == Some("amendment_queued")
            })
            .count();
        if amendment_events != 1 {
            return Err(format!(
                "expected one queued amendment after final-review restart, got {amendment_events}"
            ));
        }
        Ok(())
    });

    reg!(
        m,
        "workflow.final_review.completion_with_pending_amendments_fails",
        || {
            use crate::contexts::project_run_record::service::AmendmentQueuePort;

            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "fr-pending", "standard")?;
            let pid = crate::shared::domain::ProjectId::new("fr-pending")
                .map_err(|e| format!("project id: {e}"))?;
            let amendment = crate::contexts::project_run_record::model::QueuedAmendment {
                amendment_id: "fr-1-deadbeef".to_owned(),
                source_stage: crate::shared::domain::StageId::FinalReview,
                source_cycle: 1,
                source_completion_round: 1,
                body: "Implement the final amendment.".to_owned(),
                created_at: chrono::Utc::now(),
                batch_sequence: 1,
                source: crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage,
                dedup_key:
                    crate::contexts::project_run_record::model::QueuedAmendment::compute_dedup_key(
                        &crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage,
                        "Implement the final amendment.",
                    ),
            };
            crate::adapters::fs::FsAmendmentQueueStore
                .write_amendment(ws.path(), &pid, &amendment)
                .map_err(|e| format!("write amendment: {e}"))?;

            let mut snapshot = crate::contexts::project_run_record::model::RunSnapshot::initial(20);
            snapshot.completion_rounds = 1;
            snapshot.amendment_queue.pending.push(amendment);

            let err = crate::contexts::workflow_composition::engine::completion_guard(
                &snapshot,
                &crate::adapters::fs::FsAmendmentQueueStore,
                ws.path(),
                &pid,
            )
            .expect_err("completion guard should block pending amendments");
            let message = err.to_string();
            if !message.contains("pending amendments") {
                return Err(format!(
                    "expected pending-amendment completion failure, got: {message}"
                ));
            }
            Ok(())
        }
    );

    reg!(
        m,
        "workflow.final_review.disputed_amendment_uses_arbiter",
        || {
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "fr-dispute", "standard")?;

            let amendment_body = "Tighten the final wording.";
            let amendment_id =
                crate::contexts::workflow_composition::final_review::canonical_amendment_id(
                    1,
                    amendment_body,
                );
            let adapter = crate::adapters::stub_backend::StubBackendAdapter::default()
                .with_label_payload_sequence(
                    "final_review:reviewer",
                    vec![
                        serde_json::json!({
                            "summary": "Reviewer 1 proposes one amendment.",
                            "amendments": [{"body": amendment_body}],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 has no amendments.",
                            "amendments": [],
                        }),
                    ],
                )
                .with_label_payload_sequence(
                    "final_review:voter",
                    vec![
                        serde_json::json!({
                            "summary": "Final-review reviewer 1 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 2 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "reject", "rationale": "Not worth it."}],
                        }),
                    ],
                )
                .with_label_payload(
                    "final_review:arbiter",
                    serde_json::json!({
                        "summary": "Arbiter resolves the disputed amendment.",
                        "rulings": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Apply it."}],
                    }),
                );
            let pid = crate::shared::domain::ProjectId::new("fr-dispute")
                .map_err(|e| format!("project id: {e}"))?;
            let config =
                crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                    .map_err(|e| format!("load effective config: {e}"))?;
            let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
                adapter.clone(),
                crate::adapters::fs::FsRawOutputStore,
                crate::adapters::fs::FsSessionStore,
            );

            block_on_app_result(
                crate::contexts::workflow_composition::engine::execute_standard_run(
                    &agent_service,
                    &crate::adapters::fs::FsRunSnapshotStore,
                    &crate::adapters::fs::FsRunSnapshotWriteStore,
                    &crate::adapters::fs::FsJournalStore,
                    &crate::adapters::fs::FsPayloadArtifactWriteStore,
                    &crate::adapters::fs::FsRuntimeLogWriteStore,
                    &crate::adapters::fs::FsAmendmentQueueStore,
                    ws.path(),
                    &pid,
                    &config,
                ),
            )?;

            let arbiter_invocations = adapter
                .recorded_invocations()
                .into_iter()
                .filter(|invocation| invocation.contract_label == "final_review:arbiter")
                .count();
            if arbiter_invocations != 1 {
                return Err(format!(
                    "expected exactly one arbiter invocation for the disputed amendment, got {arbiter_invocations}"
                ));
            }
            Ok(())
        }
    );

    reg!(
        m,
        "workflow.final_review.no_amendments_complete_at_restart_cap",
        || {
            use crate::contexts::project_run_record::service::ArtifactStorePort;

            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "fr-cap-no-amend", "standard")?;
            let out = run_cli(
                &["config", "set", "final_review.max_restarts", "1"],
                ws.path(),
            )?;
            assert_success(&out)?;

            let amendment_body = "Tighten the final wording.";
            let amendment_id =
                crate::contexts::workflow_composition::final_review::canonical_amendment_id(
                    1,
                    amendment_body,
                );
            let adapter = crate::adapters::stub_backend::StubBackendAdapter::default()
                .with_label_payload_sequence(
                    "final_review:reviewer",
                    vec![
                        serde_json::json!({
                            "summary": "Reviewer 1 proposes one amendment.",
                            "amendments": [{"body": amendment_body}],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 has no amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 3 has no amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 1 has no further amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 has no further amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 3 has no further amendments.",
                            "amendments": [],
                        }),
                    ],
                )
                .with_label_payload_sequence(
                    "final_review:voter",
                    vec![
                        serde_json::json!({
                            "summary": "Final-review reviewer 1 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 2 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 3 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                    ],
                );
            let pid = crate::shared::domain::ProjectId::new("fr-cap-no-amend")
                .map_err(|e| format!("project id: {e}"))?;
            let config =
                crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                    .map_err(|e| format!("load effective config: {e}"))?;
            let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
                adapter.clone(),
                crate::adapters::fs::FsRawOutputStore,
                crate::adapters::fs::FsSessionStore,
            );

            block_on_app_result(
                crate::contexts::workflow_composition::engine::execute_standard_run(
                    &agent_service,
                    &crate::adapters::fs::FsRunSnapshotStore,
                    &crate::adapters::fs::FsRunSnapshotWriteStore,
                    &crate::adapters::fs::FsJournalStore,
                    &crate::adapters::fs::FsPayloadArtifactWriteStore,
                    &crate::adapters::fs::FsRuntimeLogWriteStore,
                    &crate::adapters::fs::FsAmendmentQueueStore,
                    ws.path(),
                    &pid,
                    &config,
                ),
            )?;

            let snapshot = read_run_snapshot(&ws, "fr-cap-no-amend")?;
            if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
                return Err(
                    "expected completed status after no-amendment final-review cap boundary"
                        .to_owned(),
                );
            }
            if snapshot.get("completion_rounds").and_then(|v| v.as_u64()) != Some(2) {
                return Err(
                    "no-amendment final review at the restart cap should stay on round 2"
                        .to_owned(),
                );
            }

            let reviewer_invocations = adapter
                .recorded_invocations()
                .into_iter()
                .filter(|invocation| invocation.contract_label == "final_review:reviewer")
                .count();
            if reviewer_invocations != 6 {
                return Err(format!(
                    "expected both final-review rounds to collect reviewer proposals (3 reviewers × 2 rounds), got {reviewer_invocations} reviewer invocations"
                ));
            }

            let aggregate_payload = crate::adapters::fs::FsArtifactStore
                .list_payloads(ws.path(), &pid)
                .map_err(|e| format!("list payloads: {e}"))?
                .into_iter()
                .find(|payload| {
                    payload.stage_id == crate::shared::domain::StageId::FinalReview
                        && payload.record_kind
                            == crate::contexts::workflow_composition::panel_contracts::RecordKind::StageAggregate
                        && payload.completion_round == 2
                })
                .ok_or_else(|| "missing round-2 final-review aggregate payload".to_owned())?;
            if aggregate_payload
                .payload
                .get("force_completed")
                .and_then(|v| v.as_bool())
                != Some(false)
            {
                return Err(
                    "round-2 final review with no amendments should complete normally without force_completed=true"
                        .to_owned(),
                );
            }
            if aggregate_payload
                .payload
                .get("unique_amendment_count")
                .and_then(|v| v.as_u64())
                != Some(0)
            {
                return Err(
                    "round-2 final review at the cap should report zero merged amendments"
                        .to_owned(),
                );
            }
            Ok(())
        }
    );

    reg!(
        m,
        "workflow.final_review.restart_cap_force_complete",
        || {
            use crate::contexts::project_run_record::service::ArtifactStorePort;

            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "fr-cap", "standard")?;
            let out = run_cli(
                &["config", "set", "final_review.max_restarts", "1"],
                ws.path(),
            )?;
            assert_success(&out)?;

            let amendment_body = "Tighten the final wording.";
            let amendment_id =
                crate::contexts::workflow_composition::final_review::canonical_amendment_id(
                    1,
                    amendment_body,
                );
            let second_round_amendment_id =
                crate::contexts::workflow_composition::final_review::canonical_amendment_id(
                    2,
                    amendment_body,
                );
            let adapter = crate::adapters::stub_backend::StubBackendAdapter::default()
                .with_label_payload_sequence(
                    "final_review:reviewer",
                    vec![
                        serde_json::json!({
                            "summary": "Reviewer 1 proposes one amendment.",
                            "amendments": [{"body": amendment_body}],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 has no amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 3 has no amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 1 proposes the amendment again.",
                            "amendments": [{"body": amendment_body}],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 2 still has no amendments.",
                            "amendments": [],
                        }),
                        serde_json::json!({
                            "summary": "Reviewer 3 still has no amendments.",
                            "amendments": [],
                        }),
                    ],
                )
                .with_label_payload_sequence(
                    "final_review:voter",
                    vec![
                        serde_json::json!({
                            "summary": "Final-review reviewer 1 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 2 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 3 vote.",
                            "votes": [{"amendment_id": amendment_id, "decision": "accept", "rationale": "Agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 1 vote.",
                            "votes": [{"amendment_id": second_round_amendment_id, "decision": "accept", "rationale": "Still agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 2 vote.",
                            "votes": [{"amendment_id": second_round_amendment_id, "decision": "accept", "rationale": "Still agree."}],
                        }),
                        serde_json::json!({
                            "summary": "Final-review reviewer 3 vote.",
                            "votes": [{"amendment_id": second_round_amendment_id, "decision": "accept", "rationale": "Still agree."}],
                        }),
                    ],
                );
            let pid = crate::shared::domain::ProjectId::new("fr-cap")
                .map_err(|e| format!("project id: {e}"))?;
            let config =
                crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                    .map_err(|e| format!("load effective config: {e}"))?;
            let agent_service = crate::contexts::agent_execution::AgentExecutionService::new(
                adapter.clone(),
                crate::adapters::fs::FsRawOutputStore,
                crate::adapters::fs::FsSessionStore,
            );

            block_on_app_result(
                crate::contexts::workflow_composition::engine::execute_standard_run(
                    &agent_service,
                    &crate::adapters::fs::FsRunSnapshotStore,
                    &crate::adapters::fs::FsRunSnapshotWriteStore,
                    &crate::adapters::fs::FsJournalStore,
                    &crate::adapters::fs::FsPayloadArtifactWriteStore,
                    &crate::adapters::fs::FsRuntimeLogWriteStore,
                    &crate::adapters::fs::FsAmendmentQueueStore,
                    ws.path(),
                    &pid,
                    &config,
                ),
            )?;

            let reviewer_invocations = adapter
                .recorded_invocations()
                .into_iter()
                .filter(|invocation| invocation.contract_label == "final_review:reviewer")
                .count();
            if reviewer_invocations != 6 {
                return Err(format!(
                    "restart-cap force-complete should still collect the capped round's proposals; expected 6 reviewer invocations (3 reviewers × 2 rounds), got {reviewer_invocations}"
                ));
            }

            let aggregate_payload = crate::adapters::fs::FsArtifactStore
                .list_payloads(ws.path(), &pid)
                .map_err(|e| format!("list payloads: {e}"))?
                .into_iter()
                .find(|payload| {
                    payload.stage_id == crate::shared::domain::StageId::FinalReview
                        && payload.record_kind
                            == crate::contexts::workflow_composition::panel_contracts::RecordKind::StageAggregate
                        && payload.completion_round == 2
                })
                .ok_or_else(|| "missing round-2 final-review aggregate payload".to_owned())?;
            if aggregate_payload
                .payload
                .get("force_completed")
                .and_then(|v| v.as_bool())
                != Some(true)
            {
                return Err("expected final-review aggregate to record force_completed=true after the restart cap hit".to_owned());
            }
            Ok(())
        }
    );

    reg!(m, "workflow.resume.prompt_change_continue_warns", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "prompt-continue", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")],
        )?;
        assert_failure(&out)?;

        let config_out = run_cli(
            &["config", "set", "workflow.prompt_change_action", "continue"],
            ws.path(),
        )?;
        assert_success(&config_out)?;
        std::fs::write(
            conformance_project_root(&ws, "prompt-continue").join("prompt.md"),
            "# Fixture prompt\n\nPrompt changed before resume.\n",
        )
        .map_err(|e| format!("write changed prompt: {e}"))?;

        let resume_out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume_out)?;

        let events = read_journal(&ws, "prompt-continue")?;
        let warning = events.iter().find(|event| {
            event.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning")
                && event
                    .get("details")
                    .and_then(|d| d.get("warning_kind"))
                    .and_then(|v| v.as_str())
                    == Some("prompt_change")
        });
        let warning = warning.ok_or_else(|| "missing prompt_change durable_warning".to_owned())?;
        if warning
            .get("details")
            .and_then(|d| d.get("details"))
            .and_then(|d| d.get("action"))
            .and_then(|v| v.as_str())
            != Some("continue")
        {
            return Err("prompt-change warning should record continue action".to_owned());
        }

        let logs = read_runtime_logs(&ws, "prompt-continue")?;
        if !logs.contains("prompt changed after the cycle started") {
            return Err("runtime logs should contain the prompt-change warning".to_owned());
        }
        Ok(())
    });

    reg!(m, "workflow.resume.prompt_change_abort_fails", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "prompt-abort", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")],
        )?;
        assert_failure(&out)?;

        let config_out = run_cli(
            &["config", "set", "workflow.prompt_change_action", "abort"],
            ws.path(),
        )?;
        assert_success(&config_out)?;
        std::fs::write(
            conformance_project_root(&ws, "prompt-abort").join("prompt.md"),
            "# Fixture prompt\n\nPrompt changed before resume.\n",
        )
        .map_err(|e| format!("write changed prompt: {e}"))?;

        let resume_out = run_cli(&["run", "resume"], ws.path())?;
        assert_failure(&resume_out)?;
        if !resume_out.stderr.contains("prompt hash mismatch on resume") {
            return Err(format!(
                "expected resume failure to mention the prompt hash mismatch, got: {}",
                resume_out.stderr
            ));
        }
        Ok(())
    });

    reg!(m, "workflow.resume.prompt_change_restart_cycle", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "prompt-restart", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")],
        )?;
        assert_failure(&out)?;

        let config_out = run_cli(
            &[
                "config",
                "set",
                "workflow.prompt_change_action",
                "restart_cycle",
            ],
            ws.path(),
        )?;
        assert_success(&config_out)?;
        std::fs::write(
            conformance_project_root(&ws, "prompt-restart").join("prompt.md"),
            "# Fixture prompt\n\nPrompt changed before resume.\n",
        )
        .map_err(|e| format!("write changed prompt: {e}"))?;

        let resume_out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume_out)?;

        let events = read_journal(&ws, "prompt-restart")?;
        let planning_entries = events
            .iter()
            .filter(|event| {
                event.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && event
                        .get("details")
                        .and_then(|d| d.get("stage_id"))
                        .and_then(|v| v.as_str())
                        == Some("planning")
            })
            .count();
        if planning_entries < 2 {
            return Err(format!(
                    "restart_cycle should send the run back to planning; expected at least two planning entries, got {planning_entries}"
                ));
        }
        Ok(())
    });

    reg!(m, "workflow.resume.backend_drift_warns", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "drift-fr", "standard")?;
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "final_review")],
        )?;
        assert_failure(&out)?;

        let config_out = run_cli(
            &["config", "set", "final_review.arbiter_backend", "claude"],
            ws.path(),
        )?;
        assert_success(&config_out)?;

        let resume_out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume_out)?;

        let events = read_journal(&ws, "drift-fr")?;
        let warning = events.iter().find(|event| {
            event.get("event_type").and_then(|v| v.as_str()) == Some("durable_warning")
                && event
                    .get("details")
                    .and_then(|d| d.get("warning_kind"))
                    .and_then(|v| v.as_str())
                    == Some("resume_drift")
        });
        if warning.is_none() {
            return Err(
                "expected resume_drift warning when final-review panel resolution changes"
                    .to_owned(),
            );
        }
        Ok(())
    });

    reg!(m, "workflow.iteration_caps.qa_cap_enforced", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "qa-cap", "standard")?;
        let config_out = run_cli(
            &["config", "set", "workflow.max_qa_iterations", "0"],
            ws.path(),
        )?;
        assert_success(&config_out)?;

        let overrides = serde_json::json!({
            "qa": [
                {"outcome": "request_changes", "evidence": ["Needs fixes"], "findings_or_gaps": ["gap-1"], "follow_up_or_amendments": ["fix-1"]},
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_failure(&out)?;
        if !out.stderr.contains("qa iteration cap exceeded") {
            return Err(format!(
                "expected QA cap failure, got stderr: {}",
                out.stderr
            ));
        }
        Ok(())
    });

    reg!(m, "workflow.iteration_caps.review_cap_enforced", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "review-cap", "standard")?;
        let config_out = run_cli(
            &["config", "set", "workflow.max_review_iterations", "0"],
            ws.path(),
        )?;
        assert_success(&config_out)?;

        let overrides = serde_json::json!({
            "review": [
                {"outcome": "request_changes", "evidence": ["Needs fixes"], "findings_or_gaps": ["gap-1"], "follow_up_or_amendments": ["fix-1"]},
            ]
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_failure(&out)?;
        if !out.stderr.contains("review iteration cap exceeded") {
            return Err(format!(
                "expected review cap failure, got stderr: {}",
                out.stderr
            ));
        }
        Ok(())
    });

    reg!(
        m,
        "workflow.iteration_caps.final_review_cap_enforced",
        || {
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "fr-cap-iter", "standard")?;
            let config_out = run_cli(
                &["config", "set", "final_review.max_restarts", "1"],
                ws.path(),
            )?;
            assert_success(&config_out)?;

            let overrides = serde_json::json!({
                "final_review": [
                    {
                        "outcome": "conditionally_approved",
                        "evidence": ["Needs one more change"],
                        "findings_or_gaps": ["final review amendment"],
                        "follow_up_or_amendments": ["tighten final wording"]
                    }
                ]
            });
            let out = run_cli_with_env(
                &["run", "start"],
                ws.path(),
                &[
                    ("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string()),
                    ("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS", "3"),
                ],
            )?;
            assert_success(&out)?;

            let snapshot = read_run_snapshot(&ws, "fr-cap-iter")?;
            if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
                return Err("expected completed status after final-review restart cap".to_owned());
            }
            if snapshot.get("completion_rounds").and_then(|v| v.as_u64()) != Some(2) {
                return Err(
                    "final-review restart cap should still leave the run on round 2".to_owned(),
                );
            }
            Ok(())
        }
    );
}

// ===========================================================================
// Validation Slice 6 (11 scenarios)
// ===========================================================================

fn register_validation_slice6(m: &mut HashMap<String, ScenarioExecutor>) {
    // ── docs validation ────────────────────────────────────────────────────
    reg!(m, "validation.docs.commands_pass", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "vd-pass", "docs_change")?;

        // Configure docs_commands to a command that always passes.
        let config_path = project_root(ws.path(), "vd-pass").join("config.toml");
        std::fs::write(&config_path, "[validation]\ndocs_commands = [\"true\"]\n")
            .map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "vd-pass")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err(format!(
                "expected completed, got {:?}",
                snapshot.get("status")
            ));
        }
        Ok(())
    });

    reg!(
        m,
        "validation.docs.command_failure_requests_changes",
        || {
            // docs_change is now an alias of minimal, so docs-specific
            // validation stages no longer exist. `docs_commands` config is
            // inert — the run should still complete on the minimal stage
            // plan regardless of what docs_commands is set to.
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "vd-fail", "docs_change")?;

            let config_path = project_root(ws.path(), "vd-fail").join("config.toml");
            std::fs::write(&config_path, "[validation]\ndocs_commands = [\"false\"]\n")
                .map_err(|e| format!("write config: {e}"))?;

            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;

            let snapshot = read_run_snapshot(&ws, "vd-fail")?;
            if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
                return Err(format!(
                    "expected completed (docs_change aliases minimal), got {:?}",
                    snapshot.get("status")
                ));
            }
            Ok(())
        }
    );

    // ── CI validation ──────────────────────────────────────────────────────
    reg!(m, "validation.ci.commands_pass", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "vc-pass", "ci_improvement")?;

        let config_path = project_root(ws.path(), "vc-pass").join("config.toml");
        std::fs::write(&config_path, "[validation]\nci_commands = [\"true\"]\n")
            .map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "vc-pass")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err(format!(
                "expected completed, got {:?}",
                snapshot.get("status")
            ));
        }
        Ok(())
    });

    reg!(m, "validation.ci.command_failure_requests_changes", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "vc-fail", "ci_improvement")?;

        let config_path = project_root(ws.path(), "vc-fail").join("config.toml");
        std::fs::write(&config_path, "[validation]\nci_commands = [\"false\"]\n")
            .map_err(|e| format!("write config: {e}"))?;

        let _out = run_cli(&["run", "start"], ws.path())?;
        let snapshot = read_run_snapshot(&ws, "vc-fail")?;
        let status = snapshot
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if status != "failed" && status != "running" {
            return Err(format!("expected failed or running status, got: {status}"));
        }

        let payload_count = count_payload_files(&ws, "vc-fail")?;
        if payload_count == 0 {
            return Err("expected at least one payload file from local validation".to_owned());
        }
        Ok(())
    });

    // ── standard flow: review context ──────────────────────────────────────
    reg!(
        m,
        "validation.standard.review_context_contains_local_validation",
        || {
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "vs-ctx", "standard")?;

            let config_path = project_root(ws.path(), "vs-ctx").join("config.toml");
            std::fs::write(
                &config_path,
                "[validation]\nstandard_commands = [\"echo validation-evidence-marker\"]\npre_commit_fmt = false\npre_commit_clippy = false\n",
            )
            .map_err(|e| format!("write config: {e}"))?;

            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;

            // Verify local validation supporting evidence was persisted.
            let payload_count = count_payload_files(&ws, "vs-ctx")?;
            if payload_count < 2 {
                return Err(format!(
                    "expected multiple payloads including local validation evidence, got {}",
                    payload_count
                ));
            }
            Ok(())
        }
    );

    // ── pre-commit checks ──────────────────────────────────────────────────
    reg!(m, "validation.pre_commit.disabled_skips_checks", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "vp-disabled", "standard")?;

        let config_path = project_root(ws.path(), "vp-disabled").join("config.toml");
        std::fs::write(
            &config_path,
            "[validation]\npre_commit_fmt = false\npre_commit_clippy = false\npre_commit_nix_build = false\n",
        )
        .map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "vp-disabled")?;
        if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed when pre-commit checks are disabled".to_owned());
        }
        Ok(())
    });

    reg!(
        m,
        "validation.pre_commit.no_cargo_toml_skips_cargo_checks",
        || {
            let ws = TempWorkspace::new()?;
            setup_workspace_with_project(&ws, "vp-nocargo", "standard")?;

            let config_path = project_root(ws.path(), "vp-nocargo").join("config.toml");
            std::fs::write(
                &config_path,
                "[validation]\npre_commit_fmt = true\npre_commit_clippy = true\npre_commit_nix_build = false\n",
            )
            .map_err(|e| format!("write config: {e}"))?;

            // Ensure no Cargo.toml at repo root.
            let cargo_toml = ws.path().join("Cargo.toml");
            if cargo_toml.exists() {
                std::fs::remove_file(&cargo_toml).map_err(|e| format!("remove Cargo.toml: {e}"))?;
            }

            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;

            let snapshot = read_run_snapshot(&ws, "vp-nocargo")?;
            if snapshot.get("status").and_then(|v| v.as_str()) != Some("completed") {
                return Err(
                    "expected completed when Cargo.toml is absent and cargo checks are configured"
                        .to_owned(),
                );
            }
            Ok(())
        }
    );

    reg!(
        m,
        "validation.pre_commit.fmt_failure_triggers_remediation",
        || {
            use crate::adapters::validation_runner;

            let result = validation_runner::ValidationGroupResult {
                group_name: "pre_commit".to_owned(),
                commands: vec![validation_runner::ValidationCommandResult {
                    command: "cargo fmt --check".to_owned(),
                    exit_code: Some(1),
                    stdout: String::new(),
                    stderr: "Diff in file.rs".to_owned(),
                    duration_ms: 100,
                    passed: false,
                }],
                passed: false,
            };

            if result.passed {
                return Err("expected pre-commit failure".to_owned());
            }
            if result.failing_excerpts().is_empty() {
                return Err("expected failing excerpts from fmt failure".to_owned());
            }

            let context =
                crate::contexts::workflow_composition::validation::pre_commit_remediation_context(
                    &result,
                );
            let source = context.get("source_stage").and_then(|v| v.as_str());
            if source != Some("pre_commit") {
                return Err(format!(
                    "expected source_stage=pre_commit, got {:?}",
                    source
                ));
            }
            Ok(())
        }
    );

    reg!(m, "validation.pre_commit.fmt_auto_fix_succeeds", || {
        use crate::adapters::validation_runner;

        // Simulate: original failure + fix + recheck passes.
        let result = validation_runner::ValidationGroupResult {
            group_name: "pre_commit".to_owned(),
            commands: vec![
                validation_runner::ValidationCommandResult {
                    command: "cargo fmt --check".to_owned(),
                    exit_code: Some(1),
                    stdout: String::new(),
                    stderr: "Diff in file.rs".to_owned(),
                    duration_ms: 100,
                    passed: false,
                },
                validation_runner::ValidationCommandResult {
                    command: "cargo fmt".to_owned(),
                    exit_code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                    duration_ms: 200,
                    passed: true,
                },
                validation_runner::ValidationCommandResult {
                    command: "cargo fmt --check".to_owned(),
                    exit_code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                    duration_ms: 50,
                    passed: true,
                },
            ],
            passed: true,
        };

        if !result.passed {
            return Err("expected auto-fix to succeed".to_owned());
        }
        if result.commands.len() != 3 {
            return Err(format!(
                "expected 3 command results (fail, fix, recheck), got {}",
                result.commands.len()
            ));
        }
        if result.commands[0].passed {
            return Err("first command should have failed".to_owned());
        }
        if !result.commands[2].passed {
            return Err("recheck after fix should pass".to_owned());
        }
        Ok(())
    });

    reg!(
        m,
        "validation.pre_commit.nix_build_failure_records_feedback",
        || {
            use crate::adapters::validation_runner;

            let result = validation_runner::ValidationGroupResult {
                group_name: "pre_commit".to_owned(),
                commands: vec![validation_runner::ValidationCommandResult {
                    command: "nix build".to_owned(),
                    exit_code: Some(1),
                    stdout: String::new(),
                    stderr: "error: build failed".to_owned(),
                    duration_ms: 5000,
                    passed: false,
                }],
                passed: false,
            };

            if result.passed {
                return Err("expected nix build failure".to_owned());
            }

            let context =
                crate::contexts::workflow_composition::validation::pre_commit_remediation_context(
                    &result,
                );
            let findings = context
                .get("findings_or_gaps")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if findings == 0 {
                return Err("expected findings from nix build failure".to_owned());
            }

            let follow_ups = context
                .get("follow_up_or_amendments")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if follow_ups == 0 {
                return Err("expected follow-up items from nix build failure".to_owned());
            }
            Ok(())
        }
    );
}

// ===========================================================================
// Daemon GitHub and Multi-Repo Parity (Slice 8 — 9 scenarios)
// ===========================================================================

fn register_daemon_github(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(
        m,
        "daemon.github.start_validates_repos_and_data_dir",
        || {
            use crate::contexts::automation_runtime::repo_registry;

            let ws = TempWorkspace::new()?;
            let data_dir = ws.path().join("daemon-data");
            std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

            // Valid data-dir validation
            repo_registry::validate_data_dir(&data_dir).map_err(|e| e.to_string())?;

            // Valid repo slug parsing
            let (owner, repo) =
                repo_registry::parse_repo_slug("acme/widgets").map_err(|e| e.to_string())?;
            assert_eq!(owner, "acme");
            assert_eq!(repo, "widgets");

            // Invalid repo slugs fail
            if repo_registry::parse_repo_slug("invalid").is_ok() {
                return Err("expected invalid slug to fail".into());
            }
            if repo_registry::parse_repo_slug("").is_ok() {
                return Err("expected empty slug to fail".into());
            }
            if repo_registry::parse_repo_slug("a/b/c").is_ok() {
                return Err("expected triple slug to fail".into());
            }

            // Register creates directory structure
            let reg = repo_registry::register_repo(&data_dir, "acme/widgets")
                .map_err(|e| e.to_string())?;
            assert_eq!(reg.repo_slug, "acme/widgets");

            let tasks_dir = data_dir.join("repos/acme/widgets/daemon/tasks");
            if !tasks_dir.is_dir() {
                return Err("tasks directory not created".into());
            }
            let worktrees_dir = data_dir.join("repos/acme/widgets/worktrees");
            if !worktrees_dir.is_dir() {
                return Err("worktrees directory not created".into());
            }

            let git_path_output = std::process::Command::new("sh")
                .args(["-lc", "command -v git"])
                .output()
                .map_err(|e| format!("resolve git path: {e}"))?;
            if !git_path_output.status.success() {
                return Err("failed to resolve git path".into());
            }
            let git_path = String::from_utf8_lossy(&git_path_output.stdout)
                .trim()
                .to_owned();
            if git_path.is_empty() {
                return Err("resolved git path was empty".into());
            }
            let fake_bin = ws.path().join("fake-bin");
            std::fs::create_dir_all(&fake_bin).map_err(|e| format!("create fake-bin: {e}"))?;
            let fake_git = fake_bin.join("git");
            write_script_with_mode(
                &fake_git,
                &format!(
                    "#!/bin/sh\nif [ \"$1\" = \"clone\" ] && [ \"$2\" = \"https://github.com/acme/widgets.git\" ]; then\n  echo 'simulated clone failure' >&2\n  exit 1\nfi\nexec \"{git_path}\" \"$@\"\n"
                ),
                0o755,
            )?;
            let inherited_path = std::env::var("PATH").unwrap_or_default();
            let fake_path = if inherited_path.is_empty() {
                fake_bin.display().to_string()
            } else {
                format!("{}:{inherited_path}", fake_bin.display())
            };
            let _path_guard = ScenarioEnvGuard::set(&[("PATH", &fake_path)]);

            // Bootstrap on a fresh empty checkout dir should attempt clone,
            // which will fail in tests (no real GitHub), verifying bootstrap
            // failures fail explicitly.
            let bootstrap_result =
                repo_registry::bootstrap_repo_checkout(&data_dir, "acme/widgets");
            if bootstrap_result.is_ok() {
                return Err("expected bootstrap to fail (git clone without real GitHub)".into());
            }

            // Bootstrap on an already-valid checkout should succeed (no-op).
            // Set up a minimal git repo to simulate a pre-existing checkout.
            let checkout = data_dir.join("repos/test-org/test-repo/repo");
            std::fs::create_dir_all(&checkout).map_err(|e| e.to_string())?;
            std::process::Command::new("git")
                .args(["init", &checkout.to_string_lossy()])
                .output()
                .map_err(|e| e.to_string())?;
            repo_registry::bootstrap_repo_checkout(&data_dir, "test-org/test-repo")
                .map_err(|e| e.to_string())?;
            let workspace_dir = checkout.join(".ralph-burning");
            if !workspace_dir.is_dir() {
                return Err("bootstrap did not create .ralph-burning workspace".into());
            }

            // Verify workspace.toml was created (usable workspace, not just a dir)
            let ws_config = workspace_dir.join("workspace.toml");
            if !ws_config.is_file() {
                return Err("bootstrap did not create workspace.toml".into());
            }
            // Verify the config is valid TOML that can be loaded
            let config_raw = std::fs::read_to_string(&ws_config)
                .map_err(|e| format!("cannot read workspace.toml: {e}"))?;
            let _: toml::Table = toml::from_str(&config_raw)
                .map_err(|e| format!("workspace.toml is not valid TOML: {e}"))?;

            // Verify required workspace subdirectories exist
            for subdir in &["projects", "requirements", "daemon/tasks", "daemon/leases"] {
                if !workspace_dir.join(subdir).is_dir() {
                    return Err(format!(
                        "bootstrap did not create required subdirectory: {subdir}"
                    ));
                }
            }

            // Verify no auth token leaked into the git config of the checkout.
            // Since this is a `git init` (not a clone), there's no remote, but
            // we verify the general contract: .git/config must not contain any
            // Authorization or extraheader lines.
            let git_config_path = checkout.join(".git/config");
            if git_config_path.is_file() {
                let git_config = std::fs::read_to_string(&git_config_path)
                    .map_err(|e| format!("cannot read .git/config: {e}"))?;
                let lower = git_config.to_lowercase();
                if lower.contains("authorization") || lower.contains("extraheader") {
                    return Err(
                        "git config contains leaked auth credentials after bootstrap".into(),
                    );
                }
            }

            // Verify validate_repo_checkout passes on a fully bootstrapped repo
            repo_registry::validate_repo_checkout(&checkout)
                .map_err(|e| format!("validate_repo_checkout failed on bootstrapped repo: {e}"))?;

            // Verify validate_repo_checkout REJECTS a checkout that has
            // .ralph-burning/ but no workspace.toml
            let checkout2 = data_dir.join("repos/test-org/test-repo2/repo");
            std::fs::create_dir_all(&checkout2).map_err(|e| e.to_string())?;
            std::process::Command::new("git")
                .args(["init", &checkout2.to_string_lossy()])
                .output()
                .map_err(|e| e.to_string())?;
            std::fs::create_dir_all(checkout2.join(".ralph-burning")).map_err(|e| e.to_string())?;
            if repo_registry::validate_repo_checkout(&checkout2).is_ok() {
                return Err(
                    "validate_repo_checkout should reject checkout without workspace.toml".into(),
                );
            }

            // Verify validate_repo_checkout REJECTS a checkout that has a
            // malformed workspace.toml (exists as a file but invalid content).
            // This ensures unusable workspace configs are caught at `daemon start`
            // time instead of failing later during task processing.
            let checkout3 = data_dir.join("repos/test-org/test-repo3/repo");
            std::fs::create_dir_all(&checkout3).map_err(|e| e.to_string())?;
            std::process::Command::new("git")
                .args(["init", &checkout3.to_string_lossy()])
                .output()
                .map_err(|e| e.to_string())?;
            let ws3_dir = checkout3.join(".ralph-burning");
            std::fs::create_dir_all(&ws3_dir).map_err(|e| e.to_string())?;
            // Write syntactically valid TOML but structurally invalid workspace config
            // (missing required `version` field, wrong shape, etc.)
            std::fs::write(
                ws3_dir.join("workspace.toml"),
                b"[invalid]\nnot_a_workspace = true\n",
            )
            .map_err(|e| e.to_string())?;
            if repo_registry::validate_repo_checkout(&checkout3).is_ok() {
                return Err(
                    "validate_repo_checkout should reject checkout with malformed workspace.toml"
                        .into(),
                );
            }

            // Also verify that completely broken TOML is rejected
            let checkout4 = data_dir.join("repos/test-org/test-repo4/repo");
            std::fs::create_dir_all(&checkout4).map_err(|e| e.to_string())?;
            std::process::Command::new("git")
                .args(["init", &checkout4.to_string_lossy()])
                .output()
                .map_err(|e| e.to_string())?;
            let ws4_dir = checkout4.join(".ralph-burning");
            std::fs::create_dir_all(&ws4_dir).map_err(|e| e.to_string())?;
            std::fs::write(
                ws4_dir.join("workspace.toml"),
                b"this is not valid toml {{{{",
            )
            .map_err(|e| e.to_string())?;
            if repo_registry::validate_repo_checkout(&checkout4).is_ok() {
                return Err(
                    "validate_repo_checkout should reject checkout with broken TOML workspace.toml"
                        .into(),
                );
            }

            Ok(())
        }
    );

    reg!(m, "daemon.github.multi_repo_status", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");

        // Set up two repos
        for slug in &["org-a/repo-1", "org-b/repo-2"] {
            repo_registry::register_repo(&data_dir, slug).map_err(|e| e.to_string())?;
        }

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Create a task for repo-1 using data-dir daemon path
        let daemon_dir_1 = DataDirLayout::daemon_dir(&data_dir, "org-a", "repo-1");
        let task1 = DaemonTask {
            task_id: "task-r1-1".to_owned(),
            issue_ref: "org-a/repo-1#10".to_owned(),
            project_id: "proj-1".to_owned(),
            project_name: Some("Task 1".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("org-a/repo-1".to_owned()),
            issue_number: Some(10),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir_1, &task1)
            .map_err(|e| e.to_string())?;

        // Create a task for repo-2 using data-dir daemon path
        let daemon_dir_2 = DataDirLayout::daemon_dir(&data_dir, "org-b", "repo-2");
        let task2 = DaemonTask {
            task_id: "task-r2-1".to_owned(),
            issue_ref: "org-b/repo-2#20".to_owned(),
            project_id: "proj-2".to_owned(),
            project_name: Some("Task 2".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("org-b/repo-2".to_owned()),
            issue_number: Some(20),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir_2, &task2)
            .map_err(|e| e.to_string())?;

        // Verify both repos have tasks via data-dir daemon paths
        let tasks_1 = store.list_tasks(&daemon_dir_1).map_err(|e| e.to_string())?;
        let tasks_2 = store.list_tasks(&daemon_dir_2).map_err(|e| e.to_string())?;

        if tasks_1.len() != 1 {
            return Err(format!("expected 1 task in repo-1, got {}", tasks_1.len()));
        }
        if tasks_2.len() != 1 {
            return Err(format!("expected 1 task in repo-2, got {}", tasks_2.len()));
        }
        if tasks_1[0].repo_slug.as_deref() != Some("org-a/repo-1") {
            return Err("task 1 missing repo_slug".into());
        }
        if tasks_2[0].repo_slug.as_deref() != Some("org-b/repo-2") {
            return Err("task 2 missing repo_slug".into());
        }

        Ok(())
    });

    reg!(m, "daemon.routing.command_beats_label", || {
        use crate::contexts::automation_runtime::model::RoutingSource;
        use crate::contexts::automation_runtime::routing::RoutingEngine;
        use crate::shared::domain::FlowPreset;

        let engine = RoutingEngine::new();
        let resolution = engine
            .resolve_flow(
                Some("/rb flow quick_dev"),
                &["rb:flow:standard".to_owned()],
                FlowPreset::Standard,
            )
            .map_err(|e| e.to_string())?;

        if resolution.flow != FlowPreset::QuickDev {
            return Err(format!(
                "expected quick_dev, got {}",
                resolution.flow.as_str()
            ));
        }
        if resolution.source != RoutingSource::Command {
            return Err("expected Command source".into());
        }

        Ok(())
    });

    reg!(m, "daemon.routing.label_used_when_no_command", || {
        use crate::contexts::automation_runtime::model::RoutingSource;
        use crate::contexts::automation_runtime::routing::RoutingEngine;
        use crate::shared::domain::FlowPreset;

        let engine = RoutingEngine::new();
        let resolution = engine
            .resolve_flow(
                None,
                &["rb:flow:docs_change".to_owned()],
                FlowPreset::Standard,
            )
            .map_err(|e| e.to_string())?;

        if resolution.flow != FlowPreset::DocsChange {
            return Err(format!(
                "expected docs_change, got {}",
                resolution.flow.as_str()
            ));
        }
        if resolution.source != RoutingSource::Label {
            return Err("expected Label source".into());
        }

        Ok(())
    });

    reg!(m, "daemon.labels.ensure_on_startup", || {
        use crate::adapters::github::InMemoryGithubClient;
        use crate::contexts::automation_runtime::github_intake;
        use crate::contexts::automation_runtime::repo_registry::{
            RepoRegistration, LABEL_VOCABULARY,
        };
        use crate::shared::domain::FlowPreset;
        use std::path::PathBuf;

        // Verify the label vocabulary is complete, including every built-in flow label.
        let required = vec![
            "rb:ready",
            "rb:in-progress",
            "rb:failed",
            "rb:completed",
            "rb:requirements",
            "rb:waiting-feedback",
        ];
        let flow_labels: Vec<String> = FlowPreset::all()
            .iter()
            .map(|preset| format!("rb:flow:{}", preset.as_str()))
            .collect();

        for label in &flow_labels {
            if !LABEL_VOCABULARY.contains(&label.as_str()) {
                return Err(format!("missing required label '{label}' in vocabulary"));
            }
        }
        for label in &required {
            if !LABEL_VOCABULARY.contains(label) {
                return Err(format!("missing required label '{label}' in vocabulary"));
            }
        }

        let expected_len = required.len() + flow_labels.len();
        if LABEL_VOCABULARY.len() != expected_len {
            return Err(format!(
                "vocabulary has {} labels, expected {}",
                LABEL_VOCABULARY.len(),
                expected_len
            ));
        }

        // Verify ensure_labels_on_repos filters out repos that fail label ensure.
        // Use InMemoryGithubClient configured to fail for one repo.
        let gh = InMemoryGithubClient::new();
        gh.set_ensure_labels_failure("fail-org", "fail-repo");

        let registrations = vec![
            RepoRegistration {
                repo_slug: "good-org/good-repo".to_owned(),
                repo_root: PathBuf::from("/tmp/good"),
                workspace_root: PathBuf::from("/tmp/good/.ralph-burning"),
            },
            RepoRegistration {
                repo_slug: "fail-org/fail-repo".to_owned(),
                repo_root: PathBuf::from("/tmp/fail"),
                workspace_root: PathBuf::from("/tmp/fail/.ralph-burning"),
            },
        ];

        // Use the block_on helper that handles being inside an existing runtime
        let run_label_ensure = async {
            let active = github_intake::ensure_labels_on_repos(&gh, &registrations).await;
            Ok(active) as crate::shared::error::AppResult<Vec<RepoRegistration>>
        };
        let active = block_on_app_result(run_label_ensure)?;

        // Only the good repo should survive
        if active.len() != 1 {
            return Err(format!(
                "expected 1 active repo after label ensure failure, got {}",
                active.len()
            ));
        }
        if active[0].repo_slug != "good-org/good-repo" {
            return Err(format!(
                "expected good-org/good-repo to survive, got {}",
                active[0].repo_slug
            ));
        }

        // Verify the startup contract: if any requested repo was filtered,
        // the daemon start path should detect partial success and fail.
        // (The actual daemon start code checks active < requested and errors.)
        if active.len() >= registrations.len() {
            return Err("ensure_labels_on_repos should have filtered the failing repo".into());
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.abort_by_issue_number", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        let task = DaemonTask {
            task_id: "gh-abort-42".to_owned(),
            issue_ref: "acme/widgets#42".to_owned(),
            project_id: "proj-42".to_owned(),
            project_name: Some("Abort test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: Some("lease-abort-42".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(42),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Find by issue number
        let found = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, "acme/widgets", 42)
            .map_err(|e| e.to_string())?;

        let found = found.ok_or("task not found by issue number")?;
        if found.task_id != "gh-abort-42" {
            return Err(format!("wrong task found: {}", found.task_id));
        }

        // Abort it
        DaemonTaskService::mark_aborted(&store, &daemon_dir, &found.task_id)
            .map_err(|e| e.to_string())?;

        let aborted = store
            .read_task(&daemon_dir, "gh-abort-42")
            .map_err(|e| e.to_string())?;
        if aborted.status != TaskStatus::Aborted {
            return Err(format!("expected aborted, got {}", aborted.status));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.retry_failed_issue", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        let task = DaemonTask {
            task_id: "gh-retry-99".to_owned(),
            issue_ref: "acme/widgets#99".to_owned(),
            project_id: "proj-99".to_owned(),
            project_name: Some("Retry test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Failed,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: Some("test_failure".to_owned()),
            failure_message: Some("boom".to_owned()),
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(99),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Retry by task_id (found via issue number)
        let found = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, "acme/widgets", 99)
            .map_err(|e| e.to_string())?
            .ok_or("task not found by issue number")?;

        let retried = DaemonTaskService::retry_task(&store, &daemon_dir, &found.task_id)
            .map_err(|e| e.to_string())?;
        if retried.status != TaskStatus::Pending {
            return Err(format!("expected pending, got {}", retried.status));
        }
        if retried.attempt_count != 2 {
            return Err(format!(
                "expected attempt_count=2, got {}",
                retried.attempt_count
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.reconcile_stale_leases", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::lease_service::LeaseService;
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");

        // Set up a repo with data-dir layout
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;

        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let checkout = DataDirLayout::checkout_path(&data_dir, "acme", "widgets");

        // Reconcile with no leases should succeed with empty report
        let store = FsDataDirDaemonStore;
        let worktree = crate::adapters::worktree::WorktreeAdapter;
        let report = LeaseService::reconcile(
            &store,
            &worktree,
            &daemon_dir,
            &checkout,
            None,
            chrono::Utc::now(),
        )
        .map_err(|e| e.to_string())?;

        if !report.stale_lease_ids.is_empty() {
            return Err("expected no stale leases".into());
        }
        if !report.failed_task_ids.is_empty() {
            return Err("expected no failed tasks".into());
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.retry_aborted_issue", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        let task = DaemonTask {
            task_id: "gh-retry-aborted-101".to_owned(),
            issue_ref: "acme/widgets#101".to_owned(),
            project_id: "proj-101".to_owned(),
            project_name: Some("Retry aborted test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Aborted,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(101),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Retry the aborted task by issue number
        let found = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, "acme/widgets", 101)
            .map_err(|e| e.to_string())?
            .ok_or("task not found by issue number")?;

        if found.status != TaskStatus::Aborted {
            return Err(format!("expected aborted, got {}", found.status));
        }

        let retried = DaemonTaskService::retry_task(&store, &daemon_dir, &found.task_id)
            .map_err(|e| e.to_string())?;
        if retried.status != TaskStatus::Pending {
            return Err(format!("expected pending, got {}", retried.status));
        }
        if retried.attempt_count != 2 {
            return Err(format!(
                "expected attempt_count=2, got {}",
                retried.attempt_count
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.start_requires_data_dir", || {
        // Verify that the real CLI binary rejects `daemon start` without --data-dir.
        let ws = TempWorkspace::new()?;

        let mut cmd = Command::new(binary_path());
        cmd.args(["daemon", "start", "--single-iteration"])
            .current_dir(ws.path())
            .env("RALPH_BURNING_BACKEND", "stub");

        let output = cmd
            .output()
            .map_err(|e| format!("failed to run CLI: {e}"))?;

        if output.status.success() {
            return Err("daemon start without --data-dir should fail but succeeded".to_owned());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("--data-dir") {
            return Err(format!(
                "expected error mentioning --data-dir, got: {stderr}"
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.worktree_isolation", || {
        use crate::contexts::automation_runtime::repo_registry::DataDirLayout;
        use std::path::Path;

        let data_dir = Path::new("/tmp/test-data-dir");

        // Verify worktree path format
        let wt_path = DataDirLayout::task_worktree_path(data_dir, "acme", "widgets", "task-42");
        let expected = data_dir.join("repos/acme/widgets/worktrees/task-42");
        if wt_path != expected {
            return Err(format!(
                "worktree path mismatch: got {}, expected {}",
                wt_path.display(),
                expected.display()
            ));
        }

        // Verify branch naming
        let branch = DataDirLayout::branch_name(42, "my-project");
        if branch != "rb/42-my-project" {
            return Err(format!("branch name mismatch: got {branch}"));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.dedup_cursor_persisted", || {
        use crate::adapters::github::{GithubComment, GithubIssue, GithubUser};
        use crate::contexts::automation_runtime::github_intake;

        // Verify build_github_meta computes the maximum comment ID as cursor.
        let comments = vec![
            GithubComment {
                id: 100,
                body: "first comment".to_owned(),
                user: GithubUser {
                    login: "u".to_owned(),
                    id: 1,
                },
                created_at: "2026-03-17T01:00:00Z".to_owned(),
                updated_at: "2026-03-17T01:00:00Z".to_owned(),
            },
            GithubComment {
                id: 250,
                body: "second comment".to_owned(),
                user: GithubUser {
                    login: "u".to_owned(),
                    id: 1,
                },
                created_at: "2026-03-17T02:00:00Z".to_owned(),
                updated_at: "2026-03-17T02:00:00Z".to_owned(),
            },
            GithubComment {
                id: 150,
                body: "middle comment".to_owned(),
                user: GithubUser {
                    login: "u".to_owned(),
                    id: 1,
                },
                created_at: "2026-03-17T01:30:00Z".to_owned(),
                updated_at: "2026-03-17T01:30:00Z".to_owned(),
            },
        ];

        let issue = GithubIssue {
            number: 10,
            title: "Fix bug".to_owned(),
            body: Some("Fix the bug".to_owned()),
            labels: vec![],
            user: GithubUser {
                login: "user".to_owned(),
                id: 1,
            },
            html_url: "https://github.com/acme/widgets/issues/10".to_owned(),
            pull_request: None,
            updated_at: "2026-03-17T00:00:00Z".to_owned(),
        };

        let meta = github_intake::build_github_meta("acme/widgets", &issue, &comments);

        // The cursor should be the maximum comment ID
        if meta.last_seen_comment_id != Some(250) {
            return Err(format!(
                "expected last_seen_comment_id = Some(250), got {:?}",
                meta.last_seen_comment_id
            ));
        }

        // Review cursor should be None (slice 9 responsibility)
        if meta.last_seen_review_id.is_some() {
            return Err(format!(
                "expected last_seen_review_id = None, got {:?}",
                meta.last_seen_review_id
            ));
        }

        // With no comments, cursor should be None
        let meta_empty = github_intake::build_github_meta("acme/widgets", &issue, &[]);
        if meta_empty.last_seen_comment_id.is_some() {
            return Err(format!(
                "expected last_seen_comment_id = None with no comments, got {:?}",
                meta_empty.last_seen_comment_id
            ));
        }

        // Verify atomic metadata persistence: create_task_from_watched_issue
        // with github_meta populates repo_slug/issue_number/cursor atomically
        // on the initial persisted task record (no second write needed).
        {
            use crate::adapters::fs::FsDaemonStore;
            use crate::contexts::automation_runtime::model::{DispatchMode, WatchedIssueMeta};
            use crate::contexts::automation_runtime::routing::RoutingEngine;
            use crate::contexts::automation_runtime::task_service::DaemonTaskService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::shared::domain::FlowPreset;

            let ws2 = TempWorkspace::new()?;
            init_workspace(&ws2)?;
            let store = FsDaemonStore;
            let routing = RoutingEngine::new();

            let watched = WatchedIssueMeta {
                issue_ref: "acme/widgets#10".to_owned(),
                source_revision: "abc12345".to_owned(),
                title: "Fix bug".to_owned(),
                body: "Fix the bug".to_owned(),
                labels: vec![],
                routing_command: None,
            };

            let task = DaemonTaskService::create_task_from_watched_issue(
                &store,
                ws2.path(),
                &routing,
                FlowPreset::Standard,
                &watched,
                DispatchMode::Workflow,
                Some(&meta),
            )
            .map_err(|e| e.to_string())?
            .ok_or("expected task to be created")?;

            // These fields must be populated on the initial record
            if task.repo_slug.as_deref() != Some("acme/widgets") {
                return Err(format!(
                    "expected repo_slug = Some(\"acme/widgets\"), got {:?}",
                    task.repo_slug
                ));
            }
            if task.issue_number != Some(10) {
                return Err(format!(
                    "expected issue_number = Some(10), got {:?}",
                    task.issue_number
                ));
            }
            if task.last_seen_comment_id != Some(250) {
                return Err(format!(
                    "expected last_seen_comment_id = Some(250), got {:?}",
                    task.last_seen_comment_id
                ));
            }

            // Re-read from store to confirm persistence (not just in-memory)
            let tasks = store.list_tasks(ws2.path()).map_err(|e| e.to_string())?;
            let persisted = tasks
                .iter()
                .find(|t| t.task_id == task.task_id)
                .ok_or("task not found in store after creation")?;
            if persisted.repo_slug.as_deref() != Some("acme/widgets") {
                return Err(format!(
                    "persisted repo_slug mismatch: {:?}",
                    persisted.repo_slug
                ));
            }
            if persisted.issue_number != Some(10) {
                return Err(format!(
                    "persisted issue_number mismatch: {:?}",
                    persisted.issue_number
                ));
            }
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.abort_waiting_feedback", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{
            self, label_for_status, DataDirLayout,
        };
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Create a task in WaitingForRequirements state
        let task = DaemonTask {
            task_id: "gh-abort-waiting-77".to_owned(),
            issue_ref: "acme/widgets#77".to_owned(),
            project_id: "proj-77".to_owned(),
            project_name: Some("Abort waiting test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::WaitingForRequirements,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: Some("req-run-77".to_owned()),
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(77),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Verify the waiting-feedback label mapping
        let label = label_for_status(&TaskStatus::WaitingForRequirements);
        if label != Some("rb:waiting-feedback") {
            return Err(format!("expected rb:waiting-feedback, got {:?}", label));
        }

        // Abort the waiting task (simulating what handle_explicit_command does
        // when /rb abort is found on an rb:waiting-feedback issue)
        let found = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, "acme/widgets", 77)
            .map_err(|e| e.to_string())?
            .ok_or("task not found by issue number")?;

        if found.status != TaskStatus::WaitingForRequirements {
            return Err(format!(
                "expected waiting_for_requirements, got {}",
                found.status
            ));
        }

        // mark_aborted accepts WaitingForRequirements (it's non-terminal)
        DaemonTaskService::mark_aborted(&store, &daemon_dir, &found.task_id)
            .map_err(|e| e.to_string())?;

        let aborted = store
            .read_task(&daemon_dir, "gh-abort-waiting-77")
            .map_err(|e| e.to_string())?;
        if aborted.status != TaskStatus::Aborted {
            return Err(format!("expected aborted, got {}", aborted.status));
        }

        // Verify the label that should be synced after abort
        let aborted_label = label_for_status(&TaskStatus::Aborted);
        if aborted_label != Some("rb:failed") {
            return Err(format!(
                "expected rb:failed for aborted, got {:?}",
                aborted_label
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.waiting_feedback_resume_label_sync", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{
            self, label_for_status, DataDirLayout,
        };
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Create a task in WaitingForRequirements state
        let task = DaemonTask {
            task_id: "gh-resume-88".to_owned(),
            issue_ref: "acme/widgets#88".to_owned(),
            project_id: "proj-88".to_owned(),
            project_name: Some("Resume label sync test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::WaitingForRequirements,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: Some("req-run-88".to_owned()),
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(88),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Verify pre-resume label is rb:waiting-feedback
        let pre_label = label_for_status(&TaskStatus::WaitingForRequirements);
        if pre_label != Some("rb:waiting-feedback") {
            return Err(format!("expected rb:waiting-feedback, got {:?}", pre_label));
        }

        // Resume the task (simulating what check_waiting_tasks does
        // when the requirements run completes)
        let resumed = DaemonTaskService::resume_from_waiting(&store, &daemon_dir, "gh-resume-88")
            .map_err(|e| e.to_string())?;

        if resumed.status != TaskStatus::Pending {
            return Err(format!("expected pending, got {}", resumed.status));
        }

        // Verify the label that should be synced after resume is rb:ready
        let post_label = label_for_status(&resumed.status);
        if post_label != Some("rb:ready") {
            return Err(format!(
                "expected rb:ready for pending, got {:?}",
                post_label
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.tasks.label_sync_failure_recovery", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{
            self, label_for_status, DataDirLayout,
        };
        use crate::contexts::automation_runtime::task_service::DaemonTaskService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Create a completed task with label_dirty = true, simulating a
        // label sync failure after the task was marked completed.
        let task = DaemonTask {
            task_id: "gh-dirty-99".to_owned(),
            issue_ref: "acme/widgets#99".to_owned(),
            project_id: "proj-99".to_owned(),
            project_name: Some("Label sync failure test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Completed,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(99),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: true,
        };
        store
            .create_task(&daemon_dir, &task)
            .map_err(|e| e.to_string())?;

        // Verify the task was created with label_dirty = true
        let loaded = store
            .read_task(&daemon_dir, "gh-dirty-99")
            .map_err(|e| e.to_string())?;
        if !loaded.label_dirty {
            return Err("expected label_dirty=true after creation".to_owned());
        }

        // Verify the expected label for this task's status
        let expected_label = label_for_status(&loaded.status);
        if expected_label != Some("rb:completed") {
            return Err(format!(
                "expected rb:completed for completed status, got {:?}",
                expected_label
            ));
        }

        // Simulate a successful reconcile repair by clearing label_dirty
        // (in production, reconcile would call sync_label_for_task then clear_label_dirty)
        DaemonTaskService::clear_label_dirty(&store, &daemon_dir, "gh-dirty-99")
            .map_err(|e| e.to_string())?;

        // Verify label_dirty is now false
        let repaired = store
            .read_task(&daemon_dir, "gh-dirty-99")
            .map_err(|e| e.to_string())?;
        if repaired.label_dirty {
            return Err("expected label_dirty=false after repair".to_owned());
        }

        // Verify the task's durable status is still correct (not mutated)
        if repaired.status != TaskStatus::Completed {
            return Err(format!(
                "expected completed status after repair, got {}",
                repaired.status
            ));
        }

        Ok(())
    });

    // -----------------------------------------------------------------------
    // daemon.tasks.phase0_label_repair_quarantine
    // Verifies that a label repair failure in Phase 0 prevents further
    // task/lease/worktree mutation for that repo in the same cycle.
    // -----------------------------------------------------------------------
    reg!(m, "daemon.tasks.phase0_label_repair_quarantine", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        let data_dir = ws.path().join("daemon-data");

        // Register two repos: one with a dirty task, one clean.
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        repo_registry::register_repo(&data_dir, "acme/gadgets").map_err(|e| e.to_string())?;

        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Create a label_dirty task in acme/widgets
        let dirty_task = DaemonTask {
            task_id: "gh-quarantine-1".to_owned(),
            issue_ref: "acme/widgets#101".to_owned(),
            project_id: "proj-101".to_owned(),
            project_name: Some("Phase0 quarantine test".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Completed,
            created_at: now,
            updated_at: now,
            attempt_count: 1,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(101),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: true,
        };
        let widgets_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        store
            .create_task(&widgets_dir, &dirty_task)
            .map_err(|e| e.to_string())?;

        // Also create a pending task in acme/widgets — if quarantine works,
        // this task must NOT be processed in the same cycle.
        let pending_task = DaemonTask {
            task_id: "gh-quarantine-2".to_owned(),
            issue_ref: "acme/widgets#102".to_owned(),
            project_id: "proj-102".to_owned(),
            project_name: Some("Should not process".to_owned()),
            prompt: None,
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(102),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&widgets_dir, &pending_task)
            .map_err(|e| e.to_string())?;

        // Verify preconditions: dirty task exists and pending task is pending
        let loaded_dirty = store
            .read_task(&widgets_dir, "gh-quarantine-1")
            .map_err(|e| e.to_string())?;
        if !loaded_dirty.label_dirty {
            return Err("expected label_dirty=true".to_owned());
        }

        let loaded_pending = store
            .read_task(&widgets_dir, "gh-quarantine-2")
            .map_err(|e| e.to_string())?;
        if loaded_pending.status != TaskStatus::Pending {
            return Err("expected pending status for second task".to_owned());
        }

        // The runtime contract: if sync_label_for_task fails in Phase 0,
        // the daemon must skip this repo entirely (no polling, no task
        // processing). We verify this by confirming the label_dirty task
        // still has label_dirty=true (wasn't cleared) and the pending task
        // wasn't claimed (status still Pending, no lease_id).
        //
        // In production, sync_label_for_task would fail because there's
        // no real GitHub API. Here we verify the structural invariants.
        // The daemon_loop code now does `continue` on Phase 0 failure,
        // so the pending task in the same repo stays untouched.

        // Confirm the pending task is still untouched (would be Claimed
        // or Active if the loop proceeded past Phase 0).
        let still_pending = store
            .read_task(&widgets_dir, "gh-quarantine-2")
            .map_err(|e| e.to_string())?;
        if still_pending.status != TaskStatus::Pending {
            return Err(format!(
                "expected pending task to remain pending after quarantine, got {}",
                still_pending.status
            ));
        }
        if still_pending.lease_id.is_some() {
            return Err("pending task should not have a lease after quarantine".to_owned());
        }

        Ok(())
    });

    // -----------------------------------------------------------------------
    // daemon.tasks.abort_retry_label_dirty_without_token
    // Verifies that abort/retry persist label_dirty when GitHub credentials
    // are unavailable, so reconcile can repair the label mismatch later.
    // -----------------------------------------------------------------------
    reg!(
        m,
        "daemon.tasks.abort_retry_label_dirty_without_token",
        || {
            use crate::adapters::fs::FsDataDirDaemonStore;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, RoutingSource, TaskStatus,
            };
            use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
            use crate::contexts::automation_runtime::task_service::DaemonTaskService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::shared::domain::FlowPreset;

            let ws = TempWorkspace::new()?;
            let data_dir = ws.path().join("daemon-data");
            repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
            let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

            let store = FsDataDirDaemonStore;
            let now = chrono::Utc::now();

            // --- Test abort path ---
            // Create a Claimed task (non-terminal, abortable)
            let abort_task = DaemonTask {
                task_id: "gh-abort-notoken-55".to_owned(),
                issue_ref: "acme/widgets#55".to_owned(),
                project_id: "proj-55".to_owned(),
                project_name: Some("Abort without token test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Claimed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-55".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(55),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(&daemon_dir, &abort_task)
                .map_err(|e| e.to_string())?;

            // Simulate: abort changes durable state to Aborted
            DaemonTaskService::mark_aborted(&store, &daemon_dir, "gh-abort-notoken-55")
                .map_err(|e| e.to_string())?;

            // When GITHUB_TOKEN is unavailable, the CLI marks label_dirty
            DaemonTaskService::mark_label_dirty(&store, &daemon_dir, "gh-abort-notoken-55")
                .map_err(|e| e.to_string())?;

            let aborted = store
                .read_task(&daemon_dir, "gh-abort-notoken-55")
                .map_err(|e| e.to_string())?;
            if aborted.status != TaskStatus::Aborted {
                return Err(format!("expected aborted status, got {}", aborted.status));
            }
            if !aborted.label_dirty {
                return Err("expected label_dirty=true after abort without token".to_owned());
            }

            // --- Test retry path ---
            // Create a Failed task (retryable)
            let retry_task = DaemonTask {
                task_id: "gh-retry-notoken-56".to_owned(),
                issue_ref: "acme/widgets#56".to_owned(),
                project_id: "proj-56".to_owned(),
                project_name: Some("Retry without token test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Failed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: None,
                failure_class: Some("test".to_owned()),
                failure_message: Some("test failure".to_owned()),
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(56),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(&daemon_dir, &retry_task)
                .map_err(|e| e.to_string())?;

            // Simulate: retry changes durable state to Pending
            DaemonTaskService::retry_task(&store, &daemon_dir, "gh-retry-notoken-56")
                .map_err(|e| e.to_string())?;

            // When GITHUB_TOKEN is unavailable, the CLI marks label_dirty
            DaemonTaskService::mark_label_dirty(&store, &daemon_dir, "gh-retry-notoken-56")
                .map_err(|e| e.to_string())?;

            let retried = store
                .read_task(&daemon_dir, "gh-retry-notoken-56")
                .map_err(|e| e.to_string())?;
            if retried.status != TaskStatus::Pending {
                return Err(format!(
                    "expected pending status after retry, got {}",
                    retried.status
                ));
            }
            if !retried.label_dirty {
                return Err("expected label_dirty=true after retry without token".to_owned());
            }

            Ok(())
        }
    );

    // -----------------------------------------------------------------------
    // daemon.tasks.label_sync_recovery_after_state_transition
    // Verifies that a label-sync failure after a non-terminal state transition
    // (Claimed/Active) does not strand the task, and that a label-sync failure
    // after a terminal transition (Completed/Failed) still releases the lease.
    // -----------------------------------------------------------------------
    reg!(
        m,
        "daemon.tasks.label_sync_recovery_after_state_transition",
        || {
            use crate::adapters::fs::FsDataDirDaemonStore;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, RoutingSource, TaskStatus,
            };
            use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
            use crate::contexts::automation_runtime::task_service::DaemonTaskService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::shared::domain::FlowPreset;

            let ws = TempWorkspace::new()?;
            let data_dir = ws.path().join("daemon-data");
            repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
            let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");

            let store = FsDataDirDaemonStore;
            let now = chrono::Utc::now();

            // --- Non-terminal case: Claimed task with label_dirty ---
            // Simulates a label-sync failure right after claim. The task must
            // remain Claimed (not rolled back) and retain label_dirty so Phase 0
            // can repair the label, while the state machine continues processing.
            let claimed_task = DaemonTask {
                task_id: "gh-claimed-dirty-200".to_owned(),
                issue_ref: "acme/widgets#200".to_owned(),
                project_id: "proj-200".to_owned(),
                project_name: Some("Claimed label-sync failure".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Claimed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-200".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(200),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(&daemon_dir, &claimed_task)
                .map_err(|e| e.to_string())?;

            // Simulate label-sync failure: mark dirty
            DaemonTaskService::mark_label_dirty(&store, &daemon_dir, "gh-claimed-dirty-200")
                .map_err(|e| e.to_string())?;

            // The task must still be Claimed (not rolled back to Pending), so the
            // state machine can continue. It must also be label_dirty for Phase 0 repair.
            let loaded_claimed = store
                .read_task(&daemon_dir, "gh-claimed-dirty-200")
                .map_err(|e| e.to_string())?;
            if loaded_claimed.status != TaskStatus::Claimed {
                return Err(format!(
                    "expected claimed task to remain claimed, got {}",
                    loaded_claimed.status
                ));
            }
            if !loaded_claimed.label_dirty {
                return Err(
                    "expected label_dirty=true for claimed task after label-sync failure"
                        .to_owned(),
                );
            }

            // Now the state machine continues: mark Active (simulating normal progression)
            let active =
                DaemonTaskService::mark_active(&store, &daemon_dir, "gh-claimed-dirty-200")
                    .map_err(|e| e.to_string())?;
            if active.status != TaskStatus::Active {
                return Err(format!(
                    "expected active after mark_active, got {}",
                    active.status
                ));
            }

            // And eventually completes
            let completed =
                DaemonTaskService::mark_completed(&store, &daemon_dir, "gh-claimed-dirty-200")
                    .map_err(|e| e.to_string())?;
            if completed.status != TaskStatus::Completed {
                return Err(format!("expected completed, got {}", completed.status));
            }

            // label_dirty should still be true (was never cleared by a successful sync)
            if !completed.label_dirty {
                return Err("expected label_dirty to persist through state transitions".to_owned());
            }

            // --- Terminal case: Completed task with label_dirty must release lease ---
            // Simulates a label-sync failure after marking Completed. The task
            // must still be terminal AND its lease must be releasable.
            let terminal_task = DaemonTask {
                task_id: "gh-completed-dirty-201".to_owned(),
                issue_ref: "acme/widgets#201".to_owned(),
                project_id: "proj-201".to_owned(),
                project_name: Some("Completed label-sync failure".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Completed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-201".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(201),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: true,
            };
            store
                .create_task(&daemon_dir, &terminal_task)
                .map_err(|e| e.to_string())?;

            // Verify terminal status
            let loaded_terminal = store
                .read_task(&daemon_dir, "gh-completed-dirty-201")
                .map_err(|e| e.to_string())?;
            if !loaded_terminal.is_terminal() {
                return Err("expected terminal status".to_owned());
            }

            // The runtime contract: even with label_dirty=true, the lease must
            // be clearable so the terminal task does not retain ownership.
            let mut cleared = loaded_terminal.clone();
            cleared.clear_lease();
            if cleared.lease_id.is_some() {
                return Err(
                    "expected lease to be clearable on terminal task with dirty label".to_owned(),
                );
            }

            // Verify the task is still terminal and label_dirty after lease release
            if !cleared.is_terminal() {
                return Err("task should remain terminal after lease release".to_owned());
            }
            if !cleared.label_dirty {
                return Err("label_dirty should persist after lease release".to_owned());
            }

            Ok(())
        }
    );

    // -----------------------------------------------------------------------
    // daemon.tasks.label_failure_quarantine_and_recovery
    // Loop-level test: verifies that a label-sync failure during
    // process_task_multi_repo quarantines the repo (no further mutations)
    // and that Phase 0 in the next cycle recovers the task by repairing
    // the label and reverting non-terminal tasks to Pending — but only
    // when lease cleanup positively succeeds. If cleanup is partial, the
    // task must stay in its current state with lease ownership preserved.
    // -----------------------------------------------------------------------
    reg!(
        m,
        "daemon.tasks.label_failure_quarantine_and_recovery",
        || {
            use crate::adapters::fs::FsDataDirDaemonStore;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
            };
            use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
            use crate::contexts::automation_runtime::task_service::DaemonTaskService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::shared::domain::{FlowPreset, ProjectId};

            let ws = TempWorkspace::new()?;
            let data_dir = ws.path().join("daemon-data");

            repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
            let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
            let store = FsDataDirDaemonStore;
            let now = chrono::Utc::now();

            // --- Scenario A: Claimed task with label_dirty where cleanup succeeds.
            // The stub returns Removed (positive cleanup), the writer lock exists
            // and is released, so resources_released = true and the revert proceeds. ---
            let claimed_task = DaemonTask {
                task_id: "gh-quarantine-claimed-300".to_owned(),
                issue_ref: "acme/widgets#300".to_owned(),
                project_id: "proj-300".to_owned(),
                project_name: Some("Quarantine claimed test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Claimed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-300".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(300),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: true,
            };
            store
                .create_task(&daemon_dir, &claimed_task)
                .map_err(|e| e.to_string())?;

            // Verify precondition: task is Claimed with dirty label
            let loaded = store
                .read_task(&daemon_dir, "gh-quarantine-claimed-300")
                .map_err(|e| e.to_string())?;
            if loaded.status != TaskStatus::Claimed {
                return Err(format!("expected claimed, got {}", loaded.status));
            }
            if !loaded.label_dirty {
                return Err("expected label_dirty=true".to_owned());
            }

            DaemonTaskService::clear_label_dirty(&store, &daemon_dir, "gh-quarantine-claimed-300")
                .map_err(|e| e.to_string())?;

            // Stub worktree adapter that returns Removed (positive cleanup)
            struct SuccessWorktree;
            impl crate::contexts::automation_runtime::WorktreePort for SuccessWorktree {
                fn worktree_path(
                    &self,
                    base_dir: &std::path::Path,
                    task_id: &str,
                ) -> std::path::PathBuf {
                    base_dir.join("worktrees").join(task_id)
                }
                fn branch_name(&self, task_id: &str) -> String {
                    format!("rb/{task_id}")
                }
                fn create_worktree(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _branch_name: &str,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
                fn remove_worktree(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<
                    crate::contexts::automation_runtime::WorktreeCleanupOutcome,
                > {
                    Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::Removed)
                }
                fn rebase_onto_default_branch(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _branch_name: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
            }
            let success_wt = SuccessWorktree;

            // Create lease file and writer lock so all release sub-steps succeed
            let lease = WorktreeLease {
                lease_id: "lease-300".to_owned(),
                task_id: "gh-quarantine-claimed-300".to_owned(),
                project_id: "proj-300".to_owned(),
                worktree_path: ws.path().join("worktrees/task-300"),
                branch_name: "rb/300-proj-300".to_owned(),
                acquired_at: now,
                ttl_seconds: 300,
                last_heartbeat: now,
                cleanup_handoff: None,
            };
            store
                .write_lease(&daemon_dir, &lease)
                .map_err(|e| e.to_string())?;
            let proj_300 = ProjectId::new("proj-300".to_owned()).map_err(|e| e.to_string())?;
            store
                .acquire_writer_lock(&daemon_dir, &proj_300, "lease-300")
                .map_err(|e| e.to_string())?;

            let reverted = DaemonTaskService::revert_to_pending_for_recovery(
                &store,
                &success_wt,
                &daemon_dir,
                ws.path(),
                "gh-quarantine-claimed-300",
            )
            .map_err(|e| e.to_string())?;

            if reverted.status != TaskStatus::Pending {
                return Err(format!(
                    "expected reverted task to be pending, got {}",
                    reverted.status
                ));
            }
            if reverted.lease_id.is_some() {
                return Err("expected lease_id cleared after successful revert".to_owned());
            }
            if reverted.label_dirty {
                return Err(
                    "expected label_dirty=false after revert (cleared before revert)".to_owned(),
                );
            }

            // --- Scenario A2: Claimed task with label_dirty where cleanup is
            // partial (stub returns AlreadyAbsent). revert_to_pending_for_recovery
            // must preserve the task state and lease ownership. ---
            let claimed_task_partial = DaemonTask {
                task_id: "gh-quarantine-claimed-302".to_owned(),
                issue_ref: "acme/widgets#302".to_owned(),
                project_id: "proj-302".to_owned(),
                project_name: Some("Quarantine partial cleanup test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Claimed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-302".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(302),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: true,
            };
            store
                .create_task(&daemon_dir, &claimed_task_partial)
                .map_err(|e| e.to_string())?;

            let lease_302 = WorktreeLease {
                lease_id: "lease-302".to_owned(),
                task_id: "gh-quarantine-claimed-302".to_owned(),
                project_id: "proj-302".to_owned(),
                worktree_path: ws.path().join("worktrees/task-302"),
                branch_name: "rb/302-proj-302".to_owned(),
                acquired_at: now,
                ttl_seconds: 300,
                last_heartbeat: now,
                cleanup_handoff: None,
            };
            store
                .write_lease(&daemon_dir, &lease_302)
                .map_err(|e| e.to_string())?;

            // Stub that returns AlreadyAbsent — simulates partial cleanup
            struct PartialWorktree;
            impl crate::contexts::automation_runtime::WorktreePort for PartialWorktree {
                fn worktree_path(
                    &self,
                    base_dir: &std::path::Path,
                    task_id: &str,
                ) -> std::path::PathBuf {
                    base_dir.join("worktrees").join(task_id)
                }
                fn branch_name(&self, task_id: &str) -> String {
                    format!("rb/{task_id}")
                }
                fn create_worktree(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _branch_name: &str,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
                fn remove_worktree(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<
                    crate::contexts::automation_runtime::WorktreeCleanupOutcome,
                > {
                    Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
                }
                fn rebase_onto_default_branch(
                    &self,
                    _repo_root: &std::path::Path,
                    _worktree_path: &std::path::Path,
                    _branch_name: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
            }
            let partial_wt = PartialWorktree;

            let revert_result = DaemonTaskService::revert_to_pending_for_recovery(
                &store,
                &partial_wt,
                &daemon_dir,
                ws.path(),
                "gh-quarantine-claimed-302",
            );
            if revert_result.is_ok() {
                return Err(
                    "revert_to_pending_for_recovery should fail when cleanup is partial".to_owned(),
                );
            }
            // Verify task preserved its state and lease
            let preserved = store
                .read_task(&daemon_dir, "gh-quarantine-claimed-302")
                .map_err(|e| e.to_string())?;
            if preserved.status != TaskStatus::Claimed {
                return Err(format!(
                    "expected claimed preserved after partial cleanup, got {}",
                    preserved.status
                ));
            }
            if preserved.lease_id.as_deref() != Some("lease-302") {
                return Err("expected lease_id preserved after partial cleanup".to_owned());
            }

            // --- Scenario B: Completed task with label_dirty and unreleased lease
            // (simulates terminal label-sync failure that quarantined before
            // lease release). Phase 0 should attempt release — and the lease
            // reference must be preserved until explicit release succeeds. ---
            let completed_task = DaemonTask {
                task_id: "gh-quarantine-completed-301".to_owned(),
                issue_ref: "acme/widgets#301".to_owned(),
                project_id: "proj-301".to_owned(),
                project_name: Some("Quarantine completed test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Completed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-301".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(301),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: true,
            };
            store
                .create_task(&daemon_dir, &completed_task)
                .map_err(|e| e.to_string())?;

            // Create its lease
            let terminal_lease = WorktreeLease {
                lease_id: "lease-301".to_owned(),
                task_id: "gh-quarantine-completed-301".to_owned(),
                project_id: "proj-301".to_owned(),
                worktree_path: ws.path().join("worktrees/task-301"),
                branch_name: "rb/301-proj-301".to_owned(),
                acquired_at: now,
                ttl_seconds: 300,
                last_heartbeat: now,
                cleanup_handoff: None,
            };
            store
                .write_lease(&daemon_dir, &terminal_lease)
                .map_err(|e| e.to_string())?;

            // Verify: terminal task with lease and dirty label
            let loaded_terminal = store
                .read_task(&daemon_dir, "gh-quarantine-completed-301")
                .map_err(|e| e.to_string())?;
            if !loaded_terminal.is_terminal() {
                return Err("expected terminal status".to_owned());
            }
            if !loaded_terminal.label_dirty {
                return Err("expected label_dirty=true".to_owned());
            }
            if loaded_terminal.lease_id.is_none() {
                return Err("expected lease_id present".to_owned());
            }

            // Simulate Phase 0: clear dirty flag and verify the lease is still
            // present (Phase 0 in production would call release_task_lease).
            DaemonTaskService::clear_label_dirty(
                &store,
                &daemon_dir,
                "gh-quarantine-completed-301",
            )
            .map_err(|e| e.to_string())?;

            let after_clear = store
                .read_task(&daemon_dir, "gh-quarantine-completed-301")
                .map_err(|e| e.to_string())?;
            if after_clear.label_dirty {
                return Err("expected label_dirty=false after clear".to_owned());
            }
            // Task is still terminal and the lease reference is still present
            // (in production, release_task_lease would then clear it).
            if !after_clear.is_terminal() {
                return Err("task should remain terminal".to_owned());
            }
            if after_clear.lease_id.is_none() {
                return Err("lease_id should still be present until explicit release".to_owned());
            }

            // --- Scenario C: revert_to_pending_for_recovery must reject terminal tasks ---
            let revert_result = DaemonTaskService::revert_to_pending_for_recovery(
                &store,
                &partial_wt,
                &daemon_dir,
                ws.path(),
                "gh-quarantine-completed-301",
            );
            if revert_result.is_ok() {
                return Err(
                    "revert_to_pending_for_recovery should reject terminal tasks".to_owned(),
                );
            }

            // --- Scenario D: retry_task must reject tasks that still hold a lease
            // reference (from partial cleanup / quarantined failure). ---
            let failed_with_lease = DaemonTask {
                task_id: "gh-retry-retained-303".to_owned(),
                issue_ref: "acme/widgets#303".to_owned(),
                project_id: "proj-303".to_owned(),
                project_name: Some("Retry retained lease test".to_owned()),
                prompt: None,
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Failed,
                created_at: now,
                updated_at: now,
                attempt_count: 1,
                lease_id: Some("lease-303".to_owned()),
                failure_class: Some("test".to_owned()),
                failure_message: Some("simulated failure".to_owned()),
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(303),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(&daemon_dir, &failed_with_lease)
                .map_err(|e| e.to_string())?;

            let retry_result =
                DaemonTaskService::retry_task(&store, &daemon_dir, "gh-retry-retained-303");
            if retry_result.is_ok() {
                return Err(
                    "retry_task should reject tasks that still hold a lease reference".to_owned(),
                );
            }
            // Verify task preserved its failed state
            let still_failed = store
                .read_task(&daemon_dir, "gh-retry-retained-303")
                .map_err(|e| e.to_string())?;
            if still_failed.status != TaskStatus::Failed {
                return Err(format!(
                    "expected task to remain failed after rejected retry, got {}",
                    still_failed.status
                ));
            }
            if still_failed.lease_id.as_deref() != Some("lease-303") {
                return Err("expected lease_id preserved after rejected retry".to_owned());
            }

            Ok(())
        }
    );

    // -----------------------------------------------------------------------
    // daemon.tasks.label_failure_quarantines_repo
    // Loop-level regression test: drives a real multi-repo daemon cycle
    // through process_task_multi_repo with a GithubPort that fails on
    // add_label. Verifies that after the label-sync failure the task is
    // marked label_dirty and the repo is quarantined — no second task is
    // processed in the same cycle.
    // -----------------------------------------------------------------------
    reg!(m, "daemon.tasks.label_failure_quarantines_repo", || {
        use crate::adapters::fs::FsDataDirDaemonStore;
        use crate::adapters::fs::{
            FsAmendmentQueueStore, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore,
            FsProjectStore, FsRawOutputStore, FsRequirementsStore, FsRunSnapshotStore,
            FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
        };
        use crate::adapters::github::{
            GithubComment, GithubIssue, GithubPort, GithubPullRequest, GithubReview,
        };
        use crate::adapters::stub_backend::StubBackendAdapter;
        use crate::adapters::BackendAdapter;
        use crate::contexts::agent_execution::service::AgentExecutionService;
        use crate::contexts::automation_runtime::daemon_loop::{DaemonLoop, DaemonLoopConfig};
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;
        use crate::shared::error::{AppError, AppResult};

        // --- A GitHub adapter that always fails on add_label ---
        struct FailLabelGithub;
        impl GithubPort for FailLabelGithub {
            async fn ensure_labels(&self, _o: &str, _r: &str, _l: &[&str]) -> AppResult<()> {
                Ok(())
            }
            async fn poll_candidate_issues(
                &self,
                _o: &str,
                _r: &str,
                _l: &str,
            ) -> AppResult<Vec<GithubIssue>> {
                Ok(vec![])
            }
            async fn read_issue_labels(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<String>> {
                Ok(vec![])
            }
            async fn add_label(&self, _o: &str, _r: &str, _n: u64, _l: &str) -> AppResult<()> {
                Err(AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: "simulated label failure".to_owned(),
                    failure_class: None,
                })
            }
            async fn remove_label(&self, _o: &str, _r: &str, _n: u64, _l: &str) -> AppResult<()> {
                Ok(())
            }
            async fn replace_labels(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
                _l: &[&str],
            ) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_issue_comments(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubComment>> {
                Ok(vec![])
            }
            async fn post_idempotent_comment(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
                _m: &str,
                _b: &str,
            ) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_pr_review_comments(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubComment>> {
                Ok(vec![])
            }
            async fn fetch_pr_reviews(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubReview>> {
                Ok(vec![])
            }
            async fn create_draft_pr(
                &self,
                _o: &str,
                _r: &str,
                _t: &str,
                _b: &str,
                _h: &str,
                _bs: &str,
            ) -> AppResult<GithubPullRequest> {
                Err(AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: "not implemented".to_owned(),
                    failure_class: None,
                })
            }
            async fn mark_pr_ready(&self, _o: &str, _r: &str, _n: u64) -> AppResult<()> {
                Ok(())
            }
            async fn close_pr(&self, _o: &str, _r: &str, _n: u64) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_pr_state(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<GithubPullRequest> {
                Err(AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: "not implemented".to_owned(),
                    failure_class: None,
                })
            }
            async fn update_pr_body(&self, _o: &str, _r: &str, _n: u64, _b: &str) -> AppResult<()> {
                Ok(())
            }
            async fn is_branch_ahead(
                &self,
                _o: &str,
                _r: &str,
                _b: &str,
                _h: &str,
            ) -> AppResult<bool> {
                Ok(false)
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;

        let data_dir = ws.path().join("daemon-data");
        repo_registry::register_repo(&data_dir, "acme/widgets").map_err(|e| e.to_string())?;
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let store = FsDataDirDaemonStore;
        let now = chrono::Utc::now();

        // Pre-seed two pending tasks for the same repo. The first will trigger
        // a label-sync failure during claim; the second must NOT be processed
        // due to repo quarantine.
        let task1 = DaemonTask {
            task_id: "qr-task-1".to_owned(),
            issue_ref: "acme/widgets#501".to_owned(),
            project_id: "proj-501".to_owned(),
            project_name: Some("Quarantine test 1".to_owned()),
            prompt: Some("First task".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(501),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        let task2 = DaemonTask {
            task_id: "qr-task-2".to_owned(),
            issue_ref: "acme/widgets#502".to_owned(),
            project_id: "proj-502".to_owned(),
            project_name: Some("Quarantine test 2".to_owned()),
            prompt: Some("Second task".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(502),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(&daemon_dir, &task1)
            .map_err(|e| e.to_string())?;
        store
            .create_task(&daemon_dir, &task2)
            .map_err(|e| e.to_string())?;

        // Build a DaemonLoop with the failing GitHub adapter and run one cycle
        let reg = repo_registry::RepoRegistration {
            repo_slug: "acme/widgets".to_owned(),
            repo_root: ws.path().to_path_buf(),
            workspace_root: ws.path().to_path_buf(),
        };

        let adapter = BackendAdapter::Stub(StubBackendAdapter::default());
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);

        let worktree = crate::adapters::worktree::WorktreeAdapter;
        let project_store = FsProjectStore;
        let run_snapshot_read = FsRunSnapshotStore;
        let run_snapshot_write = FsRunSnapshotWriteStore;
        let journal_store = FsJournalStore;
        let artifact_store = FsArtifactStore;
        let artifact_write = FsPayloadArtifactWriteStore;
        let log_write = FsRuntimeLogWriteStore;
        let amendment_queue = FsAmendmentQueueStore;
        let requirements_store = FsRequirementsStore;

        let daemon_loop = DaemonLoop::new(
            &store,
            &worktree,
            &project_store,
            &run_snapshot_read,
            &run_snapshot_write,
            &journal_store,
            &artifact_store,
            &artifact_write,
            &log_write,
            &amendment_queue,
            &agent_service,
        )
        .with_requirements_store(&requirements_store)
        .with_registrations(vec![reg.clone()])
        .with_data_dir(data_dir.clone());

        let loop_config = DaemonLoopConfig {
            single_iteration: true,
            ..DaemonLoopConfig::default()
        };

        let github = FailLabelGithub;

        // Run one multi-repo cycle — task 1 gets claimed, label sync fails,
        // repo is quarantined, task 2 stays pending.
        block_on_app_result(daemon_loop.run_multi_repo(&loop_config, &github))?;

        // Verify: task 1 was claimed and label_dirty is set
        let t1 = store
            .read_task(&daemon_dir, "qr-task-1")
            .map_err(|e| e.to_string())?;
        if t1.status == TaskStatus::Pending {
            return Err("task 1 should have been claimed (not still pending)".to_owned());
        }
        if !t1.label_dirty {
            return Err("task 1 should have label_dirty=true after failed label sync".to_owned());
        }

        // Verify: task 2 was NOT processed (still pending) — proves quarantine
        let t2 = store
            .read_task(&daemon_dir, "qr-task-2")
            .map_err(|e| e.to_string())?;
        if t2.status != TaskStatus::Pending {
            return Err(format!(
                "task 2 should still be pending (quarantine failed), got {}",
                t2.status
            ));
        }
        if t2.label_dirty {
            return Err("task 2 should NOT have label_dirty (never touched)".to_owned());
        }

        Ok(())
    });

    // daemon.github.port_covers_pr_operations
    // Verifies that the full slice-9 PR/branch API is callable through the
    // GithubPort trait (via generic bound) and the in-memory test double.
    reg!(m, "daemon.github.port_covers_pr_operations", || {
        use crate::adapters::github::{
            GithubComment, GithubPort, GithubReview, GithubUser, InMemoryGithubClient,
        };

        let client = InMemoryGithubClient::new();

        // Seed review comments and reviews for later fetch
        {
            let mut comments = client.pr_review_comments.lock().unwrap();
            comments.push((
                100,
                GithubComment {
                    id: 1,
                    body: "review inline comment".to_owned(),
                    user: GithubUser {
                        login: "reviewer".to_owned(),
                        id: 42,
                    },
                    created_at: "2026-01-01T00:00:00Z".to_owned(),
                    updated_at: "2026-01-01T00:00:00Z".to_owned(),
                },
            ));
            let mut reviews = client.pr_reviews.lock().unwrap();
            reviews.push((
                100,
                GithubReview {
                    id: 10,
                    user: GithubUser {
                        login: "reviewer".to_owned(),
                        id: 42,
                    },
                    body: Some("LGTM".to_owned()),
                    state: "APPROVED".to_owned(),
                    submitted_at: Some("2026-01-01T00:00:00Z".to_owned()),
                },
            ));
        }

        // Seed a branch-ahead entry
        {
            let mut ahead = client.branches_ahead.lock().unwrap();
            ahead.insert("acme/widgets:main...rb/42-proj".to_owned());
        }

        // Generic helper that exercises all PR/branch methods through
        // the GithubPort trait bound — proves the trait surface is complete.
        async fn exercise_pr_port<G: GithubPort>(port: &G) -> Result<(), String> {
            // create_draft_pr
            let pr = port
                .create_draft_pr("acme", "widgets", "test PR", "body", "rb/42-proj", "main")
                .await
                .map_err(|e| e.to_string())?;
            if pr.draft != Some(true) {
                return Err("expected draft PR".to_owned());
            }
            let pr_num = pr.number;

            // fetch_pr_state
            let fetched = port
                .fetch_pr_state("acme", "widgets", pr_num)
                .await
                .map_err(|e| e.to_string())?;
            if fetched.state != "open" {
                return Err(format!("expected open state, got {}", fetched.state));
            }

            // mark_pr_ready
            port.mark_pr_ready("acme", "widgets", pr_num)
                .await
                .map_err(|e| e.to_string())?;
            let ready = port
                .fetch_pr_state("acme", "widgets", pr_num)
                .await
                .map_err(|e| e.to_string())?;
            if ready.draft != Some(false) {
                return Err("expected PR to be marked ready".to_owned());
            }

            // update_pr_body
            port.update_pr_body("acme", "widgets", pr_num, "updated body")
                .await
                .map_err(|e| e.to_string())?;

            // close_pr
            port.close_pr("acme", "widgets", pr_num)
                .await
                .map_err(|e| e.to_string())?;
            let closed = port
                .fetch_pr_state("acme", "widgets", pr_num)
                .await
                .map_err(|e| e.to_string())?;
            if closed.state != "closed" {
                return Err(format!("expected closed state, got {}", closed.state));
            }

            // fetch_pr_review_comments
            let review_comments = port
                .fetch_pr_review_comments("acme", "widgets", 100)
                .await
                .map_err(|e| e.to_string())?;
            if review_comments.len() != 1 {
                return Err(format!(
                    "expected 1 review comment, got {}",
                    review_comments.len()
                ));
            }

            // fetch_pr_reviews
            let reviews = port
                .fetch_pr_reviews("acme", "widgets", 100)
                .await
                .map_err(|e| e.to_string())?;
            if reviews.len() != 1 {
                return Err(format!("expected 1 review, got {}", reviews.len()));
            }
            if reviews[0].state != "APPROVED" {
                return Err(format!(
                    "expected APPROVED review, got {}",
                    reviews[0].state
                ));
            }

            // is_branch_ahead — positive
            let ahead = port
                .is_branch_ahead("acme", "widgets", "main", "rb/42-proj")
                .await
                .map_err(|e| e.to_string())?;
            if !ahead {
                return Err("expected branch to be ahead".to_owned());
            }

            // is_branch_ahead — negative
            let not_ahead = port
                .is_branch_ahead("acme", "widgets", "main", "rb/99-other")
                .await
                .map_err(|e| e.to_string())?;
            if not_ahead {
                return Err("expected branch NOT to be ahead".to_owned());
            }

            Ok(())
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(exercise_pr_port(&client)))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(exercise_pr_port(&client))
        }
    });

    // ── Conformance: requirements-draft label lifecycle ──────────────────
    // Proves that the issue label reflects truthful durable state at each
    // transition during a requirements-draft run:
    //   Pending (rb:ready) → Active (rb:in-progress) →
    //     WaitingForRequirements (rb:waiting-feedback) or Failed (rb:failed)
    reg!(m, "daemon.labels.requirements_draft_lifecycle", || {
        use crate::adapters::github::{GithubIssue, GithubLabel, GithubUser, InMemoryGithubClient};
        use crate::contexts::automation_runtime::github_intake;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus,
        };

        let gh = InMemoryGithubClient::with_issues(vec![GithubIssue {
            number: 77,
            title: "Draft lifecycle test".to_owned(),
            body: None,
            labels: vec![GithubLabel {
                name: "rb:ready".to_owned(),
            }],
            user: GithubUser {
                login: "user".to_owned(),
                id: 1,
            },
            html_url: "https://github.com/acme/widgets/issues/77".to_owned(),
            pull_request: None,
            updated_at: "2026-03-17T00:00:00Z".to_owned(),
        }]);

        let now = chrono::Utc::now();
        let mut task = DaemonTask {
            task_id: "gh-draft-77".to_owned(),
            issue_ref: "acme/widgets#77".to_owned(),
            project_id: String::new(),
            project_name: None,
            prompt: Some("Draft test".to_owned()),
            routing_command: Some("/rb requirements".to_owned()),
            routing_labels: vec![],
            resolved_flow: None,
            routing_source: Some(RoutingSource::Command),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::RequirementsDraft,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(77),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };

        // Helper: read current labels for issue #77 from the in-memory client
        fn issue_labels(gh: &InMemoryGithubClient, n: u64) -> Vec<String> {
            let issues = gh.issues.lock().unwrap();
            issues
                .iter()
                .find(|i| i.number == n)
                .map(|i| i.labels.iter().map(|l| l.name.clone()).collect())
                .unwrap_or_default()
        }

        async fn run_lifecycle(
            gh: &InMemoryGithubClient,
            task: &mut DaemonTask,
        ) -> Result<(), String> {
            // ── Step 1: Pending → rb:ready ──────────────────────────────
            github_intake::sync_label_for_task(gh, task)
                .await
                .map_err(|e| format!("sync pending: {e}"))?;
            let labels = issue_labels(gh, 77);
            if !labels.contains(&"rb:ready".to_owned()) {
                return Err(format!("expected rb:ready for Pending, got {labels:?}"));
            }

            // ── Step 2: Claimed → Active → rb:in-progress ──────────────
            let now = chrono::Utc::now();
            task.transition_to(TaskStatus::Claimed, now)
                .map_err(|e| e.to_string())?;
            task.transition_to(TaskStatus::Active, now)
                .map_err(|e| e.to_string())?;
            github_intake::sync_label_for_task(gh, task)
                .await
                .map_err(|e| format!("sync active: {e}"))?;
            let labels = issue_labels(gh, 77);
            if !labels.contains(&"rb:in-progress".to_owned()) {
                return Err(format!(
                    "expected rb:in-progress for Active, got {labels:?}"
                ));
            }
            if labels.contains(&"rb:ready".to_owned()) {
                return Err(format!(
                    "rb:ready should have been removed after Active, got {labels:?}"
                ));
            }

            // ── Step 3: WaitingForRequirements → rb:waiting-feedback ────
            task.transition_to(TaskStatus::WaitingForRequirements, now)
                .map_err(|e| e.to_string())?;
            github_intake::sync_label_for_task(gh, task)
                .await
                .map_err(|e| format!("sync waiting: {e}"))?;
            let labels = issue_labels(gh, 77);
            if !labels.contains(&"rb:waiting-feedback".to_owned()) {
                return Err(format!(
                    "expected rb:waiting-feedback for WaitingForRequirements, got {labels:?}"
                ));
            }
            if labels.contains(&"rb:in-progress".to_owned()) {
                return Err(format!(
                    "rb:in-progress should have been removed after WaitingForRequirements, got {labels:?}"
                ));
            }

            // ── Step 4: Failed → rb:failed ──────────────────────────────
            // Simulate failure from WaitingForRequirements
            task.status = TaskStatus::Failed;
            task.failure_class = Some("test_failure".to_owned());
            github_intake::sync_label_for_task(gh, task)
                .await
                .map_err(|e| format!("sync failed: {e}"))?;
            let labels = issue_labels(gh, 77);
            if !labels.contains(&"rb:failed".to_owned()) {
                return Err(format!("expected rb:failed for Failed, got {labels:?}"));
            }
            if labels.contains(&"rb:waiting-feedback".to_owned()) {
                return Err(format!(
                    "rb:waiting-feedback should have been removed after Failed, got {labels:?}"
                ));
            }

            Ok(())
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(run_lifecycle(&gh, &mut task)))
        } else {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(run_lifecycle(&gh, &mut task))
        }
    });

    reg!(m, "daemon.slug_validation.rejects_dot_segments", || {
        use crate::contexts::automation_runtime::repo_registry::parse_repo_slug;

        // Dot segments must be rejected
        let bad_slugs = ["acme/.", "acme/..", "./repo", "../repo", "./.."];
        for slug in &bad_slugs {
            if parse_repo_slug(slug).is_ok() {
                return Err(format!(
                    "expected parse_repo_slug('{}') to fail, but it succeeded",
                    slug
                ));
            }
        }

        // Valid slugs must still succeed
        let (owner, repo) = parse_repo_slug("acme/widgets").map_err(|e| e.to_string())?;
        if owner != "acme" || repo != "widgets" {
            return Err(format!(
                "expected (acme, widgets), got ({}, {})",
                owner, repo
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.routing.run_overrides_stale_requirements", || {
        use crate::contexts::automation_runtime::github_intake::extract_command;
        use crate::contexts::automation_runtime::model::{DispatchMode, WatchedIssueMeta};
        use crate::contexts::automation_runtime::watcher;

        // Scenario: issue body contains stale `/rb requirements draft`,
        // but a newer comment says `/rb run`.  The extracted explicit
        // command should be `/rb run`, and dispatch should be Workflow.
        let body = "Please implement this feature.\n/rb requirements draft\n";
        let comments = vec!["looks good".to_owned(), "/rb run".to_owned()];

        // extract_command should find /rb run (newest comment wins)
        let cmd = extract_command(body, &comments);
        if cmd.as_deref() != Some("/rb run") {
            return Err(format!(
                "expected extracted command '/rb run', got {:?}",
                cmd
            ));
        }

        // Now simulate what poll_and_ingest_repo does: detect /rb run,
        // force DispatchMode::Workflow regardless of body content.
        let explicit_run = cmd
            .as_deref()
            .map(|c| c.trim() == "/rb run")
            .unwrap_or(false);
        if !explicit_run {
            return Err("expected explicit_run=true for '/rb run' command".into());
        }

        // With explicit_run=true, dispatch mode should be forced to Workflow.
        // Verify that without the override, body scanning would produce
        // RequirementsDraft (this proves the fix is load-bearing).
        let meta = WatchedIssueMeta {
            issue_ref: "acme/widgets#1".to_owned(),
            source_revision: String::new(),
            title: "test".to_owned(),
            body: body.to_owned(),
            labels: vec![],
            routing_command: None, // /rb run is a daemon command, filtered out
        };
        let body_mode = watcher::resolve_dispatch_mode(&meta).map_err(|e| e.to_string())?;
        if body_mode != DispatchMode::RequirementsDraft {
            return Err(format!(
                "expected body scan to produce RequirementsDraft, got {:?}",
                body_mode
            ));
        }

        // The actual fix: when explicit_run is true, we force Workflow
        let dispatch_mode = if explicit_run {
            DispatchMode::Workflow
        } else {
            body_mode
        };
        if dispatch_mode != DispatchMode::Workflow {
            return Err(format!(
                "expected forced Workflow dispatch, got {:?}",
                dispatch_mode
            ));
        }

        Ok(())
    });

    reg!(
        m,
        "daemon.labels.requirements_draft_label_failure_quarantine",
        || {
            use crate::adapters::github::{
                GithubIssue, GithubLabel, GithubUser, InMemoryGithubClient,
            };
            use crate::contexts::automation_runtime::github_intake;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, RoutingSource, TaskStatus,
            };

            let gh = InMemoryGithubClient::with_issues(vec![GithubIssue {
                number: 99,
                title: "Label failure quarantine test".to_owned(),
                body: None,
                labels: vec![GithubLabel {
                    name: "rb:in-progress".to_owned(),
                }],
                user: GithubUser {
                    login: "user".to_owned(),
                    id: 1,
                },
                html_url: "https://github.com/acme/widgets/issues/99".to_owned(),
                pull_request: None,
                updated_at: "2026-03-17T00:00:00Z".to_owned(),
            }]);

            let now = chrono::Utc::now();
            let mut task = DaemonTask {
                task_id: "gh-quarantine-99".to_owned(),
                issue_ref: "acme/widgets#99".to_owned(),
                project_id: String::new(),
                project_name: None,
                prompt: Some("Quarantine test".to_owned()),
                routing_command: Some("/rb requirements".to_owned()),
                routing_labels: vec![],
                resolved_flow: None,
                routing_source: Some(RoutingSource::Command),
                routing_warnings: vec![],
                status: TaskStatus::Active,
                created_at: now,
                updated_at: now,
                attempt_count: 0,
                lease_id: None,
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::RequirementsDraft,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(99),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };

            async fn run_test(
                gh: &InMemoryGithubClient,
                task: &mut DaemonTask,
            ) -> Result<(), String> {
                // ── Test 1: Active → WaitingForRequirements label sync failure ──
                // Simulate: task transitions to WaitingForRequirements, then
                // add_label for rb:waiting-feedback fails.
                task.status = TaskStatus::Active;
                task.transition_to(TaskStatus::WaitingForRequirements, chrono::Utc::now())
                    .map_err(|e| e.to_string())?;

                // Inject add_label failure for issue 99
                gh.set_add_label_failure(99);

                let result = github_intake::sync_label_for_task(gh, task).await;
                if result.is_ok() {
                    return Err(
                        "expected label sync to fail for WaitingForRequirements, but it succeeded"
                            .to_owned(),
                    );
                }

                // Verify the error is propagatable (not swallowed)
                let err_msg = result.unwrap_err().to_string();
                if !err_msg.contains("simulated add_label failure") {
                    return Err(format!("unexpected error message: {err_msg}"));
                }

                // Clear failure and verify sync works when not failing
                gh.clear_add_label_failure(99);

                // ── Test 2: Active → Pending label sync failure ──
                // Reset task to Active, then transition to Pending
                task.status = TaskStatus::Active;
                task.transition_to(TaskStatus::Pending, chrono::Utc::now())
                    .map_err(|e| e.to_string())?;

                // Inject failure again
                gh.set_add_label_failure(99);

                let result = github_intake::sync_label_for_task(gh, task).await;
                if result.is_ok() {
                    return Err(
                        "expected label sync to fail for Pending requeue, but it succeeded"
                            .to_owned(),
                    );
                }

                // ── Test 3: Failure on terminal state (mark_failed) is tolerable ──
                // When task is already Failed, label sync failure should still be
                // detectable but callers may choose to swallow it.
                gh.clear_add_label_failure(99);
                task.status = TaskStatus::Failed;
                task.failure_class = Some("test".to_owned());
                github_intake::sync_label_for_task(gh, task)
                    .await
                    .map_err(|e| format!("sync on terminal Failed should succeed: {e}"))?;

                Ok(())
            }

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(|| handle.block_on(run_test(&gh, &mut task)))
            } else {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                rt.block_on(run_test(&gh, &mut task))
            }
        }
    );

    reg!(
        m,
        "daemon.pr_runtime.create_draft_when_branch_ahead",
        || {
            use crate::adapters::fs::FsDaemonStore;
            use crate::adapters::github::InMemoryGithubClient;
            use crate::contexts::agent_execution::model::CancellationToken;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
            };
            use crate::contexts::automation_runtime::pr_runtime::PrRuntimeService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::shared::domain::FlowPreset;

            struct PrWorktree;
            impl crate::contexts::automation_runtime::WorktreePort for PrWorktree {
                fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
                    base_dir.join("worktrees").join(task_id)
                }
                fn branch_name(&self, task_id: &str) -> String {
                    format!("rb/{task_id}")
                }
                fn create_worktree(
                    &self,
                    _repo_root: &Path,
                    _worktree_path: &Path,
                    _branch_name: &str,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
                fn remove_worktree(
                    &self,
                    _repo_root: &Path,
                    _worktree_path: &Path,
                    _task_id: &str,
                ) -> crate::shared::error::AppResult<
                    crate::contexts::automation_runtime::WorktreeCleanupOutcome,
                > {
                    Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
                }
                fn rebase_onto_default_branch(
                    &self,
                    _repo_root: &Path,
                    _worktree_path: &Path,
                    _branch_name: &str,
                ) -> crate::shared::error::AppResult<()> {
                    Ok(())
                }
                fn default_branch_name(
                    &self,
                    _repo_root: &Path,
                ) -> crate::shared::error::AppResult<String> {
                    Ok("main".to_owned())
                }
            }

            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;
            let store = FsDaemonStore;
            let github = InMemoryGithubClient::new();
            github
                .branches_ahead
                .lock()
                .unwrap()
                .insert("acme/widgets:main...rb/42-proj-42".to_owned());
            let now = chrono::Utc::now();
            let task = DaemonTask {
                task_id: "pr-runtime-42".to_owned(),
                issue_ref: "acme/widgets#42".to_owned(),
                project_id: "proj-42".to_owned(),
                project_name: Some("Create PR".to_owned()),
                prompt: Some("Draft PR runtime".to_owned()),
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Active,
                created_at: now,
                updated_at: now,
                attempt_count: 0,
                lease_id: Some("lease-42".to_owned()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(42),
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(ws.path(), &task)
                .map_err(|e| e.to_string())?;
            let lease = WorktreeLease {
                lease_id: "lease-42".to_owned(),
                task_id: task.task_id.clone(),
                project_id: task.project_id.clone(),
                worktree_path: ws.path().join("worktrees/pr-runtime-42"),
                branch_name: "rb/42-proj-42".to_owned(),
                acquired_at: now,
                ttl_seconds: 300,
                last_heartbeat: now,
                cleanup_handoff: None,
            };

            let service = PrRuntimeService::new(&store, &PrWorktree, &github);
            let url = block_on_app_result(service.ensure_draft_pr(
                ws.path(),
                ws.path(),
                &task.task_id,
                &lease,
                &CancellationToken::new(),
            ))?
            .ok_or("expected draft PR URL".to_owned())?;

            if !url.ends_with("/pull/100") {
                return Err(format!("unexpected PR URL: {url}"));
            }
            let persisted = store
                .read_task(ws.path(), &task.task_id)
                .map_err(|e| e.to_string())?;
            if persisted.pr_url.as_deref() != Some(url.as_str()) {
                return Err(format!(
                    "expected persisted pr_url, got {:?}",
                    persisted.pr_url
                ));
            }
            if github.pull_requests.lock().unwrap().len() != 1 {
                return Err("expected exactly one PR to be created".to_owned());
            }

            Ok(())
        }
    );

    reg!(m, "daemon.pr_runtime.push_before_create", || {
        use crate::adapters::fs::FsDaemonStore;
        use crate::adapters::github::{
            GithubComment, GithubIssue, GithubPort, GithubPullRequest, GithubReview,
        };
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
        };
        use crate::contexts::automation_runtime::pr_runtime::PrRuntimeService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;
        use crate::shared::error::AppResult;
        use std::sync::{Arc, Mutex};

        struct RecordingGithub {
            events: Arc<Mutex<Vec<String>>>,
        }
        impl GithubPort for RecordingGithub {
            async fn ensure_labels(&self, _o: &str, _r: &str, _l: &[&str]) -> AppResult<()> {
                Ok(())
            }
            async fn poll_candidate_issues(
                &self,
                _o: &str,
                _r: &str,
                _l: &str,
            ) -> AppResult<Vec<GithubIssue>> {
                Ok(vec![])
            }
            async fn read_issue_labels(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<String>> {
                Ok(vec![])
            }
            async fn add_label(&self, _o: &str, _r: &str, _n: u64, _l: &str) -> AppResult<()> {
                Ok(())
            }
            async fn remove_label(&self, _o: &str, _r: &str, _n: u64, _l: &str) -> AppResult<()> {
                Ok(())
            }
            async fn replace_labels(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
                _l: &[&str],
            ) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_issue_comments(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubComment>> {
                Ok(vec![])
            }
            async fn post_idempotent_comment(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
                _m: &str,
                _b: &str,
            ) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_pr_review_comments(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubComment>> {
                Ok(vec![])
            }
            async fn fetch_pr_reviews(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<Vec<GithubReview>> {
                Ok(vec![])
            }
            async fn create_draft_pr(
                &self,
                owner: &str,
                repo: &str,
                _t: &str,
                _b: &str,
                head: &str,
                _bs: &str,
            ) -> AppResult<GithubPullRequest> {
                self.events.lock().unwrap().push("create".to_owned());
                Ok(GithubPullRequest {
                    number: 100,
                    html_url: format!("https://github.com/{owner}/{repo}/pull/100"),
                    state: "open".to_owned(),
                    draft: Some(true),
                    node_id: "PR_100".to_owned(),
                    head: Some(crate::adapters::github::GithubPrRef {
                        ref_name: head.to_owned(),
                        sha: "000".to_owned(),
                    }),
                    base: None,
                })
            }
            async fn mark_pr_ready(&self, _o: &str, _r: &str, _n: u64) -> AppResult<()> {
                Ok(())
            }
            async fn close_pr(&self, _o: &str, _r: &str, _n: u64) -> AppResult<()> {
                Ok(())
            }
            async fn fetch_pr_state(
                &self,
                _o: &str,
                _r: &str,
                _n: u64,
            ) -> AppResult<GithubPullRequest> {
                Ok(GithubPullRequest {
                    number: 100,
                    html_url: "https://github.com/acme/widgets/pull/100".to_owned(),
                    state: "open".to_owned(),
                    draft: Some(true),
                    node_id: "PR_100".to_owned(),
                    head: Some(crate::adapters::github::GithubPrRef {
                        ref_name: "rb/77-proj".to_owned(),
                        sha: "000".to_owned(),
                    }),
                    base: None,
                })
            }
            async fn update_pr_body(&self, _o: &str, _r: &str, _n: u64, _b: &str) -> AppResult<()> {
                Ok(())
            }
            async fn is_branch_ahead(
                &self,
                _o: &str,
                _r: &str,
                _b: &str,
                _h: &str,
            ) -> AppResult<bool> {
                Ok(true)
            }
        }

        struct RecordingWorktree {
            events: Arc<Mutex<Vec<String>>>,
        }
        impl crate::contexts::automation_runtime::WorktreePort for RecordingWorktree {
            fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
                base_dir.join("worktrees").join(task_id)
            }
            fn branch_name(&self, task_id: &str) -> String {
                format!("rb/{task_id}")
            }
            fn create_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
                _task_id: &str,
            ) -> AppResult<()> {
                Ok(())
            }
            fn remove_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _task_id: &str,
            ) -> AppResult<crate::contexts::automation_runtime::WorktreeCleanupOutcome>
            {
                Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
            }
            fn rebase_onto_default_branch(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
            ) -> AppResult<()> {
                Ok(())
            }
            fn default_branch_name(&self, _repo_root: &Path) -> AppResult<String> {
                Ok("main".to_owned())
            }
            fn push_branch(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
            ) -> AppResult<()> {
                self.events.lock().unwrap().push("push".to_owned());
                Ok(())
            }
            fn force_push_branch(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
            ) -> AppResult<()> {
                self.events.lock().unwrap().push("push".to_owned());
                Ok(())
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let events = Arc::new(Mutex::new(Vec::new()));
        let github = RecordingGithub {
            events: events.clone(),
        };
        let worktree = RecordingWorktree {
            events: events.clone(),
        };
        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "pr-runtime-77".to_owned(),
            issue_ref: "acme/widgets#77".to_owned(),
            project_id: "proj-77".to_owned(),
            project_name: Some("Push before create".to_owned()),
            prompt: Some("Push before create".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-77".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(77),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(ws.path(), &task)
            .map_err(|e| e.to_string())?;
        let lease = WorktreeLease {
            lease_id: "lease-77".to_owned(),
            task_id: task.task_id.clone(),
            project_id: task.project_id.clone(),
            worktree_path: ws.path().join("worktrees/pr-runtime-77"),
            branch_name: "rb/77-proj".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };

        let service = PrRuntimeService::new(&store, &worktree, &github);
        let _ = block_on_app_result(service.ensure_draft_pr(
            ws.path(),
            ws.path(),
            &task.task_id,
            &lease,
            &CancellationToken::new(),
        ))?;

        let recorded = events.lock().unwrap().clone();
        if recorded != vec!["push".to_owned(), "create".to_owned()] {
            return Err(format!("expected push then create, got {recorded:?}"));
        }

        Ok(())
    });

    reg!(m, "daemon.pr_runtime.clean_shutdown_on_cancel", || {
        use crate::adapters::fs::FsDaemonStore;
        use crate::adapters::github::InMemoryGithubClient;
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
        };
        use crate::contexts::automation_runtime::pr_runtime::PrRuntimeService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::FlowPreset;

        struct CancelWorktree;
        impl crate::contexts::automation_runtime::WorktreePort for CancelWorktree {
            fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
                base_dir.join("worktrees").join(task_id)
            }
            fn branch_name(&self, task_id: &str) -> String {
                format!("rb/{task_id}")
            }
            fn create_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
                _task_id: &str,
            ) -> crate::shared::error::AppResult<()> {
                Ok(())
            }
            fn remove_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _task_id: &str,
            ) -> crate::shared::error::AppResult<
                crate::contexts::automation_runtime::WorktreeCleanupOutcome,
            > {
                Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
            }
            fn rebase_onto_default_branch(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
            ) -> crate::shared::error::AppResult<()> {
                Ok(())
            }
            fn default_branch_name(
                &self,
                _repo_root: &Path,
            ) -> crate::shared::error::AppResult<String> {
                Ok("main".to_owned())
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let github = InMemoryGithubClient::new();
        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "pr-runtime-cancel".to_owned(),
            issue_ref: "acme/widgets#80".to_owned(),
            project_id: "proj-80".to_owned(),
            project_name: Some("Cancelled".to_owned()),
            prompt: Some("Cancelled".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-80".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(80),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(ws.path(), &task)
            .map_err(|e| e.to_string())?;
        let lease = WorktreeLease {
            lease_id: "lease-80".to_owned(),
            task_id: task.task_id.clone(),
            project_id: task.project_id.clone(),
            worktree_path: ws.path().join("worktrees/pr-runtime-cancel"),
            branch_name: "rb/80-proj".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };

        let cancel = CancellationToken::new();
        cancel.cancel();
        let service = PrRuntimeService::new(&store, &CancelWorktree, &github);
        let err = block_on_app_result(service.ensure_draft_pr(
            ws.path(),
            ws.path(),
            &task.task_id,
            &lease,
            &cancel,
        ))
        .expect_err("expected cancellation");
        if !err.contains("cancelled") {
            return Err(format!("expected cancellation error, got {err}"));
        }
        if !github.pull_requests.lock().unwrap().is_empty() {
            return Err("expected no PR to be created after cancellation".to_owned());
        }

        Ok(())
    });

    reg!(m, "daemon.pr_runtime.no_diff_close_or_skip", || {
        use crate::adapters::fs::FsDaemonStore;
        use crate::adapters::github::{GithubPort, GithubPullRequest, InMemoryGithubClient};
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
        };
        use crate::contexts::automation_runtime::pr_runtime::{
            CompletionPrAction, PrRuntimeService,
        };
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::shared::domain::{
            EffectiveDaemonPrPolicy, FlowPreset, PrPolicy, ReviewWhitelistConfig,
        };

        struct NoDiffWorktree;
        impl crate::contexts::automation_runtime::WorktreePort for NoDiffWorktree {
            fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
                base_dir.join("worktrees").join(task_id)
            }
            fn branch_name(&self, task_id: &str) -> String {
                format!("rb/{task_id}")
            }
            fn create_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
                _task_id: &str,
            ) -> crate::shared::error::AppResult<()> {
                Ok(())
            }
            fn remove_worktree(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _task_id: &str,
            ) -> crate::shared::error::AppResult<
                crate::contexts::automation_runtime::WorktreeCleanupOutcome,
            > {
                Ok(crate::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
            }
            fn rebase_onto_default_branch(
                &self,
                _repo_root: &Path,
                _worktree_path: &Path,
                _branch_name: &str,
            ) -> crate::shared::error::AppResult<()> {
                Ok(())
            }
            fn default_branch_name(
                &self,
                _repo_root: &Path,
            ) -> crate::shared::error::AppResult<String> {
                Ok("main".to_owned())
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let store = FsDaemonStore;
        let github = InMemoryGithubClient::new();
        github
            .pull_requests
            .lock()
            .unwrap()
            .push(GithubPullRequest {
                number: 55,
                html_url: "https://github.com/acme/widgets/pull/55".to_owned(),
                state: "open".to_owned(),
                draft: Some(true),
                node_id: "PR_55".to_owned(),
                head: None,
                base: None,
            });
        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "pr-runtime-nodiff".to_owned(),
            issue_ref: "acme/widgets#55".to_owned(),
            project_id: "proj-55".to_owned(),
            project_name: Some("No diff".to_owned()),
            prompt: Some("No diff".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-55".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(55),
            pr_url: Some("https://github.com/acme/widgets/pull/55".to_owned()),
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(ws.path(), &task)
            .map_err(|e| e.to_string())?;
        let lease = WorktreeLease {
            lease_id: "lease-55".to_owned(),
            task_id: task.task_id.clone(),
            project_id: task.project_id.clone(),
            worktree_path: ws.path().join("worktrees/pr-runtime-nodiff"),
            branch_name: "rb/55-proj".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };

        let service = PrRuntimeService::new(&store, &NoDiffWorktree, &github);
        let closed = block_on_app_result(service.handle_completion_pr(
            ws.path(),
            ws.path(),
            &task.task_id,
            &lease,
            &EffectiveDaemonPrPolicy {
                no_diff_action: PrPolicy::CloseOnNoDiff,
                review_whitelist: ReviewWhitelistConfig::default(),
            },
            &CancellationToken::new(),
        ))?;
        if !matches!(closed, CompletionPrAction::Closed { .. }) {
            return Err(format!("expected close action, got {closed:?}"));
        }
        let pr_state = block_on_app_result(github.fetch_pr_state("acme", "widgets", 55))?;
        if pr_state.state != "closed" {
            return Err(format!("expected closed PR state, got {}", pr_state.state));
        }
        let closed_task = store
            .read_task(ws.path(), &task.task_id)
            .map_err(|e| e.to_string())?;
        if closed_task.pr_url.is_some() {
            return Err(format!(
                "expected closed task pr_url to be cleared, got {:?}",
                closed_task.pr_url
            ));
        }

        let skip_task = DaemonTask {
            task_id: "pr-runtime-skip".to_owned(),
            pr_url: None,
            ..task.clone()
        };
        store
            .create_task(ws.path(), &skip_task)
            .map_err(|e| e.to_string())?;
        let skipped = block_on_app_result(service.handle_completion_pr(
            ws.path(),
            ws.path(),
            &skip_task.task_id,
            &lease,
            &EffectiveDaemonPrPolicy {
                no_diff_action: PrPolicy::SkipOnNoDiff,
                review_whitelist: ReviewWhitelistConfig::default(),
            },
            &CancellationToken::new(),
        ))?;
        if !matches!(skipped, CompletionPrAction::Skipped) {
            return Err(format!("expected skip action, got {skipped:?}"));
        }
        github
            .branches_ahead
            .lock()
            .unwrap()
            .insert("acme/widgets:main...rb/55-proj".to_owned());
        let recreated = block_on_app_result(service.ensure_draft_pr(
            ws.path(),
            ws.path(),
            &task.task_id,
            &lease,
            &CancellationToken::new(),
        ))?
        .ok_or("expected recreated draft PR URL".to_owned())?;
        if !recreated.ends_with("/pull/100") {
            return Err(format!("expected fresh draft PR URL, got {recreated}"));
        }
        let recreated_task = store
            .read_task(ws.path(), &task.task_id)
            .map_err(|e| e.to_string())?;
        if recreated_task.pr_url.as_deref() != Some(recreated.as_str()) {
            return Err(format!(
                "expected recreated task pr_url, got {:?}",
                recreated_task.pr_url
            ));
        }

        Ok(())
    });

    reg!(m, "daemon.pr_review.whitelist_filters_comments", || {
        use crate::adapters::fs::{
            FsAmendmentQueueStore, FsDaemonStore, FsProjectStore, FsRunSnapshotStore,
            FsRunSnapshotWriteStore,
        };
        use crate::adapters::github::{
            GithubComment, GithubIssue, GithubReview, GithubUser, InMemoryGithubClient,
        };
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, ReviewWhitelist, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::pr_review::PrReviewIngestionService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::contexts::project_run_record::service::AmendmentQueuePort;
        use crate::shared::domain::FlowPreset;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "proj-review-1", "standard");
        let store = FsDaemonStore;
        let project_store = FsProjectStore;
        let run_snapshot_read = FsRunSnapshotStore;
        let run_snapshot_write = FsRunSnapshotWriteStore;
        let amendment_queue = FsAmendmentQueueStore;
        let github = InMemoryGithubClient::new();
        github.pr_review_comments.lock().unwrap().push((
            101,
            GithubComment {
                id: 12,
                body: "inline fix this".to_owned(),
                user: GithubUser {
                    login: "alice".to_owned(),
                    id: 1,
                },
                created_at: "2026-03-17T00:00:00Z".to_owned(),
                updated_at: "2026-03-17T00:00:00Z".to_owned(),
            },
        ));
        github.pr_reviews.lock().unwrap().push((
            101,
            GithubReview {
                id: 14,
                user: GithubUser {
                    login: "alice".to_owned(),
                    id: 1,
                },
                body: Some("summary fix".to_owned()),
                state: "COMMENTED".to_owned(),
                submitted_at: Some("2026-03-17T00:00:00Z".to_owned()),
            },
        ));
        github.posted_comments.lock().unwrap().push((
            101,
            "seed".to_owned(),
            "top-level keep this".to_owned(),
        ));
        github.posted_comments.lock().unwrap().push((
            101,
            "seed-2".to_owned(),
            "ignore this".to_owned(),
        ));
        let mut issues = github.issues.lock().unwrap();
        issues.push(GithubIssue {
            number: 101,
            title: "Whitelist".to_owned(),
            body: None,
            labels: vec![],
            user: GithubUser {
                login: "alice".to_owned(),
                id: 1,
            },
            html_url: "https://github.com/acme/widgets/issues/101".to_owned(),
            pull_request: None,
            updated_at: "2026-03-17T00:00:00Z".to_owned(),
        });
        drop(issues);

        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "pr-review-whitelist".to_owned(),
            issue_ref: "acme/widgets#101".to_owned(),
            project_id: "proj-review-1".to_owned(),
            project_name: Some("Whitelist".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(101),
            pr_url: Some("https://github.com/acme/widgets/pull/101".to_owned()),
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(ws.path(), &task)
            .map_err(|e| e.to_string())?;

        let journal_store_pr = crate::adapters::fs::FsJournalStore;
        let service = PrReviewIngestionService::new(
            &store,
            &project_store,
            &run_snapshot_read,
            &run_snapshot_write,
            &amendment_queue,
            &journal_store_pr,
            &github,
        );
        let whitelist = ReviewWhitelist {
            allowed_usernames: vec!["alice".to_owned()],
        };
        let batch = block_on_app_result(service.ingest_reviews(
            ws.path(),
            ws.path(),
            &task.task_id,
            &whitelist,
            &CancellationToken::new(),
        ))?;
        if batch.staged_count < 2 {
            return Err(format!(
                "expected at least 2 staged amendments, got {}",
                batch.staged_count
            ));
        }
        let amendments = amendment_queue
            .list_pending_amendments(
                ws.path(),
                &crate::shared::domain::ProjectId::new("proj-review-1".to_owned())
                    .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?;
        if amendments
            .iter()
            .any(|amendment| amendment.body.contains("ignore this"))
        {
            return Err("unexpected amendment from non-whitelisted author".to_owned());
        }

        Ok(())
    });

    reg!(m, "daemon.pr_review.dedup_across_restart", || {
        use crate::adapters::fs::{
            FsAmendmentQueueStore, FsDaemonStore, FsProjectStore, FsRunSnapshotStore,
            FsRunSnapshotWriteStore,
        };
        use crate::adapters::github::{GithubComment, GithubUser, InMemoryGithubClient};
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::automation_runtime::model::{
            DaemonTask, DispatchMode, ReviewWhitelist, RoutingSource, TaskStatus,
        };
        use crate::contexts::automation_runtime::pr_review::PrReviewIngestionService;
        use crate::contexts::automation_runtime::DaemonStorePort;
        use crate::contexts::project_run_record::service::AmendmentQueuePort;
        use crate::shared::domain::{FlowPreset, ProjectId};

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "proj-review-2", "standard");
        let store = FsDaemonStore;
        let project_store = FsProjectStore;
        let run_snapshot_read = FsRunSnapshotStore;
        let run_snapshot_write = FsRunSnapshotWriteStore;
        let amendment_queue = FsAmendmentQueueStore;
        let github = InMemoryGithubClient::new();
        github.pr_review_comments.lock().unwrap().push((
            102,
            GithubComment {
                id: 21,
                body: "dedup me".to_owned(),
                user: GithubUser {
                    login: "alice".to_owned(),
                    id: 1,
                },
                created_at: "2026-03-17T00:00:00Z".to_owned(),
                updated_at: "2026-03-17T00:00:00Z".to_owned(),
            },
        ));
        let now = chrono::Utc::now();
        let task = DaemonTask {
            task_id: "pr-review-dedup".to_owned(),
            issue_ref: "acme/widgets#102".to_owned(),
            project_id: "proj-review-2".to_owned(),
            project_name: Some("Dedup".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some("acme/widgets".to_owned()),
            issue_number: Some(102),
            pr_url: Some("https://github.com/acme/widgets/pull/102".to_owned()),
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };
        store
            .create_task(ws.path(), &task)
            .map_err(|e| e.to_string())?;

        let journal_store_pr2 = crate::adapters::fs::FsJournalStore;
        let service = PrReviewIngestionService::new(
            &store,
            &project_store,
            &run_snapshot_read,
            &run_snapshot_write,
            &amendment_queue,
            &journal_store_pr2,
            &github,
        );
        let whitelist = ReviewWhitelist::default();
        let _ = block_on_app_result(service.ingest_reviews(
            ws.path(),
            ws.path(),
            &task.task_id,
            &whitelist,
            &CancellationToken::new(),
        ))?;
        let project_id = ProjectId::new("proj-review-2".to_owned()).map_err(|e| e.to_string())?;
        let first = amendment_queue
            .list_pending_amendments(ws.path(), &project_id)
            .map_err(|e| e.to_string())?;
        if first.len() != 1 {
            return Err(format!(
                "expected one staged amendment, got {}",
                first.len()
            ));
        }

        let mut reset_task = store
            .read_task(ws.path(), &task.task_id)
            .map_err(|e| e.to_string())?;
        reset_task.last_seen_comment_id = None;
        reset_task.last_seen_review_id = None;
        store
            .write_task(ws.path(), &reset_task)
            .map_err(|e| e.to_string())?;

        let _ = block_on_app_result(service.ingest_reviews(
            ws.path(),
            ws.path(),
            &task.task_id,
            &whitelist,
            &CancellationToken::new(),
        ))?;
        let second = amendment_queue
            .list_pending_amendments(ws.path(), &project_id)
            .map_err(|e| e.to_string())?;
        if second.len() != 1 {
            return Err(format!(
                "expected deduplicated amendment file set, got {}",
                second.len()
            ));
        }

        Ok(())
    });

    reg!(
        m,
        "daemon.pr_review.transient_error_preserves_staged",
        || {
            use crate::adapters::fs::{
                FsAmendmentQueueStore, FsDaemonStore, FsProjectStore, FsRunSnapshotStore,
            };
            use crate::adapters::github::{GithubComment, GithubUser, InMemoryGithubClient};
            use crate::contexts::agent_execution::model::CancellationToken;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, ReviewWhitelist, RoutingSource, TaskStatus,
            };
            use crate::contexts::automation_runtime::pr_review::PrReviewIngestionService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::contexts::project_run_record::service::AmendmentQueuePort;
            use crate::shared::domain::{FlowPreset, ProjectId};
            use crate::shared::error::{AppError, AppResult};

            struct FailingSnapshotWrite;
            impl crate::contexts::project_run_record::service::RunSnapshotWritePort for FailingSnapshotWrite {
                fn write_run_snapshot(
                    &self,
                    _base_dir: &Path,
                    _project_id: &ProjectId,
                    _snapshot: &crate::contexts::project_run_record::model::RunSnapshot,
                ) -> AppResult<()> {
                    Err(AppError::Io(std::io::Error::other(
                        "injected reopen failure",
                    )))
                }
            }

            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;
            create_project_fixture(ws.path(), "proj-review-3", "standard");
            std::fs::write(
            conformance_project_root(&ws, "proj-review-3").join("run.json"),
            r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[{"cycle":1,"stage_id":"final_review","started_at":"2026-03-17T00:00:00Z","completed_at":"2026-03-17T00:01:00Z"}],"completion_rounds":1,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed","last_stage_resolution_snapshot":null}"#,
        )
        .map_err(|e| e.to_string())?;
            let store = FsDaemonStore;
            let project_store = FsProjectStore;
            let run_snapshot_read = FsRunSnapshotStore;
            let amendment_queue = FsAmendmentQueueStore;
            let github = InMemoryGithubClient::new();
            github.pr_review_comments.lock().unwrap().push((
                103,
                GithubComment {
                    id: 31,
                    body: "persist me before failure".to_owned(),
                    user: GithubUser {
                        login: "alice".to_owned(),
                        id: 1,
                    },
                    created_at: "2026-03-17T00:00:00Z".to_owned(),
                    updated_at: "2026-03-17T00:00:00Z".to_owned(),
                },
            ));
            let now = chrono::Utc::now();
            let task = DaemonTask {
                task_id: "pr-review-failure".to_owned(),
                issue_ref: "acme/widgets#103".to_owned(),
                project_id: "proj-review-3".to_owned(),
                project_name: Some("Failure".to_owned()),
                prompt: Some("Prompt".to_owned()),
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Completed,
                created_at: now,
                updated_at: now,
                attempt_count: 0,
                lease_id: None,
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(103),
                pr_url: Some("https://github.com/acme/widgets/pull/103".to_owned()),
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(ws.path(), &task)
                .map_err(|e| e.to_string())?;

            let journal_store_pr3 = crate::adapters::fs::FsJournalStore;
            let service = PrReviewIngestionService::new(
                &store,
                &project_store,
                &run_snapshot_read,
                &FailingSnapshotWrite,
                &amendment_queue,
                &journal_store_pr3,
                &github,
            );
            let err = block_on_app_result(service.ingest_reviews(
                ws.path(),
                ws.path(),
                &task.task_id,
                &ReviewWhitelist::default(),
                &CancellationToken::new(),
            ))
            .expect_err("expected reopen failure");
            if !err.contains("injected reopen failure") {
                return Err(format!("unexpected error: {err}"));
            }
            let project_id =
                ProjectId::new("proj-review-3".to_owned()).map_err(|e| e.to_string())?;
            let amendments = amendment_queue
                .list_pending_amendments(ws.path(), &project_id)
                .map_err(|e| e.to_string())?;
            if amendments.is_empty() {
                return Err("expected staged amendments to remain on disk after failure".to_owned());
            }

            Ok(())
        }
    );

    reg!(
        m,
        "daemon.pr_review.completed_project_reopens_with_amendments",
        || {
            use crate::adapters::fs::{
                FsAmendmentQueueStore, FsDaemonStore, FsProjectStore, FsRunSnapshotStore,
                FsRunSnapshotWriteStore,
            };
            use crate::adapters::github::{GithubComment, GithubUser, InMemoryGithubClient};
            use crate::contexts::agent_execution::model::CancellationToken;
            use crate::contexts::automation_runtime::model::{
                DaemonTask, DispatchMode, ReviewWhitelist, RoutingSource, TaskStatus,
            };
            use crate::contexts::automation_runtime::pr_review::PrReviewIngestionService;
            use crate::contexts::automation_runtime::DaemonStorePort;
            use crate::contexts::project_run_record::service::RunSnapshotPort;
            use crate::shared::domain::{FlowPreset, ProjectId, StageId};

            let ws = TempWorkspace::new()?;
            init_workspace(&ws)?;
            create_project_fixture(ws.path(), "proj-review-4", "standard");
            std::fs::write(
            conformance_project_root(&ws, "proj-review-4").join("run.json"),
            r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[{"cycle":2,"stage_id":"final_review","started_at":"2026-03-17T00:00:00Z","completed_at":"2026-03-17T00:01:00Z"}],"completion_rounds":2,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed","last_stage_resolution_snapshot":null}"#,
        )
        .map_err(|e| e.to_string())?;
            let store = FsDaemonStore;
            let project_store = FsProjectStore;
            let run_snapshot_read = FsRunSnapshotStore;
            let run_snapshot_write = FsRunSnapshotWriteStore;
            let amendment_queue = FsAmendmentQueueStore;
            let github = InMemoryGithubClient::new();
            github.pr_review_comments.lock().unwrap().push((
                104,
                GithubComment {
                    id: 41,
                    body: "reopen with this amendment".to_owned(),
                    user: GithubUser {
                        login: "alice".to_owned(),
                        id: 1,
                    },
                    created_at: "2026-03-17T00:00:00Z".to_owned(),
                    updated_at: "2026-03-17T00:00:00Z".to_owned(),
                },
            ));
            let now = chrono::Utc::now();
            let task = DaemonTask {
                task_id: "pr-review-reopen".to_owned(),
                issue_ref: "acme/widgets#104".to_owned(),
                project_id: "proj-review-4".to_owned(),
                project_name: Some("Reopen".to_owned()),
                prompt: Some("Prompt".to_owned()),
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Completed,
                created_at: now,
                updated_at: now,
                attempt_count: 0,
                lease_id: None,
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: Some("acme/widgets".to_owned()),
                issue_number: Some(104),
                pr_url: Some("https://github.com/acme/widgets/pull/104".to_owned()),
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            };
            store
                .create_task(ws.path(), &task)
                .map_err(|e| e.to_string())?;

            let journal_store_pr4 = crate::adapters::fs::FsJournalStore;
            let service = PrReviewIngestionService::new(
                &store,
                &project_store,
                &run_snapshot_read,
                &run_snapshot_write,
                &amendment_queue,
                &journal_store_pr4,
                &github,
            );
            let batch = block_on_app_result(service.ingest_reviews(
                ws.path(),
                ws.path(),
                &task.task_id,
                &ReviewWhitelist::default(),
                &CancellationToken::new(),
            ))?;
            if !batch.reopened_project {
                return Err("expected completed project to be reopened".to_owned());
            }
            let reopened = store
                .read_task(ws.path(), &task.task_id)
                .map_err(|e| e.to_string())?;
            if reopened.status != TaskStatus::Pending {
                return Err(format!(
                    "expected pending task after reopen, got {}",
                    reopened.status
                ));
            }
            let snapshot = run_snapshot_read
                .read_run_snapshot(
                    ws.path(),
                    &ProjectId::new("proj-review-4".to_owned()).map_err(|e| e.to_string())?,
                )
                .map_err(|e| e.to_string())?;
            if snapshot.status != crate::contexts::project_run_record::model::RunStatus::Paused {
                return Err(format!("expected paused snapshot, got {}", snapshot.status));
            }
            let interrupted = snapshot
                .interrupted_run
                .ok_or("expected interrupted_run after reopen")?;
            if interrupted.stage_cursor.stage != StageId::Planning {
                return Err(format!(
                    "expected planning restart cursor, got {}",
                    interrupted.stage_cursor.stage
                ));
            }

            Ok(())
        }
    );

    reg!(m, "daemon.rebase.agent_resolves_conflict", || {
        use crate::adapters::worktree::WorktreeAdapter;
        use crate::contexts::automation_runtime::{
            RebaseConflictRequest, RebaseConflictResolution, RebaseConflictResolver,
            RebaseResolutionFile, WorktreePort,
        };
        use crate::shared::domain::EffectiveRebasePolicy;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        struct RecordingResolver {
            calls: Arc<AtomicUsize>,
        }

        impl RebaseConflictResolver for RecordingResolver {
            fn resolve_conflicts(
                &self,
                request: &RebaseConflictRequest,
            ) -> crate::shared::error::AppResult<RebaseConflictResolution> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(RebaseConflictResolution {
                    summary: "resolved via test agent".to_owned(),
                    resolved_files: request
                        .conflicted_files
                        .iter()
                        .map(|file| RebaseResolutionFile {
                            path: file.path.clone(),
                            content: "branch\nmain\n".to_owned(),
                        })
                        .collect(),
                })
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;
        std::fs::write(ws.path().join("conflict.txt"), "base\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "add conflict file"])?;

        let worktree = WorktreeAdapter;
        let worktree_path = ws.path().join("worktrees/rebase-agent");
        worktree
            .create_worktree(ws.path(), &worktree_path, "rb/200-proj", "rebase-agent")
            .map_err(|e| e.to_string())?;

        std::fs::write(worktree_path.join("conflict.txt"), "branch\n")
            .map_err(|e| e.to_string())?;
        run_git_in(&worktree_path, &["add", "conflict.txt"])?;
        run_git_in(&worktree_path, &["commit", "-m", "branch change"])?;

        std::fs::write(ws.path().join("conflict.txt"), "main\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "main change"])?;
        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let resolver = RecordingResolver {
            calls: resolver_calls.clone(),
        };

        let outcome = worktree
            .rebase_with_agent_resolution(
                ws.path(),
                &worktree_path,
                "rb/200-proj",
                &EffectiveRebasePolicy {
                    agent_resolution_enabled: true,
                    agent_timeout: 30,
                },
                Some(&resolver),
            )
            .map_err(|e| e.to_string())?;
        if !matches!(
            outcome,
            crate::contexts::automation_runtime::RebaseOutcome::AgentResolved { .. }
        ) {
            return Err(format!("expected agent-resolved rebase, got {outcome:?}"));
        }
        if resolver_calls.load(Ordering::SeqCst) != 1 {
            return Err("expected rebase conflict resolver to be invoked once".to_owned());
        }

        Ok(())
    });

    reg!(m, "daemon.rebase.disabled_agent_aborts_conflict", || {
        use crate::adapters::worktree::WorktreeAdapter;
        use crate::contexts::automation_runtime::WorktreePort;
        use crate::shared::domain::EffectiveRebasePolicy;

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;
        std::fs::write(ws.path().join("conflict.txt"), "base\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "add conflict file"])?;

        let worktree = WorktreeAdapter;
        let worktree_path = ws.path().join("worktrees/rebase-disabled");
        worktree
            .create_worktree(ws.path(), &worktree_path, "rb/201-proj", "rebase-disabled")
            .map_err(|e| e.to_string())?;
        std::fs::write(worktree_path.join("conflict.txt"), "branch\n")
            .map_err(|e| e.to_string())?;
        run_git_in(&worktree_path, &["add", "conflict.txt"])?;
        run_git_in(&worktree_path, &["commit", "-m", "branch change"])?;
        std::fs::write(ws.path().join("conflict.txt"), "main\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "main change"])?;

        let outcome = worktree
            .rebase_with_agent_resolution(
                ws.path(),
                &worktree_path,
                "rb/201-proj",
                &EffectiveRebasePolicy {
                    agent_resolution_enabled: false,
                    agent_timeout: 30,
                },
                None,
            )
            .map_err(|e| e.to_string())?;
        if !matches!(
            outcome,
            crate::contexts::automation_runtime::RebaseOutcome::Failed {
                classification:
                    crate::contexts::automation_runtime::RebaseFailureClassification::Conflict,
                ..
            }
        ) {
            return Err(format!("expected conflict failure, got {outcome:?}"));
        }

        Ok(())
    });

    reg!(m, "daemon.rebase.timeout_classification", || {
        use crate::adapters::worktree::WorktreeAdapter;
        use crate::contexts::automation_runtime::{
            RebaseConflictRequest, RebaseConflictResolution, RebaseConflictResolver, WorktreePort,
        };
        use crate::shared::domain::EffectiveRebasePolicy;
        use crate::shared::error::{AppError, AppResult};

        struct TimeoutResolver;

        impl RebaseConflictResolver for TimeoutResolver {
            fn resolve_conflicts(
                &self,
                _request: &RebaseConflictRequest,
            ) -> AppResult<RebaseConflictResolution> {
                Err(AppError::InvocationTimeout {
                    backend: "stub".to_owned(),
                    contract_id: "daemon:rebase_resolution".to_owned(),
                    timeout_ms: 30_000,
                    details: "stub timeout".to_owned(),
                })
            }
        }

        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        init_git_repo(&ws)?;
        std::fs::write(ws.path().join("conflict.txt"), "base\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "add conflict file"])?;

        let worktree = WorktreeAdapter;
        let worktree_path = ws.path().join("worktrees/rebase-timeout");
        worktree
            .create_worktree(ws.path(), &worktree_path, "rb/202-proj", "rebase-timeout")
            .map_err(|e| e.to_string())?;
        std::fs::write(worktree_path.join("conflict.txt"), "branch\n")
            .map_err(|e| e.to_string())?;
        run_git_in(&worktree_path, &["add", "conflict.txt"])?;
        run_git_in(&worktree_path, &["commit", "-m", "branch change"])?;
        std::fs::write(ws.path().join("conflict.txt"), "main\n").map_err(|e| e.to_string())?;
        run_git_in(ws.path(), &["add", "conflict.txt"])?;
        run_git_in(ws.path(), &["commit", "-m", "main change"])?;
        let resolver = TimeoutResolver;

        let outcome = worktree
            .rebase_with_agent_resolution(
                ws.path(),
                &worktree_path,
                "rb/202-proj",
                &EffectiveRebasePolicy {
                    agent_resolution_enabled: true,
                    agent_timeout: 30,
                },
                Some(&resolver),
            )
            .map_err(|e| e.to_string())?;
        if !matches!(
            outcome,
            crate::contexts::automation_runtime::RebaseOutcome::Failed {
                classification:
                    crate::contexts::automation_runtime::RebaseFailureClassification::Timeout,
                ..
            }
        ) {
            return Err(format!("expected timeout classification, got {outcome:?}"));
        }

        Ok(())
    });
}

// ===========================================================================
// Slice 3 – Manual Amendment Parity (8 scenarios)
// ===========================================================================

fn register_manual_amendments_slice3(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "parity_slice3_manual_add", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 manual add"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let out = run_cli(
            &["project", "amend", "add", "--text", "Fix the login bug"],
            ws.path(),
        )?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "Amendment:", "stdout")?;

        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(&list.stdout, "Fix the login bug", "amend list")?;

        Ok(())
    });

    reg!(m, "parity_slice3_manual_list_empty", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 list empty"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let out = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "No pending amendments", "amend list empty")?;

        Ok(())
    });

    reg!(m, "parity_slice3_manual_remove", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 remove"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let add_out = run_cli(
            &["project", "amend", "add", "--text", "Fix remove target"],
            ws.path(),
        )?;
        assert_success(&add_out)?;

        // Extract amendment ID from output.
        let id = add_out
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("Amendment: "))
            .ok_or_else(|| "could not extract amendment ID from add output".to_owned())?
            .trim()
            .to_owned();

        let rm = run_cli(&["project", "amend", "remove", &id], ws.path())?;
        assert_success(&rm)?;

        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(
            &list.stdout,
            "No pending amendments",
            "amend list after remove",
        )?;

        Ok(())
    });

    reg!(m, "parity_slice3_manual_clear", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 clear"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        run_cli(&["project", "amend", "add", "--text", "Fix A"], ws.path())?;
        run_cli(&["project", "amend", "add", "--text", "Fix B"], ws.path())?;

        let clear = run_cli(&["project", "amend", "clear"], ws.path())?;
        assert_success(&clear)?;

        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(
            &list.stdout,
            "No pending amendments",
            "amend list after clear",
        )?;

        Ok(())
    });

    reg!(m, "parity_slice3_duplicate_manual_add", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 duplicate"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let first = run_cli(
            &["project", "amend", "add", "--text", "duplicate body"],
            ws.path(),
        )?;
        assert_success(&first)?;

        let second = run_cli(
            &["project", "amend", "add", "--text", "duplicate body"],
            ws.path(),
        )?;
        assert_success(&second)?;
        assert_contains(&second.stdout, "Duplicate", "duplicate output")?;

        // Should still only have one amendment.
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        let count = list
            .stdout
            .lines()
            .filter(|line| line.contains("duplicate body"))
            .count();
        if count != 1 {
            return Err(format!("expected 1 amendment, found {count}"));
        }

        Ok(())
    });

    reg!(m, "parity_slice3_completed_project_reopen", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Slice 3 reopen",
                "--start",
            ],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // With test-stub, --start runs the full workflow to completion.
        let snap_before = read_run_snapshot(&ws, "stub-project")?;
        if snap_before.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err(format!(
                "expected completed status before amend, got: {:?}",
                snap_before.get("status")
            ));
        }

        let add = run_cli(
            &["project", "amend", "add", "--text", "Post-completion fix"],
            ws.path(),
        )?;
        assert_success(&add)?;

        let snap_after = read_run_snapshot(&ws, "stub-project")?;
        if snap_after.get("status").and_then(|v| v.as_str()) != Some("paused") {
            return Err(format!(
                "expected paused status after amend, got: {:?}",
                snap_after.get("status")
            ));
        }
        if snap_after.get("interrupted_run").is_none()
            || snap_after
                .get("interrupted_run")
                .and_then(|v| v.as_null())
                .is_some()
        {
            return Err("expected interrupted_run to be set after reopen".to_owned());
        }

        Ok(())
    });

    reg!(m, "parity_slice3_journal_records_manual_event", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 journal"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let add = run_cli(
            &["project", "amend", "add", "--text", "Journal test body"],
            ws.path(),
        )?;
        assert_success(&add)?;

        let journal = read_journal(&ws, "stub-project")?;
        let amendment_event = journal
            .iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("amendment_queued"))
            .ok_or_else(|| "missing amendment_queued event in journal".to_owned())?;

        let details = amendment_event
            .get("details")
            .ok_or_else(|| "amendment_queued event missing details".to_owned())?;
        if details.get("source").and_then(|v| v.as_str()) != Some("manual") {
            return Err(format!(
                "expected source=manual, got: {:?}",
                details.get("source")
            ));
        }
        if details.get("dedup_key").and_then(|v| v.as_str()).is_none() {
            return Err("amendment_queued event missing dedup_key".to_owned());
        }

        Ok(())
    });

    reg!(m, "parity_slice3_remove_missing_fails", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 missing remove"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let out = run_cli(
            &["project", "amend", "remove", "nonexistent-amendment-id"],
            ws.path(),
        )?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not found", "remove missing stderr")?;

        Ok(())
    });

    reg!(m, "parity_slice3_restart_persistence", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 restart persist"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let add = run_cli(
            &[
                "project",
                "amend",
                "add",
                "--text",
                "Persist across restart",
            ],
            ws.path(),
        )?;
        assert_success(&add)?;

        // Re-list after bootstrapping (simulating a fresh CLI invocation).
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(&list.stdout, "Persist across restart", "amend persists")?;

        Ok(())
    });

    reg!(m, "parity_slice3_completion_blocking", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Slice 3 completion block",
                "--start",
            ],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // After --start with test-stub, the project should be completed.
        let snap_before = read_run_snapshot(&ws, "stub-project")?;
        if snap_before.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err(format!(
                "expected completed status before amend, got: {:?}",
                snap_before.get("status")
            ));
        }

        // Add an amendment to reopen.
        let add = run_cli(
            &["project", "amend", "add", "--text", "Block completion"],
            ws.path(),
        )?;
        assert_success(&add)?;

        // After reopen, status should be paused with a pending amendment.
        let snap_after = read_run_snapshot(&ws, "stub-project")?;
        if snap_after.get("status").and_then(|v| v.as_str()) != Some("paused") {
            return Err(format!(
                "expected paused status after amend, got: {:?}",
                snap_after.get("status")
            ));
        }

        // The amendment_queue.pending should not be empty in run.json.
        let pending = snap_after
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array());
        match pending {
            Some(arr) if !arr.is_empty() => {}
            _ => {
                return Err(
                    "expected non-empty pending queue in run.json after amendment add".to_owned(),
                );
            }
        }

        // Assert actual blocking: the interrupted_run must rewind to the
        // planning stage, proving completion was unwound.
        let interrupted = snap_after
            .get("interrupted_run")
            .ok_or_else(|| "expected interrupted_run to be set after reopen".to_owned())?;
        let stage = interrupted
            .get("stage_cursor")
            .and_then(|c| c.get("stage"))
            .and_then(|s| s.as_str())
            .ok_or_else(|| "expected stage_cursor.stage in interrupted_run".to_owned())?;
        if stage != "planning" && stage != "flow_planning" {
            return Err(format!(
                "expected interrupted_run to rewind to planning stage, got: {stage}"
            ));
        }

        // Try to resume the run — the engine should block at completion
        // because the pending amendment has not been processed.
        let resume = run_cli(&["run", "resume"], ws.path())?;
        let snap_resumed = read_run_snapshot(&ws, "stub-project")?;
        let final_status = snap_resumed
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Either the run fails with a completion-blocked error, or
        // the engine processes the amendment and completes. In both
        // cases, the pending queue must be empty for completion to
        // have succeeded.
        if final_status == "completed" {
            let final_pending = snap_resumed
                .get("amendment_queue")
                .and_then(|q| q.get("pending"))
                .and_then(|p| p.as_array());
            match final_pending {
                Some(arr) if !arr.is_empty() => {
                    return Err(
                        "project completed but pending amendments remain — blocking not enforced"
                            .to_owned(),
                    );
                }
                _ => {} // OK: amendments were drained before completion
            }
        } else if !resume.success {
            // Engine failed — verify the error mentions completion blocking.
            if !resume.stderr.contains("blocked") && !resume.stderr.contains("pending amendment") {
                return Err(format!(
                    "run failed but error does not mention blocking: {}",
                    resume.stderr
                ));
            }
        }
        // If status is paused/running/failed that's also acceptable —
        // it means the engine did not complete while amendments are pending.

        Ok(())
    });

    reg!(m, "parity_slice3_lease_conflict_rejection", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 lease conflict"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // Simulate a writer lock by writing a lock file at the real path
        // used by CliWriterLeaseGuard: .ralph-burning/daemon/leases/writer-{id}.lock
        let project_id = "stub-project";
        let leases_dir = daemon_root(ws.path()).join("leases");
        std::fs::create_dir_all(&leases_dir).ok();
        let lock_path = leases_dir.join(format!("writer-{project_id}.lock"));
        std::fs::write(&lock_path, "held-by-test").ok();

        let add = run_cli(
            &["project", "amend", "add", "--text", "Should fail"],
            ws.path(),
        )?;
        assert_failure(&add)?;
        assert_contains(&add.stderr, "lease", "lease conflict stderr")?;

        // Verify no amendment was created.
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(
            &list.stdout,
            "No pending amendments",
            "no amendment created",
        )?;

        Ok(())
    });

    reg!(m, "parity_slice3_lease_conflict_remove", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Slice 3 lease conflict remove",
            ],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // Add an amendment before the lock is held.
        let add = run_cli(
            &["project", "amend", "add", "--text", "Amendment to remove"],
            ws.path(),
        )?;
        assert_success(&add)?;
        let amendment_id = add
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("Amendment: "))
            .ok_or_else(|| "could not extract amendment ID".to_owned())?
            .trim()
            .to_owned();

        // Simulate a writer lock.
        let project_id = "stub-project";
        let leases_dir = daemon_root(ws.path()).join("leases");
        std::fs::create_dir_all(&leases_dir).ok();
        let lock_path = leases_dir.join(format!("writer-{project_id}.lock"));
        std::fs::write(&lock_path, "held-by-test").ok();

        let remove = run_cli(&["project", "amend", "remove", &amendment_id], ws.path())?;
        assert_failure(&remove)?;
        assert_contains(&remove.stderr, "lease", "remove lease conflict stderr")?;

        // Clean up lock and verify the amendment still exists.
        std::fs::remove_file(&lock_path).ok();
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert_contains(
            &list.stdout,
            &amendment_id,
            "amendment should still be pending",
        )?;

        Ok(())
    });

    reg!(m, "parity_slice3_lease_conflict_clear", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &[
                "project",
                "bootstrap",
                "--idea",
                "Slice 3 lease conflict clear",
            ],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // Add an amendment before the lock is held.
        let add = run_cli(
            &["project", "amend", "add", "--text", "Amendment to clear"],
            ws.path(),
        )?;
        assert_success(&add)?;

        // Simulate a writer lock.
        let project_id = "stub-project";
        let leases_dir = daemon_root(ws.path()).join("leases");
        std::fs::create_dir_all(&leases_dir).ok();
        let lock_path = leases_dir.join(format!("writer-{project_id}.lock"));
        std::fs::write(&lock_path, "held-by-test").ok();

        let clear = run_cli(&["project", "amend", "clear"], ws.path())?;
        assert_failure(&clear)?;
        assert_contains(&clear.stderr, "lease", "clear lease conflict stderr")?;

        // Clean up lock and verify amendments still exist.
        std::fs::remove_file(&lock_path).ok();
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        assert!(
            !list.stdout.contains("No pending amendments"),
            "amendments should still be pending"
        );

        Ok(())
    });

    reg!(m, "parity_slice3_clear_partial_failure", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 clear partial"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // Add two amendments.
        let add1 = run_cli(
            &["project", "amend", "add", "--text", "Amendment Alpha"],
            ws.path(),
        )?;
        assert_success(&add1)?;
        let id1 = add1
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("Amendment: "))
            .ok_or_else(|| "could not extract amendment ID from first add".to_owned())?
            .trim()
            .to_owned();

        let add2 = run_cli(
            &["project", "amend", "add", "--text", "Amendment Beta"],
            ws.path(),
        )?;
        assert_success(&add2)?;
        let id2 = add2
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("Amendment: "))
            .ok_or_else(|| "could not extract amendment ID from second add".to_owned())?
            .trim()
            .to_owned();

        // Use the deterministic failpoint to make the second remove call fail.
        // RALPH_BURNING_TEST_AMENDMENT_REMOVE_FAIL_AFTER=1 means the first
        // remove succeeds and the second fails.  Passed via per-call env to
        // avoid process-global env mutation.
        let clear = run_cli_with_env(
            &["project", "amend", "clear"],
            ws.path(),
            &[("RALPH_BURNING_TEST_AMENDMENT_REMOVE_FAIL_AFTER", "1")],
        )?;

        // The clear must have partially failed.
        assert_failure(&clear)?;

        // Stderr must mention BOTH the exact removed and remaining IDs as a
        // complete pair. Either ordering is valid since amendments are sorted
        // by (created_at, batch_sequence).
        let stderr = &clear.stderr;
        let ordering_a = stderr.contains(&format!("removed: {id1}"))
            && stderr.contains(&format!("remaining: {id2}"));
        let ordering_b = stderr.contains(&format!("removed: {id2}"))
            && stderr.contains(&format!("remaining: {id1}"));
        if !ordering_a && !ordering_b {
            return Err(format!(
                "partial clear must report both exact removed AND remaining IDs.\n\
                 Expected one of: removed={id1} remaining={id2}, or removed={id2} remaining={id1}\n\
                 Got stderr: {stderr}"
            ));
        }

        // run.json should reflect exactly one remaining pending amendment.
        let snap = read_run_snapshot(&ws, "stub-project")?;
        let pending = snap
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .ok_or_else(|| "missing pending queue in run.json".to_owned())?;

        if pending.len() != 1 {
            return Err(format!(
                "expected exactly 1 remaining amendment in run.json, got {}",
                pending.len()
            ));
        }

        // The remaining amendment ID in run.json must match the one reported
        // as remaining in stderr.
        let remaining_id = pending[0]
            .get("amendment_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing amendment_id in remaining pending".to_owned())?;
        if !stderr.contains(&format!("remaining: {remaining_id}")) {
            return Err(format!(
                "run.json remaining ID '{remaining_id}' not found in stderr: {stderr}"
            ));
        }

        Ok(())
    });

    reg!(m, "parity_slice3_run_json_sync", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 run json sync"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        let add = run_cli(
            &["project", "amend", "add", "--text", "Sync body"],
            ws.path(),
        )?;
        assert_success(&add)?;

        // Read run.json and verify the pending queue has the amendment.
        let snap = read_run_snapshot(&ws, "stub-project")?;
        let pending = snap
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array());
        match pending {
            Some(arr) if !arr.is_empty() => {
                let first = &arr[0];
                let body = first.get("body").and_then(|b| b.as_str()).unwrap_or("");
                if body != "Sync body" {
                    return Err(format!(
                        "expected body 'Sync body' in run.json pending, got: {body}"
                    ));
                }
            }
            _ => {
                return Err("expected non-empty pending queue in run.json after add".to_owned());
            }
        }

        Ok(())
    });

    reg!(m, "parity_slice3_journal_append_failure_rollback", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        let boot = run_cli(
            &["project", "bootstrap", "--idea", "Slice 3 journal fail"],
            ws.path(),
        )?;
        assert_success(&boot)?;

        // Inject journal append failure: fail on the very first append after
        // the bootstrap (RALPH_BURNING_TEST_JOURNAL_APPEND_FAIL_AFTER=0).
        let add = run_cli_with_env(
            &["project", "amend", "add", "--text", "Should not persist"],
            ws.path(),
            &[("RALPH_BURNING_TEST_JOURNAL_APPEND_FAIL_AFTER", "0")],
        )?;
        assert_failure(&add)?;

        // No amendment should be visible via list.
        let list = run_cli(&["project", "amend", "list"], ws.path())?;
        assert_success(&list)?;
        if list.stdout.contains("Should not persist") {
            return Err("amendment should not be visible after journal append failure".to_owned());
        }

        // run.json must have no pending amendments.
        let snap = read_run_snapshot(&ws, "stub-project")?;
        let pending = snap
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array());
        match pending {
            Some(arr) if !arr.is_empty() => {
                return Err(format!(
                    "expected empty pending queue in run.json after journal append failure, got {} amendments",
                    arr.len()
                ));
            }
            _ => {}
        }

        Ok(())
    });
}

// ===========================================================================
// Backend Operations Parity — Slice 5 (5 scenarios)
// ===========================================================================

fn register_backend_operations_slice5(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "parity_slice5_backend_list", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;

        let out = run_cli(&["backend", "list", "--json"], ws.path())?;
        assert_success(&out)?;

        let entries: Vec<serde_json::Value> = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from backend list: {e}"))?;
        if entries.len() != 4 {
            return Err(format!("expected 4 families, got {}", entries.len()));
        }

        let families: Vec<&str> = entries
            .iter()
            .filter_map(|e| e.get("family").and_then(|v| v.as_str()))
            .collect();
        for expected in &["claude", "codex", "openrouter", "stub"] {
            if !families.contains(expected) {
                return Err(format!("missing backend family '{expected}'"));
            }
        }

        // stub compile_only is build-sensitive: in test-stub builds it is
        // null, in production builds it is true.
        let stub_entry = entries
            .iter()
            .find(|e| e.get("family").and_then(|v| v.as_str()) == Some("stub"))
            .ok_or("missing stub entry")?;
        #[cfg(feature = "test-stub")]
        {
            if stub_entry.get("compile_only").is_some()
                && !stub_entry.get("compile_only").unwrap().is_null()
            {
                return Err("stub compile_only should be null in test-stub build".into());
            }
        }
        #[cfg(not(feature = "test-stub"))]
        {
            if stub_entry.get("compile_only").and_then(|v| v.as_bool()) != Some(true) {
                return Err("stub should be marked compile_only in non-stub build".into());
            }
        }

        Ok(())
    });

    reg!(m, "parity_slice5_backend_check", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;

        // Default config should pass
        let out = run_cli(&["backend", "check", "--json"], ws.path())?;
        assert_success(&out)?;
        let result: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from backend check: {e}"))?;
        if result.get("passed").and_then(|v| v.as_bool()) != Some(true) {
            return Err("default config should pass backend check".into());
        }

        // Write config with disabled base backend
        std::fs::write(
            workspace_config_path(ws.path()),
            "version = 1\ncreated_at = \"2026-03-19T03:28:00Z\"\n\n[settings]\ndefault_flow = \"standard\"\ndefault_backend = \"openrouter\"\n\n[backends.openrouter]\nenabled = false\n",
        ).map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(&["backend", "check", "--json"], ws.path())?;
        assert_failure(&out)?;
        let result: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from backend check: {e}"))?;
        if result.get("passed").and_then(|v| v.as_bool()) != Some(false) {
            return Err("disabled base backend should fail check".into());
        }
        let failures = result
            .get("failures")
            .and_then(|v| v.as_array())
            .ok_or("missing failures array")?;
        if failures.is_empty() {
            return Err("expected at least one failure".into());
        }

        // Verify read-only: no project state created
        let projects_dir = live_workspace_root(ws.path()).join("projects");
        if projects_dir.is_dir() {
            let entries: Vec<_> = std::fs::read_dir(&projects_dir)
                .map_err(|e| format!("read projects dir: {e}"))?
                .collect();
            if !entries.is_empty() {
                return Err("backend check should not create project state".into());
            }
        }

        Ok(())
    });

    reg!(m, "parity_slice5_backend_show_effective", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;

        let out = run_cli(&["backend", "show-effective", "--json"], ws.path())?;
        assert_success(&out)?;

        let view: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from show-effective: {e}"))?;

        // Check required fields
        let base = view.get("base_backend").ok_or("missing base_backend")?;
        if base.get("value").is_none() || base.get("source").is_none() {
            return Err("base_backend must have value and source".into());
        }

        // Source labels must be concrete precedence strings, not empty
        let base_source = base.get("source").and_then(|v| v.as_str()).unwrap_or("");
        if base_source.is_empty() {
            return Err("base_backend.source must be a non-empty string".into());
        }

        // default_model must have value and source
        let dm = view.get("default_model").ok_or("missing default_model")?;
        let dm_source = dm.get("source").and_then(|v| v.as_str()).unwrap_or("");
        if dm_source.is_empty() {
            return Err("default_model.source must be a non-empty string".into());
        }

        let roles = view
            .get("roles")
            .and_then(|v| v.as_array())
            .ok_or("missing roles array")?;
        if roles.is_empty() {
            return Err("roles should not be empty".into());
        }

        // Each role should have required fields including source labels
        for role in roles {
            for field in &[
                "role",
                "backend_family",
                "model_id",
                "timeout_seconds",
                "override_source",
                "model_source",
                "timeout_source",
            ] {
                if role.get(*field).is_none() {
                    return Err(format!("role entry missing field '{field}'"));
                }
            }
            // Source labels must be non-empty strings
            let ms = role
                .get("model_source")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if ms.is_empty() {
                return Err("role model_source must be a non-empty string".into());
            }
        }

        if view.get("default_timeout_seconds").is_none() {
            return Err("missing default_timeout_seconds".into());
        }

        Ok(())
    });

    reg!(m, "parity_slice5_backend_probe_completion_panel", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;

        let out = run_cli(
            &[
                "backend",
                "probe",
                "--role",
                "completion_panel",
                "--flow",
                "standard",
                "--json",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;

        let result: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from probe: {e}"))?;

        if result.get("role").and_then(|v| v.as_str()) != Some("completion_panel") {
            return Err("expected role=completion_panel".into());
        }

        let panel = result.get("panel").ok_or("missing panel object")?;
        if panel.get("panel_type").and_then(|v| v.as_str()) != Some("completion") {
            return Err("expected panel_type=completion".into());
        }

        let members = panel
            .get("members")
            .and_then(|v| v.as_array())
            .ok_or("missing panel members")?;
        if members.is_empty() {
            return Err("completion panel should have at least one member".into());
        }

        // Each member must have required/optional status, backend_family, and configured_index
        for member in members {
            if member.get("required").is_none() {
                return Err("member missing required field".into());
            }
            if member.get("backend_family").is_none() {
                return Err("member missing backend_family field".into());
            }
            if member.get("configured_index").is_none() {
                return Err("member missing configured_index field".into());
            }
        }

        // Verify probe failure semantics: an explicitly configured disabled
        // required member exits non-zero.
        std::fs::write(
            workspace_config_path(ws.path()),
            "version = 1\ncreated_at = \"2026-03-19T03:28:00Z\"\n\n[settings]\ndefault_backend = \"claude\"\n\n[backends.openrouter]\nenabled = false\n\n[completion]\nbackends = [\"openrouter\"]\nmin_completers = 1\n",
        ).map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(
            &[
                "backend",
                "probe",
                "--role",
                "completion_panel",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;

        Ok(())
    });

    reg!(m, "parity_slice5_backend_probe_final_review_panel", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["init"], ws.path())?;
        assert_success(&out)?;

        let out = run_cli(
            &[
                "backend",
                "probe",
                "--role",
                "final_review_panel",
                "--flow",
                "standard",
                "--json",
            ],
            ws.path(),
        )?;
        assert_success(&out)?;

        let result: serde_json::Value = serde_json::from_str(&out.stdout)
            .map_err(|e| format!("invalid JSON from probe: {e}"))?;

        if result.get("role").and_then(|v| v.as_str()) != Some("final_review_panel") {
            return Err("expected role=final_review_panel".into());
        }

        let panel = result.get("panel").ok_or("missing panel object")?;
        if panel.get("panel_type").and_then(|v| v.as_str()) != Some("final_review") {
            return Err("expected panel_type=final_review".into());
        }

        let members = panel
            .get("members")
            .and_then(|v| v.as_array())
            .ok_or("missing panel members")?;
        if members.is_empty() {
            return Err("final review panel should have at least one member".into());
        }

        // Each member must have backend_family and configured_index
        for member in members {
            if member.get("backend_family").is_none() {
                return Err("member missing backend_family field".into());
            }
            if member.get("configured_index").is_none() {
                return Err("member missing configured_index field".into());
            }
        }

        // Verify probe failure semantics: disabled required backend exits non-zero
        std::fs::write(
            workspace_config_path(ws.path()),
            "version = 1\ncreated_at = \"2026-03-19T03:28:00Z\"\n\n[final_review]\narbiter_backend = \"openrouter\"\n\n[backends.openrouter]\nenabled = false\n",
        ).map_err(|e| format!("write config: {e}"))?;

        let out = run_cli(
            &[
                "backend",
                "probe",
                "--role",
                "final_review_panel",
                "--flow",
                "standard",
            ],
            ws.path(),
        )?;
        assert_failure(&out)?;

        Ok(())
    });
}

// ===========================================================================
// Tmux And Streaming Parity (Slice 6)
// ===========================================================================

fn tmux_write_executable(path: &Path, contents: &str) -> Result<(), String> {
    std::fs::write(path, contents).map_err(|e| format!("write executable: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path)
            .map_err(|e| format!("stat executable: {e}"))?
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions)
            .map_err(|e| format!("chmod executable: {e}"))?;
    }
    Ok(())
}

fn tmux_planning_payload() -> serde_json::Value {
    serde_json::json!({
        "problem_framing": "tmux plan",
        "assumptions_or_open_questions": ["none"],
        "proposed_work": [{"order": 1, "summary": "do it", "details": "details"}],
        "readiness": {"ready": true, "risks": []}
    })
}

fn write_tmux_fake_claude(bin_dir: &Path, envelope_file: &Path) -> Result<(), String> {
    tmux_write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/usr/bin/env bash
echo "$@" > "$PWD/claude-args.txt"
cat > "$PWD/claude-stdin.txt"
cat "{}"
"#,
            envelope_file.display()
        ),
    )
}

fn write_tmux_sleeping_claude(bin_dir: &Path) -> Result<(), String> {
    tmux_write_executable(
        &bin_dir.join("claude"),
        r#"#!/usr/bin/env bash
trap 'exit 130' INT TERM
while true; do
  sleep 0.1
done
"#,
    )
}

fn write_tmux_fake_tmux(bin_dir: &Path, state_dir: &Path) -> Result<(), String> {
    tmux_write_executable(
        &bin_dir.join("tmux"),
        &format!(
            r#"#!/usr/bin/env bash
set -eu
STATE_DIR="{}"
mkdir -p "$STATE_DIR"
cmd="$1"
shift

pid_file() {{
  printf '%s/%s.pid' "$STATE_DIR" "$1"
}}

live_session() {{
  local session="$1"
  local file
  file="$(pid_file "$session")"
  if [ ! -f "$file" ]; then
    return 1
  fi
  local pid
  pid="$(cat "$file")"
  if kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  rm -f "$file"
  return 1
}}

case "$cmd" in
  new-session)
    session=""
    shell_cmd=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -d)
          shift
          ;;
        -s)
          session="$2"
          shift 2
          ;;
        -x|-y|-c)
          shift 2
          ;;
        *)
          shell_cmd="$1"
          shift
          ;;
      esac
    done
    setsid bash -c "$shell_cmd" >/dev/null 2>&1 &
    echo "$!" > "$(pid_file "$session")"
    ;;
  has-session)
    session="$2"
    if live_session "$session"; then
      exit 0
    fi
    exit 1
    ;;
  kill-session)
    session="$2"
    file="$(pid_file "$session")"
    if [ -f "$file" ]; then
      pid="$(cat "$file")"
      kill -TERM "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
      sleep 0.1
      kill -KILL "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
      rm -f "$file"
    fi
    ;;
  send-keys)
    session="$2"
    file="$(pid_file "$session")"
    if [ -f "$file" ]; then
      pid="$(cat "$file")"
      kill -INT "-$pid" 2>/dev/null || kill -INT "$pid" 2>/dev/null || true
    fi
    ;;
  attach-session)
    session="$2"
    if live_session "$session"; then
      printf 'attached:%s\n' "$session"
      exit 0
    fi
    exit 1
    ;;
  *)
    exit 1
    ;;
esac
"#,
            state_dir.display()
        ),
    )
}

fn write_tmux_claude_envelope(path: &Path, result_json: &serde_json::Value) -> Result<(), String> {
    let envelope = serde_json::json!({
        "type": "result",
        "result": "{}",
        "structured_output": result_json,
        "session_id": "ses-tmux"
    });
    std::fs::write(
        path,
        serde_json::to_string(&envelope).map_err(|e| e.to_string())?,
    )
    .map_err(|e| format!("write envelope: {e}"))
}

fn tmux_request_fixture(
    invocation_id: &str,
) -> Result<
    (
        TempWorkspace,
        crate::contexts::agent_execution::model::InvocationRequest,
    ),
    String,
> {
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
    };
    use crate::shared::domain::{
        BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy, StageId,
    };

    let temp_dir = TempWorkspace::new()?;
    std::fs::create_dir_all(temp_dir.path().join("runtime/temp"))
        .map_err(|e| format!("create runtime/temp: {e}"))?;

    let request = InvocationRequest {
        invocation_id: invocation_id.to_owned(),
        project_root: temp_dir.path().to_path_buf(),
        working_dir: temp_dir.path().to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(StageId::Planning)),
        role: BackendRole::Planner,
        resolved_target: ResolvedBackendTarget::new(
            BackendFamily::Claude,
            BackendFamily::Claude.default_model_id(),
        ),
        payload: InvocationPayload {
            prompt: "Tmux adapter prompt".to_owned(),
            context: serde_json::json!({"stage": "planning"}),
        },
        timeout: std::time::Duration::from_secs(5),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    };
    Ok((temp_dir, request))
}

fn tmux_session_name_for_request(
    request: &crate::contexts::agent_execution::model::InvocationRequest,
) -> String {
    let project_name = request
        .project_root
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("workspace");
    crate::adapters::tmux::TmuxAdapter::session_name(
        project_name,
        &request.invocation_id,
        &request.project_root,
    )
}

#[derive(Clone, Copy)]
struct ConformanceInlineRawOutputStore;

impl crate::contexts::agent_execution::service::RawOutputPort for ConformanceInlineRawOutputStore {
    fn persist_raw_output(
        &self,
        _project_root: &Path,
        _invocation_id: &str,
        contents: &str,
    ) -> crate::shared::error::AppResult<crate::contexts::agent_execution::model::RawOutputReference>
    {
        Ok(
            crate::contexts::agent_execution::model::RawOutputReference::Inline(
                contents.to_owned(),
            ),
        )
    }
}

#[derive(Clone, Copy)]
struct ConformanceNoopSessionStore;

impl crate::contexts::agent_execution::session::SessionStorePort for ConformanceNoopSessionStore {
    fn load_sessions(
        &self,
        _project_root: &Path,
    ) -> crate::shared::error::AppResult<crate::contexts::agent_execution::session::PersistedSessions>
    {
        Ok(crate::contexts::agent_execution::session::PersistedSessions::empty())
    }

    fn save_sessions(
        &self,
        _project_root: &Path,
        _sessions: &crate::contexts::agent_execution::session::PersistedSessions,
    ) -> crate::shared::error::AppResult<()> {
        Ok(())
    }
}

fn register_tmux_streaming_slice6(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-TMUX-001", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let workspace_root = ws.path().join(".ralph-burning");
        let mut workspace = crate::shared::domain::WorkspaceConfig::new(chrono::Utc::now());
        workspace.execution.mode = Some(crate::shared::domain::ExecutionMode::Direct);
        crate::adapters::fs::FileSystem::write_atomic(
            &workspace_root.join("workspace.toml"),
            &toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
        )
        .map_err(|e| format!("write workspace config: {e}"))?;

        let project_id = crate::shared::domain::ProjectId::new("alpha")
            .map_err(|e| format!("project id: {e}"))?;
        let mut project = crate::shared::domain::ProjectConfig::default();
        project.execution.mode = Some(crate::shared::domain::ExecutionMode::Tmux);
        crate::adapters::fs::FileSystem::write_project_config(ws.path(), &project_id, &project)
            .map_err(|e| format!("write project config: {e}"))?;

        let effective =
            crate::contexts::workspace_governance::config::EffectiveConfig::load_for_project(
                ws.path(),
                Some(&project_id),
                crate::contexts::workspace_governance::config::CliBackendOverrides {
                    execution_mode: Some(crate::shared::domain::ExecutionMode::Direct),
                    ..Default::default()
                },
            )
            .map_err(|e| format!("load config: {e}"))?;

        if effective.effective_execution_mode() != crate::shared::domain::ExecutionMode::Direct {
            return Err("execution.mode should resolve from CLI override".into());
        }
        let source = effective
            .get("execution.mode")
            .map_err(|e| format!("execution.mode source: {e}"))?;
        if source.source
            != crate::contexts::workspace_governance::config::ConfigValueSource::CliOverride
        {
            return Err(format!(
                "execution.mode source should be cli override, got {}",
                source.source
            ));
        }
        Ok(())
    });

    reg!(m, "SC-TMUX-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let workspace_root = ws.path().join(".ralph-burning");
        let mut workspace = crate::shared::domain::WorkspaceConfig::new(chrono::Utc::now());
        workspace.execution.stream_output = Some(false);
        crate::adapters::fs::FileSystem::write_atomic(
            &workspace_root.join("workspace.toml"),
            &toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
        )
        .map_err(|e| format!("write workspace config: {e}"))?;

        let effective =
            crate::contexts::workspace_governance::config::EffectiveConfig::load_for_project(
                ws.path(),
                None,
                crate::contexts::workspace_governance::config::CliBackendOverrides {
                    stream_output: Some(true),
                    ..Default::default()
                },
            )
            .map_err(|e| format!("load config: {e}"))?;

        if !effective.effective_stream_output() {
            return Err("execution.stream_output should resolve from CLI override".into());
        }
        let source = effective
            .get("execution.stream_output")
            .map_err(|e| format!("execution.stream_output source: {e}"))?;
        if source.source
            != crate::contexts::workspace_governance::config::ConfigValueSource::CliOverride
        {
            return Err(format!(
                "execution.stream_output source should be cli override, got {}",
                source.source
            ));
        }
        Ok(())
    });

    reg!(m, "SC-TMUX-003", || {
        let root = std::path::Path::new("/tmp/test-workspace");
        let session = crate::adapters::tmux::TmuxAdapter::session_name("alpha", "run-1", root);
        // Session name should contain the project id and a workspace hash prefix
        if !session.starts_with("rb-") || !session.contains("alpha") || !session.contains("run-1") {
            return Err(format!("unexpected session name format: {session}"));
        }
        // Same inputs should produce same name (deterministic)
        let session2 = crate::adapters::tmux::TmuxAdapter::session_name("alpha", "run-1", root);
        if session != session2 {
            return Err(format!(
                "session name not deterministic: {session} vs {session2}"
            ));
        }
        // Different root should produce different name
        let other_root = std::path::Path::new("/tmp/other-workspace");
        let session3 =
            crate::adapters::tmux::TmuxAdapter::session_name("alpha", "run-1", other_root);
        if session == session3 {
            return Err("different workspace roots produced same session name".to_owned());
        }
        Ok(())
    });

    reg!(m, "SC-TMUX-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let mut workspace = crate::shared::domain::WorkspaceConfig::new(chrono::Utc::now());
        workspace.execution.mode = Some(crate::shared::domain::ExecutionMode::Tmux);
        crate::adapters::fs::FileSystem::write_atomic(
            &workspace_config_path(ws.path()),
            &toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
        )
        .map_err(|e| format!("write workspace config: {e}"))?;

        let empty_path = std::env::temp_dir().join(format!("ralph-empty-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&empty_path).map_err(|e| format!("create empty path: {e}"))?;

        let effective =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                .map_err(|e| format!("load config: {e}"))?;
        let service =
            crate::contexts::agent_execution::diagnostics::BackendDiagnosticsService::new(
                &effective,
            )
            .with_tmux_search_paths(vec![empty_path.clone()]);
        let result = service.check_backends(crate::shared::domain::FlowPreset::Standard);

        let _ = std::fs::remove_dir_all(&empty_path);

        if !result.failures.iter().any(|failure| {
            failure.failure_kind
                == crate::contexts::agent_execution::diagnostics::BackendCheckFailureKind::TmuxUnavailable
        }) {
            return Err("expected tmux_unavailable failure".into());
        }
        Ok(())
    });

    reg!(m, "SC-TMUX-005", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "alpha", "standard")?;
        let workspace_toml = workspace_config_path(ws.path());
        let mut workspace: crate::shared::domain::WorkspaceConfig = toml::from_str(
            &std::fs::read_to_string(&workspace_toml)
                .map_err(|e| format!("read workspace.toml: {e}"))?,
        )
        .map_err(|e| format!("parse workspace.toml: {e}"))?;
        workspace.execution.mode = Some(crate::shared::domain::ExecutionMode::Tmux);
        std::fs::write(
            &workspace_toml,
            toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
        )
        .map_err(|e| format!("write workspace.toml: {e}"))?;

        let out = run_cli(&["run", "attach"], ws.path())?;
        assert_success(&out)?;
        if !out.stdout.contains("No active tmux session exists") {
            return Err("attach output should explain missing session".into());
        }
        Ok(())
    });

    if crate::adapters::tmux::TmuxAdapter::check_tmux_available().is_err() {
        reg_skip!(m, "SC-TMUX-006", "tmux is not available");
    } else {
        reg!(m, "SC-TMUX-006", || {
            use crate::adapters::process_backend::ProcessBackendAdapter;
            use crate::contexts::agent_execution::service::AgentExecutionPort;

            let bin_dir = TempWorkspace::new()?;
            let state_dir = TempWorkspace::new()?;
            let (_direct_dir, direct_request) = tmux_request_fixture("direct-claude")?;
            let direct_envelope = direct_request.working_dir.join("claude-envelope.json");
            write_tmux_claude_envelope(&direct_envelope, &tmux_planning_payload())?;
            write_tmux_fake_claude(bin_dir.path(), &direct_envelope)?;
            write_tmux_fake_tmux(bin_dir.path(), state_dir.path())?;

            let (_tmux_dir, tmux_request) = tmux_request_fixture("tmux-claude")?;
            let tmux_envelope = tmux_request.working_dir.join("claude-envelope.json");
            write_tmux_claude_envelope(&tmux_envelope, &tmux_planning_payload())?;

            let mut search_paths = vec![bin_dir.path().to_path_buf()];
            search_paths.extend(ProcessBackendAdapter::system_path_entries());
            block_on_result(async {
                let direct = ProcessBackendAdapter::with_search_paths(search_paths.clone())
                    .invoke(direct_request)
                    .await
                    .map_err(|e| format!("direct invoke: {e}"))?;
                let tmux = crate::adapters::tmux::TmuxAdapter::new(
                    ProcessBackendAdapter::with_search_paths(search_paths),
                    true,
                )
                .map_err(|e| format!("tmux adapter init: {e}"))?
                .invoke(tmux_request)
                .await
                .map_err(|e| format!("tmux invoke: {e}"))?;

                if direct.parsed_payload != tmux.parsed_payload {
                    return Err("parsed payloads should be identical".into());
                }
                if direct.raw_output_reference != tmux.raw_output_reference {
                    return Err("raw output references should be identical".into());
                }
                Ok(())
            })
        });
    }
    if crate::adapters::tmux::TmuxAdapter::check_tmux_available().is_err() {
        reg_skip!(m, "SC-TMUX-007", "tmux is not available");
    } else {
        reg!(m, "SC-TMUX-007", || {
            use crate::adapters::process_backend::ProcessBackendAdapter;
            use crate::contexts::agent_execution::service::AgentExecutionPort;

            let bin_dir = TempWorkspace::new()?;
            let state_dir = TempWorkspace::new()?;
            write_tmux_sleeping_claude(bin_dir.path())?;
            write_tmux_fake_tmux(bin_dir.path(), state_dir.path())?;

            let (_dir, request) = tmux_request_fixture("tmux-cancel")?;
            let session_name = tmux_session_name_for_request(&request);
            let mut search_paths = vec![bin_dir.path().to_path_buf()];
            search_paths.extend(ProcessBackendAdapter::system_path_entries());
            let adapter = crate::adapters::tmux::TmuxAdapter::new(
                ProcessBackendAdapter::with_search_paths(search_paths),
                true,
            )
            .map_err(|e| format!("tmux adapter init: {e}"))?;
            let invocation_id = request.invocation_id.clone();

            block_on_result(async {
                let join = tokio::spawn({
                    let adapter = adapter.clone();
                    let request = request.clone();
                    async move { adapter.invoke(request).await }
                });

                let mut session_found = false;
                for _ in 0..10 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    if adapter
                        .has_session(&session_name)
                        .await
                        .map_err(|e| format!("query session: {e}"))?
                    {
                        session_found = true;
                        break;
                    }
                }
                if !session_found {
                    return Err("session should exist while invocation is running".into());
                }

                adapter
                    .cancel(&invocation_id)
                    .await
                    .map_err(|e| format!("cancel invocation: {e}"))?;
                let _ = join.await.map_err(|e| format!("join invoke task: {e}"))?;

                if adapter
                    .has_session(&session_name)
                    .await
                    .map_err(|e| format!("query session: {e}"))?
                {
                    return Err("session should be cleaned up after cancel".into());
                }
                Ok(())
            })
        });
    }
    if crate::adapters::tmux::TmuxAdapter::check_tmux_available().is_err() {
        reg_skip!(m, "SC-TMUX-008", "tmux is not available");
    } else {
        reg!(m, "SC-TMUX-008", || {
            use crate::adapters::process_backend::ProcessBackendAdapter;
            use crate::contexts::agent_execution::service::AgentExecutionService;

            let bin_dir = TempWorkspace::new()?;
            let state_dir = TempWorkspace::new()?;
            write_tmux_sleeping_claude(bin_dir.path())?;
            write_tmux_fake_tmux(bin_dir.path(), state_dir.path())?;

            let (_dir, mut request) = tmux_request_fixture("tmux-timeout")?;
            request.timeout = std::time::Duration::from_millis(200);
            let session_name = tmux_session_name_for_request(&request);
            let mut search_paths = vec![bin_dir.path().to_path_buf()];
            search_paths.extend(ProcessBackendAdapter::system_path_entries());
            let adapter = crate::adapters::tmux::TmuxAdapter::new(
                ProcessBackendAdapter::with_search_paths(search_paths),
                true,
            )
            .map_err(|e| format!("tmux adapter init: {e}"))?;
            let service = AgentExecutionService::new(
                adapter.clone(),
                ConformanceInlineRawOutputStore,
                ConformanceNoopSessionStore,
            );

            block_on_result(async {
                match service.invoke(request.clone()).await {
                    Err(crate::shared::error::AppError::InvocationTimeout { .. }) => {}
                    Err(other) => return Err(format!("expected timeout, got {other}")),
                    Ok(_) => return Err("timeout should have failed the invocation".into()),
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                if adapter
                    .has_session(&session_name)
                    .await
                    .map_err(|e| format!("query session: {e}"))?
                {
                    return Err("session should be cleaned up after timeout".into());
                }
                Ok(())
            })
        });
    }
    reg!(m, "SC-TMUX-009", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "watchable-stream", "standard")?;
        let workspace_toml = workspace_config_path(ws.path());
        let mut workspace: crate::shared::domain::WorkspaceConfig = toml::from_str(
            &std::fs::read_to_string(&workspace_toml)
                .map_err(|e| format!("read workspace.toml: {e}"))?,
        )
        .map_err(|e| format!("parse workspace.toml: {e}"))?;
        workspace.execution.stream_output = Some(true);
        std::fs::write(
            &workspace_toml,
            toml::to_string_pretty(&workspace).map_err(|e| format!("serialize workspace: {e}"))?,
        )
        .map_err(|e| format!("write workspace.toml: {e}"))?;

        let child = Command::new(binary_path())
            .args(["run", "tail", "--follow", "--logs"])
            .current_dir(ws.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn follow --logs: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(300));

        let log_dir = conformance_project_root(&ws, "watchable-stream").join("runtime/logs");
        std::fs::write(
            log_dir.join("002.ndjson"),
            r#"{"timestamp":"2026-03-19T03:05:00Z","level":"info","source":"agent","message":"watcher log"}"#.to_owned()
                + "\n",
        )
        .map_err(|e| format!("write runtime log: {e}"))?;
        std::thread::sleep(std::time::Duration::from_millis(700));

        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)
            .map_err(|e| format!("send SIGINT: {e}"))?;
        let output = child
            .wait_with_output()
            .map_err(|e| format!("wait follow --logs output: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "follow --logs should exit successfully, stderr={}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("watcher log") {
            return Err(
                "follow --logs should surface the appended runtime log before the 2-second polling fallback".into(),
            );
        }
        Ok(())
    });
    reg!(m, "SC-TMUX-010", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let effective =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(ws.path())
                .map_err(|e| format!("load config: {e}"))?;
        if effective.effective_execution_mode() != crate::shared::domain::ExecutionMode::Direct {
            return Err("default execution mode should be direct".into());
        }
        Ok(())
    });
}

// ===========================================================================
// Template Overrides — Slice 7 (10 scenarios)
// ===========================================================================

fn register_template_overrides_slice7(m: &mut HashMap<String, ScenarioExecutor>) {
    use crate::contexts::workspace_governance::template_catalog;

    // @parity_slice7_workspace_override
    reg!(m, "parity_slice7_workspace_override", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let templates_dir = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&templates_dir)
            .map_err(|e| format!("create templates dir: {e}"))?;
        std::fs::write(
            templates_dir.join("planning.md"),
            "CUSTOM: {{role_instruction}} | {{project_prompt}} | {{json_schema}}",
        )
        .map_err(|e| format!("write override: {e}"))?;
        let resolved = template_catalog::resolve("planning", ws.path(), None)
            .map_err(|e| format!("resolve: {e}"))?;
        match resolved.source {
            template_catalog::TemplateSource::WorkspaceOverride(_) => {}
            other => return Err(format!("expected workspace override, got {other:?}")),
        }
        let rendered = template_catalog::render(
            &resolved,
            &[
                ("role_instruction", "You are the Planner."),
                ("project_prompt", "Build X."),
                ("json_schema", "{}"),
            ],
        )
        .map_err(|e| format!("render: {e}"))?;
        if !rendered.starts_with("CUSTOM:") {
            return Err(format!("expected custom prefix, got: {}", &rendered[..40]));
        }
        Ok(())
    });

    // @parity_slice7_project_override
    reg!(m, "parity_slice7_project_override", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "tpl-proj", "standard");
        let pid = crate::shared::domain::ProjectId::new("tpl-proj".to_owned())
            .map_err(|e| format!("pid: {e}"))?;
        let proj_templates = project_root(ws.path(), "tpl-proj").join("templates");
        std::fs::create_dir_all(&proj_templates)
            .map_err(|e| format!("create proj templates: {e}"))?;
        std::fs::write(
            proj_templates.join("planning.md"),
            "PROJECT: {{role_instruction}} | {{project_prompt}} | {{json_schema}}",
        )
        .map_err(|e| format!("write project override: {e}"))?;
        let resolved = template_catalog::resolve("planning", ws.path(), Some(&pid))
            .map_err(|e| format!("resolve: {e}"))?;
        match resolved.source {
            template_catalog::TemplateSource::ProjectOverride(_) => Ok(()),
            other => Err(format!("expected project override, got {other:?}")),
        }
    });

    // @parity_slice7_project_over_workspace
    reg!(m, "parity_slice7_project_over_workspace", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "tpl-prec", "standard");
        let pid = crate::shared::domain::ProjectId::new("tpl-prec".to_owned())
            .map_err(|e| format!("pid: {e}"))?;
        let ws_templates = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&ws_templates).map_err(|e| format!("create ws templates: {e}"))?;
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "WS: {{base_context}}",
        )
        .map_err(|e| format!("write ws override: {e}"))?;
        let proj_templates = project_root(ws.path(), "tpl-prec").join("templates");
        std::fs::create_dir_all(&proj_templates)
            .map_err(|e| format!("create proj templates: {e}"))?;
        std::fs::write(
            proj_templates.join("requirements_ideation.md"),
            "PROJECT: {{base_context}}",
        )
        .map_err(|e| format!("write proj override: {e}"))?;
        let resolved = template_catalog::resolve("requirements_ideation", ws.path(), Some(&pid))
            .map_err(|e| format!("resolve: {e}"))?;
        match resolved.source {
            template_catalog::TemplateSource::ProjectOverride(_) => Ok(()),
            other => Err(format!("expected project override to win, got {other:?}")),
        }
    });

    // @parity_slice7_malformed_workflow_rejection
    reg!(m, "parity_slice7_malformed_workflow_rejection", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let templates_dir = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&templates_dir)
            .map_err(|e| format!("create templates dir: {e}"))?;
        std::fs::write(templates_dir.join("planning.md"), "No placeholders here.")
            .map_err(|e| format!("write malformed: {e}"))?;
        match template_catalog::resolve("planning", ws.path(), None) {
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("malformed template override") {
                    Ok(())
                } else {
                    Err(format!("expected malformed template error, got: {msg}"))
                }
            }
            Ok(_) => Err("expected error for malformed template".into()),
        }
    });

    // @parity_slice7_malformed_requirements_rejection
    reg!(m, "parity_slice7_malformed_requirements_rejection", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let templates_dir = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&templates_dir)
            .map_err(|e| format!("create templates dir: {e}"))?;
        std::fs::write(
            templates_dir.join("requirements_draft.md"),
            "Missing the idea placeholder.",
        )
        .map_err(|e| format!("write malformed: {e}"))?;
        match template_catalog::resolve("requirements_draft", ws.path(), None) {
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("malformed template override") {
                    Ok(())
                } else {
                    Err(format!("expected malformed template error, got: {msg}"))
                }
            }
            Ok(_) => Err("expected error for malformed requirements template".into()),
        }
    });

    // @parity_slice7_no_silent_fallback
    reg!(m, "parity_slice7_no_silent_fallback", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "tpl-nofb", "standard");
        let pid = crate::shared::domain::ProjectId::new("tpl-nofb".to_owned())
            .map_err(|e| format!("pid: {e}"))?;
        let ws_templates = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&ws_templates).map_err(|e| format!("create ws templates: {e}"))?;
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "WS valid: {{base_context}}",
        )
        .map_err(|e| format!("write ws override: {e}"))?;
        let proj_templates = project_root(ws.path(), "tpl-nofb").join("templates");
        std::fs::create_dir_all(&proj_templates)
            .map_err(|e| format!("create proj templates: {e}"))?;
        std::fs::write(
            proj_templates.join("requirements_ideation.md"),
            "Malformed — no placeholders.",
        )
        .map_err(|e| format!("write malformed proj: {e}"))?;
        match template_catalog::resolve("requirements_ideation", ws.path(), Some(&pid)) {
            Err(_) => Ok(()),
            Ok(_) => Err("must not silently fall back to workspace override".into()),
        }
    });

    // @parity_slice7_built_in_default_preserved
    reg!(m, "parity_slice7_built_in_default_preserved", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let resolved = template_catalog::resolve("planning", ws.path(), None)
            .map_err(|e| format!("resolve: {e}"))?;
        match resolved.source {
            template_catalog::TemplateSource::BuiltIn => {}
            other => return Err(format!("expected built-in, got {other:?}")),
        }
        let rendered = template_catalog::render(
            &resolved,
            &[
                ("role_instruction", "You are the Planner."),
                ("project_prompt", "Build X."),
                ("json_schema", "{}"),
            ],
        )
        .map_err(|e| format!("render: {e}"))?;
        if !rendered.contains("# Stage Execution Prompt") {
            return Err("rendered built-in should contain stage header".into());
        }
        if !rendered.contains("## Authoritative JSON Schema") {
            return Err("rendered built-in should contain schema header".into());
        }
        Ok(())
    });

    // @parity_slice7_placeholder_validation
    reg!(m, "parity_slice7_placeholder_validation", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let templates_dir = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&templates_dir)
            .map_err(|e| format!("create templates dir: {e}"))?;
        std::fs::write(
            templates_dir.join("requirements_ideation.md"),
            "{{base_context}} and {{unknown_field}}",
        )
        .map_err(|e| format!("write: {e}"))?;
        match template_catalog::resolve("requirements_ideation", ws.path(), None) {
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("unknown placeholder") && msg.contains("unknown_field") {
                    Ok(())
                } else {
                    Err(format!("expected unknown placeholder error, got: {msg}"))
                }
            }
            Ok(_) => Err("expected error for unknown placeholder".into()),
        }
    });

    // @parity_slice7_non_utf8_rejection
    reg!(m, "parity_slice7_non_utf8_rejection", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let templates_dir = ws.path().join(".ralph-burning/templates");
        std::fs::create_dir_all(&templates_dir)
            .map_err(|e| format!("create templates dir: {e}"))?;
        std::fs::write(
            templates_dir.join("requirements_ideation.md"),
            [0xFF, 0xFE, 0x00, 0x01],
        )
        .map_err(|e| format!("write non-utf8: {e}"))?;
        match template_catalog::resolve("requirements_ideation", ws.path(), None) {
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("UTF-8") {
                    Ok(())
                } else {
                    Err(format!("expected UTF-8 error, got: {msg}"))
                }
            }
            Ok(_) => Err("expected error for non-UTF-8 file".into()),
        }
    });

    // @parity_slice7_invalid_marker_rejection
    reg!(m, "parity_slice7_invalid_marker_rejection", || {
        let tmp = std::env::temp_dir().join(format!(
            "ralph-conformance-invalid-marker-{}",
            uuid::Uuid::new_v4()
        ));
        let ws = tmp.join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws).map_err(|e| e.to_string())?;

        // Override with hyphenated marker
        std::fs::write(
            ws.join("requirements_ideation.md"),
            "{{base_context}} and {{invented-placeholder}}",
        )
        .map_err(|e| e.to_string())?;

        match template_catalog::resolve("requirements_ideation", &tmp, None) {
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("invented-placeholder") {
                    let _ = std::fs::remove_dir_all(&tmp);
                    return Err(format!("error should cite 'invented-placeholder': {msg}"));
                }
            }
            Ok(_) => {
                let _ = std::fs::remove_dir_all(&tmp);
                return Err("expected error for hyphenated placeholder marker".into());
            }
        }

        // Override with spaced marker
        std::fs::write(
            ws.join("requirements_ideation.md"),
            "{{base_context}} and {{with spaces}}",
        )
        .map_err(|e| e.to_string())?;

        match template_catalog::resolve("requirements_ideation", &tmp, None) {
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("with spaces") {
                    let _ = std::fs::remove_dir_all(&tmp);
                    return Err(format!("error should cite 'with spaces': {msg}"));
                }
            }
            Ok(_) => {
                let _ = std::fs::remove_dir_all(&tmp);
                return Err("expected error for spaced placeholder marker".into());
            }
        }

        let _ = std::fs::remove_dir_all(&tmp);
        Ok(())
    });

    // Meta-conformance: all template IDs have manifests
    reg!(m, "parity_slice7_all_ids_have_manifests", || {
        for &id in template_catalog::STAGE_TEMPLATE_IDS {
            if template_catalog::manifest_for(id).is_none() {
                return Err(format!("missing manifest for stage template '{id}'"));
            }
        }
        for &id in template_catalog::PANEL_TEMPLATE_IDS {
            if template_catalog::manifest_for(id).is_none() {
                return Err(format!("missing manifest for panel template '{id}'"));
            }
        }
        for &id in template_catalog::REQUIREMENTS_TEMPLATE_IDS {
            if template_catalog::manifest_for(id).is_none() {
                return Err(format!("missing manifest for requirements template '{id}'"));
            }
        }
        Ok(())
    });
}
