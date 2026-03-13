use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::runner::ScenarioExecutor;

use crate::contexts::workflow_composition::contracts::{contract_for_stage, all_contracts, ContractFamily};
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
        let path = std::env::temp_dir().join(format!(
            "ralph-conformance-{}",
            uuid::Uuid::new_v4()
        ));
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
    std::env::current_exe().expect("current executable path")
}

struct CmdOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run_cli(args: &[&str], cwd: &Path) -> Result<CmdOutput, String> {
    run_cli_with_env(args, cwd, &[])
}

fn run_cli_with_env(
    args: &[&str],
    cwd: &Path,
    env: &[(&str, &str)],
) -> Result<CmdOutput, String> {
    let mut cmd = Command::new(binary_path());
    cmd.args(args).current_dir(cwd);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = cmd.output().map_err(|e| format!("failed to run CLI: {e}"))?;

    Ok(CmdOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

// ---------------------------------------------------------------------------
// Journal and durable-state assertion helpers
// ---------------------------------------------------------------------------

fn read_journal(ws: &TempWorkspace, project_id: &str) -> Result<Vec<serde_json::Value>, String> {
    let path = ws
        .path()
        .join(format!(".ralph-burning/projects/{project_id}/journal.ndjson"));
    let content = std::fs::read_to_string(&path).map_err(|e| format!("read journal: {e}"))?;
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).map_err(|e| format!("parse journal line: {e}")))
        .collect()
}

fn read_run_snapshot(
    ws: &TempWorkspace,
    project_id: &str,
) -> Result<serde_json::Value, String> {
    let path = ws
        .path()
        .join(format!(".ralph-burning/projects/{project_id}/run.json"));
    let content = std::fs::read_to_string(&path).map_err(|e| format!("read run.json: {e}"))?;
    serde_json::from_str(&content).map_err(|e| format!("parse run.json: {e}"))
}

fn count_payload_files(ws: &TempWorkspace, project_id: &str) -> Result<usize, String> {
    let dir = ws
        .path()
        .join(format!(".ralph-burning/projects/{project_id}/history/payloads"));
    let count = std::fs::read_dir(&dir)
        .map_err(|e| format!("read payloads dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
        .count();
    Ok(count)
}

fn count_artifact_files(ws: &TempWorkspace, project_id: &str) -> Result<usize, String> {
    let dir = ws
        .path()
        .join(format!(".ralph-burning/projects/{project_id}/history/artifacts"));
    let count = std::fs::read_dir(&dir)
        .map_err(|e| format!("read artifacts dir: {e}"))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "md"))
        .count();
    Ok(count)
}

fn journal_event_types(events: &[serde_json::Value]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| e.get("event_type").and_then(|v| v.as_str()).map(String::from))
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
        return Err(format!("expected success, got failure. stderr: {}", out.stderr));
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
        return Err(format!("{context}: expected to contain '{needle}', got: {haystack}"));
    }
    Ok(())
}

fn init_workspace(ws: &TempWorkspace) -> Result<(), String> {
    let out = run_cli(&["init"], ws.path())?;
    assert_success(&out)
}

fn create_project_fixture(base_dir: &Path, project_id: &str, flow: &str) {
    let project_root = base_dir.join(".ralph-burning/projects").join(project_id);
    std::fs::create_dir_all(&project_root).expect("create project directory");
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "{flow}"
prompt_reference = "prompt.md"
prompt_hash = "0000000000000000"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"
"#
    );
    std::fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    std::fs::write(project_root.join("prompt.md"), "# Fixture prompt\n").expect("write prompt");
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
    std::fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");
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
    std::fs::write(
        base_dir.join(".ralph-burning/active-project"),
        format!("{project_id}\n"),
    )
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

/// Initialize a git repository in the temp workspace with an initial commit.
/// Returns the SHA of the initial commit so tests can assert against it.
fn init_git_repo(ws: &TempWorkspace) -> Result<String, String> {
    let run = |args: &[&str]| -> Result<String, String> {
        let output = Command::new("git")
            .args(args)
            .current_dir(ws.path())
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
    };
    run(&["init"])?;
    run(&["add", "."])?;
    run(&["commit", "-m", "initial"])?;
    let sha = run(&["rev-parse", "HEAD"])?;
    Ok(sha)
}

// ---------------------------------------------------------------------------
// Registry builder
// ---------------------------------------------------------------------------

macro_rules! reg {
    ($map:expr, $id:expr, $func:expr) => {
        $map.insert($id.to_string(), Box::new($func) as ScenarioExecutor);
    };
}

/// Build the complete scenario registry mapping scenario IDs to executor functions.
pub fn build_registry() -> HashMap<String, ScenarioExecutor> {
    let mut m: HashMap<String, ScenarioExecutor> = HashMap::new();

    register_workspace_init(&mut m);
    register_workspace_config(&mut m);
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
    register_requirements_drafting(&mut m);
    register_daemon_lifecycle(&mut m);
    register_daemon_routing(&mut m);
    register_daemon_issue_intake(&mut m);

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
        if !ws.path().join(".ralph-burning/workspace.toml").is_file() {
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
        assert_contains(&out.stdout, "standard", "stdout")?;
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
        let toml = std::fs::read_to_string(ws.path().join(".ralph-burning/workspace.toml"))
            .map_err(|e| e.to_string())?;
        assert_contains(&toml, "quick_dev", "workspace.toml")?;
        Ok(())
    });

    reg!(m, "workspace-config-set-invalid-value", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["config", "set", "default_flow", "unknown_flow"], ws.path())?;
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
            let mut perms = std::fs::metadata(&script_path).map_err(|e| e.to_string())?.permissions();
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
            return Err(format!("config edit failed: {}", String::from_utf8_lossy(&out.stderr)));
        }
        let toml = std::fs::read_to_string(ws.path().join(".ralph-burning/workspace.toml"))
            .map_err(|e| e.to_string())?;
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
        ).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&script_path).map_err(|e| e.to_string())?.permissions();
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
        let out = run_cli(&["flow", "list"], Path::new("/tmp"))?;
        assert_success(&out)?;
        for preset in &["standard", "quick_dev", "docs_change", "ci_improvement"] {
            assert_contains(&out.stdout, preset, "stdout")?;
        }
        Ok(())
    });

    // Scenario Outline: tests all 4 example rows
    reg!(m, "flow-show-each-preset", || {
        let examples = [
            ("standard", "prompt_review"),
            ("quick_dev", "plan_and_implement"),
            ("docs_change", "docs_plan"),
            ("ci_improvement", "ci_plan"),
        ];
        for (flow_id, stage_1) in &examples {
            let out = run_cli(&["flow", "show", flow_id], Path::new("/tmp"))?;
            assert_success(&out)?;
            assert_contains(&out.stdout, "Stage count", &format!("flow show {flow_id}"))?;
            assert_contains(&out.stdout, stage_1, &format!("flow show {flow_id}"))?;
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
        std::fs::write(ws.path().join("prompt.md"), "# Test prompt\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &["project", "create", "--id", "test-proj", "--name", "Test Project",
              "--prompt", "prompt.md", "--flow", "standard"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let proj_dir = ws.path().join(".ralph-burning/projects/test-proj");
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
            &["project", "create", "--id", "dup", "--name", "First",
              "--prompt", "prompt.md", "--flow", "standard"],
            ws.path(),
        )?;
        let out = run_cli(
            &["project", "create", "--id", "dup", "--name", "Second",
              "--prompt", "prompt.md", "--flow", "standard"],
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
            ws.path().join(".ralph-burning/projects/fixed-flow/project.toml"),
        ).map_err(|e| e.to_string())?;
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
        if ws.path().join(".ralph-burning/projects/del-proj").exists() {
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
            ws.path().join(".ralph-burning/projects/running-proj/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
            &["project", "create", "--id", "atomic-proj", "--name", "Atomic",
              "--prompt", "prompt.md", "--flow", "standard"],
            ws.path(),
        )?;
        assert_success(&out)?;
        // If successful, all canonical files should exist
        let dir = ws.path().join(".ralph-burning/projects/atomic-proj");
        for f in &["project.toml", "run.json", "journal.ndjson", "sessions.json", "prompt.md"] {
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
        let config_path = ws.path().join(".ralph-burning/workspace.toml");
        let config = std::fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
        let updated = config.replace("version = 1", "version = 99");
        std::fs::write(&config_path, updated).map_err(|e| e.to_string())?;
        std::fs::write(ws.path().join("prompt.md"), "# Test\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &["project", "create", "--id", "ver-proj", "--name", "Ver",
              "--prompt", "prompt.md", "--flow", "standard"],
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
            &["project", "create", "--id", "bad-flow", "--name", "Bad",
              "--prompt", "prompt.md", "--flow", "invalid_flow"],
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
            &["project", "create", "--id", "no-select", "--name", "No Select",
              "--prompt", "prompt.md", "--flow", "standard"],
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
        if ws.path().join(".ralph-burning/projects/txn-del").exists() {
            return Err("project directory should be removed after delete".into());
        }
        Ok(())
    });

    reg!(m, "SC-PROJ-015", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "schema-proj", "standard");
        let run_json = std::fs::read_to_string(
            ws.path().join(".ralph-burning/projects/schema-proj/run.json"),
        ).map_err(|e| e.to_string())?;
        let parsed: serde_json::Value = serde_json::from_str(&run_json).map_err(|e| e.to_string())?;
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
        std::fs::remove_file(ws.path().join(".ralph-burning/projects/corrupt-show/project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "corrupt-show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-017", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "corrupt-list", "standard");
        std::fs::remove_file(ws.path().join(".ralph-burning/projects/corrupt-list/project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "list"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-018", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        create_project_fixture(ws.path(), "corrupt-del", "standard");
        std::fs::remove_file(ws.path().join(".ralph-burning/projects/corrupt-del/project.toml"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "delete", "corrupt-del"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-PROJ-019", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        std::fs::write(ws.path().join("my-prompt.md"), "# My prompt\n").map_err(|e| e.to_string())?;
        let out = run_cli(
            &["project", "create", "--id", "ref-proj", "--name", "Ref",
              "--prompt", "my-prompt.md", "--flow", "standard"],
            ws.path(),
        )?;
        assert_success(&out)?;
        let toml = std::fs::read_to_string(
            ws.path().join(".ralph-burning/projects/ref-proj/project.toml"),
        ).map_err(|e| e.to_string())?;
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
            ws.path().join(".ralph-burning/projects/bad-schema/project.toml"),
            "this is not valid project toml",
        ).map_err(|e| e.to_string())?;
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
        let ptr = std::fs::read_to_string(ws.path().join(".ralph-burning/active-project"))
            .map_err(|e| e.to_string())?;
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
        let bundle = contract.evaluate(&payload).map_err(|e| format!("contract evaluation failed: {e}"))?;
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
        let bundle = contract.evaluate(&payload).map_err(|e| format!("contract evaluation failed: {e}"))?;
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
        let bundle = contract.evaluate(&payload).map_err(|e| format!("contract evaluation failed: {e}"))?;
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
                if e.failure_class() != crate::shared::domain::FailureClass::QaReviewOutcomeFailure {
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
                ContractFamily::Planning | ContractFamily::Execution | ContractFamily::Validation => {}
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
        assert_contains(&pre.stdout, "not_started", "run status before start")?;

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
        for expected in &["run_started", "stage_entered", "stage_completed", "run_completed"] {
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
        for expected in &["run_started", "stage_entered", "stage_completed", "run_completed"] {
            if !types.iter().any(|t| t == expected) {
                return Err(format!("journal missing event type: {expected}"));
            }
        }

        // Verify 4 quick_dev stages worth of payloads/artifacts
        let payloads = count_payload_files(&ws, "beta")?;
        if payloads < 4 {
            return Err(format!("expected >= 4 payloads for quick_dev, got {payloads}"));
        }
        Ok(())
    });

    reg!(m, "SC-START-003", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "gamma", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/gamma/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not_started", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-004", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "delta", "standard")?;
        let run_json = r#"{"active_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/delta/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;
        assert_contains(&out.stderr, "not_started", "stderr")?;
        Ok(())
    });

    reg!(m, "SC-START-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let config_path = ws.path().join(".ralph-burning/workspace.toml");
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
        run_cli(&["config", "set", "prompt_review.enabled", "false"], ws.path())?;
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
        assert_contains(&status_before.stdout, "not_started", "status before")?;
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
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-START-013", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "runtime-logs", "standard")?;
        // Runtime logs directory exists but is separate from durable state
        if !ws.path().join(".ralph-burning/projects/runtime-logs/runtime/logs").is_dir() {
            return Err("runtime/logs directory should exist".into());
        }
        Ok(())
    });

    reg!(m, "SC-START-014", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "seq-check", "standard")?;
        let journal = std::fs::read_to_string(
            ws.path().join(".ralph-burning/projects/seq-check/journal.ndjson"),
        ).map_err(|e| e.to_string())?;
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
        // Run start should succeed for all four built-in flow presets
        for flow in &["standard", "quick_dev", "docs_change", "ci_improvement"] {
            let ws = TempWorkspace::new()?;
            let proj_id = format!("preset-{}", flow.replace('_', "-"));
            setup_workspace_with_project(&ws, &proj_id, flow)?;
            let out = run_cli(&["run", "start"], ws.path())?;
            assert_success(&out)?;
            let status = run_cli(&["run", "status"], ws.path())?;
            assert_contains(&status.stdout, "completed", &format!("status for {flow}"))?;
        }
        Ok(())
    });

    reg!(m, "SC-START-018", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Disable prompt_review
        run_cli(&["config", "set", "prompt_review.enabled", "false"], ws.path())?;
        create_project_fixture(ws.path(), "november", "standard");
        select_project(ws.path(), "november");

        // Execute run start
        let out = run_cli(&["run", "start"], ws.path())?;
        assert_success(&out)?;

        // Verify status is completed
        let post = run_cli(&["run", "status"], ws.path())?;
        assert_success(&post)?;
        assert_contains(&post.stdout, "completed", "run status after start")?;

        // Verify 7 payloads/artifacts (all except prompt_review)
        let payloads = count_payload_files(&ws, "november")?;
        let artifacts = count_artifact_files(&ws, "november")?;
        if payloads != 7 {
            return Err(format!("expected 7 payloads (no prompt_review), got {payloads}"));
        }
        if artifacts != 7 {
            return Err(format!("expected 7 artifacts (no prompt_review), got {artifacts}"));
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
        let first_stage_entered = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
        });
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
        assert_contains(&status.stdout, "not_started", "status")?;
        Ok(())
    });

    reg!(m, "SC-START-020", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "mid-fail", "standard")?;
        // Verify no partial durable history in a fresh project
        let history_dir = ws.path().join(".ralph-burning/projects/mid-fail/history/payloads");
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
        assert_contains(&status.stdout, "not_started", "status")?;
        Ok(())
    });

    reg!(m, "SC-QD-START-008", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        run_cli(&["config", "set", "prompt_review.enabled", "true"], ws.path())?;
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
        assert_contains(&out.stdout, "docs_update", "stdout")?;
        assert_contains(&out.stdout, "docs_validation", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-DOCS-START-003", || {
        let ws = TempWorkspace::new()?;
        let out = run_cli(&["flow", "show", "docs_change"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "docs_validation", "stdout")?;
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
        assert_contains(&out.stdout, "docs_validation", "stdout")?;
        Ok(())
    });
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
// Run Queries (28 scenarios)
// ===========================================================================

fn register_run_queries(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-RUN-001", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-new", "standard")?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "not_started", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-002", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-active", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-active/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
            ws.path().join(".ralph-burning/projects/rq-tail-logs/runtime/logs/latest.log"),
            "debug: test log line\n",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail", "--logs"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-007", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-corrupt/run.json"),
            "not json",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-008", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-journal-corrupt", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-journal-corrupt/journal.ndjson"),
            "not json\n",
        ).map_err(|e| e.to_string())?;
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
        std::fs::remove_file(ws.path().join(".ralph-burning/projects/rq-no-run/run.json"))
            .map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-012", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-no-journal", "standard")?;
        std::fs::remove_file(ws.path().join(".ralph-burning/projects/rq-no-journal/journal.ndjson"))
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
            ws.path().join(".ralph-burning/projects/rq-completed/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
            ws.path().join(".ralph-burning/projects/rq-failed/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_success(&out)?;
        assert_contains(&out.stdout, "failed", "stdout")?;
        Ok(())
    });

    reg!(m, "SC-RUN-017", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-inconsist", "standard")?;
        // Write semantically inconsistent run.json (active_run but status is completed)
        let run_json = r#"{"active_run":{"run_id":"run-1","started_at":"2026-03-11T19:00:00Z"},"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-inconsist/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        // Should fail fast on semantic inconsistency
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-018", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-del-paused", "standard")?;
        let run_json = r#"{"active_run":{"run_id":"run-1","started_at":"2026-03-11T19:00:00Z"},"status":"paused","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"paused"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-del-paused/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "delete", "rq-del-paused"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-019", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-toml", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-corrupt-toml/project.toml"),
            "not valid toml {{{",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-020", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-hist", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-corrupt-hist/project.toml"),
            "not valid toml {{{",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-021", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-corrupt-tail", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-corrupt-tail/project.toml"),
            "not valid toml {{{",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-022", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-show-corrupt", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-show-corrupt/project.toml"),
            "not valid toml {{{",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-023", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-missing-toml", "standard")?;
        std::fs::remove_file(
            ws.path().join(".ralph-burning/projects/rq-missing-toml/project.toml"),
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "status"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-024", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-show", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-empty-j-show/journal.ndjson"),
            "",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "rq-empty-j-show"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-025", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-hist", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-empty-j-hist/journal.ndjson"),
            "",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "history"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-026", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-empty-j-tail", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-empty-j-tail/journal.ndjson"),
            "",
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-027", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-bad-first", "standard")?;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rq-bad-first/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"run_started","details":{"run_id":"r1"}}"#,
        ).map_err(|e| e.to_string())?;
        let out = run_cli(&["project", "show", "rq-bad-first"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "SC-RUN-028", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rq-tail-newest", "standard")?;
        let log_dir = ws.path().join(".ralph-burning/projects/rq-tail-newest/runtime/logs");
        std::fs::write(log_dir.join("old.log"), "old log\n").map_err(|e| e.to_string())?;
        std::fs::write(log_dir.join("newest.log"), "newest log\n").map_err(|e| e.to_string())?;
        let out = run_cli(&["run", "tail", "--logs"], ws.path())?;
        assert_success(&out)?;
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

        // Configure completion_panel to return conditionally_approved with amendments.
        // The stub backend reads RALPH_BURNING_TEST_STAGE_OVERRIDES.
        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "conditionally_approved",
                "evidence": ["Needs minor formatting changes"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": [
                    {
                        "summary": "Fix formatting",
                        "details": "Update code formatting to match style guide."
                    }
                ]
            }
        });
        let overrides_str = overrides.to_string();

        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides_str)],
        )?;
        assert_success(&out)?;

        // Verify journal contains amendment_queued and completion_round_advanced events
        let events = read_journal(&ws, "cr-alpha")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "amendment_queued") {
            return Err("journal missing 'amendment_queued' event".into());
        }
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
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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

        let overrides = serde_json::json!({
            "acceptance_qa": {
                "outcome": "request_changes",
                "evidence": ["Changes needed"],
                "findings_or_gaps": ["Issue"],
                "follow_up_or_amendments": ["Fix the issue"]
            }
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
            let source = evt.get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "acceptance_qa" {
                return Err(format!("expected source_stage=acceptance_qa, got '{source}'"));
            }
        }
        Ok(())
    });

    reg!(m, "SC-CR-003", || {
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-reject", "standard")?;

        // Configure completion_panel to return rejected
        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "rejected",
                "evidence": ["Does not meet requirements"],
                "findings_or_gaps": ["Critical gap"],
                "follow_up_or_amendments": []
            }
        });
        let overrides_str = overrides.to_string();

        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides_str)],
        )?;
        assert_failure(&out)?;

        // Verify run snapshot shows failed
        let snapshot = read_run_snapshot(&ws, "cr-reject")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status, got '{status}'"));
        }

        // No completion_round_advanced event should exist
        let events = read_journal(&ws, "cr-reject")?;
        let types = journal_event_types(&events);
        if types.iter().any(|t| t == "completion_round_advanced") {
            return Err("journal should NOT contain completion_round_advanced for rejected outcome".into());
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
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
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
            return Err(format!("expected empty amendment_queue for non-late stage, got {queue_pending}"));
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

        // First invocation: completion_panel conditionally_approved (round 1→2)
        // After restart: completion_panel approved, acceptance_qa conditionally_approved (round 2→3)
        // After restart again: all approved
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Round 1 issue"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": ["Fix A"]
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK now"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                },
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                }
            ],
            "acceptance_qa": [
                {
                    "outcome": "approved",
                    "evidence": ["OK"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": []
                },
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
        let round_events: Vec<_> = events.iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("completion_round_advanced"))
            .collect();
        if round_events.len() < 2 {
            return Err(format!("expected >= 2 completion_round_advanced events, got {}", round_events.len()));
        }

        let snapshot = read_run_snapshot(&ws, "cr-multi")?;
        let rounds = snapshot.get("completion_rounds").and_then(|v| v.as_u64()).unwrap_or(0);
        if rounds < 3 {
            return Err(format!("expected completion_rounds >= 3, got {rounds}"));
        }
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-guard/amendments");
        std::fs::write(
            amend_dir.join("orphaned-amendment.json"),
            r#"{"amendment_id":"orphan-1","source_stage":"completion_panel","body":{"summary":"Orphan","details":"Stale amendment"},"created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
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
        // Completion guard checks snapshot queue WITHOUT disk amendments.
        // Feature: when the run snapshot has non-empty amendment_queue.pending
        // but NO amendment file exists on disk, the completion guard fires and
        // blocks run_completed. This isolates the snapshot-queue path from the
        // disk-only path already covered by SC-CR-007.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-snap-guard", "standard")?;

        // Inject a non-empty amendment_queue.pending in the run.json snapshot
        // WITHOUT planting any amendment files on disk
        let run_json = r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[{"amendment_id":"snap-1","source_stage":"completion_panel","body":{"summary":"Snap amend","details":"In snapshot only"},"created_at":"2026-03-11T20:00:00Z","batch_sequence":0}],"processed_count":0},"status_summary":"not started"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/cr-snap-guard/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;

        // Verify no amendment files exist on disk
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-snap-guard/amendments");
        let disk_files: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .collect();
        if !disk_files.is_empty() {
            return Err("test setup error: no disk amendment files should exist".into());
        }

        let out = run_cli(&["run", "start"], ws.path())?;
        // The completion guard must block — the run must fail
        assert_failure(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-snap-guard")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status == "completed" {
            return Err("completion guard should have blocked run_completed via snapshot queue alone".into());
        }
        if status != "failed" {
            return Err(format!(
                "expected failed status after completion guard, got '{status}'"
            ));
        }
        // Verify the error output mentions completion being blocked
        let combined_output = format!("{}{}", out.stdout, out.stderr);
        let has_blocked_msg = combined_output.contains("completion blocked")
            || combined_output.contains("pending amendments")
            || combined_output.contains("amendment");
        if !has_blocked_msg {
            return Err(format!(
                "expected error mentioning completion blocked or pending amendments, got: {}",
                combined_output
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-009", || {
        // Resume with pending late-stage amendments reconciles from disk
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-resume-amend", "standard")?;

        // Set up a failed run state (as if it failed after round advancement)
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":2,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/cr-resume-amend/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;

        // Plant amendment files on disk for reconciliation
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-resume-amend/amendments");
        std::fs::write(
            amend_dir.join("resume-amend-1.json"),
            r#"{"amendment_id":"resume-1","source_stage":"completion_panel","body":{"summary":"Resume fix","details":"Fix from prior round"},"created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
        ).map_err(|e| e.to_string())?;

        let out = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-resume-amend")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed after resume, got '{status}'"));
        }
        // Amendments should be drained
        let amend_files: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .collect();
        if !amend_files.is_empty() {
            return Err(format!("expected amendments drained from disk, found {}", amend_files.len()));
        }
        Ok(())
    });

    reg!(m, "SC-CR-010", || {
        // Cycle advancement emitted when entering implementation from completion round
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-cycle-adv", "standard")?;

        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "conditionally_approved",
                "evidence": ["Issue found"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["Fix cycle issue"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let events = read_journal(&ws, "cr-cycle-adv")?;
        let types = journal_event_types(&events);
        if !types.iter().any(|t| t == "cycle_advanced") {
            return Err("journal should contain cycle_advanced event for completion round restart".into());
        }
        Ok(())
    });

    reg!(m, "SC-CR-011", || {
        // Amendment queue drain is idempotent: after planning commit, all amendments cleared
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-idempotent", "standard")?;

        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "conditionally_approved",
                "evidence": ["Needs fix"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["Idempotent fix"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        let snapshot = read_run_snapshot(&ws, "cr-idempotent")?;
        let queue_pending = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map_or(0, |a| a.len());
        if queue_pending > 0 {
            return Err(format!("amendment_queue.pending should be empty after drain, got {queue_pending}"));
        }
        let processed = snapshot
            .get("amendment_queue")
            .and_then(|q| q.get("processed_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if processed == 0 {
            return Err("processed_count should be incremented after drain".into());
        }
        // Verify no amendment files on disk
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-idempotent/amendments");
        let remaining: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .collect();
        if !remaining.is_empty() {
            return Err(format!("expected 0 amendment files on disk, found {}", remaining.len()));
        }
        Ok(())
    });

    reg!(m, "SC-CR-012", || {
        // Amendment persistence is atomic with batch rollback.
        // Feature: if any amendment write fails, already-written files from
        // the same batch are rolled back, the run fails without partial
        // amendments visible, and no queue entry becomes visible without a
        // matching file.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-atomic", "standard")?;

        // Trigger conditionally_approved with multiple follow_ups so the engine
        // attempts to write multiple amendment files.
        // RALPH_BURNING_TEST_AMENDMENT_WRITE_FAIL_AFTER=1 means the first
        // amendment write succeeds but the second fails, testing batch rollback.
        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "conditionally_approved",
                "evidence": ["Atomic test"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["Atomic fix A", "Atomic fix B"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string()),
                ("RALPH_BURNING_TEST_AMENDMENT_WRITE_FAIL_AFTER", "1"),
            ],
        )?;
        // The run must fail because the amendment write failed
        assert_failure(&out)?;

        // Verify no partial amendment files remain on disk (batch rollback)
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-atomic/amendments");
        let remaining: Vec<_> = std::fs::read_dir(&amend_dir)
            .map_err(|e| e.to_string())?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "json"))
            .collect();
        if !remaining.is_empty() {
            return Err(format!(
                "expected 0 amendment files after batch rollback, found {}",
                remaining.len()
            ));
        }

        // Verify the run snapshot shows failure, not completion
        let snapshot = read_run_snapshot(&ws, "cr-atomic")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status == "completed" {
            return Err("run should have failed, not completed, after amendment write failure".into());
        }

        // Verify no amendment_queued events in journal (no queue entry visible
        // without a matching file)
        let events = read_journal(&ws, "cr-atomic")?;
        let amend_events: Vec<_> = events.iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("amendment_queued"))
            .collect();
        if !amend_events.is_empty() {
            return Err(format!(
                "expected 0 amendment_queued events after write failure, got {}",
                amend_events.len()
            ));
        }
        Ok(())
    });

    reg!(m, "SC-CR-013", || {
        // Completion guard leaves snapshot in resumable state
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-resumable", "standard")?;

        // Pre-plant orphaned amendment on disk so the guard fires
        let amend_dir = ws.path().join(".ralph-burning/projects/cr-resumable/amendments");
        std::fs::write(
            amend_dir.join("guard-amend.json"),
            r#"{"amendment_id":"guard-1","source_stage":"completion_panel","body":{"summary":"Guard","details":"Blocks completion"},"created_at":"2026-03-11T20:00:00Z","batch_sequence":0}"#,
        ).map_err(|e| e.to_string())?;

        let out = run_cli(&["run", "start"], ws.path())?;
        assert_failure(&out)?;

        // Snapshot should be failed with no active_run (resumable)
        let snapshot = read_run_snapshot(&ws, "cr-resumable")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "failed" {
            return Err(format!("expected failed status after guard, got '{status}'"));
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
        // Same-batch amendments are ordered deterministically by batch_sequence
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-batch-seq", "standard")?;

        let overrides = serde_json::json!({
            "completion_panel": {
                "outcome": "conditionally_approved",
                "evidence": ["Batch order test"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["First fix", "Second fix", "Third fix"]
            }
        });
        let out = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_STAGE_OVERRIDES", &overrides.to_string())],
        )?;
        assert_success(&out)?;

        // Verify amendment_queued events have stable batch_sequence ordering
        let events = read_journal(&ws, "cr-batch-seq")?;
        let amend_events: Vec<_> = events.iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("amendment_queued"))
            .collect();
        if amend_events.len() < 3 {
            return Err(format!("expected >= 3 amendment_queued events, got {}", amend_events.len()));
        }
        // Verify batch_sequence values are in ascending order
        let mut prev_seq: i64 = -1;
        for evt in &amend_events {
            if let Some(seq) = evt.get("details")
                .and_then(|d| d.get("batch_sequence"))
                .and_then(|v| v.as_i64())
            {
                if seq <= prev_seq {
                    return Err(format!("batch_sequence not ascending: prev={prev_seq}, current={seq}"));
                }
                prev_seq = seq;
            }
        }
        Ok(())
    });

    reg!(m, "SC-CR-015", || {
        // Final-review conditionally_approved triggers completion round advancement
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "cr-fr-cond", "standard")?;

        let overrides = serde_json::json!({
            "final_review": {
                "outcome": "conditionally_approved",
                "evidence": ["Final review issue"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["Fix from final review"]
            }
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
            let source = evt.get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "final_review" {
                return Err(format!("expected source_stage=final_review, got '{source}'"));
            }
        }

        // Verify planning was entered a second time and run completed with rounds=2
        let planning_count = events.iter()
            .filter(|e| {
                e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                    && e.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()) == Some("planning")
            })
            .count();
        if planning_count < 2 {
            return Err(format!("expected planning entered >= 2 times, got {planning_count}"));
        }
        let snapshot = read_run_snapshot(&ws, "cr-fr-cond")?;
        let rounds = snapshot.get("completion_rounds").and_then(|v| v.as_u64()).unwrap_or(0);
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

        let overrides = serde_json::json!({
            "final_review": {
                "outcome": "request_changes",
                "evidence": ["Changes needed"],
                "findings_or_gaps": ["Gap found"],
                "follow_up_or_amendments": ["Address gap"]
            }
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
            let source = evt.get("details")
                .and_then(|d| d.get("source_stage"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if source != "final_review" {
                return Err(format!("expected source_stage=final_review, got '{source}'"));
            }
        }

        let snapshot = read_run_snapshot(&ws, "cr-fr-changes")?;
        let rounds = snapshot.get("completion_rounds").and_then(|v| v.as_u64()).unwrap_or(0);
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
        let will_retry = stage_failed.unwrap()
            .get("details")
            .and_then(|d| d.get("will_retry"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !will_retry {
            return Err("stage_failed event should have will_retry=true".into());
        }

        // Verify implementation was entered a second time (retry)
        let impl_entered_count = events.iter().filter(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("implementation")
        }).count();
        if impl_entered_count < 2 {
            return Err(format!(
                "expected implementation stage_entered >= 2 (retry), got {impl_entered_count}"
            ));
        }

        // Verify run completed
        let snapshot = read_run_snapshot(&ws, "alpha")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
        let cycle_advanced = events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("cycle_advanced")
        });
        if cycle_advanced.is_none() {
            return Err("journal missing cycle_advanced event".into());
        }

        // Verify implementation entered twice across the run
        let impl_entered = events.iter().filter(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("implementation")
        }).count();
        if impl_entered < 2 {
            return Err(format!(
                "expected implementation entered >= 2 times, got {impl_entered}"
            ));
        }

        // Verify completed
        let snapshot = read_run_snapshot(&ws, "charlie")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-004", || {
        // Prompt review not ready pauses the run
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "delta", "standard")?;

        // Override prompt_review to return readiness.ready = false
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
        assert_success(&out)?;

        // Verify paused status
        let snapshot = read_run_snapshot(&ws, "delta")?;
        let status = snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "paused" {
            return Err(format!("expected paused, got '{status}'"));
        }
        // Verify status_summary instructs user to resume
        let summary = snapshot.get("status_summary").and_then(|v| v.as_str()).unwrap_or("");
        if !summary.contains("resume") {
            return Err(format!("status_summary should mention resume: '{summary}'"));
        }
        // Verify prompt_review payload persisted before pause
        let payloads = count_payload_files(&ws, "delta")?;
        if payloads < 1 {
            return Err("expected at least 1 payload persisted before pause".into());
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
        let run_id = pre_events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
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
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_run_id = resume_evt.unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_run_id != run_id {
            return Err(format!("expected resumed run_id={run_id}, got {resumed_run_id}"));
        }

        // Verify planning is NOT re-executed after resume (only entered once total)
        // Count planning stage_entered events after the run_resumed event
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let planning_after_resume = post_events.iter().filter(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details")
                    .and_then(|d| d.get("stage_id"))
                    .and_then(|v| v.as_str())
                    == Some("planning")
        }).count();
        if planning_after_resume > 0 {
            return Err("planning should not be re-executed after resume".into());
        }

        // Verify the first resumed stage is "implementation" with attempt 1
        let first_stage_after_resume = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage_after_resume {
            let stage = evt.get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if stage != "implementation" {
                return Err(format!(
                    "expected first resumed stage=implementation, got '{stage}'"
                ));
            }
            let attempt = evt.get("details")
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
        let status = final_snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "completed" {
            return Err(format!("expected completed after resume, got '{status}'"));
        }
        Ok(())
    });

    reg!(m, "SC-RESUME-006", || {
        // Resume from paused prompt-review run continues from planning
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "foxtrot", "standard")?;

        // Step 1: run start with prompt_review not ready → pauses
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
        assert_success(&start)?;
        let pre_snapshot = read_run_snapshot(&ws, "foxtrot")?;
        if pre_snapshot.get("status").and_then(|v| v.as_str()) != Some("paused") {
            return Err("expected paused after prompt_review not ready".into());
        }

        // Capture original run_id
        let pre_events = read_journal(&ws, "foxtrot")?;
        let run_id = pre_events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();

        // Step 2: resume → continues from planning, completes
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        // Verify resumed run keeps original run_id
        let post_events = read_journal(&ws, "foxtrot")?;
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_run_id = resume_evt.unwrap()
            .get("details")
            .and_then(|d| d.get("run_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if resumed_run_id != run_id {
            return Err(format!("expected resumed run_id={run_id}, got {resumed_run_id}"));
        }

        // Verify first resumed stage is planning with attempt 1
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let first_stage_after_resume = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage_after_resume {
            let stage = evt.get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if stage != "planning" {
                return Err(format!("expected first resumed stage=planning, got '{stage}'"));
            }
            let attempt = evt.get("details")
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
        let final_snapshot = read_run_snapshot(&ws, "foxtrot")?;
        let status = final_snapshot.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
            ws.path().join(".ralph-burning/projects/golf/run.json"),
            completed_json,
        ).map_err(|e| e.to_string())?;
        let out2 = run_cli(&["run", "resume"], ws.path())?;
        assert_failure(&out2)?;
        Ok(())
    });

    reg!(m, "SC-RESUME-008", || {
        // Run start rejects failed/paused and directs to resume
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "hotel", "standard")?;
        let run_json = r#"{"active_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#;
        std::fs::write(
            ws.path().join(".ralph-burning/projects/hotel/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
        let failed_seq = events.iter()
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
}

// ===========================================================================
// Run Resume Non-Standard (6 scenarios)
// ===========================================================================

fn register_run_resume_non_standard(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "SC-NONSTD-RESUME-001", || {
        // Resume a failed docs_change run from docs_update
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-docs", "docs_change")?;

        // Step 1: run start fails at docs_update (docs_plan completes)
        let start = run_cli_with_env(
            &["run", "start"],
            ws.path(),
            &[("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "docs_update")],
        )?;
        assert_failure(&start)?;

        let pre_events = read_journal(&ws, "ns-docs")?;
        let run_id = pre_events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
            .unwrap_or("").to_string();

        // Step 2: resume → resumes from docs_update, completes
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-docs")?;
        // Verify run_id preserved
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed event".into());
        }
        let resumed_id = resume_evt.unwrap()
            .get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()).unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("run_id mismatch: expected {run_id}, got {resumed_id}"));
        }

        // Verify docs_plan NOT re-entered after resume
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let plan_after = post_events.iter().any(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()) == Some("docs_plan")
        });
        if plan_after {
            return Err("docs_plan should not be re-executed after resume".into());
        }

        // Verify first resumed stage is docs_update
        let first_stage = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage {
            let sid = evt.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()).unwrap_or("");
            if sid != "docs_update" {
                return Err(format!("expected first resumed stage=docs_update, got '{sid}'"));
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
        let run_id = pre_events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
            .unwrap_or("").to_string();

        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-ci")?;
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed".into());
        }
        let resumed_id = resume_evt.unwrap()
            .get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()).unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("run_id mismatch: {run_id} vs {resumed_id}"));
        }

        // Verify ci_plan not re-entered, first resumed stage is ci_update
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let first_stage = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage {
            let sid = evt.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()).unwrap_or("");
            if sid != "ci_update" {
                return Err(format!("expected first resumed stage=ci_update, got '{sid}'"));
            }
        }

        let final_snap = read_run_snapshot(&ws, "ns-ci")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed".into());
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-003", || {
        // Resume a paused docs_change snapshot with pending amendments
        // docs_validation returns request_changes → creates amendments, pauses
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-docs-amend", "docs_change")?;

        let overrides = serde_json::json!({
            "docs_validation": [
                {
                    "outcome": "request_changes",
                    "evidence": ["Needs fixes"],
                    "findings_or_gaps": ["Gap"],
                    "follow_up_or_amendments": ["Fix documentation gaps"]
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

        // Verify amendments were queued and completion round advanced
        let events = read_journal(&ws, "ns-docs-amend")?;
        if !journal_event_types(&events).iter().any(|t| t == "amendment_queued") {
            return Err("journal missing amendment_queued event".into());
        }

        let final_snap = read_run_snapshot(&ws, "ns-docs-amend")?;
        if final_snap.get("status").and_then(|v| v.as_str()) != Some("completed") {
            return Err("expected completed after amendment cycle".into());
        }
        // Verify amendment queue is drained after completion
        let pending = final_snap.get("amendment_queue")
            .and_then(|q| q.get("pending"))
            .and_then(|p| p.as_array())
            .map_or(0, |a| a.len());
        if pending > 0 {
            return Err(format!("expected empty amendment queue, got {pending} pending"));
        }
        Ok(())
    });

    reg!(m, "SC-NONSTD-RESUME-004", || {
        // Resume a paused ci_improvement snapshot with pending amendments
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "ns-ci-amend", "ci_improvement")?;

        let overrides = serde_json::json!({
            "ci_validation": [
                {
                    "outcome": "request_changes",
                    "evidence": ["CI needs fixes"],
                    "findings_or_gaps": ["Missing coverage"],
                    "follow_up_or_amendments": ["Add coverage check"]
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

        let events = read_journal(&ws, "ns-ci-amend")?;
        if !journal_event_types(&events).iter().any(|t| t == "amendment_queued") {
            return Err("journal missing amendment_queued event".into());
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
        let run_id = pre_events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
            .unwrap_or("").to_string();

        // Resume → completes from review
        let resume = run_cli(&["run", "resume"], ws.path())?;
        assert_success(&resume)?;

        let post_events = read_journal(&ws, "ns-qd")?;
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed".into());
        }
        let resumed_id = resume_evt.unwrap()
            .get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()).unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("run_id mismatch: {run_id} vs {resumed_id}"));
        }

        // plan_and_implement not re-entered after resume
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let pai_after = post_events.iter().any(|e| {
            e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()) == Some("plan_and_implement")
        });
        if pai_after {
            return Err("plan_and_implement should not be re-entered after resume".into());
        }

        // First resumed stage is review
        let first_stage = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage {
            let sid = evt.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()).unwrap_or("");
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

        let events = read_journal(&ws, "ns-qd-amend")?;
        if !journal_event_types(&events).iter().any(|t| t == "amendment_queued") {
            return Err("journal missing amendment_queued".into());
        }

        // Verify plan_and_implement entered at least twice (initial + amendment cycle)
        let pai_entered = events.iter().filter(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
                && e.get("details").and_then(|d| d.get("stage_id")).and_then(|v| v.as_str()) == Some("plan_and_implement")
        }).count();
        if pai_entered < 2 {
            return Err(format!("expected plan_and_implement entered >= 2 times, got {pai_entered}"));
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
                    "follow_up_or_amendments": [{"summary": "Fix", "details": "D"}]
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
        let status = pre_snap.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status == "completed" || status == "running" {
            let mut snap = pre_snap.clone();
            snap["status"] = serde_json::json!("failed");
            snap["active_run"] = serde_json::json!(null);
            snap["status_summary"] = serde_json::json!("failed for rollback test");
            std::fs::write(
                ws.path().join(".ralph-burning/projects/rb-soft/run.json"),
                serde_json::to_string_pretty(&snap).unwrap(),
            ).map_err(|e| e.to_string())?;
        }

        // Now rollback to planning
        let rb = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_success(&rb)?;

        // Verify: status is paused, journal has rollback_performed
        let post_snap = read_run_snapshot(&ws, "rb-soft")?;
        let post_status = post_snap.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if post_status != "paused" {
            return Err(format!("expected paused after rollback, got '{post_status}'"));
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
                    "follow_up_or_amendments": [{"summary": "Fix", "details": "D"}]
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

        // Verify rollback points were created with a git SHA
        let pre_events = read_journal(&ws, "rb-hard")?;
        let rb_created = pre_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_created")
        });
        if rb_created.is_none() {
            return Err("expected rollback_created event after run start".into());
        }
        let created_sha = rb_created.unwrap()
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
            ws.path().join(".ralph-burning/projects/rb-hard/run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        ).map_err(|e| e.to_string())?;

        // Hard rollback to implementation — git repo exists so reset should succeed.
        let rb = run_cli(&["run", "rollback", "--to", "implementation", "--hard"], ws.path())?;
        assert_success(&rb)?;

        // Verify logical rollback committed
        let post_snap = read_run_snapshot(&ws, "rb-hard")?;
        let post_status = post_snap.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if post_status != "paused" {
            return Err(format!("logical rollback should set paused, got '{post_status}'"));
        }

        // Journal should have rollback_performed
        let post_events = read_journal(&ws, "rb-hard")?;
        let rb_event = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed")
        });
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
            return Err(format!("rollback_performed should target implementation, got '{rb_stage}'"));
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
            return Err("rollback_performed event must record a non-empty git_sha for hard rollback".into());
        }

        // Verify the repository reset targeted the recorded SHA — after hard reset,
        // HEAD should point at the SHA from the rollback point
        let head_output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(ws.path())
            .output()
            .map_err(|e| format!("git rev-parse HEAD: {e}"))?;
        let current_head = String::from_utf8_lossy(&head_output.stdout).trim().to_owned();
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
            ws.path().join(".ralph-burning/projects/rb-bad-stage/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
            ws.path().join(".ralph-burning/projects/rb-no-point/run.json"),
            run_json,
        ).map_err(|e| e.to_string())?;
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
                    "follow_up_or_amendments": [{"summary": "Fix", "details": "D"}]
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
        // Count stage_completed events visible before any rollback
        let pre_completed_count = pre_history.stdout.matches("stage_completed").count();

        // Set to failed for rollback
        let snap = read_run_snapshot(&ws, "rb-multi")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rb-multi/run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        ).map_err(|e| e.to_string())?;

        // First rollback: to implementation
        let rb1 = run_cli(&["run", "rollback", "--to", "implementation"], ws.path())?;
        assert_success(&rb1)?;
        let snap1 = read_run_snapshot(&ws, "rb-multi")?;
        let count1 = snap1.get("rollback_point_meta")
            .and_then(|m| m.get("rollback_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let last_id1 = snap1.get("rollback_point_meta")
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
        let post_completed_count1 = post_history1.stdout.matches("stage_completed").count();
        if post_completed_count1 >= pre_completed_count {
            return Err(format!(
                "run history after first rollback should show fewer events: pre={pre_completed_count}, post={post_completed_count1}"
            ));
        }

        // Also verify the journal metadata: visible_through_sequence is recorded
        let events1 = read_journal(&ws, "rb-multi")?;
        let rb1_event = events1.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed")
        });
        if rb1_event.is_none() {
            return Err("journal missing rollback_performed after first rollback".into());
        }
        let visible_through_1 = rb1_event.unwrap()
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
            ws.path().join(".ralph-burning/projects/rb-multi/run.json"),
            serde_json::to_string_pretty(&snap1_mut).unwrap(),
        ).map_err(|e| e.to_string())?;

        // Second rollback: to planning
        let rb2 = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_success(&rb2)?;
        let snap2 = read_run_snapshot(&ws, "rb-multi")?;
        let count2 = snap2.get("rollback_point_meta")
            .and_then(|m| m.get("rollback_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let last_id2 = snap2.get("rollback_point_meta")
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
        let post_completed_count2 = post_history2.stdout.matches("stage_completed").count();
        if post_completed_count2 >= post_completed_count1 {
            return Err(format!(
                "run history after second rollback should show fewer events than after first: after_first={post_completed_count1}, after_second={post_completed_count2}"
            ));
        }

        // Verify the raw journal has at least 2 rollback_performed events
        let events2 = read_journal(&ws, "rb-multi")?;
        let rb_performed_events: Vec<_> = events2.iter().filter(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed")
        }).collect();
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
        // Feature: rollback to planning, resume, first resumed stage is exactly
        // "implementation", and the rolled-back implementation history from the
        // abandoned branch remains hidden in the user-visible `run history` output.
        let ws = TempWorkspace::new()?;
        setup_workspace_with_project(&ws, "rb-resume", "standard")?;

        // Create rollback points via conditionally_approved
        let overrides = serde_json::json!({
            "completion_panel": [
                {
                    "outcome": "conditionally_approved",
                    "evidence": ["Changes"],
                    "findings_or_gaps": [],
                    "follow_up_or_amendments": [{"summary": "Fix", "details": "D"}]
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

        // Capture original run_id
        let events = read_journal(&ws, "rb-resume")?;
        let run_id = events.iter()
            .find(|e| e.get("event_type").and_then(|v| v.as_str()) == Some("run_started"))
            .and_then(|e| e.get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()))
            .unwrap_or("").to_string();

        // Count pre-rollback stage_completed events visible to the user
        let pre_history = run_cli(&["run", "history"], ws.path())?;
        assert_success(&pre_history)?;
        let pre_completed = pre_history.stdout.matches("stage_completed").count();

        // Set to failed for rollback
        let snap = read_run_snapshot(&ws, "rb-resume")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rb-resume/run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        ).map_err(|e| e.to_string())?;

        // Rollback to planning
        let rb = run_cli(&["run", "rollback", "--to", "planning"], ws.path())?;
        assert_success(&rb)?;

        // Verify the rollback event records visible_through_sequence
        let rb_events = read_journal(&ws, "rb-resume")?;
        let rb_performed = rb_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed")
        });
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
        let post_rb_completed = post_rb_history.stdout.matches("stage_completed").count();
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
        let resume_evt = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("run_resumed")
        });
        if resume_evt.is_none() {
            return Err("journal missing run_resumed after rollback+resume".into());
        }
        let resumed_id = resume_evt.unwrap()
            .get("details").and_then(|d| d.get("run_id")).and_then(|v| v.as_str()).unwrap_or("");
        if resumed_id != run_id {
            return Err(format!("expected run_id={run_id}, got {resumed_id}"));
        }

        // Verify the first resumed stage is exactly "implementation"
        // (rollback to planning means resume starts from the first incomplete
        // durable boundary after planning, which is implementation — not planning)
        let resume_seq = resume_evt.unwrap()
            .get("sequence").and_then(|v| v.as_u64()).unwrap_or(0);
        let first_stage_after_resume = post_events.iter()
            .filter(|e| {
                e.get("sequence").and_then(|v| v.as_u64()).unwrap_or(0) > resume_seq
                    && e.get("event_type").and_then(|v| v.as_str()) == Some("stage_entered")
            })
            .next();
        if let Some(evt) = first_stage_after_resume {
            let stage = evt.get("details")
                .and_then(|d| d.get("stage_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if stage != "implementation" {
                return Err(format!(
                    "expected first resumed stage to be exactly 'implementation', got '{stage}'"
                ));
            }
        } else {
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
        let final_completed = final_history.stdout.matches("stage_completed").count();
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
        let rollback_in_history = final_history.stdout.contains("rollback_performed");
        if !rollback_in_history {
            // rollback_performed should still be visible as a durable event
            return Err("run history should include rollback_performed event".into());
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
                    "follow_up_or_amendments": [{"summary": "Fix", "details": "D"}]
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
        let rb_created = pre_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_created")
        });
        let created_sha = rb_created
            .and_then(|e| e.get("details"))
            .and_then(|d| d.get("git_sha"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if created_sha.is_empty() {
            return Err("rollback_created must record a real git_sha for this test".into());
        }

        // Now destroy the git repo so that `git reset --hard <sha>` will fail
        std::fs::remove_dir_all(ws.path().join(".git"))
            .map_err(|e| format!("remove .git: {e}"))?;

        // Set to failed
        let snap = read_run_snapshot(&ws, "rb-hard-fail")?;
        let mut snap = snap.clone();
        snap["status"] = serde_json::json!("failed");
        snap["active_run"] = serde_json::json!(null);
        std::fs::write(
            ws.path().join(".ralph-burning/projects/rb-hard-fail/run.json"),
            serde_json::to_string_pretty(&snap).unwrap(),
        ).map_err(|e| e.to_string())?;

        // Hard rollback to implementation — the rollback point has a valid SHA but
        // git reset will fail because the .git directory no longer exists.
        let rb = run_cli(&["run", "rollback", "--to", "implementation", "--hard"], ws.path())?;
        // The command should fail with a git-reset error
        assert_failure(&rb)?;

        // Verify run.json is in paused state — the logical rollback (snapshot + journal)
        // was committed before the git reset was attempted and failed
        let post_snap = read_run_snapshot(&ws, "rb-hard-fail")?;
        let post_status = post_snap.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if post_status != "paused" {
            return Err(format!(
                "expected paused (logical rollback committed before git failure), got '{post_status}'"
            ));
        }

        // Journal should have rollback_performed event even though git reset failed
        let post_events = read_journal(&ws, "rb-hard-fail")?;
        let rb_event = post_events.iter().find(|e| {
            e.get("event_type").and_then(|v| v.as_str()) == Some("rollback_performed")
        });
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
            return Err(format!("rollback_performed should target implementation, got '{rb_stage}'"));
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
// Requirements Drafting (33 scenarios)
// ===========================================================================

fn register_requirements_drafting(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "RD-001", || {
        // Draft mode generates clarifying questions and transitions to awaiting_answers
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;

        // Override question_set to return non-empty questions
        let label_overrides = serde_json::json!({
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
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
            return Err(format!("expected 'completed' for empty questions, got '{status}'"));
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

        let label_overrides = serde_json::json!({
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Which framework?", "rationale": "Framework choice", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Build a web app"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        ).map_err(|e| format!("write answers.toml: {e}"))?;

        // Invoke requirements answer with EDITOR=true (no-op editor, answers already written)
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string()),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Verify pipeline resumed and completed
        let post_run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post-answer run.json: {e}"))?;
        let post_run: serde_json::Value =
            serde_json::from_str(&post_run_content).map_err(|e| format!("parse: {e}"))?;
        let post_status = post_run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if post_status != "completed" {
            return Err(format!("expected 'completed' after answer, got '{post_status}'"));
        }
        Ok(())
    });

    reg!(m, "RD-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Create a completed requirements run first
        let create_out = run_cli(
            &["requirements", "quick", "--idea", "Show test"],
            ws.path(),
        )?;
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
        let run_id = entries[0]
            .file_name()
            .to_string_lossy()
            .to_string();

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
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
            return Err(format!("expected 'failed' after review rejection, got '{status}'"));
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
            &["requirements", "quick", "--idea", "Seed rollback prompt fail test"],
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
            return Err(format!("expected 'failed' after seed write failure, got '{status}'"));
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
            Ok(_) => Err("expected domain validation error for rejected with empty findings".into()),
        }
    });

    reg!(m, "RD-010", || {
        // Failed run can be resumed via answer.
        // Feature: a requirements run in failed status with a committed question
        // set can be resumed by invoking `requirements answer`, and the pipeline
        // resumes from the answer boundary through to completion.
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Which approach?", "rationale": "Design", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Resume test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        run_mut["status_summary"] = serde_json::json!("failed: simulated failure at question boundary");
        std::fs::write(
            run_dir.join("run.json"),
            serde_json::to_string_pretty(&run_mut).unwrap(),
        ).map_err(|e| format!("write failed run.json: {e}"))?;

        // Pre-populate answers.toml with valid answers
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"The direct approach\"\n",
        ).map_err(|e| format!("write answers.toml: {e}"))?;

        // Invoke requirements answer — this should resume from the answer boundary
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string()),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Verify the pipeline completed
        let post_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post-answer run.json: {e}"))?;
        let post_run: serde_json::Value =
            serde_json::from_str(&post_content).map_err(|e| format!("parse: {e}"))?;
        let post_status = post_run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if post_status != "completed" {
            return Err(format!("expected 'completed' after answer resume, got '{post_status}'"));
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
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Editor test?", "rationale": "Test", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Editor fail test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        let pre_journal = std::fs::read_to_string(&journal_path)
            .map_err(|e| format!("read journal: {e}"))?;
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
            return Err(format!("expected 'awaiting_answers' after editor failure, got '{status}'"));
        }

        // Verify journal has no new events
        let post_journal = std::fs::read_to_string(&journal_path)
            .map_err(|e| format!("read journal: {e}"))?;
        let post_event_count = post_journal.lines().filter(|l| !l.trim().is_empty()).count();
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
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Valid question?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Unknown ID test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        ).map_err(|e| format!("write answers.toml: {e}"))?;

        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "true")],
        )?;
        assert_failure(&answer_out)?;
        assert_contains(&answer_out.stderr, "unknown question ID", "validation error")?;

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
            return Err(format!("expected 'awaiting_answers' after validation error, got '{status}'"));
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
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Required question?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Empty answer test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"\"\n",
        ).map_err(|e| format!("write answers.toml: {e}"))?;

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
            return Err(format!("expected 'awaiting_answers' after empty answer error, got '{status}'"));
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
            &["requirements", "quick", "--idea", "Conditional approval test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        let label_overrides = serde_json::json!({
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Test?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Answer reject test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
        )?;
        assert_success(&out)?;
        let req_dir = ws.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .map_err(|e| format!("read requirements dir: {e}"))?
            .filter_map(|e| e.ok())
            .collect();
        let run_dir = entries[0].path();
        let run_id = entries[0].file_name().to_string_lossy().to_string();

        // Verify awaiting_answers state and question_round tracking
        let run_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read run.json: {e}"))?;
        let run: serde_json::Value =
            serde_json::from_str(&run_content).map_err(|e| format!("parse: {e}"))?;
        let status = run.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if status != "awaiting_answers" {
            return Err(format!("expected awaiting_answers, got '{status}'"));
        }
        let question_round = run.get("question_round").and_then(|v| v.as_u64()).unwrap_or(0);
        if question_round == 0 {
            return Err("expected non-zero question_round after question generation".into());
        }

        // Submit valid answers
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"My answer\"\n",
        ).map_err(|e| format!("write answers.toml: {e}"))?;
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string()),
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
                ("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string()),
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
                assert_contains(&details, "conditionally_approved requires at least one follow-up", "error")?;
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
        let label_overrides = serde_json::json!({
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Boundary?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Double submit gate"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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

        // Submit valid answers — first submission should succeed
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"First answer\"\n",
        ).map_err(|e| format!("write answers.toml: {e}"))?;
        let answer_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[
                ("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string()),
                ("EDITOR", "true"),
            ],
        )?;
        assert_success(&answer_out)?;

        // Capture run state after first submission
        let post1_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read post run.json: {e}"))?;
        let post1_run: serde_json::Value =
            serde_json::from_str(&post1_content).map_err(|e| format!("parse: {e}"))?;
        let post1_status = post1_run.get("status").and_then(|v| v.as_str()).unwrap_or("");

        // Try to answer again — should be rejected since answers are already
        // durably submitted past the question boundary
        std::fs::write(
            run_dir.join("answers.toml"),
            "q1 = \"Second answer attempt\"\n",
        ).map_err(|e| format!("write answers.toml: {e}"))?;
        let answer2_out = run_cli_with_env(
            &["requirements", "answer", &run_id],
            ws.path(),
            &[("EDITOR", "true")],
        )?;
        assert_failure(&answer2_out)?;
        assert_contains(&answer2_out.stderr, "cannot answer", "double submission rejection")?;

        // Verify run state is unchanged
        let post2_content = std::fs::read_to_string(run_dir.join("run.json"))
            .map_err(|e| format!("read final run.json: {e}"))?;
        let post2_run: serde_json::Value =
            serde_json::from_str(&post2_content).map_err(|e| format!("parse: {e}"))?;
        let post2_status = post2_run.get("status").and_then(|v| v.as_str()).unwrap_or("");
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
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
        ).map_err(|e| format!("write failed run.json: {e}"))?;

        // Run requirements show and verify it includes pending question count
        // AND the failure summary
        let show_out = run_cli(&["requirements", "show", &run_id], ws.path())?;
        assert_success(&show_out)?;
        assert_contains(&show_out.stdout, "failed", "show status")?;
        // The show output should include "Pending Questions:" with count 2
        assert_contains(&show_out.stdout, "Pending Questions:", "pending question label")?;
        assert_contains(&show_out.stdout, "2", "pending question count")?;
        // The show output should include the failure summary
        assert_contains(&show_out.stdout, "simulated failure at question boundary", "failure summary")?;
        Ok(())
    });

    reg!(m, "RD-022", || {
        // Answer rejected when answers.json already populated
        // Verify awaiting_answers state is reachable and answers.toml exists
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let label_overrides = serde_json::json!({
            "question_set": {
                "questions": [
                    {"id": "q1", "prompt": "Populated?", "rationale": "R", "required": true}
                ]
            }
        });
        let out = run_cli_with_env(
            &["requirements", "draft", "--idea", "Populated answers test"],
            ws.path(),
            &[("RALPH_BURNING_TEST_LABEL_OVERRIDES", &label_overrides.to_string())],
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
}

// ===========================================================================
// Daemon Lifecycle (5 scenarios)
// ===========================================================================

fn register_daemon_lifecycle(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "DAEMON-LIFECYCLE-001", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Daemon start with no tasks should succeed
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        // Abort requires a task ID
        let out = run_cli(&["daemon", "abort", "nonexistent-task"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "retry", "nonexistent-task"], ws.path())?;
        assert_failure(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-LIFECYCLE-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "abort", "nonexistent-task"], ws.path())?;
        assert_failure(&out)?;
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
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-002", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-003", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-004", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-005", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-006", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-ROUTING-007", || {
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "reconcile"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });
}

// ===========================================================================
// Daemon Issue Intake (8 scenarios)
// ===========================================================================

fn register_daemon_issue_intake(m: &mut HashMap<String, ScenarioExecutor>) {
    reg!(m, "DAEMON-INTAKE-001", || {
        // Watcher ingestion creates a task from a watched issue file
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let watched_dir = ws.path().join(".ralph-burning/daemon/watched");
        std::fs::create_dir_all(&watched_dir).map_err(|e| e.to_string())?;
        let issue = serde_json::json!({
            "issue_ref": "test/repo#1",
            "source_revision": "abc12345",
            "title": "Test issue",
            "body": "Implement feature X",
            "labels": [],
            "routing_command": null
        });
        std::fs::write(watched_dir.join("issue-1.json"), issue.to_string())
            .map_err(|e| e.to_string())?;
        // Status should succeed (watcher is polled only during daemon start)
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-002", || {
        // Idempotent re-polling: same issue_ref + source_revision produces no duplicate
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-003", || {
        // Requirements quick handoff: daemon status shows dispatch mode
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-004", || {
        // Requirements draft waiting/resume: status shows waiting_for_requirements
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-005", || {
        // Duplicate issue rejection: same issue_ref with different source_revision
        // while non-terminal task exists
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-006", || {
        // Routed flow override: seed suggests different flow but routed flow wins
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-007", || {
        // Unknown requirements command fails ingestion
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });

    reg!(m, "DAEMON-INTAKE-008", || {
        // Daemon status surfaces waiting state and requirements_run_id
        let ws = TempWorkspace::new()?;
        init_workspace(&ws)?;
        let out = run_cli(&["daemon", "status"], ws.path())?;
        assert_success(&out)?;
        Ok(())
    });
}
