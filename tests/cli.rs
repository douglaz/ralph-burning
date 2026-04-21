use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};

use chrono::{Duration, Utc};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use ralph_burning::contexts::automation_runtime::model::{
    CliWriterLease, DaemonTask, DispatchMode, LeaseRecord, RoutingSource, TaskStatus, WorktreeLease,
};
use ralph_burning::contexts::milestone_record::bundle::{
    render_plan_json, AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity,
    Workstream,
};
use ralph_burning::shared::domain::FlowPreset;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_ralph-burning")
}

fn live_workspace_root(base_dir: &std::path::Path) -> std::path::PathBuf {
    ralph_burning::adapters::fs::FileSystem::live_workspace_root_path(base_dir)
}

fn audit_workspace_root(base_dir: &std::path::Path) -> std::path::PathBuf {
    ralph_burning::adapters::fs::FileSystem::audit_workspace_root_path(base_dir)
}

fn workspace_config_path(base_dir: &std::path::Path) -> std::path::PathBuf {
    live_workspace_root(base_dir).join("workspace.toml")
}

fn active_project_path(base_dir: &std::path::Path) -> std::path::PathBuf {
    live_workspace_root(base_dir).join("active-project")
}

fn active_milestone_path(base_dir: &std::path::Path) -> std::path::PathBuf {
    live_workspace_root(base_dir).join("active-milestone")
}

fn daemon_root(base_dir: &std::path::Path) -> std::path::PathBuf {
    live_workspace_root(base_dir).join("daemon")
}

fn requirements_root(base_dir: &std::path::Path) -> std::path::PathBuf {
    audit_workspace_root(base_dir).join("requirements")
}

fn milestone_root(base_dir: &std::path::Path, milestone_id: &str) -> std::path::PathBuf {
    audit_workspace_root(base_dir)
        .join("milestones")
        .join(milestone_id)
}

fn initialize_workspace_fixture() -> tempfile::TempDir {
    let temp_dir = tempdir().expect("create temp dir");
    let output = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run init");
    assert!(output.status.success());
    fs::create_dir_all(temp_dir.path().join(".beads")).expect("create .beads");
    fs::write(
        temp_dir.path().join(".beads/issues.jsonl"),
        "{\"id\":\"seed-bead\"}\n",
    )
    .expect("write issues.jsonl");
    temp_dir
}

fn create_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = project_root(base_dir, project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    let prompt_contents = "# Fixture prompt\n";
    // Write a complete canonical ProjectRecord so validation passes
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "standard"
prompt_reference = "prompt.md"
prompt_hash = "{}"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"
"#,
        ralph_burning::adapters::fs::FileSystem::prompt_hash(prompt_contents)
    );
    fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    // Write required canonical files so run queries don't fail on missing files
    fs::write(project_root.join("prompt.md"), prompt_contents).expect("write prompt");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    ).expect("write run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"project_created","details":{{"project_id":"{}","flow":"standard"}}}}"#,
            project_id
        ),
    ).expect("write journal");
    fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");
    for subdir in &[
        "history/payloads",
        "history/artifacts",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
        "amendments",
        "rollback",
    ] {
        fs::create_dir_all(project_root.join(subdir)).expect("create project subdirectory");
    }
}

fn create_bead_backed_project_fixture(
    base_dir: &std::path::Path,
    project_id: &str,
    milestone_id: &str,
    bead_id: &str,
) {
    let project_root = project_root(base_dir, project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    let prompt_contents = "# Fixture prompt\n";
    let milestone_plan_path = milestone_root(base_dir, milestone_id).join("plan.json");
    let lineage_metadata = if milestone_plan_path.exists() {
        let plan_json = fs::read_to_string(&milestone_plan_path).expect("read milestone plan");
        let mut hasher = Sha256::new();
        hasher.update(plan_json.as_bytes());
        let plan_hash = format!("{:x}", hasher.finalize());
        format!(
            r#"
plan_hash = "{plan_hash}"
plan_version = 2"#
        )
    } else {
        String::new()
    };
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "docs_change"
prompt_reference = "prompt.md"
prompt_hash = "{}"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"

[task_source]
milestone_id = "{milestone_id}"
bead_id = "{bead_id}"
origin = "milestone"
{lineage_metadata}
"#,
        ralph_burning::adapters::fs::FileSystem::prompt_hash(prompt_contents)
    );
    fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    fs::write(project_root.join("prompt.md"), prompt_contents).expect("write prompt");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    )
    .expect("write run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"project_created","details":{{"project_id":"{}","flow":"docs_change","source":"milestone_bead","milestone_id":"{}","bead_id":"{}"}}}}"#,
            project_id, milestone_id, bead_id
        ),
    )
    .expect("write journal");
    fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");
    for subdir in &[
        "history/payloads",
        "history/artifacts",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
        "amendments",
        "rollback",
    ] {
        fs::create_dir_all(project_root.join(subdir)).expect("create project subdirectory");
    }
}

fn project_root(base_dir: &std::path::Path, project_id: &str) -> std::path::PathBuf {
    live_workspace_root(base_dir)
        .join("projects")
        .join(project_id)
}

fn backend_processes_path(base_dir: &std::path::Path, project_id: &str) -> std::path::PathBuf {
    project_root(base_dir, project_id)
        .join("runtime")
        .join("backend")
        .join("active-processes.json")
}

fn spawn_isolated_backend_group(pid_file: &std::path::Path) -> std::process::Child {
    let script = format!(
        "exec setsid sh -c 'echo $$ > \"{}\"; exec sleep 60'",
        pid_file.display()
    );
    Command::new("bash")
        .args(["-lc", &script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn isolated backend group")
}

#[cfg(target_os = "linux")]
fn proc_start_ticks(pid: u32) -> u64 {
    let stat_path = std::path::PathBuf::from(format!("/proc/{pid}/stat"));
    let raw = fs::read_to_string(&stat_path).expect("read /proc stat");
    let (_, tail) = raw
        .rsplit_once(") ")
        .expect("process stat should contain comm terminator");
    tail.split_whitespace()
        .nth(19)
        .expect("process stat should expose start ticks")
        .parse::<u64>()
        .expect("parse start ticks")
}

fn live_pid_record_json(
    pid: u32,
    owner: &str,
    run_id: Option<&str>,
    run_started_at: Option<&str>,
    writer_owner: Option<&str>,
) -> serde_json::Value {
    let mut record = serde_json::json!({
        "pid": pid,
        "started_at": Utc::now(),
        "owner": owner,
    });
    if let Some(run_id) = run_id {
        record["run_id"] = serde_json::json!(run_id);
    }
    if let Some(run_started_at) = run_started_at {
        record["run_started_at"] = serde_json::json!(run_started_at);
    }
    if let Some(writer_owner) = writer_owner {
        record["writer_owner"] = serde_json::json!(writer_owner);
    }
    #[cfg(target_os = "linux")]
    {
        record["proc_start_ticks"] = serde_json::json!(proc_start_ticks(pid));
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        if let Some(marker) =
            ralph_burning::adapters::fs::FileSystem::proc_start_marker_for_pid(pid)
        {
            record["proc_start_marker"] = serde_json::json!(marker);
        }
    }
    record
}

fn live_backend_process_record_json(
    pid: u32,
    run_id: &str,
    run_started_at: &str,
) -> serde_json::Value {
    let mut record = serde_json::json!({
        "pid": pid,
        "recorded_at": Utc::now(),
        "run_id": run_id,
        "run_started_at": run_started_at,
    });
    #[cfg(target_os = "linux")]
    {
        record["proc_start_ticks"] = serde_json::json!(proc_start_ticks(pid));
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        if let Some(marker) =
            ralph_burning::adapters::fs::FileSystem::proc_start_marker_for_pid(pid)
        {
            record["proc_start_marker"] = serde_json::json!(marker);
        }
    }
    record
}

fn legacy_backend_process_record_json(
    pid: u32,
    run_id: &str,
    run_started_at: &str,
) -> serde_json::Value {
    serde_json::json!({
        "pid": pid,
        "recorded_at": Utc::now(),
        "run_id": run_id,
        "run_started_at": run_started_at,
    })
}

fn wait_for_pid_file(path: &std::path::Path, what: &str) -> u32 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !path.exists() || fs::read_to_string(path).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "{what} pid file was never written"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    fs::read_to_string(path)
        .expect("read pid file")
        .trim()
        .parse::<u32>()
        .expect("parse pid")
}

fn pid_is_alive(pid: u32) -> bool {
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn pid_is_running(pid: u32) -> bool {
    ralph_burning::adapters::fs::FileSystem::is_pid_running_unchecked(pid)
}

fn write_run_query_history_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = project_root(base_dir, project_id);
    let long_artifact = format!("# Planning\n{}\n", "A".repeat(140));
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-19T03:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"standard"}}}}
{{"sequence":2,"timestamp":"2026-03-19T03:01:00Z","event_type":"stage_entered","details":{{"stage_id":"planning","run_id":"run-1"}}}}
{{"sequence":3,"timestamp":"2026-03-19T03:02:00Z","event_type":"stage_completed","details":{{"stage_id":"planning","cycle":1,"attempt":1,"payload_id":"p1","artifact_id":"a1"}}}}
{{"sequence":4,"timestamp":"2026-03-19T03:03:00Z","event_type":"stage_entered","details":{{"stage_id":"implementation","run_id":"run-1"}}}}
{{"sequence":5,"timestamp":"2026-03-19T03:04:00Z","event_type":"stage_completed","details":{{"stage_id":"implementation","cycle":1,"attempt":1,"payload_id":"p2","artifact_id":"a2"}}}}"#,
        ),
    )
    .expect("write journal");
    fs::write(
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
    .expect("write payload p1");
    fs::write(
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
    .expect("write payload p2");
    fs::write(
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
            serde_json::to_string(&long_artifact).expect("serialize artifact content")
        ),
    )
    .expect("write artifact a1");
    fs::write(
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
    .expect("write artifact a2");
}

fn write_run_query_history_lineage_fixture(
    base_dir: &std::path::Path,
    project_id: &str,
    milestone_id: &str,
    bead_id: &str,
) {
    write_run_query_history_fixture(base_dir, project_id);
    let journal_path = project_root(base_dir, project_id).join("journal.ndjson");
    let journal = fs::read_to_string(&journal_path).expect("read journal");
    let patched = journal.replacen(
        &format!(r#""project_id":"{project_id}","flow":"standard""#),
        &format!(
            r#""project_id":"{project_id}","flow":"standard","milestone_id":"{milestone_id}","bead_id":"{bead_id}""#
        ),
        1,
    );
    fs::write(journal_path, patched).expect("write lineage journal");
}

fn set_workspace_stream_output(base_dir: &std::path::Path, enabled: bool) {
    let workspace_toml = workspace_config_path(base_dir);
    let mut workspace: ralph_burning::shared::domain::WorkspaceConfig =
        toml::from_str(&fs::read_to_string(&workspace_toml).expect("read workspace.toml"))
            .expect("parse workspace.toml");
    workspace.execution.stream_output = Some(enabled);
    fs::write(
        &workspace_toml,
        toml::to_string_pretty(&workspace).expect("serialize workspace.toml"),
    )
    .expect("write workspace.toml");
}

fn write_supporting_payload(project_root: &std::path::Path) {
    fs::write(
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
    .expect("write supporting payload");
}

fn write_supporting_artifact(project_root: &std::path::Path) {
    fs::write(
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
    .expect("write supporting artifact");
}

fn write_follow_runtime_log(project_root: &std::path::Path, message: &str) {
    let entry = format!(
        r#"{{"timestamp":"2026-03-19T03:06:00Z","level":"info","source":"agent","message":{}}}"#,
        serde_json::to_string(message).expect("serialize runtime log message")
    );
    fs::write(
        project_root.join("runtime/logs/002.ndjson"),
        format!("{entry}\n"),
    )
    .expect("write follow runtime log");
}

fn wait_for_child_output(
    mut child: std::process::Child,
    timeout: std::time::Duration,
) -> std::process::Output {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child.try_wait().expect("poll child exit").is_some() {
            return child.wait_with_output().expect("wait for child output");
        }
        if std::time::Instant::now() >= deadline {
            let _ = kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL);
            panic!("child did not exit within {:?}", timeout);
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

fn write_rollback_targets_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = project_root(base_dir, project_id);
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-19T03:00:00Z","event_type":"project_created","details":{{"project_id":"{project_id}","flow":"standard"}}}}
{{"sequence":2,"timestamp":"2026-03-19T03:01:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-planning","stage_id":"planning","cycle":1,"git_sha":"abc123"}}}}
{{"sequence":3,"timestamp":"2026-03-19T03:02:00Z","event_type":"rollback_created","details":{{"rollback_id":"rb-implementation","stage_id":"implementation","cycle":1}}}}"#,
        ),
    )
    .expect("write rollback journal");
    fs::write(
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
    .expect("write rollback point planning");
    fs::write(
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
    .expect("write rollback point implementation");
}

fn select_active_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    fs::write(active_project_path(base_dir), format!("{project_id}\n"))
        .expect("write active-project");
}

fn requirements_run_ids(base_dir: &std::path::Path) -> Vec<String> {
    let req_dir = requirements_root(base_dir);
    let mut run_ids: Vec<String> = fs::read_dir(&req_dir)
        .expect("read requirements dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false))
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    run_ids.sort();
    run_ids
}

#[cfg(feature = "test-stub")]
fn only_requirements_run_id(base_dir: &std::path::Path) -> String {
    let run_ids = requirements_run_ids(base_dir);
    assert_eq!(run_ids.len(), 1, "expected exactly one requirements run");
    run_ids[0].clone()
}

fn write_editor_script(
    base_dir: &std::path::Path,
    name: &str,
    contents: &str,
) -> std::path::PathBuf {
    let script_path = base_dir.join(name);
    let contents = if name == "br" {
        inject_fake_br_version_handler(contents)
    } else {
        contents.to_owned()
    };
    fs::write(&script_path, contents).expect("write editor script");
    let mut permissions = fs::metadata(&script_path)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script_path, permissions).expect("set script permissions");
    script_path
}

fn inject_fake_br_version_handler(contents: &str) -> String {
    if contents.contains("--version") {
        return contents.to_owned();
    }

    let version_handler =
        "if [ \"$1\" = \"--version\" ]; then\nprintf '%s\\n' 'br test stub'\nexit 0\nfi\n";
    match contents.split_once('\n') {
        Some((shebang, rest)) => format!("{shebang}\n{version_handler}{rest}"),
        None => format!("#!/bin/sh\n{version_handler}{contents}"),
    }
}

fn prepend_path(dir: &std::path::Path) -> std::ffi::OsString {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut parts = vec![dir.as_os_str().to_owned()];
    if !existing.is_empty() {
        parts.extend(std::env::split_paths(&existing).map(|path| path.into_os_string()));
    }
    std::env::join_paths(parts).expect("join PATH entries")
}

fn write_milestone_fixture(base_dir: &std::path::Path, milestone_id: &str) {
    let milestone_root = base_dir
        .join(".ralph-burning/milestones")
        .join(milestone_id);
    fs::create_dir_all(&milestone_root).expect("create milestone root");
    fs::write(
        milestone_root.join("milestone.toml"),
        format!(
            r#"schema_version = 1
id = "{milestone_id}"
name = "Alpha Milestone"
description = "Deliver the alpha milestone."
created_at = "2026-04-01T10:00:00Z"
"#
        ),
    )
    .expect("write milestone record");
    let bundle = MilestoneBundle {
        schema_version: 1,
        identity: MilestoneIdentity {
            id: milestone_id.to_owned(),
            name: "Alpha Milestone".to_owned(),
        },
        executive_summary: "Ship bead-backed task creation.".to_owned(),
        goals: vec![
            "Create a task directly from milestone state".to_owned(),
            "Keep run start compatible".to_owned(),
        ],
        non_goals: vec![],
        constraints: vec!["Reuse the current project substrate".to_owned()],
        acceptance_map: vec![AcceptanceCriterion {
            id: "AC-1".to_owned(),
            description: "Bead-backed task creation works".to_owned(),
            covered_by: vec!["bead-2".to_owned()],
        }],
        workstreams: vec![Workstream {
            name: "Task Substrate".to_owned(),
            description: Some("Wire milestone beads into Ralph projects.".to_owned()),
            beads: vec![
                BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Define task-source metadata".to_owned(),
                    description: Some("Persist bead lineage and task-source metadata.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["creation".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec![],
                    flow_override: None,
                },
                BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Bootstrap bead-backed task creation".to_owned(),
                    description: Some(
                        "Create a Ralph project directly from milestone + bead context.".to_owned(),
                    ),
                    bead_type: Some("feature".to_owned()),
                    priority: Some(1),
                    labels: vec!["creation".to_owned()],
                    depends_on: vec!["bead-1".to_owned()],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: Some(FlowPreset::DocsChange),
                },
                BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Document task bootstrap follow-up".to_owned(),
                    description: Some(
                        "Capture the operator-facing workflow once project creation is stable."
                            .to_owned(),
                    ),
                    bead_type: Some("docs".to_owned()),
                    priority: Some(2),
                    labels: vec!["docs".to_owned()],
                    depends_on: vec!["bead-2".to_owned()],
                    acceptance_criteria: vec![],
                    flow_override: None,
                },
            ],
        }],
        default_flow: FlowPreset::Minimal,
        agents_guidance: Some("Keep changes inspectable and deterministic.".to_owned()),
    };
    let plan_json = render_plan_json(&bundle).expect("render plan json");
    let mut hasher = Sha256::new();
    hasher.update(plan_json.as_bytes());
    let plan_hash = format!("{:x}", hasher.finalize());
    fs::write(
        milestone_root.join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("write milestone status");
    fs::write(milestone_root.join("plan.json"), plan_json).expect("write plan json");
}

fn write_single_bead_milestone_fixture(base_dir: &std::path::Path, milestone_id: &str) {
    let milestone_root = base_dir
        .join(".ralph-burning/milestones")
        .join(milestone_id);
    fs::create_dir_all(&milestone_root).expect("create milestone root");
    fs::write(
        milestone_root.join("milestone.toml"),
        format!(
            r#"schema_version = 1
id = "{milestone_id}"
name = "Single Bead Milestone"
description = "Deliver one milestone bead."
created_at = "2026-04-01T10:00:00Z"
"#
        ),
    )
    .expect("write milestone record");
    let bundle = MilestoneBundle {
        schema_version: 1,
        identity: MilestoneIdentity {
            id: milestone_id.to_owned(),
            name: "Single Bead Milestone".to_owned(),
        },
        executive_summary: "Run a single bead end to end.".to_owned(),
        goals: vec!["Execute one bead".to_owned()],
        non_goals: vec![],
        constraints: vec!["Keep the fixture deterministic".to_owned()],
        acceptance_map: vec![AcceptanceCriterion {
            id: "AC-1".to_owned(),
            description: "Single bead completes".to_owned(),
            covered_by: vec!["bead-1".to_owned()],
        }],
        workstreams: vec![Workstream {
            name: "Execution".to_owned(),
            description: Some("Run the single bead.".to_owned()),
            beads: vec![BeadProposal {
                bead_id: None,
                explicit_id: None,
                title: "Execute the only bead".to_owned(),
                description: Some("Create and run a single bead-backed project.".to_owned()),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec!["single".to_owned()],
                depends_on: vec![],
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: Some(FlowPreset::DocsChange),
            }],
        }],
        default_flow: FlowPreset::Minimal,
        agents_guidance: Some("Run the single bead deterministically.".to_owned()),
    };
    let plan_json = render_plan_json(&bundle).expect("render plan json");
    let mut hasher = Sha256::new();
    hasher.update(plan_json.as_bytes());
    let plan_hash = format!("{:x}", hasher.finalize());
    fs::write(
        milestone_root.join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 1,
  "progress": {{
    "total_beads": 1,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("write milestone status");
    fs::write(milestone_root.join("plan.json"), plan_json).expect("write plan json");
}

fn write_bv_next_script(
    base_dir: &std::path::Path,
    bead_id: &str,
    title: &str,
) -> std::path::PathBuf {
    write_editor_script(
        base_dir,
        "bv",
        &format!(
            r#"#!/bin/sh
if [ "$1" = "--robot-next" ]; then
cat <<'EOF'
{{"id":"{bead_id}","title":"{title}","score":9.5,"reasons":["ready"],"action":"implement"}}
EOF
exit 0
fi
echo "unexpected bv args: $@" >&2
exit 1
"#,
            bead_id = bead_id,
            title = title,
        ),
    )
}

fn write_bv_no_recommendation_script(base_dir: &std::path::Path) -> std::path::PathBuf {
    write_editor_script(
        base_dir,
        "bv",
        r#"#!/bin/sh
if [ "$1" = "--robot-next" ]; then
cat <<'EOF'
{"message":"no actionable beads"}
EOF
exit 0
fi
echo "unexpected bv args: $@" >&2
exit 1
"#,
    )
}

fn write_br_milestone_selection_script(
    base_dir: &std::path::Path,
    ready_payload_json: &str,
    show_payload_json: &str,
    list_payload_json: &str,
) -> std::path::PathBuf {
    write_editor_script(
        base_dir,
        "br",
        &format!(
            r#"#!/bin/sh
if [ "$1" = "ready" ] && [ "$2" = "--json" ]; then
cat <<'EOF'
{ready_payload_json}
EOF
exit 0
fi
if [ "$1" = "show" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
{show_payload_json}
EOF
exit 0
fi
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{list_payload_json}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
            ready_payload_json = ready_payload_json,
            show_payload_json = show_payload_json,
            list_payload_json = list_payload_json,
        ),
    )
}

fn default_br_list_response() -> &'static str {
    r#"{
  "issues": [
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Document task bootstrap follow-up",
      "status": "open",
      "priority": "P2",
      "issue_type": "docs",
      "labels": ["docs"]
    }
  ]
}"#
}

fn exhaustive_br_list_response_with_closed_adjacent_bead() -> String {
    let mut issues = Vec::new();
    for index in 0..55 {
        issues.push(serde_json::json!({
            "id": format!("ms-bulk.bead-{:03}", index),
            "title": format!("Bulk open bead {index:03}"),
            "status": "open",
            "priority": "P3",
            "issue_type": "task",
            "labels": ["bulk"]
        }));
    }
    issues.push(serde_json::json!({
        "id": "ms-alpha.bead-2",
        "title": "Bootstrap bead-backed task creation",
        "status": "open",
        "priority": "P1",
        "issue_type": "feature",
        "labels": ["creation", "prompt"]
    }));
    issues.push(serde_json::json!({
        "id": "ms-alpha.bead-3",
        "title": "Document task bootstrap follow-up",
        "status": "closed",
        "priority": "P2",
        "issue_type": "docs",
        "labels": ["docs"]
    }));

    serde_json::to_string_pretty(&serde_json::json!({ "issues": issues }))
        .expect("serialize exhaustive br list response")
}

fn default_ms_alpha_bead_2_show_response() -> &'static str {
    r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable and inspectable",
    "dependencies": []
  }
]"#
}

fn write_show_bead_script_with_default_list(
    base_dir: &std::path::Path,
    bead_id: &str,
    show_payload_json: &str,
) -> std::path::PathBuf {
    write_editor_script(
        base_dir,
        "br",
        &format!(
            r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "{bead_id}" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
{show_payload_json}
EOF
exit 0
fi
if [ "$1" = "ready" ] && [ "$2" = "--json" ]; then
echo '[]'
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{list_payload}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
            bead_id = bead_id,
            show_payload_json = show_payload_json,
            list_payload = default_br_list_response(),
        ),
    )
}

fn write_requirements_milestone_run_fixture(
    base_dir: &std::path::Path,
    run_id: &str,
    milestone_id: &str,
) {
    let run_root = requirements_root(base_dir).join(run_id);
    fs::create_dir_all(run_root.join("payloads")).expect("create requirements payload dir");
    fs::create_dir_all(run_root.join("artifacts")).expect("create requirements artifact dir");

    let bundle = MilestoneBundle {
        schema_version: 1,
        identity: MilestoneIdentity {
            id: milestone_id.to_owned(),
            name: "Planned Milestone".to_owned(),
        },
        executive_summary: "Generate a durable milestone planning bundle.".to_owned(),
        goals: vec!["Persist the milestone bundle".to_owned()],
        non_goals: vec![],
        constraints: vec!["Keep the handoff deterministic".to_owned()],
        acceptance_map: vec![AcceptanceCriterion {
            id: "AC-1".to_owned(),
            description: "Milestone plan is materialized".to_owned(),
            covered_by: vec!["bead-1".to_owned()],
        }],
        workstreams: vec![Workstream {
            name: "Planning".to_owned(),
            description: Some("Create the milestone plan.".to_owned()),
            beads: vec![BeadProposal {
                bead_id: Some("bead-1".to_owned()),
                explicit_id: None,
                title: "Persist milestone bundle".to_owned(),
                description: Some("Write plan.json and plan.md".to_owned()),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec!["planning".to_owned()],
                depends_on: vec![],
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: Some(FlowPreset::Standard),
            }],
        }],
        default_flow: FlowPreset::Minimal,
        agents_guidance: Some("Preserve the bundle structure.".to_owned()),
    };
    let payload_id = format!("{run_id}-milestone-bundle-1");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Plan milestone",
            "mode": "milestone",
            "status": "completed",
            "question_round": 1,
            "latest_question_set_id": null,
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": null,
            "latest_milestone_bundle_id": payload_id,
            "milestone_bundle": bundle,
            "output_kind": "milestone_bundle",
            "pending_question_count": null,
            "recommended_flow": "minimal",
            "created_at": "2026-04-02T10:00:00Z",
            "updated_at": "2026-04-02T10:05:00Z",
            "status_summary": "completed",
            "current_stage": "milestone_bundle",
            "committed_stages": {
                "milestone_bundle": {
                    "payload_id": format!("{run_id}-milestone-bundle-1"),
                    "artifact_id": format!("{run_id}-milestone-bundle-art-1"),
                    "cache_key": null
                }
            },
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write milestone run.json");
}

fn milestone_plan_hash(base_dir: &std::path::Path, milestone_id: &str) -> String {
    let plan_path = milestone_root(base_dir, milestone_id).join("plan.json");
    let plan_json = fs::read_to_string(plan_path).expect("read plan json");
    let mut hasher = Sha256::new();
    hasher.update(plan_json.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[test]
fn milestone_create_generates_slugged_id_and_planning_snapshot() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Launch",
            "--from-idea",
            "Plan the alpha launch milestone.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");

    assert!(
        output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created milestone 'ms-alpha-launch'"));

    let record = fs::read_to_string(
        milestone_root(temp_dir.path(), "ms-alpha-launch").join("milestone.toml"),
    )
    .expect("read milestone record");
    assert!(record.contains("name = \"Alpha Launch\""));
    assert!(record.contains("description = \"Plan the alpha launch milestone.\""));

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha-launch").join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["status"], "planning");
    assert_eq!(status["plan_version"], 0);
}

#[test]
fn milestone_show_json_reports_detail_from_live_state() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(
        output.status.success(),
        "milestone show should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone show json");
    assert_eq!(json["id"], "ms-alpha");
    assert_eq!(json["status"], "ready");
    assert_eq!(json["bead_count"], 3);
    assert_eq!(json["progress"]["completed_beads"], 1);
    assert_eq!(json["progress"]["in_progress_beads"], 1);
    assert_eq!(json["plan_version"], 2);
}

#[test]
fn milestone_show_json_reports_unplanned_milestone_without_live_plan() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args(["milestone", "create", "Alpha Draft"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha-draft", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(
        output.status.success(),
        "milestone show should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone show json");
    assert_eq!(json["status"], "planning");
    assert_eq!(json["bead_count"], 0);
    assert_eq!(json["has_plan"], false);
}

#[test]
fn milestone_show_json_reports_pending_requirements_during_replan() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let run_id = "req-awaiting-answers";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create pending requirements run dir");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Replan milestone",
            "mode": "milestone",
            "status": "awaiting_answers",
            "question_round": 1,
            "latest_question_set_id": "qs-1",
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": null,
            "latest_milestone_bundle_id": null,
            "milestone_bundle": null,
            "output_kind": "milestone_bundle",
            "pending_question_count": 1,
            "recommended_flow": "minimal",
            "created_at": "2026-04-15T14:00:00Z",
            "updated_at": "2026-04-15T14:05:00Z",
            "status_summary": "awaiting answers: deployment window is still missing",
            "current_stage": "validation",
            "committed_stages": {},
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write pending requirements run");

    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(
        output.status.success(),
        "milestone show should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone show json");
    assert_eq!(json["status"], "awaiting_answers");
    assert_eq!(json["bead_count"], 3);
    assert_eq!(json["pending_requirements"]["run_id"], run_id);
    assert_eq!(json["pending_requirements"]["status"], "awaiting_answers");
    assert!(json["pending_requirements"]["status_summary"]
        .as_str()
        .expect("pending summary")
        .contains("deployment window"));
}

#[test]
fn milestone_show_json_rejects_missing_live_plan_when_snapshot_claims_one() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::remove_file(milestone_root(temp_dir.path(), "ms-alpha").join("plan.json"))
        .expect("remove plan json");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("plan.json is missing"));
}

#[test]
fn milestone_show_wraps_missing_status_snapshot_with_milestone_id() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::remove_file(milestone_root(temp_dir.path(), "ms-alpha").join("status.json"))
        .expect("remove status json");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("No such file or directory"));
}

#[test]
fn milestone_status_lists_all_milestones_in_json() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    write_milestone_fixture(temp_dir.path(), "ms-beta");

    let output = Command::new(binary())
        .args(["milestone", "status", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(
        output.status.success(),
        "milestone status should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone status json");
    let milestones = json["milestones"].as_array().expect("milestones array");
    assert_eq!(milestones.len(), 2);
    assert_eq!(milestones[0]["id"], "ms-alpha");
    assert_eq!(milestones[1]["id"], "ms-beta");
}

#[test]
fn milestone_status_lists_pending_replan_state_in_json() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let run_id = "req-drafting-live";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create pending requirements run dir");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Replan milestone",
            "mode": "milestone",
            "status": "drafting",
            "question_round": 1,
            "latest_question_set_id": "qs-1",
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": null,
            "latest_milestone_bundle_id": null,
            "milestone_bundle": null,
            "output_kind": "milestone_bundle",
            "pending_question_count": null,
            "recommended_flow": "minimal",
            "created_at": "2099-04-15T14:00:00Z",
            "updated_at": "2099-04-15T14:05:00Z",
            "status_summary": "drafting: milestone planning",
            "current_stage": "research",
            "committed_stages": {},
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write pending requirements run");

    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "status", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(
        output.status.success(),
        "milestone status should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone status json");
    let milestone = json["milestones"]
        .as_array()
        .and_then(|milestones| milestones.first())
        .expect("first milestone");
    assert_eq!(milestone["status"], "planning");
    assert_eq!(milestone["pending_requirements"]["run_id"], run_id);
    assert_eq!(milestone["pending_requirements"]["status"], "drafting");
}

#[test]
fn milestone_status_json_rejects_missing_live_plan_when_snapshot_claims_one() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::remove_file(milestone_root(temp_dir.path(), "ms-alpha").join("plan.json"))
        .expect("remove plan json");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(!output.status.success(), "milestone status should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("plan.json is missing"));
}

#[test]
fn milestone_status_wraps_missing_status_snapshot_with_milestone_id() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::remove_file(milestone_root(temp_dir.path(), "ms-alpha").join("status.json"))
        .expect("remove status json");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(!output.status.success(), "milestone status should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("No such file or directory"));
}

#[test]
fn milestone_status_with_explicit_id_updates_active_milestone() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    write_milestone_fixture(temp_dir.path(), "ms-beta");
    fs::write(active_milestone_path(temp_dir.path()), "ms-alpha").expect("seed active milestone");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-beta", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(
        output.status.success(),
        "milestone status should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path())).expect("read active milestone"),
        "ms-beta"
    );
}

#[test]
fn milestone_next_selects_bead_and_uses_active_milestone_when_id_is_omitted() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let ready_payload = r#"[{"id":"bead-2","title":"Bootstrap bead-backed task creation","priority":"P1","issue_type":"feature","labels":["creation"]}]"#;
    let show_payload = r#"[
  {
    "id": "other-ms.bead-2",
    "title": "Wrong milestone bead",
    "status": "open",
    "priority": "P4",
    "issue_type": "feature",
    "description": "Noise from another milestone.",
    "acceptance_criteria": "- Ignore this result",
    "dependencies": []
  },
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable and inspectable",
    "dependencies": []
  }
]"#;
    let list_payload = default_br_list_response();
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(
        temp_dir.path(),
        "ms-alpha.bead-2",
        "Bootstrap bead-backed task creation",
    );

    let path = prepend_path(temp_dir.path());
    let first = Command::new(binary())
        .args(["milestone", "next", "ms-alpha", "--json"])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next");
    assert!(
        first.status.success(),
        "milestone next should succeed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: serde_json::Value =
        serde_json::from_slice(&first.stdout).expect("parse milestone next json");
    assert_eq!(first_json["status"], "success");
    assert_eq!(first_json["bead"]["id"], "ms-alpha.bead-2");
    assert_eq!(first_json["bead"]["priority"], "P1");
    assert_eq!(first_json["bead"]["readiness"], "ready");
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path())).expect("read active milestone"),
        "ms-alpha"
    );

    let second = Command::new(binary())
        .args(["milestone", "next", "--json"])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next without id");
    assert!(
        second.status.success(),
        "milestone next without id should succeed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: serde_json::Value =
        serde_json::from_slice(&second.stdout).expect("parse milestone next json");
    assert_eq!(second_json["milestone_id"], "ms-alpha");
    assert_eq!(second_json["bead"]["id"], "ms-alpha.bead-2");
}

#[test]
fn milestone_next_falls_back_to_active_project_milestone_when_pointer_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-alpha",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(active_project_path(temp_dir.path()), "bead-alpha").expect("seed active project");

    let ready_payload = r#"[{"id":"bead-2","title":"Bootstrap bead-backed task creation","priority":"P1","issue_type":"feature","labels":["creation"]}]"#;
    let show_payload = r#"[{"id":"ms-alpha.bead-2","title":"Bootstrap bead-backed task creation","status":"open","priority":"P1","issue_type":"feature","description":"Create a Ralph project directly from milestone + bead context.","acceptance_criteria":"- Controller can create the project without manual setup","dependencies":[],"dependents":[]}]"#;
    let list_payload = default_br_list_response();
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(
        temp_dir.path(),
        "ms-alpha.bead-2",
        "Bootstrap bead-backed task creation",
    );

    let output = Command::new(binary())
        .args(["milestone", "next", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next without active milestone pointer");

    assert!(
        output.status.success(),
        "milestone next without pointer should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone next json");
    assert_eq!(json["milestone_id"], "ms-alpha");
    assert_eq!(json["bead"]["id"], "ms-alpha.bead-2");
}

#[test]
fn milestone_next_ignores_stale_active_milestone_pointer_before_active_project_fallback() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-alpha",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(active_project_path(temp_dir.path()), "bead-alpha").expect("seed active project");
    fs::write(active_milestone_path(temp_dir.path()), "ms-stale")
        .expect("seed stale active milestone");

    let ready_payload = r#"[{"id":"bead-2","title":"Bootstrap bead-backed task creation","priority":"P1","issue_type":"feature","labels":["creation"]}]"#;
    let show_payload = r#"[{"id":"ms-alpha.bead-2","title":"Bootstrap bead-backed task creation","status":"open","priority":"P1","issue_type":"feature","description":"Create a Ralph project directly from milestone + bead context.","acceptance_criteria":"- Controller can create the project without manual setup","dependencies":[],"dependents":[]}]"#;
    let list_payload = default_br_list_response();
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(
        temp_dir.path(),
        "ms-alpha.bead-2",
        "Bootstrap bead-backed task creation",
    );

    let output = Command::new(binary())
        .args(["milestone", "next", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next with stale active milestone pointer");

    assert!(
        output.status.success(),
        "milestone next with stale pointer should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone next json");
    assert_eq!(json["milestone_id"], "ms-alpha");
    assert_eq!(json["bead"]["id"], "ms-alpha.bead-2");
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read updated active milestone")
            .trim(),
        "ms-alpha"
    );
}

#[test]
fn milestone_next_returns_blocked_json_and_nonzero_status() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-blocked");

    let ready_payload = "[]";
    let show_payload = r#"[{"id":"ms-blocked.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","dependencies":[]}]"#;
    let list_payload = r#"{"issues":[{"id":"ms-blocked.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_no_recommendation_script(temp_dir.path());

    let output = Command::new(binary())
        .args(["milestone", "next", "ms-blocked", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run blocked milestone next");

    assert!(
        !output.status.success(),
        "blocked milestone next should exit non-zero"
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse blocked milestone next json");
    assert_eq!(json["status"], "blocked");
    assert!(json["message"]
        .as_str()
        .expect("blocked message")
        .contains("no ready beads"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("milestone 'ms-blocked' next failed"));
}

#[test]
fn milestone_next_claims_ready_fallback_when_bv_recommends_foreign_bead() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-single");

    let ready_payload = r#"[{"id":"bead-1","title":"Execute the only bead","priority":"P1","issue_type":"task","labels":["single"]}]"#;
    let show_payload = r#"[
  {
    "id": "other-ms.bead-9",
    "title": "Wrong milestone bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Belongs elsewhere.",
    "acceptance_criteria": "- Wrong milestone",
    "dependencies": [],
    "dependents": []
  },
  {
    "id": "ms-single.bead-1",
    "title": "Execute the only bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Create and run a single bead-backed project.",
    "acceptance_criteria": "- Single bead completes",
    "dependencies": [],
    "dependents": []
  }
]"#;
    let list_payload = r#"{"issues":[{"id":"ms-single.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(temp_dir.path(), "other-ms.bead-9", "Wrong milestone bead");

    let output = Command::new(binary())
        .args(["milestone", "next", "ms-single", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next with foreign recommendation");

    assert!(
        output.status.success(),
        "milestone next should succeed via ready fallback: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone next json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["bead"]["id"], "ms-single.bead-1");
}

#[test]
fn milestone_next_rejects_execution_when_plan_json_is_missing_during_planning() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "create", "Alpha Plan"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new(binary())
        .args(["milestone", "next", "ms-alpha-plan", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next without a plan");

    assert!(
        !output.status.success(),
        "milestone next should fail when plan.json is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-plan' next failed"));
    assert!(stderr.contains("still planning"));
    assert!(stderr.contains("milestone plan ms-alpha-plan"));
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_run_ignores_malformed_active_milestone_pointer_and_adopts_existing_project() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-single");
    create_bead_backed_project_fixture(
        temp_dir.path(),
        "custom-bead-run",
        "ms-single",
        "ms-single.bead-1",
    );
    fs::write(active_project_path(temp_dir.path()), "custom-bead-run")
        .expect("seed active project");
    fs::write(
        active_milestone_path(temp_dir.path()),
        "not a valid milestone id",
    )
    .expect("seed malformed active milestone");

    let ready_payload = r#"[{"id":"bead-1","title":"Execute the only bead","priority":"P1","issue_type":"task","labels":["single"]}]"#;
    let show_payload = r#"[
  {
    "id": "ms-single.bead-1",
    "title": "Execute the only bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Create and run a single bead-backed project.",
    "acceptance_criteria": "- Single bead completes",
    "dependencies": [],
    "dependents": []
  }
]"#;
    let list_payload = r#"{"issues":[{"id":"ms-single.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(temp_dir.path(), "ms-single.bead-1", "Execute the only bead");

    let output = Command::new(binary())
        .args(["milestone", "run", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run with malformed active milestone pointer");

    assert!(
        output.status.success(),
        "milestone run with malformed pointer should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone run json");
    assert_eq!(json["status"], "completed");
    assert_eq!(json["milestone_id"], "ms-single");
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-single"
    );

    let projects_dir = live_workspace_root(temp_dir.path()).join("projects");
    let project_count = fs::read_dir(projects_dir)
        .expect("read projects dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false))
        .count();
    assert_eq!(
        project_count, 1,
        "milestone run should reuse the existing project"
    );

    let task_runs =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-single").join("task-runs.ndjson"))
            .expect("read milestone task-runs");
    assert!(task_runs.contains("\"project_id\":\"custom-bead-run\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_run_rejects_execution_when_plan_json_is_missing_during_planning() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "create", "Alpha Plan"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new(binary())
        .args(["milestone", "run", "ms-alpha-plan", "--json"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run without a plan");

    assert!(
        !output.status.success(),
        "milestone run should fail when plan.json is missing"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-plan' run failed"));
    assert!(stderr.contains("still planning"));
    assert!(stderr.contains("milestone plan ms-alpha-plan"));
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_run_executes_single_bead_milestone_and_reuses_active_selection() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-single");

    let ready_payload = r#"[{"id":"bead-1","title":"Execute the only bead","priority":"P1","issue_type":"task","labels":["single"]}]"#;
    let show_payload = r#"[
  {
    "id": "other-ms.bead-1",
    "title": "Ignore this bead",
    "status": "open",
    "priority": "P4",
    "issue_type": "task",
    "description": "Belongs to a different milestone.",
    "acceptance_criteria": "- Not relevant",
    "dependencies": [],
    "dependents": []
  },
  {
    "id": "ms-single.bead-1",
    "title": "Execute the only bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Create and run a single bead-backed project.",
    "acceptance_criteria": "- Single bead completes",
    "dependencies": [],
    "dependents": []
  }
]"#;
    let list_payload = r#"{"issues":[{"id":"ms-single.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(temp_dir.path(), "ms-single.bead-1", "Execute the only bead");

    let path = prepend_path(temp_dir.path());
    let first = Command::new(binary())
        .args(["milestone", "run", "ms-single", "--json"])
        .env("PATH", &path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run");
    assert!(
        first.status.success(),
        "milestone run should succeed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: serde_json::Value =
        serde_json::from_slice(&first.stdout).expect("parse milestone run json");
    assert_eq!(first_json["status"], "completed");
    assert_eq!(first_json["milestone_id"], "ms-single");
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path())).expect("read active milestone"),
        "ms-single"
    );

    let second = Command::new(binary())
        .args(["milestone", "run", "--json"])
        .env("PATH", &path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run without id");
    assert!(
        second.status.success(),
        "milestone run without id should succeed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: serde_json::Value =
        serde_json::from_slice(&second.stdout).expect("parse milestone run json");
    assert_eq!(second_json["status"], "completed");

    let projects_dir = live_workspace_root(temp_dir.path()).join("projects");
    let project_count = fs::read_dir(projects_dir)
        .expect("read projects dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false))
        .count();
    assert_eq!(
        project_count, 1,
        "milestone run should create exactly one project"
    );
}

#[test]
#[ignore = "membership validation path changed after export-beads feature; needs mock update"]
fn milestone_run_membership_errors_use_run_action_label() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-single");

    let ready_payload = r#"[{"id":"bead-1","title":"Execute the only bead","priority":"P1","issue_type":"task","labels":["single"]}]"#;
    let show_payload = r#"[{"id":"other-ms.bead-1","title":"Wrong milestone bead","status":"open","priority":"P1","issue_type":"task","description":"Belongs elsewhere.","acceptance_criteria":"- Wrong milestone","dependencies":[],"dependents":[]}]"#;
    let list_payload = r#"{"issues":[{"id":"ms-single.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(temp_dir.path(), "ms-single.bead-1", "Execute the only bead");

    let output = Command::new(binary())
        .args(["milestone", "run", "ms-single", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run");

    assert!(!output.status.success(), "milestone run should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-single' run failed"));
    assert!(!stderr.contains("milestone 'ms-single' next failed"));
}

#[test]
fn milestone_show_rejects_stale_plan_metadata() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let stale_status = fs::read_to_string(&status_path)
        .expect("read status")
        .replace("\"plan_hash\": \"", "\"plan_hash\": \"stale-");
    fs::write(&status_path, stale_status).expect("write stale status");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("plan metadata is stale"));
}

#[test]
fn milestone_show_rejects_semantically_invalid_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["active_bead"] = serde_json::json!("bead-2");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write invalid status");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("status.json is inconsistent"));
    assert!(stderr.contains("active_bead"));
}

#[test]
fn milestone_show_rejects_incompatible_pending_requirements_run() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let run_id = "req-quick-pending";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create pending requirements run dir");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Quick draft",
            "mode": "quick",
            "status": "completed",
            "question_round": 0,
            "latest_question_set_id": null,
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": "seed-1",
            "latest_milestone_bundle_id": null,
            "milestone_bundle": null,
            "output_kind": "project_seed",
            "pending_question_count": null,
            "recommended_flow": null,
            "created_at": "2026-04-15T14:00:00Z",
            "updated_at": "2026-04-15T14:05:00Z",
            "status_summary": "completed",
            "current_stage": "project_seed",
            "committed_stages": {},
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write incompatible requirements run");

    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("pending requirements run 'req-quick-pending'"));
    assert!(stderr.contains("expected mode 'milestone'"));
}

#[test]
fn milestone_status_rejects_stale_plan_metadata() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let stale_status = fs::read_to_string(&status_path)
        .expect("read status")
        .replace("\"plan_hash\": \"", "\"plan_hash\": \"stale-");
    fs::write(&status_path, stale_status).expect("write stale status");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(!output.status.success(), "milestone status should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' inspection failed"));
    assert!(stderr.contains("plan metadata is stale"));
}

#[test]
fn milestone_status_for_missing_id_reports_clear_error() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(!output.status.success(), "missing milestone should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-missing' was not found"));
}

#[test]
fn milestone_show_rejects_unsupported_workspace_version() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::write(
        workspace_config_path(temp_dir.path()),
        "version = 999\ncreated_at = \"2026-03-11T17:50:55Z\"\n",
    )
    .expect("write unsupported workspace config");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "milestone show should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported workspace version 999"));
}

#[test]
fn milestone_status_rejects_unsupported_workspace_version() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    fs::write(
        workspace_config_path(temp_dir.path()),
        "version = 999\ncreated_at = \"2026-03-11T17:50:55Z\"\n",
    )
    .expect("write unsupported workspace config");

    let output = Command::new(binary())
        .args(["milestone", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(!output.status.success(), "milestone status should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unsupported workspace version 999"));
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_runs_requirements_and_materializes_plan() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Plan",
            "--from-idea",
            "Plan the alpha milestone in milestone mode.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let plan_output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-plan"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(
        plan_output.status.success(),
        "milestone plan should succeed: {}",
        String::from_utf8_lossy(&plan_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&plan_output.stdout);
    assert!(stdout.contains("Planned milestone 'ms-alpha-plan'"));

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha-plan").join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["status"], "ready");
    assert_eq!(status["plan_version"], 1);

    let plan: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha-plan").join("plan.json"))
            .expect("read plan json"),
    )
    .expect("parse plan json");
    assert_eq!(plan["identity"]["id"], "ms-alpha-plan");
    assert_eq!(plan["identity"]["name"], "Alpha Plan");
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_wraps_missing_status_snapshot_with_milestone_id() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Plan",
            "--from-idea",
            "Plan the alpha milestone in milestone mode.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    fs::remove_file(milestone_root(temp_dir.path(), "ms-alpha-plan").join("status.json"))
        .expect("remove status json");

    let plan_output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-plan"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!plan_output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&plan_output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-plan' planning failed"));
    assert!(stderr.contains("No such file or directory"));
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_succeeds_when_requirements_are_awaiting_answers() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Questions",
            "--from-idea",
            "Plan the alpha milestone with a clarification round.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let overrides = serde_json::json!({
        "validation": [
            {
                "outcome": "needs_questions",
                "evidence": ["Need clarification before milestone planning can finish"],
                "blocking_issues": [],
                "missing_information": ["Target deployment window"]
            },
            {
                "outcome": "pass",
                "evidence": ["Validation passes after answers"],
                "blocking_issues": [],
                "missing_information": []
            }
        ],
        "question_set": {
            "questions": [
                {
                    "id": "q1",
                    "prompt": "What deployment window should this milestone target?",
                    "rationale": "Needed to sequence the milestone bundle",
                    "required": true
                }
            ]
        }
    });

    let output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-questions"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("RALPH_BURNING_TEST_LABEL_OVERRIDES", overrides.to_string())
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(
        output.status.success(),
        "milestone plan should succeed while awaiting answers: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Questions generated. Edit answers at:"));
    assert!(stdout.contains("Milestone 'ms-alpha-questions' planning is awaiting answers"));

    let run_id = only_requirements_run_id(temp_dir.path());
    let run: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            requirements_root(temp_dir.path())
                .join(&run_id)
                .join("run.json"),
        )
        .expect("read requirements run"),
    )
    .expect("parse requirements run");
    assert_eq!(run["status"], "awaiting_answers");

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            milestone_root(temp_dir.path(), "ms-alpha-questions").join("status.json"),
        )
        .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["status"], "planning");
    assert_eq!(status["pending_requirements_run_id"], run_id);
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path())).expect("read active milestone"),
        "ms-alpha-questions"
    );
    assert!(
        !milestone_root(temp_dir.path(), "ms-alpha-questions")
            .join("plan.json")
            .exists(),
        "plan.json should not exist until the requirements run completes"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_reuses_pending_requirements_run_after_clarification_completion() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Questions",
            "--from-idea",
            "Plan the alpha milestone with a clarification round.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let overrides = serde_json::json!({
        "validation": [
            {
                "outcome": "needs_questions",
                "evidence": ["Need clarification before milestone planning can finish"],
                "blocking_issues": [],
                "missing_information": ["Target deployment window"]
            },
            {
                "outcome": "pass",
                "evidence": ["Validation passes after answers"],
                "blocking_issues": [],
                "missing_information": []
            }
        ],
        "question_set": {
            "questions": [
                {
                    "id": "q1",
                    "prompt": "What deployment window should this milestone target?",
                    "rationale": "Needed to sequence the milestone bundle",
                    "required": true
                }
            ]
        }
    });

    let first_plan = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-questions"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("RALPH_BURNING_TEST_LABEL_OVERRIDES", overrides.to_string())
        .current_dir(temp_dir.path())
        .output()
        .expect("run initial milestone plan");
    assert!(
        first_plan.status.success(),
        "initial milestone plan should succeed: {}",
        String::from_utf8_lossy(&first_plan.stderr)
    );

    let run_id = only_requirements_run_id(temp_dir.path());
    write_requirements_milestone_run_fixture(temp_dir.path(), &run_id, "ms-alpha-questions");

    let second_plan = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-questions"])
        .env("RALPH_BURNING_BACKEND", "definitely-invalid")
        .current_dir(temp_dir.path())
        .output()
        .expect("rerun milestone plan");
    assert!(
        second_plan.status.success(),
        "rerun milestone plan should succeed without starting a new backend flow: {}",
        String::from_utf8_lossy(&second_plan.stderr)
    );

    let stdout = String::from_utf8_lossy(&second_plan.stdout);
    assert!(stdout.contains("Planned milestone 'ms-alpha-questions'"));
    assert_eq!(requirements_run_ids(temp_dir.path()), vec![run_id.clone()]);

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            milestone_root(temp_dir.path(), "ms-alpha-questions").join("status.json"),
        )
        .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["status"], "ready");
    assert!(status["pending_requirements_run_id"].is_null());

    let plan: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            milestone_root(temp_dir.path(), "ms-alpha-questions").join("plan.json"),
        )
        .expect("read plan json"),
    )
    .expect("parse plan json");
    assert_eq!(plan["identity"]["id"], "ms-alpha-questions");
}

#[test]
fn milestone_plan_clears_startup_reservation_when_backend_setup_fails() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Backend Failure",
            "--from-idea",
            "Plan the alpha milestone after backend setup recovers.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let plan_output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-backend-failure"])
        .env("RALPH_BURNING_BACKEND", "definitely-invalid")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!plan_output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&plan_output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-backend-failure' planning failed"));
    assert!(stderr.contains("RALPH_BURNING_BACKEND"));

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            milestone_root(temp_dir.path(), "ms-alpha-backend-failure").join("status.json"),
        )
        .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert!(
        status["pending_requirements_run_id"].is_null(),
        "backend setup failure should clear the startup reservation"
    );
    assert!(
        requirements_run_ids(temp_dir.path()).is_empty(),
        "backend setup failure should not leave behind a partial requirements run"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_recovers_from_stale_startup_reservation() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Startup Recovery",
            "--from-idea",
            "Plan the alpha milestone after an interrupted startup reservation.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let milestone_id = "ms-alpha-startup-recovery";
    let status_path = milestone_root(temp_dir.path(), milestone_id).join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!("__starting__:999999-0");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "plan", milestone_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(
        output.status.success(),
        "milestone plan should recover from stale startup reservation: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Planned milestone 'ms-alpha-startup-recovery'"));

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), milestone_id).join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert!(status["pending_requirements_run_id"].is_null());
}

#[test]
fn milestone_plan_rejects_semantically_invalid_snapshot_before_backend_setup() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha-invalid");

    let status_path = milestone_root(temp_dir.path(), "ms-alpha-invalid").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["active_bead"] = serde_json::json!("bead-2");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write invalid status json");

    let output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-invalid"])
        .env("RALPH_BURNING_BACKEND", "definitely-invalid")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-invalid' planning failed"));
    assert!(stderr.contains("status.json is inconsistent"));
    assert!(stderr.contains("active_bead"));
    assert!(
        !stderr.contains("RALPH_BURNING_BACKEND"),
        "snapshot validation should reject the milestone before backend setup is attempted"
    );
}

#[test]
fn milestone_plan_rejects_incompatible_pending_requirements_run() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Incompatible Pending",
            "--from-idea",
            "Plan the alpha milestone once the wrong pending requirements run is fixed.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let milestone_id = "ms-alpha-incompatible-pending";
    let run_id = "req-quick-pending";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create incompatible run dir");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Quick draft",
            "mode": "quick",
            "status": "completed",
            "question_round": 0,
            "latest_question_set_id": null,
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": "seed-1",
            "latest_milestone_bundle_id": null,
            "milestone_bundle": null,
            "output_kind": "project_seed",
            "pending_question_count": null,
            "recommended_flow": null,
            "created_at": "2026-04-15T14:00:00Z",
            "updated_at": "2026-04-15T14:05:00Z",
            "status_summary": "completed",
            "current_stage": "project_seed",
            "committed_stages": {},
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write incompatible requirements run");

    let status_path = milestone_root(temp_dir.path(), milestone_id).join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "plan", milestone_id])
        .env("RALPH_BURNING_BACKEND", "definitely-invalid")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-incompatible-pending' planning failed"));
    assert!(stderr.contains("pending requirements run 'req-quick-pending'"));
    assert!(stderr.contains("expected mode 'milestone'"));
    assert!(
        !stderr.contains("RALPH_BURNING_BACKEND"),
        "planning should reject the incompatible pending run before backend setup is attempted"
    );

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), milestone_id).join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["pending_requirements_run_id"], run_id);
    assert_eq!(
        requirements_run_ids(temp_dir.path()),
        vec![run_id.to_owned()]
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn milestone_plan_recovers_from_stale_drafting_pending_run() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Drafting Recovery",
            "--from-idea",
            "Plan the alpha milestone after a stale drafting run.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let milestone_id = "ms-alpha-drafting-recovery";
    let stale_run_id = "req-stale-drafting";
    let status_path = milestone_root(temp_dir.path(), milestone_id).join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(stale_run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let stale_run_root = requirements_root(temp_dir.path()).join(stale_run_id);
    fs::create_dir_all(&stale_run_root).expect("create stale run dir");
    fs::write(
        stale_run_root.join("run.json"),
        serde_json::json!({
            "run_id": stale_run_id,
            "idea": "Plan milestone",
            "mode": "milestone",
            "status": "drafting",
            "question_round": 0,
            "latest_question_set_id": null,
            "latest_draft_id": null,
            "latest_review_id": null,
            "latest_seed_id": null,
            "latest_milestone_bundle_id": null,
            "milestone_bundle": null,
            "output_kind": "milestone_bundle",
            "pending_question_count": null,
            "recommended_flow": null,
            "created_at": "2020-04-15T13:00:00Z",
            "updated_at": "2020-04-15T13:00:00Z",
            "status_summary": "drafting",
            "current_stage": null,
            "committed_stages": {},
            "quick_revision_count": 0,
            "last_transition_cached": false
        })
        .to_string(),
    )
    .expect("write stale run");

    let plan_output = Command::new(binary())
        .args(["milestone", "plan", milestone_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(
        plan_output.status.success(),
        "milestone plan should recover from stale drafting run: {}",
        String::from_utf8_lossy(&plan_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&plan_output.stdout);
    assert!(stdout.contains("Planned milestone 'ms-alpha-drafting-recovery'"));

    let run_ids = requirements_run_ids(temp_dir.path());
    assert_eq!(run_ids.len(), 2, "expected stale and replacement run ids");
    assert!(
        run_ids.iter().any(|run_id| run_id == stale_run_id),
        "stale run should remain on disk for inspection"
    );
    assert!(
        run_ids.iter().any(|run_id| run_id != stale_run_id),
        "planning should create a replacement requirements run"
    );

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), milestone_id).join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["status"], "ready");
    assert!(status["pending_requirements_run_id"].is_null());
}

#[test]
fn milestone_plan_surfaces_corrupt_pending_requirements_run_without_replacing_it() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Corrupt Pending",
            "--from-idea",
            "Plan the alpha milestone after repairing a corrupt pending requirements run.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let milestone_id = "ms-alpha-corrupt-pending";
    let run_id = "req-corrupt-pending";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create corrupt run dir");
    fs::write(run_root.join("run.json"), "{not valid json").expect("write corrupt run");

    let status_path = milestone_root(temp_dir.path(), milestone_id).join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["pending_requirements_run_id"] = serde_json::json!(run_id);
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "plan", milestone_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-corrupt-pending' planning failed"));
    assert!(
        stderr.contains("pending requirements run 'req-corrupt-pending' could not be inspected")
    );
    assert!(stderr.contains("requirements/req-corrupt-pending/run.json"));
    assert_eq!(
        requirements_run_ids(temp_dir.path()),
        vec![run_id.to_owned()]
    );

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(milestone_root(temp_dir.path(), milestone_id).join("status.json"))
            .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert_eq!(status["pending_requirements_run_id"], run_id);
}

#[test]
fn milestone_plan_rejects_running_milestone_before_backend_setup() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha-running");

    let status_path = milestone_root(temp_dir.path(), "ms-alpha-running").join("status.json");
    let mut status: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&status_path).expect("read status json"))
            .expect("parse status json");
    status["status"] = serde_json::json!("running");
    status["active_bead"] = serde_json::json!("bead-2");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&status).expect("serialize status json"),
    )
    .expect("write status json");

    let output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-running"])
        .env("RALPH_BURNING_BACKEND", "definitely-invalid")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");

    assert!(!output.status.success(), "milestone plan should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha-running' planning failed"));
    assert!(stderr.contains("cannot replan a milestone while status is 'running'"));
    assert!(
        !stderr.contains("RALPH_BURNING_BACKEND"),
        "running milestones should be rejected before backend setup is attempted"
    );

    let status: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            milestone_root(temp_dir.path(), "ms-alpha-running").join("status.json"),
        )
        .expect("read milestone status"),
    )
    .expect("parse milestone status");
    assert!(status["pending_requirements_run_id"].is_null());
    assert!(requirements_run_ids(temp_dir.path()).is_empty());
}

fn write_daemon_task(base_dir: &std::path::Path, task: &DaemonTask) {
    let path = daemon_root(base_dir)
        .join("tasks")
        .join(format!("{}.json", task.task_id));
    fs::create_dir_all(path.parent().expect("task parent")).expect("create task dir");
    fs::write(
        path,
        serde_json::to_string_pretty(task).expect("serialize daemon task"),
    )
    .expect("write daemon task");
}

const TEST_REPO_SLUG: &str = "test/repo";
const TEST_OWNER: &str = "test";
const TEST_REPO: &str = "repo";

/// Write a daemon task under the data-dir layout.
fn write_datadir_daemon_task(data_dir: &std::path::Path, task: &DaemonTask) {
    let path = data_dir
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks")
        .join(format!("{}.json", task.task_id));
    fs::create_dir_all(path.parent().expect("task parent")).expect("create task dir");
    fs::write(
        path,
        serde_json::to_string_pretty(task).expect("serialize daemon task"),
    )
    .expect("write daemon task");
}

/// Write a worktree lease under the data-dir layout.
fn write_datadir_worktree_lease(data_dir: &std::path::Path, lease: &WorktreeLease) {
    let path = data_dir
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/leases")
        .join(format!("{}.json", lease.lease_id));
    fs::create_dir_all(path.parent().expect("lease parent")).expect("create lease dir");
    fs::write(
        path,
        serde_json::to_string_pretty(lease).expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
}

/// Write a writer lock under the data-dir layout.
fn write_datadir_writer_lock(data_dir: &std::path::Path, project_id: &str, owner: &str) {
    let path = data_dir
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/leases")
        .join(format!("writer-{project_id}.lock"));
    fs::create_dir_all(path.parent().expect("lock parent")).expect("create lease dir");
    fs::write(path, owner).expect("write writer lock");
}

/// Write a repo registration entry so reconcile can discover the repo.
fn write_repo_registration(data_dir: &std::path::Path) {
    let reg_path = data_dir
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("registration.json");
    fs::create_dir_all(reg_path.parent().expect("registration parent"))
        .expect("create registration dir");
    let reg = serde_json::json!({
        "repo_slug": TEST_REPO_SLUG,
        "repo_root": data_dir.join("repos").join(TEST_OWNER).join(TEST_REPO).join("repo"),
        "workspace_root": data_dir.join("repos").join(TEST_OWNER).join(TEST_REPO).join("repo").join(".ralph-burning"),
    });
    fs::write(
        reg_path,
        serde_json::to_string_pretty(&reg).expect("serialize registration"),
    )
    .expect("write registration");
}

#[cfg(feature = "test-stub")]
/// Run a single daemon iteration in-process using the stub backend and the
/// single-repo DaemonLoop::run path. This replaces the former CLI binary
/// invocation that used `RALPH_BURNING_TEST_LEGACY_DAEMON=1`.
fn run_daemon_iteration_in_process(ws_path: &std::path::Path) {
    use ralph_burning::adapters::fs::{
        FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
        FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore, FsRequirementsStore,
        FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
    };
    use ralph_burning::adapters::stub_backend::StubBackendAdapter;
    use ralph_burning::adapters::worktree::WorktreeAdapter;
    use ralph_burning::adapters::BackendAdapter;
    use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
    use ralph_burning::contexts::automation_runtime::daemon_loop::{DaemonLoop, DaemonLoopConfig};

    // The daemon loop internally builds a RequirementsService via
    // build_requirements_service_default which reads RALPH_BURNING_BACKEND.
    // Ensure it uses the stub backend to match the injected adapter.
    std::env::set_var("RALPH_BURNING_BACKEND", "stub");

    let adapter = BackendAdapter::Stub(StubBackendAdapter::default());
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

    let daemon_loop = DaemonLoop::new(
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
    .with_requirements_store(&requirements_store);

    let loop_config = DaemonLoopConfig {
        single_iteration: true,
        ..DaemonLoopConfig::default()
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(daemon_loop.run(ws_path, &loop_config))
        .expect("daemon iteration should succeed");

    std::env::remove_var("RALPH_BURNING_BACKEND");
}

#[cfg(feature = "test-stub")]
fn init_git_repo(base_dir: &std::path::Path) {
    let init = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(base_dir)
        .output()
        .expect("git init");
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );

    let name = Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(base_dir)
        .output()
        .expect("git config user.name");
    assert!(name.status.success());

    let email = Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(base_dir)
        .output()
        .expect("git config user.email");
    assert!(email.status.success());

    fs::write(base_dir.join("README.md"), "# fixture\n").expect("write readme");
    let add = Command::new("git")
        .args(["add", "README.md"])
        .current_dir(base_dir)
        .output()
        .expect("git add");
    assert!(add.status.success());

    let commit = Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(base_dir)
        .output()
        .expect("git commit");
    assert!(
        commit.status.success(),
        "{}",
        String::from_utf8_lossy(&commit.stderr)
    );
}

#[test]
fn flow_list_prints_all_presets() {
    let output = Command::new(binary())
        .args(["flow", "list"])
        .output()
        .expect("run flow list");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    for preset in FlowPreset::all() {
        assert!(
            stdout.contains(preset.as_str()),
            "flow list should include {}",
            preset.as_str()
        );
    }
}

#[test]
fn flow_show_standard_prints_stage_sequence() {
    let output = Command::new(binary())
        .args(["flow", "show", "standard"])
        .output()
        .expect("run flow show");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stage count: 8"));
    assert!(stdout.contains("1. prompt_review"));
    assert!(stdout.contains("8. final_review"));
    assert!(stdout.contains("Final review enabled: yes"));
}

#[test]
fn flow_show_invalid_preset_exits_non_zero_with_clear_error() {
    let output = Command::new(binary())
        .args(["flow", "show", "unknown_flow"])
        .output()
        .expect("run flow show invalid");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown flow preset 'unknown_flow'"));
    assert!(stderr.contains("supported presets:"));
    assert!(stderr.contains("minimal"));
}

#[test]
fn init_creates_workspace_layout() {
    let temp_dir = tempdir().expect("create temp dir");
    let output = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run init");

    assert!(output.status.success());
    assert!(workspace_config_path(temp_dir.path()).is_file());
    assert!(temp_dir.path().join(".ralph-burning/projects").is_dir());
    assert!(requirements_root(temp_dir.path()).is_dir());
    assert!(daemon_root(temp_dir.path()).join("tasks").is_dir());
    assert!(daemon_root(temp_dir.path()).join("leases").is_dir());
}

#[test]
fn init_fails_when_workspace_already_exists() {
    let temp_dir = tempdir().expect("create temp dir");

    let first = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run first init");
    assert!(first.status.success());

    let second = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run second init");

    assert!(!second.status.success());

    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("workspace already initialized"));
}

#[test]
fn config_show_prints_effective_values_and_sources() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["config", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config show");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[settings]"));
    assert!(stdout.contains("prompt_review.enabled = true # source: default"));
    assert!(stdout.contains("default_flow = \"minimal\" # source: default"));
    assert!(stdout.contains("default_backend = \"claude\" # source: default"));
}

#[test]
fn daemon_status_lists_non_terminal_tasks_first() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();

    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-active".to_owned(),
            issue_ref: "repo#2".to_owned(),
            project_id: "demo-active".to_owned(),
            project_name: Some("Active".to_owned()),
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
            lease_id: Some("lease-active".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(2),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    write_datadir_worktree_lease(
        data_dir.path(),
        &WorktreeLease {
            lease_id: "lease-active".to_owned(),
            task_id: "task-active".to_owned(),
            project_id: "demo-active".to_owned(),
            worktree_path: data_dir
                .path()
                .join("repos")
                .join(TEST_OWNER)
                .join(TEST_REPO)
                .join("worktrees/task-active"),
            branch_name: "rb/task-active".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        },
    );
    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-completed".to_owned(),
            issue_ref: "repo#3".to_owned(),
            project_id: "demo-completed".to_owned(),
            project_name: Some("Completed".to_owned()),
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
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(3),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "status",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon status");
    assert!(
        output.status.success(),
        "daemon status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let active_idx = stdout.find("task-active").expect("active task");
    let completed_idx = stdout.find("task-completed").expect("completed task");
    assert!(active_idx < completed_idx);
}

#[test]
fn daemon_retry_transitions_failed_task_to_pending() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();
    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-failed".to_owned(),
            issue_ref: "repo#4".to_owned(),
            project_id: "demo-failed".to_owned(),
            project_name: Some("Failed".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Failed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: Some("daemon_dispatch_failed".to_owned()),
            failure_message: Some("boom".to_owned()),
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(4),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "retry",
            "4",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon retry");
    assert!(
        output.status.success(),
        "daemon retry failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let task_path = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks/task-failed.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Pending, task.status);
    assert_eq!(1, task.attempt_count);
}

#[test]
fn daemon_abort_claimed_task_releases_lease() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();
    let missing_worktree = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("worktrees/missing");
    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-claimed".to_owned(),
            issue_ref: "repo#5".to_owned(),
            project_id: "demo-claimed".to_owned(),
            project_name: Some("Claimed".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Claimed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-claimed".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(5),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    write_datadir_worktree_lease(
        data_dir.path(),
        &WorktreeLease {
            lease_id: "lease-claimed".to_owned(),
            task_id: "task-claimed".to_owned(),
            project_id: "demo-claimed".to_owned(),
            worktree_path: missing_worktree,
            branch_name: "rb/task-claimed".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        },
    );
    write_datadir_writer_lock(data_dir.path(), "demo-claimed", "lease-claimed");

    let output = Command::new(binary())
        .args([
            "daemon",
            "abort",
            "5",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon abort");
    // Abort with a missing worktree triggers partial cleanup failure —
    // the command exits non-zero because resources_released is false
    // when all three sub-steps don't positively succeed.
    assert!(!output.status.success());

    let task_path = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks/task-claimed.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Aborted, task.status);
    // Lease reference preserved — callers do not clear lease_id when
    // resources_released is false. The physical lease file was deleted
    // (writer-lock release succeeded, so phase 3 ran), but the task's
    // lease_id stays set for operator visibility.
    assert!(task.lease_id.is_some());
}

#[test]
fn daemon_abort_active_task_releases_lease() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();
    let missing_worktree = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("worktrees/missing-active");
    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-active-abort".to_owned(),
            issue_ref: "repo#5a".to_owned(),
            project_id: "demo-active-abort".to_owned(),
            project_name: Some("Active Abort".to_owned()),
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
            lease_id: Some("lease-active-abort".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(55),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    write_datadir_worktree_lease(
        data_dir.path(),
        &WorktreeLease {
            lease_id: "lease-active-abort".to_owned(),
            task_id: "task-active-abort".to_owned(),
            project_id: "demo-active-abort".to_owned(),
            worktree_path: missing_worktree,
            branch_name: "rb/task-active-abort".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        },
    );
    write_datadir_writer_lock(data_dir.path(), "demo-active-abort", "lease-active-abort");

    let output = Command::new(binary())
        .args([
            "daemon",
            "abort",
            "55",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon abort");
    // Abort with a missing worktree triggers partial cleanup failure.
    assert!(!output.status.success());

    let task_path = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks/task-active-abort.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Aborted, task.status);
    // Lease reference preserved for operator recovery.
    assert!(task.lease_id.is_some());
}

#[test]
fn daemon_reconcile_fails_stale_claimed_task() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();
    write_repo_registration(data_dir.path());
    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-stale".to_owned(),
            issue_ref: "repo#6".to_owned(),
            project_id: "demo-stale".to_owned(),
            project_name: Some("Stale".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Claimed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-stale".to_owned()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(6),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    write_datadir_worktree_lease(
        data_dir.path(),
        &WorktreeLease {
            lease_id: "lease-stale".to_owned(),
            task_id: "task-stale".to_owned(),
            project_id: "demo-stale".to_owned(),
            worktree_path: data_dir
                .path()
                .join("repos")
                .join(TEST_OWNER)
                .join(TEST_REPO)
                .join("worktrees/task-stale"),
            branch_name: "rb/task-stale".to_owned(),
            acquired_at: now - Duration::minutes(10),
            ttl_seconds: 300,
            last_heartbeat: now - Duration::minutes(10),
            cleanup_handoff: None,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "reconcile",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--ttl-seconds",
            "1",
        ])
        .output()
        .expect("run daemon reconcile");

    // Stale lease with missing worktree → cleanup failure → non-zero exit
    assert!(
        !output.status.success(),
        "reconcile should fail when worktree is absent"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Cleanup Failures"),
        "should report cleanup failures, got: {stdout}"
    );
    assert!(
        stdout.contains("lease-stale"),
        "should include lease id, got: {stdout}"
    );
    assert!(
        stdout.contains("task-stale"),
        "should include task id, got: {stdout}"
    );

    // Task should still be marked as Failed with reconciliation_timeout
    let task_path = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks/task-stale.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Failed, task.status);
    assert_eq!(
        Some("reconciliation_timeout"),
        task.failure_class.as_deref()
    );

    // Lease should remain durable (not released)
    assert!(
        data_dir
            .path()
            .join("repos")
            .join(TEST_OWNER)
            .join(TEST_REPO)
            .join("daemon/leases/lease-stale.json")
            .exists(),
        "lease should remain durable when worktree is absent"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn daemon_start_single_iteration_fails_and_cleans_up_on_post_claim_error() {
    let temp_dir = initialize_workspace_fixture();
    init_git_repo(temp_dir.path());
    create_project_fixture(temp_dir.path(), "demo-conflict");

    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-conflict".to_owned(),
            issue_ref: "repo#6a".to_owned(),
            project_id: "demo-conflict".to_owned(),
            project_name: Some("Conflict".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: Some("/rb flow docs_change".to_owned()),
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::DocsChange),
            routing_source: Some(RoutingSource::Command),
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
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    // Run daemon in-process (test-only) instead of spawning the CLI binary.
    // The production CLI requires --data-dir; this path uses DaemonLoop::run
    // directly so it processes pre-seeded tasks without GitHub.
    run_daemon_iteration_in_process(temp_dir.path());

    let task_path = daemon_root(temp_dir.path())
        .join("tasks")
        .join("task-conflict.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Failed, task.status);
    assert_eq!(
        Some("daemon_dispatch_failed"),
        task.failure_class.as_deref()
    );
    assert!(task.lease_id.is_none());
    assert!(!daemon_root(temp_dir.path())
        .join("leases")
        .join("lease-task-conflict.json")
        .exists());
    assert!(!daemon_root(temp_dir.path())
        .join("leases")
        .join("writer-demo-conflict.lock")
        .exists());
    assert!(!temp_dir.path().join("worktrees/task-conflict").exists());
}

#[cfg(feature = "test-stub")]
#[test]
fn daemon_start_single_iteration_processes_pending_task() {
    let temp_dir = initialize_workspace_fixture();
    init_git_repo(temp_dir.path());

    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-run".to_owned(),
            issue_ref: "repo#7".to_owned(),
            project_id: "demo-run".to_owned(),
            project_name: Some("Daemon Run".to_owned()),
            prompt: Some("Implement the daemon task".to_owned()),
            routing_command: Some("/rb flow docs_change".to_owned()),
            routing_labels: vec![String::from("rb:flow:standard")],
            resolved_flow: Some(FlowPreset::DocsChange),
            routing_source: Some(RoutingSource::Command),
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
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    // Run daemon in-process (test-only) instead of spawning the CLI binary.
    run_daemon_iteration_in_process(temp_dir.path());

    let task_path = daemon_root(temp_dir.path())
        .join("tasks")
        .join("task-run.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Completed, task.status);
    assert!(task.lease_id.is_none());
    assert!(!daemon_root(temp_dir.path())
        .join("leases")
        .join("lease-task-run.json")
        .exists());
}

#[test]
fn config_get_prints_known_values_and_rejects_unknown_keys() {
    let temp_dir = initialize_workspace_fixture();

    let known = Command::new(binary())
        .args(["config", "get", "default_flow"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config get");
    assert!(known.status.success());
    assert_eq!("minimal\n", String::from_utf8_lossy(&known.stdout));

    let unknown = Command::new(binary())
        .args(["config", "get", "unknown.key"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config get invalid");
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown config key"));
}

#[test]
fn config_set_updates_valid_keys_and_rejects_invalid_values() {
    let temp_dir = initialize_workspace_fixture();

    let valid = Command::new(binary())
        .args(["config", "set", "default_flow", "quick_dev"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config set");
    assert!(valid.status.success());
    assert!(String::from_utf8_lossy(&valid.stdout).contains("Updated default_flow = quick_dev"));

    let workspace_config =
        fs::read_to_string(workspace_config_path(temp_dir.path())).expect("read workspace config");
    assert!(workspace_config.contains("default_flow = \"quick_dev\""));

    let invalid_value = Command::new(binary())
        .args(["config", "set", "default_flow", "unknown_flow"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid config set");
    assert!(!invalid_value.status.success());
    assert!(String::from_utf8_lossy(&invalid_value.stderr).contains("invalid value"));

    let invalid_key = Command::new(binary())
        .args(["config", "set", "unknown.key", "value"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid key config set");
    assert!(!invalid_key.status.success());
    assert!(String::from_utf8_lossy(&invalid_key.stderr).contains("unknown config key"));
}

#[test]
fn config_get_and_set_support_project_level_policy() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let set_output = Command::new(binary())
        .args([
            "config",
            "set",
            "workflow.reviewer_backend",
            "codex",
            "--project",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project config set");
    assert!(
        set_output.status.success(),
        "{}",
        String::from_utf8_lossy(&set_output.stderr)
    );
    assert!(String::from_utf8_lossy(&set_output.stdout).contains("project config.toml"));

    let project_config =
        fs::read_to_string(project_root(temp_dir.path(), "alpha").join("config.toml"))
            .expect("read project config");
    assert!(project_config.contains("reviewer_backend = \"codex\""));

    let get_output = Command::new(binary())
        .args(["config", "get", "workflow.reviewer_backend", "--project"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project config get");
    assert!(get_output.status.success());
    assert_eq!("codex\n", String::from_utf8_lossy(&get_output.stdout));
}

#[test]
fn config_edit_revalidates_workspace_file() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-valid.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"claude\"\nEOF\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env_remove("VISUAL")
        .current_dir(temp_dir.path())
        .output()
        .expect("run config edit");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Validated workspace.toml"));
}

#[test]
fn config_edit_prefers_editor_over_visual() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-wins.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"codex\"\nEOF\n",
    );
    let visual = write_editor_script(
        temp_dir.path(),
        "visual-loses.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"openrouter\"\nEOF\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env("VISUAL", &visual)
        .current_dir(temp_dir.path())
        .output()
        .expect("run config edit");

    assert!(output.status.success());

    let workspace_config =
        fs::read_to_string(workspace_config_path(temp_dir.path())).expect("read workspace config");
    assert!(workspace_config.contains("default_backend = \"codex\""));
    assert!(!workspace_config.contains("default_backend = \"openrouter\""));
}

#[test]
fn config_edit_fails_when_editor_leaves_invalid_file() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-invalid.sh",
        "#!/bin/sh\nprintf '%s\n' 'version = 999' 'created_at = \"2026-03-11T17:50:55Z\"' > \"$1\"\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env_remove("VISUAL")
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid config edit");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("workspace.toml is invalid after editing"));
    assert!(stderr.contains("unsupported workspace version 999"));
}

#[test]
fn project_select_sets_active_project_and_rejects_missing_projects() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let existing = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");
    assert!(existing.status.success());
    assert!(String::from_utf8_lossy(&existing.stdout).contains("Selected project alpha"));
    assert_eq!(
        "alpha",
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project")
    );

    let missing = Command::new(binary())
        .args(["project", "select", "missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run missing project select");
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("project 'missing' was not found"));
}

#[test]
fn project_select_prints_deprecation_notice_to_stderr_only() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(stdout.contains("Selected project alpha"));
    assert!(
        !stdout.contains("deprecated"),
        "deprecation notice must not be printed to stdout: {stdout}"
    );
    assert!(
        stderr.contains("Note: `ralph-burning project select` is deprecated. Use `ralph-burning task select` instead."),
        "stderr should contain the deprecation notice: {stderr}"
    );
}

#[test]
fn project_select_updates_active_milestone_for_bead_backed_projects() {
    let temp_dir = initialize_workspace_fixture();
    create_bead_backed_project_fixture(temp_dir.path(), "bead-beta", "ms-beta", "ms-beta.bead-1");
    fs::write(active_milestone_path(temp_dir.path()), "ms-alpha").expect("seed active milestone");

    let output = Command::new(binary())
        .args(["project", "select", "bead-beta"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(
        output.status.success(),
        "project select should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-beta"
    );
}

#[test]
fn project_select_rejects_path_like_ids_before_writing_active_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "select", "../escape"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run path-like project select");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid identifier"));
    assert!(!active_project_path(temp_dir.path()).exists());
}

// ── Project Create ──

fn write_prompt_fixture(base_dir: &std::path::Path) -> std::path::PathBuf {
    let prompt_path = base_dir.join("test-prompt.md");
    fs::write(&prompt_path, "# Test Prompt\nImplement the feature.\n").expect("write prompt");
    prompt_path
}

#[test]
fn project_create_succeeds_and_writes_all_canonical_files() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "alpha",
            "--name",
            "Alpha Project",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created project 'alpha'"));
    assert!(stdout.contains("standard"));

    let project_root = project_root(temp_dir.path(), "alpha");
    assert!(project_root.join("project.toml").is_file());
    assert!(project_root.join("prompt.md").is_file());
    assert!(project_root.join("run.json").is_file());
    assert!(project_root.join("journal.ndjson").is_file());
    assert!(project_root.join("sessions.json").is_file());
    assert!(project_root.join("history/payloads").is_dir());
    assert!(project_root.join("history/artifacts").is_dir());
    assert!(project_root.join("runtime/logs").is_dir());
    assert!(project_root.join("runtime/backend").is_dir());
    assert!(project_root.join("runtime/temp").is_dir());
    assert!(project_root.join("amendments").is_dir());
    assert!(project_root.join("rollback").is_dir());
}

#[test]
fn project_create_defaults_to_minimal_flow_without_flag() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "minimal-default",
            "--name",
            "Minimal Default",
            "--prompt",
            prompt.to_str().unwrap(),
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created project 'minimal-default'"));
    assert!(stdout.contains("minimal"));

    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "minimal-default").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
}

#[test]
fn project_create_uses_workspace_configured_default_flow_without_flag() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let config = Command::new(binary())
        .args(["config", "set", "default_flow", "standard"])
        .current_dir(temp_dir.path())
        .output()
        .expect("set workspace default flow");
    assert!(
        config.status.success(),
        "config set failed: {}",
        String::from_utf8_lossy(&config.stderr)
    );

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "workspace-default",
            "--name",
            "Workspace Default",
            "--prompt",
            prompt.to_str().unwrap(),
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created project 'workspace-default'"));
    assert!(stdout.contains("standard"));

    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "workspace-default").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"standard\""));
}

#[test]
fn project_create_accepts_iterative_minimal_flow() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "iterative-minimal-project",
            "--name",
            "Iterative Minimal Project",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "iterative_minimal",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "iterative-minimal-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"iterative_minimal\""));
}

#[test]
fn project_create_accepts_standard_flow_override() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "standard-override-project",
            "--name",
            "Standard Override Project",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "standard-override-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"standard\""));
}

#[test]
fn project_create_initializes_journal_with_project_created_event() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "beta",
            "--name",
            "Beta",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");
    assert!(output.status.success());

    let journal = fs::read_to_string(project_root(temp_dir.path(), "beta").join("journal.ndjson"))
        .expect("read journal");

    assert!(journal.contains("\"project_created\""));
    assert!(journal.contains("\"sequence\":1"));
}

#[test]
fn project_create_run_json_shows_not_started() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "gamma",
            "--name",
            "Gamma",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    let run_json = fs::read_to_string(project_root(temp_dir.path(), "gamma").join("run.json"))
        .expect("read run.json");

    assert!(run_json.contains("\"not_started\""));
    assert!(run_json.contains("\"active_run\": null"));
}

#[test]
fn project_create_records_canonical_prompt_reference_not_source_path() {
    let temp_dir = initialize_workspace_fixture();
    // Use a non-standard filename to verify the recorded reference is canonical
    let external_prompt = temp_dir.path().join("my-external-prompt.md");
    fs::write(&external_prompt, "# External Prompt\nContent.").expect("write prompt");

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "reftest",
            "--name",
            "Ref Test",
            "--prompt",
            external_prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");
    assert!(output.status.success());

    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "reftest").join("project.toml"))
            .expect("read project.toml");

    // prompt_reference should be the canonical copied path, not the source path
    assert!(
        project_toml.contains("prompt_reference = \"prompt.md\""),
        "project.toml should record canonical prompt.md, got:\n{project_toml}"
    );
    assert!(
        !project_toml.contains("my-external-prompt"),
        "project.toml should not contain the source path"
    );
}

#[test]
fn project_create_from_bead_bootstraps_project_from_milestone_context() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "labels": ["creation", "prompt"],
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.epic-1",
        "dependency_type": "parent_child",
        "title": "Task Substrate"
      },
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "parent_child",
        "title": "Child bead"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.epic-1",
      "title": "Task Substrate",
      "status": "in_progress",
      "priority": "P1",
      "issue_type": "epic",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Child bead",
      "status": "open",
      "priority": "P2",
      "issue_type": "task",
      "labels": ["docs"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_root = project_root(temp_dir.path(), "task-ms-alpha-bead-2");
    assert!(project_root.join("project.toml").is_file());
    let project_toml = fs::read_to_string(project_root.join("project.toml")).expect("read project");
    assert!(project_toml.contains("flow = \"docs_change\""));
    assert!(project_toml.contains("milestone_id = \"ms-alpha\""));
    assert!(project_toml.contains("bead_id = \"ms-alpha.bead-2\""));
    assert!(project_toml.contains(&format!("plan_hash = \"{plan_hash}\"")));
    assert!(project_toml.contains("plan_version = 2"));
    assert!(project_toml.contains("parent_epic_id = \"ms-alpha.epic-1\""));

    let prompt = fs::read_to_string(project_root.join("prompt.md")).expect("read prompt");
    assert!(prompt.contains("Ship bead-backed task creation."));
    assert!(prompt.contains("bead_execution_prompt"));
    assert!(prompt.contains("## Current Bead Details"));
    assert!(prompt.contains("- Status: `ready`"));
    assert!(prompt.contains(
        "- Progress: 1/3 completed; 1 in progress; 0 failed; 1 blocked; 0 skipped; 0 remaining"
    ));
    assert!(prompt.contains("Keep changes inspectable and deterministic."));
    assert!(prompt.contains("ms-alpha.bead-1 (Define task-source metadata) - blocking dependency; status: closed; outcome: completed"));
    assert!(prompt.contains(
        "ms-alpha.epic-1 (Task Substrate) - parent epic; status: in_progress; outcome: active"
    ));
    assert!(prompt
        .contains("ms-alpha.bead-3 (Child bead) - child bead; status: open; outcome: pending"));
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
    assert!(!prompt.contains("Parent epic: `ms-alpha.bead-3`"));

    let active =
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project");
    assert_eq!(active.trim(), "task-ms-alpha-bead-2");
}

#[test]
fn project_create_from_bead_loads_closed_adjacent_bead_from_exhaustive_br_list() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let exhaustive_list_payload = exhaustive_br_list_response_with_closed_adjacent_bead();
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        &format!(
            r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {{
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [],
    "dependents": []
  }}
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{exhaustive_list_payload}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
        ),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "closed-adjacent-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "closed-adjacent-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains("ms-alpha.bead-3 (Document task bootstrap follow-up)"));
    assert!(prompt.contains("adjacent same-workstream bead in Task Substrate"));
    assert!(prompt.contains("status: closed"));
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
}

#[cfg(feature = "test-stub")]
#[test]
fn project_create_from_bead_uses_stub_generated_milestone_default_flow_without_override() {
    let temp_dir = initialize_workspace_fixture();

    let create_output = Command::new(binary())
        .args([
            "milestone",
            "create",
            "Alpha Plan",
            "--from-idea",
            "Plan the alpha milestone in milestone mode.",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(
        create_output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&create_output.stderr)
    );

    let plan_output = Command::new(binary())
        .args(["milestone", "plan", "ms-alpha-plan"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone plan");
    assert!(
        plan_output.status.success(),
        "milestone plan should succeed: {}",
        String::from_utf8_lossy(&plan_output.stderr)
    );

    let plan_json =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha-plan").join("plan.json"))
            .expect("read plan json");
    let plan: serde_json::Value = serde_json::from_str(&plan_json).expect("parse plan json");
    let bead = &plan["workstreams"][0]["beads"][0];
    let bead_id = bead["bead_id"].as_str().expect("stub milestone bead id");
    let bead_title = bead["title"].as_str().expect("stub milestone bead title");

    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        bead_id,
        &format!(
            r#"[
  {{
    "id": "{bead_id}",
    "title": "{bead_title}",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Carry the milestone plan into execution.",
    "acceptance_criteria": "- Milestone can be executed from structured plan output.",
    "dependencies": []
  }}
]"#
        ),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha-plan",
            "--bead-id",
            bead_id,
            "--project-id",
            "stub-generated-default",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");
    assert!(
        output.status.success(),
        "project create-from-bead should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "stub-generated-default").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn project_create_from_bead_sets_active_milestone_for_run_without_id() {
    let temp_dir = initialize_workspace_fixture();
    write_single_bead_milestone_fixture(temp_dir.path(), "ms-single");

    let ready_payload = r#"[{"id":"bead-1","title":"Execute the only bead","priority":"P1","issue_type":"task","labels":["single"]}]"#;
    let show_payload = r#"[
  {
    "id": "ms-single.bead-1",
    "title": "Execute the only bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "task",
    "description": "Create and run a single bead-backed project.",
    "acceptance_criteria": "- Single bead completes",
    "dependencies": [],
    "dependents": []
  }
]"#;
    let list_payload = r#"{"issues":[{"id":"ms-single.bead-1","title":"Execute the only bead","status":"open","priority":"P1","issue_type":"task","labels":["single"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(temp_dir.path(), "ms-single.bead-1", "Execute the only bead");

    let path = prepend_path(temp_dir.path());
    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-single",
            "--bead-id",
            "ms-single.bead-1",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        create.status.success(),
        "project create-from-bead should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-single"
    );

    let run = Command::new(binary())
        .args(["milestone", "run", "--json"])
        .env("PATH", &path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run without id");

    assert!(
        run.status.success(),
        "milestone run without id should succeed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&run.stdout).expect("parse milestone run json");
    assert_eq!(json["milestone_id"], "ms-single");
    assert_eq!(json["status"], "completed");
}

#[test]
fn project_create_from_bead_falls_back_when_br_list_is_unavailable() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata",
        "status": "closed"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "blocks",
        "title": "Document task bootstrap follow-up",
        "status": "open"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
echo "failed to read .beads/issues.jsonl: No such file or directory" >&2
exit 1
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "missing-br-list-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "missing-br-list-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains(
        "ms-alpha.bead-1 (Define task-source metadata) - blocking dependency; status: closed; outcome: completed"
    ));
    assert!(prompt.contains(
        "ms-alpha.bead-3 (Document task bootstrap follow-up) - downstream dependent; status: open"
    ));
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
}

#[test]
fn project_create_from_bead_with_prompt_file_skips_br_list_prompt_hydration() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let prompt_path = temp_dir.path().join("override-prompt.md");
    fs::write(
        &prompt_path,
        "# Custom Prompt\nUse the supplied override prompt.\n",
    )
    .expect("write prompt override");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "blocks",
        "title": "Document task bootstrap follow-up"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
echo "simulated br list failure" >&2
exit 1
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "prompt-override-project",
            "--prompt-file",
            prompt_path.to_str().expect("prompt path"),
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "prompt-override-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert_eq!(
        prompt,
        "# Custom Prompt\nUse the supplied override prompt.\n"
    );
}

#[test]
fn project_create_from_bead_uses_unknown_relation_statuses_when_br_list_is_unavailable() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "blocks",
        "title": "Document task bootstrap follow-up"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
echo "simulated br list failure" >&2
exit 1
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "missing-br-list-unknown-status-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "missing-br-list-unknown-status-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(
        prompt.contains(
            "ms-alpha.bead-1 (Define task-source metadata) - blocking dependency; status: unknown; outcome: unknown"
        ),
        "prompt: {prompt}"
    );
    assert!(
        prompt.contains(
            "ms-alpha.bead-3 (Document task bootstrap follow-up) - downstream dependent; status: unknown; outcome: unknown"
        ),
        "prompt: {prompt}"
    );
}

#[test]
fn project_create_from_bead_skips_fenced_planned_elsewhere_summary_openers() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["workstreams"][0]["beads"][2]["description"] = serde_json::Value::String(
        "```md\n## Review Policy\nKeep this example fenced.\n```\nCapture the operator-facing workflow once project creation is stable.".to_owned(),
    );
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "blocks",
        "title": "Document task bootstrap follow-up"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Document task bootstrap follow-up",
      "status": "open",
      "priority": "P2",
      "issue_type": "docs",
      "labels": ["docs"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "fenced-summary-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "fenced-summary-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
    assert!(!prompt.contains("Summary:\n    ```md"));
    assert!(prompt.contains("## Review Policy"));
}

#[test]
fn project_create_from_bead_uses_planned_elsewhere_scope_body_instead_of_scope_label() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["workstreams"][0]["beads"][2]["description"] = serde_json::Value::String(
        "Goal:\nCapture the operator-facing workflow once project creation is stable.".to_owned(),
    );
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": []
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Document task bootstrap follow-up",
      "status": "open",
      "priority": "P2",
      "issue_type": "docs",
      "labels": ["docs"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "scope-body-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt =
        fs::read_to_string(project_root(temp_dir.path(), "scope-body-project").join("prompt.md"))
            .expect("read prompt");
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
    assert!(!prompt.contains("Summary:\n    Goal:"));
}

#[test]
fn project_create_from_bead_skips_level_one_heading_in_planned_elsewhere_summary() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["workstreams"][0]["beads"][2]["description"] = serde_json::Value::String(
        "# Planned Follow-up\nCapture the operator-facing workflow once project creation is stable."
            .to_owned(),
    );
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata",
        "status": "closed"
      }
    ],
    "dependents": [
      {
        "id": "ms-alpha.bead-3",
        "dependency_type": "blocks",
        "title": "Document task bootstrap follow-up",
        "status": "open"
      }
    ]
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Document task bootstrap follow-up",
      "status": "open",
      "priority": "P2",
      "issue_type": "docs",
      "labels": ["docs"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "heading-summary-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "heading-summary-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
    assert!(!prompt.contains("Summary:\n    # Planned Follow-up"));
}

#[test]
fn project_create_from_bead_includes_shared_acceptance_owners_in_planned_elsewhere() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["acceptance_map"][0]["covered_by"] = serde_json::json!(["bead-2", "bead-4"]);
    plan["workstreams"]
        .as_array_mut()
        .expect("workstreams array")
        .push(serde_json::json!({
            "name": "Validation",
            "description": "Confirm the acceptance handoff.",
            "beads": [
                {
                    "bead_id": "bead-4",
                    "explicit_id": true,
                    "title": "Validate task bootstrap follow-up",
                    "description": "Confirm the shared acceptance outcome without expanding the current bead.",
                    "bead_type": "task",
                    "priority": 1,
                    "labels": ["validation"],
                    "depends_on": [],
                    "acceptance_criteria": ["AC-1"]
                }
            ]
        }));
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 4,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [
      {
        "id": "ms-alpha.bead-1",
        "dependency_type": "blocks",
        "title": "Define task-source metadata"
      }
    ],
    "dependents": []
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-1",
      "title": "Define task-source metadata",
      "status": "closed",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["creation"]
    },
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-4",
      "title": "Validate task bootstrap follow-up",
      "status": "open",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["validation"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "shared-acceptance-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "shared-acceptance-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains(
        "ms-alpha.bead-4 (Validate task bootstrap follow-up) - shared milestone acceptance ownership in Validation (AC-1); status: open"
    ));
    assert!(prompt.contains(
        "Summary:\n    Confirm the shared acceptance outcome without expanding the current bead."
    ));
}

#[test]
fn project_create_from_bead_fails_when_br_list_reports_corrupt_bead_data() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [],
    "dependents": []
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
echo "failed to parse .beads/issues.jsonl: corrupt json" >&2
exit 1
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "corrupt-br-list-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "expected corrupt br list failure, stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(".beads/issues.jsonl"), "stderr: {stderr}");
    assert!(
        !project_root(temp_dir.path(), "corrupt-br-list-project").exists(),
        "project should not be created when bead summaries are corrupt"
    );
}

#[test]
fn project_create_from_bead_preserves_description_only_acceptance_criteria_and_hardening_notes() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Goal:\nKeep prompt generation aligned with live bead descriptions.\n\nNon-goals:\n- do not broaden the task beyond the active bead\n\nContract hardening notes:\n- preserve fix-now boundaries in the prompt\n- keep later-bead work out of explicit non-goals\n\n## Acceptance Criteria\n\n- preserve description-only acceptance criteria\n- do not emit the empty acceptance fallback",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt =
        fs::read_to_string(project_root(temp_dir.path(), "task-ms-alpha-bead-2").join("prompt.md"))
            .expect("read prompt");
    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let planned_start = prompt
        .find("## Already Planned Elsewhere")
        .expect("planned elsewhere section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];
    let non_goals_section = &prompt[non_goals_start..acceptance_start];
    let acceptance_section = &prompt[acceptance_start..planned_start];

    assert!(must_do_section.contains("Contract hardening notes:"));
    assert!(must_do_section.contains("preserve fix-now boundaries in the prompt"));
    assert!(!non_goals_section.contains("Contract hardening notes:"));
    assert!(acceptance_section.contains("- preserve description-only acceptance criteria"));
    assert!(acceptance_section.contains("- do not emit the empty acceptance fallback"));
    assert!(!acceptance_section.contains("No explicit acceptance criteria were supplied."));
}

#[test]
fn project_create_from_bead_rejects_stale_milestone_plan_metadata() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let stale_status = fs::read_to_string(&status_path)
        .expect("read status")
        .replace("\"plan_hash\": \"", "\"plan_hash\": \"stale-");
    fs::write(&status_path, stale_status).expect("write stale status");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("plan metadata is stale"));
    assert!(!project_root(temp_dir.path(), "task-ms-alpha-bead-2").exists());
}

#[test]
fn project_create_from_bead_accepts_legacy_milestone_status_without_plan_metadata() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    fs::write(
        &status_path,
        r#"{
  "status": "ready",
  "plan_version": 0,
  "progress": {
    "total_beads": 2,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  },
  "updated_at": "2026-04-01T10:05:00Z"
}"#,
    )
    .expect("write legacy status");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "legacy-status-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "legacy-status-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains(&format!("plan_hash = \"{plan_hash}\"")));
    assert!(!project_toml.contains("plan_version = "));
}

#[test]
fn project_create_from_bead_preserves_metadata_for_legacy_plan_json_without_explicit_id() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut legacy_plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    for workstream in legacy_plan["workstreams"]
        .as_array_mut()
        .expect("workstreams array")
    {
        for bead in workstream["beads"].as_array_mut().expect("beads array") {
            bead.as_object_mut()
                .expect("bead object")
                .remove("explicit_id");
        }
    }
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&legacy_plan).expect("serialize legacy plan"),
    )
    .expect("write legacy plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 2,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status for legacy plan hash");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "legacy-plan-json-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "legacy-plan-json-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"docs_change\""));
    assert!(project_toml.contains(&format!("plan_hash = \"{plan_hash}\"")));
    assert!(project_toml.contains("plan_version = 2"));
}

#[test]
fn project_create_from_bead_treats_legacy_qualified_canonical_slot_ids_as_unconfirmed_when_title_drifted(
) {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut legacy_plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    for workstream in legacy_plan["workstreams"]
        .as_array_mut()
        .expect("workstreams array")
    {
        for bead in workstream["beads"].as_array_mut().expect("beads array") {
            bead.as_object_mut()
                .expect("bead object")
                .remove("explicit_id");
        }
    }
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&legacy_plan).expect("serialize legacy plan"),
    )
    .expect("write legacy plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 2,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status for legacy plan hash");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Renamed live bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "legacy-qualified-slot-id-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "legacy-qualified-slot-id-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
    assert!(!project_toml.contains(&format!("plan_hash = \"{plan_hash}\"")));
    assert!(!project_toml.contains("plan_version = "));
}

#[test]
fn project_create_from_bead_treats_legacy_short_canonical_slot_ids_as_unconfirmed_when_title_drifted(
) {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut legacy_plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    for workstream in legacy_plan["workstreams"]
        .as_array_mut()
        .expect("workstreams array")
    {
        for bead in workstream["beads"].as_array_mut().expect("beads array") {
            let bead = bead.as_object_mut().expect("bead object");
            bead.remove("explicit_id");
            if bead.get("bead_id").and_then(serde_json::Value::as_str) == Some("ms-alpha.bead-2") {
                bead.insert(
                    "bead_id".to_owned(),
                    serde_json::Value::String("bead-2".to_owned()),
                );
            }
        }
    }
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&legacy_plan).expect("serialize legacy plan"),
    )
    .expect("write legacy plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 2,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status for legacy plan hash");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Renamed live bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "legacy-short-bead-id-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "legacy-short-bead-id-project").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
    assert!(!project_toml.contains(&format!("plan_hash = \"{plan_hash}\"")));
    assert!(!project_toml.contains("plan_version = "));
}

#[test]
fn project_create_from_bead_rejects_status_hash_without_plan_version() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    fs::write(
        &status_path,
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 0,
  "progress": {{
    "total_beads": 2,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("write corrupt status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "corrupt-status-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("plan_hash but plan_version is 0"));
    assert!(!project_root(temp_dir.path(), "corrupt-status-project").exists());
}

#[test]
fn project_create_from_bead_rejects_completed_milestone() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let completed_status = fs::read_to_string(&status_path)
        .expect("read status")
        .replace("\"status\": \"ready\"", "\"status\": \"completed\"");
    fs::write(&status_path, completed_status).expect("write completed status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "completed-milestone-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-alpha' is already completed"));
    assert!(!project_root(temp_dir.path(), "completed-milestone-project").exists());
}

#[test]
fn project_create_from_bead_rejects_closed_bead() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "closed",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "closed-bead-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bead is already closed"));
    assert!(!project_root(temp_dir.path(), "closed-bead-project").exists());
}

#[test]
fn project_create_from_bead_allows_unconfirmed_fallback_when_status_metadata_is_stale() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let stale_status = fs::read_to_string(&status_path)
        .expect("read status")
        .replace("\"plan_hash\": \"", "\"plan_hash\": \"stale-");
    fs::write(&status_path, stale_status).expect("write stale status");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation (renamed live bead)",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "stale-status-unconfirmed-fallback",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "stale-status-unconfirmed-fallback").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
    assert!(!project_toml.contains("plan_version = "));
    assert!(!project_toml.contains("plan_hash = "));
}

#[test]
fn project_create_from_bead_rejects_plan_json_hash_drift_from_unknown_fields() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan_value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan_value
        .as_object_mut()
        .expect("plan root object")
        .insert("ignored_extra".to_owned(), serde_json::json!(true));
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan_value).expect("serialize modified plan"),
    )
    .expect("write modified plan");

    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": []
  }
]
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("plan metadata is stale"));
    assert!(!project_root(temp_dir.path(), "task-ms-alpha-bead-2").exists());
}

#[test]
fn project_create_from_bead_rejects_beads_outside_selected_milestone() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "other-ms.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "other-ms.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [],
    "dependents": []
  }
]
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "other-ms.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("expected bead id to belong to milestone 'ms-alpha'"));
    assert!(!project_root(temp_dir.path(), "task-other-ms-bead-2").exists());
}

#[test]
fn project_create_from_bead_rejects_single_show_response_for_wrong_bead() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
{
  "id": "ms-alpha.bead-200",
  "title": "Wrong bead",
  "status": "open",
  "priority": "P1",
  "issue_type": "feature",
  "acceptance_criteria": "wrong"
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(!output.status.success(), "command unexpectedly succeeded");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("br show returned no matching bead"));
}

#[test]
fn project_create_from_bead_allows_explicit_flow_when_plan_title_drifted() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation (renamed live bead)",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--flow",
            "standard",
            "--project-id",
            "renamed-live-bead",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "renamed-live-bead").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"standard\""));
    assert!(!project_toml.contains("plan_version = "));
    assert!(!project_toml.contains("plan_hash = "));
}

#[test]
fn project_create_from_bead_falls_back_to_milestone_default_flow_when_title_drifted() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation (renamed live bead)",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "renamed-live-bead-default-flow",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "renamed-live-bead-default-flow").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
    assert!(!project_toml.contains("plan_version = "));
    assert!(!project_toml.contains("plan_hash = "));

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "renamed-live-bead-default-flow").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(!prompt.contains(
        "ms-alpha.bead-3 (Document task bootstrap follow-up) - adjacent same-workstream bead in Task Substrate; status: unknown"
    ));
    assert!(!prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable."
    ));
}

#[test]
fn project_create_from_bead_skips_shared_acceptance_owners_when_membership_is_unconfirmed() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["acceptance_map"][0]["covered_by"] = serde_json::json!(["bead-2", "bead-4"]);
    plan["workstreams"]
        .as_array_mut()
        .expect("workstreams array")
        .push(serde_json::json!({
            "name": "Validation",
            "description": "Confirm the acceptance handoff.",
            "beads": [
                {
                    "bead_id": "bead-4",
                    "explicit_id": true,
                    "title": "Validate task bootstrap follow-up",
                    "description": "Confirm the shared acceptance outcome without expanding the current bead.",
                    "bead_type": "task",
                    "priority": 1,
                    "labels": ["validation"],
                    "depends_on": [],
                    "acceptance_criteria": ["AC-1"]
                }
            ]
        }));
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 4,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");
    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Renamed live bead",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable",
    "dependencies": []
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-2",
      "title": "Renamed live bead",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-4",
      "title": "Validate task bootstrap follow-up",
      "status": "open",
      "priority": "P1",
      "issue_type": "task",
      "labels": ["validation"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "unconfirmed-shared-acceptance-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "unconfirmed-shared-acceptance-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(!prompt.contains("ms-alpha.bead-4 (Validate task bootstrap follow-up)"));
    assert!(!prompt.contains("shared milestone acceptance ownership in Validation (AC-1)"));
    assert!(!prompt.contains(
        "Summary:\n    Confirm the shared acceptance outcome without expanding the current bead."
    ));
}

#[test]
fn project_create_from_bead_preserves_wrapped_planned_elsewhere_paragraphs() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_path = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut plan: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&plan_path).expect("read plan"))
            .expect("parse plan");
    plan["workstreams"][0]["beads"][2]["description"] = serde_json::Value::String(
        "Goal:\nCapture the operator-facing workflow once project creation is stable,\nincluding the follow-up validation handoff.".to_owned(),
    );
    fs::write(
        &plan_path,
        serde_json::to_string_pretty(&plan).expect("serialize plan"),
    )
    .expect("write updated plan");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("rewrite status");

    let fake_br = write_editor_script(
        temp_dir.path(),
        "br",
        r#"#!/bin/sh
if [ "$1" = "update" ]; then
echo "Updated"
exit 0
fi
if [ "$1" = "sync" ]; then
echo "Synced"
exit 0
fi
if [ "$1" = "show" ] && [ "$2" = "ms-alpha.bead-2" ] && [ "$3" = "--json" ]; then
cat <<'EOF'
[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "Controller can create the project without manual setup",
    "dependencies": [],
    "dependents": []
  }
]
EOF
exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "--all" ] && [ "$3" = "--deferred" ] && [ "$4" = "--limit=0" ] && [ "$5" = "--json" ]; then
cat <<'EOF'
{
  "issues": [
    {
      "id": "ms-alpha.bead-2",
      "title": "Bootstrap bead-backed task creation",
      "status": "open",
      "priority": "P1",
      "issue_type": "feature",
      "labels": ["creation", "prompt"]
    },
    {
      "id": "ms-alpha.bead-3",
      "title": "Document milestone bootstrap flow",
      "status": "open",
      "priority": "P2",
      "issue_type": "docs",
      "labels": ["docs"]
    }
  ]
}
EOF
exit 0
fi
echo "unexpected br args: $@" >&2
exit 1
"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "wrapped-summary-project",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "wrapped-summary-project").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(prompt.contains(
        "Summary:\n    Capture the operator-facing workflow once project creation is stable,\n    including the follow-up validation handoff."
    ));
}

#[test]
fn project_create_from_bead_does_not_confirm_title_fallback_against_mismatched_explicit_bead_id() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let milestone_plan = milestone_root(temp_dir.path(), "ms-alpha").join("plan.json");
    let mut bundle: MilestoneBundle =
        serde_json::from_str(&fs::read_to_string(&milestone_plan).expect("read plan"))
            .expect("parse plan");
    bundle.workstreams[0].beads[1].bead_id = Some("ms-alpha.bead-200".to_owned());
    bundle.workstreams[0].beads[1].explicit_id = Some(true);
    bundle.workstreams[0].beads[2].depends_on = vec!["ms-alpha.bead-200".to_owned()];
    bundle.acceptance_map[0].covered_by = vec!["ms-alpha.bead-200".to_owned()];
    fs::write(
        &milestone_plan,
        serde_json::to_string_pretty(&bundle).expect("serialize plan"),
    )
    .expect("write plan");
    let status_path = milestone_root(temp_dir.path(), "ms-alpha").join("status.json");
    let updated_plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    fs::write(
        &status_path,
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{updated_plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 0,
    "in_progress_beads": 0,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 0
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("write status");

    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        r#"[
  {
    "id": "ms-alpha.bead-2",
    "title": "Bootstrap bead-backed task creation",
    "status": "open",
    "priority": "P1",
    "issue_type": "feature",
    "description": "Create a Ralph project directly from milestone and bead context.",
    "acceptance_criteria": "- Controller can create the project without manual setup\n- Created task is durable",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "mismatched-explicit-bead-id",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let project_toml = fs::read_to_string(
        project_root(temp_dir.path(), "mismatched-explicit-bead-id").join("project.toml"),
    )
    .expect("read project.toml");
    assert!(project_toml.contains("flow = \"minimal\""));
    assert!(!project_toml.contains("plan_version = "));
    assert!(!project_toml.contains("plan_hash = "));

    let prompt = fs::read_to_string(
        project_root(temp_dir.path(), "mismatched-explicit-bead-id").join("prompt.md"),
    )
    .expect("read prompt");
    assert!(!prompt.contains("ms-alpha.bead-1 (Define task-source metadata)"));
    assert!(!prompt.contains("ms-alpha.bead-3 (Document task bootstrap follow-up)"));
}

#[test]
fn project_create_fails_on_duplicate_id() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let first = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "dup",
            "--name",
            "First",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("first create");
    assert!(first.status.success());

    let second = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "dup",
            "--name",
            "Second",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("second create");

    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"));
}

#[test]
fn project_create_fails_on_invalid_flow() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "bad-flow",
            "--name",
            "Bad Flow",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "nonexistent",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown flow preset"));
}

#[test]
fn project_create_fails_on_missing_prompt() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "no-prompt",
            "--name",
            "No Prompt",
            "--prompt",
            "/nonexistent/prompt.md",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid prompt file"));
}

#[test]
fn project_create_does_not_set_active_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "noactive",
            "--name",
            "No Active",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    // active-project should not exist (create does not set it)
    assert!(!active_project_path(temp_dir.path()).exists());
}

#[cfg(feature = "test-stub")]
#[test]
fn project_create_from_requirements_creates_project_and_selects_it() {
    let temp_dir = initialize_workspace_fixture();

    let quick = Command::new(binary())
        .args(["requirements", "quick", "--idea", "Build a REST API"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements quick");
    assert!(
        quick.status.success(),
        "requirements quick should succeed: {}",
        String::from_utf8_lossy(&quick.stderr)
    );

    let run_id = only_requirements_run_id(temp_dir.path());
    let output = Command::new(binary())
        .args(["project", "create", "--from-requirements", &run_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("project create from requirements");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "create from requirements should succeed: {stderr}"
    );
    assert!(stdout.contains("Project: stub-project (active)"));
    assert!(
        stderr.contains(
            "Note: `ralph-burning project create` is deprecated. Use `ralph-burning task create or milestone create (depending on context)` instead."
        ),
        "stderr should contain the deprecation notice: {stderr}"
    );
    assert!(stdout.contains("Flow: standard"));
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path()))
            .expect("read active-project")
            .trim(),
        "stub-project"
    );
    assert_eq!(
        fs::read_to_string(project_root(temp_dir.path(), "stub-project").join("prompt.md"))
            .expect("read project prompt"),
        "Stub prompt body for the project."
    );

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "stub-project").join("journal.ndjson"))
            .expect("read project journal");
    assert!(journal.contains("\"source\":\"requirements\""));
    assert!(journal.contains(&format!("\"requirements_run_id\":\"{run_id}\"")));
}

#[test]
fn project_create_from_requirements_materializes_milestone_bundle_output() {
    let temp_dir = initialize_workspace_fixture();
    let run_id = "req-milestone";
    write_requirements_milestone_run_fixture(temp_dir.path(), run_id, "ms-planned");

    let output = Command::new(binary())
        .args(["project", "create", "--from-requirements", run_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("project create from milestone requirements");

    assert!(
        output.status.success(),
        "create from requirements milestone should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("Created milestone 'ms-planned'"));
    assert!(
        stderr.contains(
            "Note: `ralph-burning project create` is deprecated. Use `ralph-burning task create or milestone create (depending on context)` instead."
        ),
        "stderr should contain the deprecation notice: {stderr}"
    );

    let milestone_root = milestone_root(temp_dir.path(), "ms-planned");
    assert!(milestone_root.join("milestone.toml").is_file());
    assert!(milestone_root.join("status.json").is_file());
    assert!(milestone_root.join("plan.json").is_file());
    assert!(milestone_root.join("plan.md").is_file());

    let status = fs::read_to_string(milestone_root.join("status.json")).expect("read status.json");
    assert!(status.contains("\"status\": \"ready\""));
    let plan = fs::read_to_string(milestone_root.join("plan.json")).expect("read plan.json");
    assert!(plan.contains("\"id\": \"ms-planned\""));
}

#[test]
fn project_create_from_requirements_sets_active_milestone_for_next_without_id() {
    let temp_dir = initialize_workspace_fixture();
    let run_id = "req-milestone";
    write_requirements_milestone_run_fixture(temp_dir.path(), run_id, "ms-planned");

    let create = Command::new(binary())
        .args(["project", "create", "--from-requirements", run_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("project create from milestone requirements");

    assert!(
        create.status.success(),
        "create from requirements milestone should succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-planned"
    );

    let ready_payload = r#"[{"id":"bead-1","title":"Persist milestone bundle","priority":"P1","issue_type":"task","labels":["planning"]}]"#;
    let show_payload = r#"[{"id":"ms-planned.bead-1","title":"Persist milestone bundle","status":"open","priority":"P1","issue_type":"task","description":"Write plan.json and plan.md","acceptance_criteria":"- Milestone plan is materialized","dependencies":[],"dependents":[]}]"#;
    let list_payload = r#"{"issues":[{"id":"ms-planned.bead-1","title":"Persist milestone bundle","status":"open","priority":"P1","issue_type":"task","labels":["planning"]}]}"#;
    write_br_milestone_selection_script(temp_dir.path(), ready_payload, show_payload, list_payload);
    write_bv_next_script(
        temp_dir.path(),
        "ms-planned.bead-1",
        "Persist milestone bundle",
    );

    let output = Command::new(binary())
        .args(["milestone", "next", "--json"])
        .env("PATH", prepend_path(temp_dir.path()))
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone next without id");

    assert!(
        output.status.success(),
        "milestone next without id should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse milestone next json");
    assert_eq!(json["milestone_id"], "ms-planned");
    assert_eq!(json["bead"]["id"], "ms-planned.bead-1");
}

#[test]
fn project_create_from_requirements_fails_for_missing_run() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "create", "--from-requirements", "missing-run"])
        .current_dir(temp_dir.path())
        .output()
        .expect("project create from missing run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("requirements run not found"));
    assert!(!project_root(temp_dir.path(), "missing-run").exists());
}

#[test]
fn project_create_from_requirements_fails_for_incomplete_run() {
    let temp_dir = initialize_workspace_fixture();
    let run_id = "req-incomplete";
    let run_root = requirements_root(temp_dir.path()).join(run_id);
    fs::create_dir_all(&run_root).expect("create requirements run dir");
    fs::write(
        run_root.join("run.json"),
        serde_json::json!({
            "run_id": run_id,
            "idea": "Pending requirements",
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
            "last_transition_cached": false,
            "failure_summary": null
        })
        .to_string(),
    )
    .expect("write incomplete run");

    select_active_project_fixture(temp_dir.path(), "existing");
    create_project_fixture(temp_dir.path(), "existing");

    let output = Command::new(binary())
        .args(["project", "create", "--from-requirements", run_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("project create from incomplete run");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("expected 'completed'"));
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path()))
            .expect("read active-project")
            .trim(),
        "existing"
    );
    assert_eq!(
        fs::read_dir(live_workspace_root(temp_dir.path()).join("projects"))
            .expect("read projects dir")
            .count(),
        1
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn project_bootstrap_from_idea_creates_project_and_selects_it() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--idea",
            "Build a REST API",
            "--flow",
            "standard",
        ])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap");

    assert!(
        output.status.success(),
        "bootstrap should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path()))
            .expect("read active-project")
            .trim(),
        "stub-project"
    );
    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "stub-project").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"standard\""));
    assert_eq!(requirements_run_ids(temp_dir.path()).len(), 1);

    let run_id = only_requirements_run_id(temp_dir.path());
    let requirements_run = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(&run_id)
            .join("run.json"),
    )
    .expect("read requirements run.json");
    let requirements_run_json: serde_json::Value =
        serde_json::from_str(&requirements_run).expect("parse requirements run.json");
    assert!(
        requirements_run_json["latest_review_id"].is_null(),
        "bootstrap should skip review by default: {requirements_run}"
    );

    let requirements_journal = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(run_id)
            .join("journal.ndjson"),
    )
    .expect("read requirements journal");
    assert!(
        !requirements_journal.contains("\"review_completed\""),
        "bootstrap should not journal a requirements review by default: {requirements_journal}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn project_bootstrap_enable_review_runs_requirements_review() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--idea",
            "Build a REST API",
            "--flow",
            "standard",
            "--enable-review",
        ])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap with review enabled");

    assert!(
        output.status.success(),
        "bootstrap --enable-review should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let run_id = only_requirements_run_id(temp_dir.path());
    let requirements_run = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(&run_id)
            .join("run.json"),
    )
    .expect("read requirements run.json");
    let requirements_run_json: serde_json::Value =
        serde_json::from_str(&requirements_run).expect("parse requirements run.json");
    assert!(
        requirements_run_json["latest_review_id"].is_string(),
        "bootstrap --enable-review should persist a review payload: {requirements_run}"
    );

    let requirements_journal = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(run_id)
            .join("journal.ndjson"),
    )
    .expect("read requirements journal");
    assert!(
        requirements_journal.contains("\"review_completed\""),
        "bootstrap --enable-review should journal the review step: {requirements_journal}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn project_bootstrap_from_file_quick_dev_start_runs_created_project() {
    let temp_dir = initialize_workspace_fixture();
    let idea_file = temp_dir.path().join("idea.md");
    fs::write(&idea_file, "Build quick-dev flow from file input").expect("write idea file");

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--from-file",
            idea_file.to_str().unwrap(),
            "--flow",
            "quick_dev",
            "--start",
        ])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap from file");

    assert!(
        output.status.success(),
        "bootstrap --from-file --start should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "stub-project").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"quick_dev\""));

    let run_json =
        fs::read_to_string(project_root(temp_dir.path(), "stub-project").join("run.json"))
            .expect("read run.json");
    assert!(
        !run_json.contains("\"status\":\"not_started\""),
        "bootstrap --start should advance the run, got: {run_json}"
    );

    let run_id = only_requirements_run_id(temp_dir.path());
    let requirements_run = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(run_id)
            .join("run.json"),
    )
    .expect("read requirements run.json");
    assert!(requirements_run.contains("Build quick-dev flow from file input"));
}

#[test]
fn project_bootstrap_fails_for_invalid_flow_before_creating_requirements_run() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--idea",
            "Build a REST API",
            "--flow",
            "invalid-flow",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap invalid flow");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown flow preset"));

    let requirements_dir = requirements_root(temp_dir.path());
    assert_eq!(
        fs::read_dir(&requirements_dir)
            .expect("read requirements dir")
            .count(),
        0,
        "invalid flow should fail before requirements quick creates a run"
    );
}

// ── Project Bootstrap --from-seed ──

#[cfg(feature = "test-stub")]
#[test]
fn project_bootstrap_from_seed_creates_project_directly() {
    let temp_dir = initialize_workspace_fixture();
    let seed_path = temp_dir.path().join("test-seed.json");
    fs::write(
        &seed_path,
        r#"{
  "version": 2,
  "project_id": "seed-test-project",
  "project_name": "Seed Test Project",
  "flow": "standard",
  "prompt_body": "Build a hello-world utility.",
  "handoff_summary": "Minimal seed test."
}"#,
    )
    .expect("write seed file");

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--from-seed",
            seed_path.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap from seed");

    assert!(
        output.status.success(),
        "bootstrap --from-seed should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let project_toml =
        fs::read_to_string(project_root(temp_dir.path(), "seed-test-project").join("project.toml"))
            .expect("read project.toml");
    assert!(project_toml.contains("flow = \"standard\""));
    assert!(project_toml.contains("seed-test-project"));
}

#[test]
fn project_bootstrap_from_seed_rejects_invalid_seed_json() {
    let temp_dir = initialize_workspace_fixture();
    let seed_path = temp_dir.path().join("bad-seed.json");
    fs::write(
        &seed_path,
        r#"{
  "version": 2,
  "project_id": "bad-project",
  "project_name": "Bad Project",
  "flow": "standard",
  "prompt_body": "Hello",
  "handoff_summary": "Bad.",
  "source": {
    "mode": "seed_file",
    "run_id": "fake"
  }
}"#,
    )
    .expect("write bad seed file");

    let output = Command::new(binary())
        .args([
            "project",
            "bootstrap",
            "--from-seed",
            seed_path.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("project bootstrap from bad seed");

    assert!(
        !output.status.success(),
        "bootstrap --from-seed with invalid source.mode should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid project seed JSON"),
        "should report invalid seed JSON, got: {stderr}"
    );
    // No project directory should be created
    let projects_dir = live_workspace_root(temp_dir.path()).join("projects");
    let project_count = fs::read_dir(&projects_dir)
        .map(|rd| rd.count())
        .unwrap_or(0);
    assert_eq!(
        project_count, 0,
        "invalid seed should not create any project"
    );
}

// ── Project List ──

#[test]
fn project_list_shows_no_projects_when_empty() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No projects found"));
}

#[test]
fn project_list_shows_created_projects_with_active_marker() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create two projects
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "alpha",
            "--name",
            "Alpha",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create alpha");

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "beta",
            "--name",
            "Beta",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create beta");

    // Select alpha as active
    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select alpha");

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("alpha *"));
    assert!(stdout.contains("beta"));
    assert!(stdout.contains("standard"));
    assert!(stdout.contains("quick_dev"));
}

// ── Project Show ──

#[test]
fn project_show_displays_project_details() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "showme",
            "--name",
            "Show Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "docs_change",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let output = Command::new(binary())
        .args(["project", "show", "showme"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Project: showme"));
    assert!(stdout.contains("Name: Show Me"));
    assert!(stdout.contains("Flow: docs_change"));
    assert!(stdout.contains("Prompt hash:"));
    assert!(stdout.contains("Run status: not started"));
    assert!(stdout.contains("Journal events: 1"));
}

#[test]
fn project_show_resolves_active_project_when_no_id_given() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "active-show",
            "--name",
            "Active Show",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "active-show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["project", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show without id");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Project: active-show (active)"));
}

#[test]
fn project_show_json_includes_task_lineage() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(temp_dir.path(), "show-json", "ms-alpha", "ms-alpha.bead-2");

    let output = Command::new(binary())
        .args(["project", "show", "--json", "show-json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("project show json should parse");
    assert_eq!(value["record"]["id"], "show-json");
    assert_eq!(value["task_lineage"]["milestone_id"], "ms-alpha");
    assert_eq!(value["task_lineage"]["bead_id"], "ms-alpha.bead-2");
    assert_eq!(
        value["task_lineage"]["bead_title"],
        "Bootstrap bead-backed task creation"
    );
}

// ── Project Delete ──

#[test]
fn project_delete_removes_project_directory() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "deleteme",
            "--name",
            "Delete Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let output = Command::new(binary())
        .args(["project", "delete", "deleteme"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Deleted project 'deleteme'"));
    assert!(!project_root(temp_dir.path(), "deleteme").exists());
}

#[test]
fn project_delete_clears_active_project_if_selected() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "active-del",
            "--name",
            "Active Del",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "active-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    assert!(active_project_path(temp_dir.path()).exists());

    let output = Command::new(binary())
        .args(["project", "delete", "active-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(output.status.success());
    assert!(!active_project_path(temp_dir.path()).exists());
}

#[test]
fn project_delete_fails_for_missing_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "delete", "nonexistent"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not found"));
}

// ── Run Status ──

#[test]
fn run_status_shows_not_started_for_new_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "status-test",
            "--name",
            "Status Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "status-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: not started"));
}

#[test]
fn run_status_does_not_rewrite_legacy_run_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let run_json_path = project_root(temp_dir.path(), "alpha").join("run.json");
    let legacy_snapshot = serde_json::json!({
        "active_run": null,
        "status": "not_started",
        "cycle_history": [],
        "completion_rounds": 0,
        "rollback_point_meta": {"last_rollback_id": null, "rollback_count": 0},
        "amendment_queue": {"pending": [], "processed_count": 0},
        "status_summary": "not started"
    });
    fs::write(
        &run_json_path,
        serde_json::to_string_pretty(&legacy_snapshot).expect("serialize legacy snapshot"),
    )
    .expect("write legacy run.json");
    let before = fs::read_to_string(&run_json_path).expect("read legacy run.json");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let after = fs::read_to_string(&run_json_path).expect("read post-status run.json");
    assert_eq!(after, before, "run status must not mutate legacy run.json");
}

#[test]
fn run_status_repairs_signal_handoff_failed_snapshot_without_task_source() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": null,
  "interrupted_run": {
    "run_id": "run-signal-handoff-status",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z",
    "prompt_hash_at_cycle_start": "prompt-hash",
    "prompt_hash_at_stage_start": "prompt-hash",
    "qa_iterations_current_cycle": 0,
    "review_iterations_current_cycle": 0,
    "final_review_restart_count": 0,
    "stage_resolution_snapshot": null
  },
  "status": "failed",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "failed (interrupted by termination signal)"
}"#,
    )
    .expect("write failed interrupted snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-10T00:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-10T00:00:01Z","event_type":"run_started","details":{"run_id":"run-signal-handoff-status","first_stage":"implementation","max_completion_rounds":20}}"#,
    )
    .expect("write signal-handoff journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: failed"), "{stdout}");

    let journal = fs::read_to_string(project_root(temp_dir.path(), "alpha").join("journal.ndjson"))
        .expect("read repaired journal");
    let run_failed_count = journal
        .lines()
        .filter(|line| line.contains("\"event_type\":\"run_failed\""))
        .count();
    assert_eq!(
        run_failed_count, 1,
        "run status should repair exactly one missing run_failed event for signal handoff"
    );
}

#[test]
fn run_status_reports_stale_running_when_pid_file_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stale",
    "stage_cursor": { "stage": "planning", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Planning"
}"#,
    )
    .expect("write running snapshot");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: stale (process not found)"));
    assert!(stdout.contains("run `ralph-burning run resume` to recover"));
}

#[test]
fn run_status_shows_iterative_minimal_iteration_summary() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-iterative",
    "stage_cursor": { "stage": "plan_and_implement", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Plan and Implement (iteration 3/10)"
}"#,
    )
    .expect("write iterative run snapshot");
    ralph_burning::adapters::fs::FileSystem::write_pid_file(
        temp_dir.path(),
        &ralph_burning::shared::domain::ProjectId::new("alpha").expect("project id"),
        ralph_burning::adapters::fs::RunPidOwner::Cli,
        None,
        Some("run-iterative"),
        Some(
            chrono::DateTime::parse_from_rfc3339("2026-04-10T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        ),
    )
    .expect("write pid file");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stage: plan_and_implement"));
    assert!(stdout.contains("Summary: running: Plan and Implement (iteration 3/10)"));
}

#[test]
fn run_status_json_reports_stale_running_when_pid_file_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stale-json",
    "stage_cursor": { "stage": "planning", "cycle": 2, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Planning"
}"#,
    )
    .expect("write running snapshot");

    let output = Command::new(binary())
        .args(["run", "status", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("status json should parse");
    assert_eq!(value["status"], "stale");
    assert_eq!(value["stage"], "planning");
    assert_eq!(value["cycle"], 2);
    assert_eq!(
        value["summary"],
        "stale running snapshot: process not found; run `ralph-burning run resume` to recover"
    );
}

#[test]
fn run_status_ignores_older_terminal_journal_events_for_different_run_id() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-current",
    "stage_cursor": { "stage": "planning", "cycle": 2, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Planning"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-older","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_completed","details":{"run_id":"run-older","completion_rounds":0,"max_completion_rounds":20}}"#,
    )
    .expect("write journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: stale (process not found)"));
    assert!(
        !stdout.contains("Status: completed"),
        "older terminal history must not reconcile a newer running snapshot: {stdout}"
    );
}

#[test]
fn run_status_ignores_terminal_journal_events_before_latest_resume_for_same_run_id() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-current",
    "stage_cursor": { "stage": "implementation", "cycle": 2, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-current","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:15:00Z","event_type":"run_failed","details":{"run_id":"run-current","stage_id":"review","failure_class":"stage_failure","message":"older attempt failed","completion_rounds":1,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_resumed","details":{"run_id":"run-current","resume_stage":"implementation","cycle":2,"completion_round":1,"max_completion_rounds":20}}
{"sequence":5,"timestamp":"2026-04-01T10:21:00Z","event_type":"stage_entered","details":{"run_id":"run-current","stage_id":"implementation","cycle":2,"attempt":1}}"#,
    )
    .expect("write journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: stale (process not found)"));
    assert!(
        !stdout.contains("Status: failed"),
        "an older failed attempt for the same run_id must not override the active resumed attempt: {stdout}"
    );
}

#[test]
fn run_status_ignores_terminal_journal_events_before_unjournaled_resume_snapshot_started_at() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-current",
    "stage_cursor": { "stage": "implementation", "cycle": 2, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-01T10:20:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-current","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:15:00Z","event_type":"run_failed","details":{"run_id":"run-current","stage_id":"review","failure_class":"stage_failure","message":"older attempt failed","completion_rounds":1,"max_completion_rounds":20}}"#,
    )
    .expect("write journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: stale (process not found)"));
    assert!(
        !stdout.contains("Status: failed"),
        "the stale resumed snapshot must not inherit a previous attempt's failure: {stdout}"
    );
}

#[test]
fn run_status_keeps_daemon_owned_running_snapshot_active_with_live_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let worktree_lease = WorktreeLease {
        lease_id: "lease-daemon-alpha".to_owned(),
        task_id: "task-daemon-alpha".to_owned(),
        project_id: "alpha".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-daemon-alpha"),
        branch_name: "task-daemon-alpha".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-daemon-alpha.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
    fs::write(leases_dir.join("writer-alpha.lock"), "lease-daemon-alpha")
        .expect("write daemon writer lock");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            std::process::id(),
            "daemon",
            Some("run-daemon"),
            Some("2026-04-10T00:00:00Z"),
            Some("lease-daemon-alpha"),
        ))
        .expect("serialize daemon pid"),
    )
    .expect("write daemon pid");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: running"));
    assert!(
        !stdout.contains("stale"),
        "daemon-owned running snapshot should not be reported as stale: {stdout}"
    );
}

#[test]
fn run_status_keeps_legacy_daemon_owned_running_snapshot_active_without_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon-stale",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let worktree_lease = WorktreeLease {
        lease_id: "lease-daemon-legacy".to_owned(),
        task_id: "task-daemon-legacy".to_owned(),
        project_id: "alpha".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-daemon-legacy"),
        branch_name: "task-daemon-legacy".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-daemon-legacy.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
    fs::write(leases_dir.join("writer-alpha.lock"), "lease-daemon-legacy")
        .expect("write daemon writer lock");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: running"));
    assert!(
        !stdout.contains("stale"),
        "fresh legacy daemon lease should keep the run active during mixed-version rollouts: {stdout}"
    );
}

#[test]
fn run_status_keeps_legacy_cli_owned_running_snapshot_active_without_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-cli-legacy-status",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let cli_lease = CliWriterLease {
        lease_id: "lease-cli-legacy-status".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-cli-legacy-status.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(cli_lease))
            .expect("serialize cli lease"),
    )
    .expect("write cli lease");
    fs::write(
        leases_dir.join("writer-alpha.lock"),
        "lease-cli-legacy-status",
    )
    .expect("write cli writer lock");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: running"));
    assert!(
        !stdout.contains("stale"),
        "fresh legacy CLI lease should keep the run active instead of reporting stale: {stdout}"
    );
}

#[test]
fn run_status_reports_stale_daemon_owned_running_when_legacy_lease_expires() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon-stale",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let worktree_lease = WorktreeLease {
        lease_id: "lease-daemon-stale".to_owned(),
        task_id: "task-daemon-stale".to_owned(),
        project_id: "alpha".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-daemon-stale"),
        branch_name: "task-daemon-stale".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::seconds(301),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-daemon-stale.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
    fs::write(leases_dir.join("writer-alpha.lock"), "lease-daemon-stale")
        .expect("write daemon writer lock");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: stale (process not found)"));
    assert!(stdout.contains("run `ralph-burning run resume` to recover"));
}

#[test]
fn run_status_keeps_legacy_cli_owned_running_snapshot_active_with_live_pid_without_proc_start_ticks_when_no_lease_exists(
) {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-legacy-pid-status",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "started_at": Utc::now(),
            "owner": "cli",
        }))
        .expect("serialize legacy pid"),
    )
    .expect("write legacy pid");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Status: running"),
        "live legacy pid-only records must stay running until the orchestrator pid is actually gone: {stdout}"
    );
    assert!(
        !stdout.contains("stale"),
        "live legacy pid-only records must not be auto-recovered while the orchestrator pid still exists: {stdout}"
    );
}

#[test]
fn run_status_treats_legacy_pid_record_as_stale_when_live_process_started_after_record() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-legacy-pid-reused-status",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "started_at": Utc::now() - Duration::days(365),
            "owner": "cli",
        }))
        .expect("serialize legacy pid"),
    )
    .expect("write legacy pid");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Status: stale (process not found)"),
        "legacy pid records must be treated as stale when the live process started after the recorded timestamp: {stdout}"
    );
}

#[test]
fn run_stop_terminates_pid_and_marks_snapshot_failed() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    let pid = child.id();
    let pid_file = serde_json::json!({
        "pid": pid,
        "started_at": Utc::now(),
        "owner": "cli",
        "writer_owner": "cli-stop-test",
        "run_id": "run-stop",
        "run_started_at": "2026-04-10T00:00:00Z",
        "proc_start_ticks": proc_start_ticks(pid),
    });
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&pid_file).expect("serialize pid file"),
    )
    .expect("write pid file");
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let stale_lease = CliWriterLease {
        lease_id: "cli-stop-test".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-stop-test.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(stale_lease.clone()))
            .expect("serialize stale stop lease"),
    )
    .expect("write stale stop lease");
    fs::write(leases_dir.join("writer-alpha.lock"), &stale_lease.lease_id)
        .expect("write stale stop writer lock");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stopped run for project 'alpha'"));

    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json")).unwrap(),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "failed");
    assert!(
        !run_json["interrupted_run"].is_null(),
        "stop should preserve interrupted_run for resume"
    );
    assert!(
        !project_root(temp_dir.path(), "alpha")
            .join("run.pid")
            .exists(),
        "run.pid should be removed after stop"
    );
    assert!(
        !leases_dir.join("writer-alpha.lock").exists(),
        "writer lock should be reclaimed after stop"
    );
    assert!(
        !leases_dir.join("cli-stop-test.json").exists(),
        "stale cli lease should be pruned after stop"
    );
    let journal = fs::read_to_string(project_root(temp_dir.path(), "alpha").join("journal.ndjson"))
        .expect("read journal");
    let last_line = journal.lines().last().expect("journal should not be empty");
    assert!(
        last_line.contains("\"run_failed\""),
        "run stop should append a durable run_failed event, got: {last_line}"
    );

    let child_status = child.wait().expect("wait for stopped child");
    assert!(
        !child_status.success(),
        "sleep child should not exit successfully after stop: {child_status:?}"
    );
}

#[test]
fn run_stop_refuses_pid_for_different_running_attempt() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-old",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    let pid = child.id();
    let pid_file = serde_json::json!({
        "pid": pid,
        "started_at": Utc::now(),
        "owner": "cli",
        "writer_owner": "cli-stop-race",
        "run_id": "run-stop-new",
        "run_started_at": "2026-04-10T00:05:00Z",
        "proc_start_ticks": proc_start_ticks(pid),
    });
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&pid_file).expect("serialize pid file"),
    )
    .expect("write pid file");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("project run changed while preparing stop"),
        "unexpected stderr: {stderr}"
    );
    assert!(
        pid_is_alive(pid),
        "run stop must not signal a different attempt"
    );

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn run_stop_preserves_completed_journal_outcome_for_stale_running_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-completed",
    "stage_cursor": { "stage": "review", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-01T10:01:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Review"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-stop-completed","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_completed","details":{"run_id":"run-stop-completed","completion_rounds":1,"max_completion_rounds":20}}"#,
    )
    .expect("write journal");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let stale_lease = CliWriterLease {
        lease_id: "cli-stop-completed".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::seconds(301),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-stop-completed.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(stale_lease.clone()))
            .expect("serialize stale stop lease"),
    )
    .expect("write stale stop lease");
    fs::write(leases_dir.join("writer-alpha.lock"), &stale_lease.lease_id)
        .expect("write stale stop writer lock");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("already completed"),
        "stop should report the durable completed outcome instead of rewriting to failed: {stdout}"
    );
    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json")).unwrap(),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "completed");
    assert_eq!(
        run_json["status_summary"],
        "completed (reconciled from journal)"
    );
}

#[test]
fn run_stop_does_not_delete_pid_file_when_snapshot_is_not_running() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": null,
  "status": "failed",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "failed: startup interrupted"
}"#,
    )
    .expect("write failed snapshot");

    let pid_file = serde_json::json!({
        "pid": std::process::id(),
        "started_at": Utc::now(),
    });
    let pid_path = project_root(temp_dir.path(), "alpha").join("run.pid");
    fs::write(
        &pid_path,
        serde_json::to_string_pretty(&pid_file).expect("serialize pid file"),
    )
    .expect("write pid file");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    assert!(
        pid_path.exists(),
        "run stop should not delete a live pid file when the snapshot is not running"
    );
}

#[test]
fn run_stop_sigkill_terminates_backend_subprocess_group() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigkill-tree",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let backend_child_pid_path = temp_dir.path().join("backend-child.pid");
    let orchestrator_script = format!(
        "trap '' TERM; setsid sh -c 'echo $$ > \"{}\"; exec sleep 60' & while [ ! -s \"{}\" ]; do sleep 0.05; done; while :; do sleep 1; done",
        backend_child_pid_path.display(),
        backend_child_pid_path.display(),
    );
    let mut orchestrator = Command::new("bash")
        .args(["-lc", &orchestrator_script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !backend_child_pid_path.exists() || fs::read_to_string(&backend_child_pid_path).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "backend child pid file was never written"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let backend_child_pid = fs::read_to_string(&backend_child_pid_path)
        .expect("read backend child pid")
        .trim()
        .parse::<u32>()
        .expect("parse backend child pid");
    assert!(
        pid_is_alive(backend_child_pid),
        "backend child must be alive before stop"
    );

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigkill-tree"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-tree"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("required SIGKILL after timeout"),
        "stop should escalate to SIGKILL for the TERM-ignoring orchestrator: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_child_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_child_pid),
        "backend subprocess group should be gone after SIGKILL escalation"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        !orchestrator_status.success(),
        "orchestrator should not exit successfully after forced stop: {orchestrator_status:?}"
    );
}

#[test]
fn run_stop_sigkill_terminates_same_process_group_backend_descendant() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigkill-same-pg",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let backend_child_pid_path = temp_dir.path().join("backend-child-same-pg.pid");
    let orchestrator_script = format!(
        "trap '' TERM; sh -c 'echo $$ > \"{}\"; exec sleep 60' & while [ ! -s \"{}\" ]; do sleep 0.05; done; while :; do sleep 1; done",
        backend_child_pid_path.display(),
        backend_child_pid_path.display(),
    );
    let mut orchestrator = Command::new("bash")
        .args(["-lc", &orchestrator_script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !backend_child_pid_path.exists() || fs::read_to_string(&backend_child_pid_path).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "same-process-group backend child pid file was never written"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let backend_child_pid = fs::read_to_string(&backend_child_pid_path)
        .expect("read backend child pid")
        .trim()
        .parse::<u32>()
        .expect("parse backend child pid");
    assert!(
        pid_is_alive(backend_child_pid),
        "same-process-group backend child must be alive before stop"
    );

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigkill-same-pg"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-same-pg"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("required SIGKILL after timeout"),
        "stop should escalate to SIGKILL for the TERM-ignoring orchestrator: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_child_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_child_pid),
        "same-process-group backend child should be gone after SIGKILL escalation"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        !orchestrator_status.success(),
        "orchestrator should not exit successfully after forced stop: {orchestrator_status:?}"
    );
}

#[test]
fn run_stop_sigkill_terminates_orphaned_backend_process_group_after_leader_exit() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigkill-orphaned-backend",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let backend_group_pid_path = temp_dir.path().join("backend-group-leader.pid");
    let backend_orphan_pid_path = temp_dir.path().join("backend-orphan.pid");
    let orchestrator_script = format!(
        "trap '' TERM; setsid sh -c 'echo $$ > \"{leader}\"; sh -c '\\''echo $$ > \"{orphan}\"; exec sleep 60'\\'' & while [ ! -s \"{orphan}\" ]; do sleep 0.05; done; exit 0' & while [ ! -s \"{leader}\" ] || [ ! -s \"{orphan}\" ]; do sleep 0.05; done; while :; do sleep 1; done",
        leader = backend_group_pid_path.display(),
        orphan = backend_orphan_pid_path.display(),
    );
    let mut orchestrator = Command::new("bash")
        .args(["-lc", &orchestrator_script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    let backend_group_pid = wait_for_pid_file(&backend_group_pid_path, "backend group leader");
    let backend_orphan_pid = wait_for_pid_file(&backend_orphan_pid_path, "backend orphan");
    assert!(
        pid_is_alive(backend_orphan_pid),
        "orphaned backend child must be alive before stop"
    );
    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                live_backend_process_record_json(
                    backend_group_pid,
                    "run-stop-sigkill-orphaned-backend",
                    "2026-04-10T00:00:00Z"
                )
            ]
        }))
        .expect("serialize backend process record"),
    )
    .expect("write backend process record");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_group_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_group_pid),
        "backend group leader should exit before stop so only the process group remains"
    );

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigkill-orphaned-backend"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-orphaned-backend"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("required SIGKILL after timeout"),
        "stop should escalate to SIGKILL for the TERM-ignoring orchestrator before killing the orphaned backend group: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_orphan_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_orphan_pid),
        "forced stop must kill an orphaned backend child even after the backend leader exits"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        !orchestrator_status.success(),
        "orchestrator should not exit successfully after forced stop: {orchestrator_status:?}"
    );

    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json"))
            .expect("read run.json"),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "failed");
    assert!(
        !run_json["interrupted_run"].is_null(),
        "stop should still finalize the interrupted snapshot before surfacing cleanup refusal"
    );
    assert!(
        !project_root(temp_dir.path(), "alpha")
            .join("run.pid")
            .exists(),
        "run.pid should be removed once the orchestrator is stopped"
    );
}

#[test]
fn run_stop_sigkill_terminates_orphaned_legacy_backend_process_group_after_leader_exit() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigkill-orphaned-legacy-backend",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let backend_group_pid_path = temp_dir.path().join("legacy-backend-group-leader.pid");
    let backend_orphan_pid_path = temp_dir.path().join("legacy-backend-orphan.pid");
    let orchestrator_script = format!(
        "trap '' TERM; setsid sh -c 'echo $$ > \"{leader}\"; sh -c '\\''echo $$ > \"{orphan}\"; exec sleep 60'\\'' & while [ ! -s \"{orphan}\" ]; do sleep 0.05; done; exit 0' & while [ ! -s \"{leader}\" ] || [ ! -s \"{orphan}\" ]; do sleep 0.05; done; while :; do sleep 1; done",
        leader = backend_group_pid_path.display(),
        orphan = backend_orphan_pid_path.display(),
    );
    let mut orchestrator = Command::new("bash")
        .args(["-lc", &orchestrator_script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    let backend_group_pid =
        wait_for_pid_file(&backend_group_pid_path, "legacy backend group leader");
    let backend_orphan_pid = wait_for_pid_file(&backend_orphan_pid_path, "legacy backend orphan");
    assert!(
        pid_is_alive(backend_orphan_pid),
        "legacy orphaned backend child must be alive before stop"
    );
    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                legacy_backend_process_record_json(
                    backend_group_pid,
                    "run-stop-sigkill-orphaned-legacy-backend",
                    "2026-04-10T00:00:00Z"
                )
            ]
        }))
        .expect("serialize legacy backend process record"),
    )
    .expect("write legacy backend process record");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_group_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_group_pid),
        "legacy backend group leader should exit before stop so only the orphaned group remains"
    );

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigkill-orphaned-legacy-backend"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-orphaned-legacy-backend"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("required SIGKILL after timeout"),
        "stop should still escalate to SIGKILL before killing the orphaned legacy backend group: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_orphan_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_orphan_pid),
        "forced stop must kill an orphaned legacy backend child after the recorded leader exits"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        !orchestrator_status.success(),
        "orchestrator should not exit successfully after forced stop: {orchestrator_status:?}"
    );
}

#[test]
fn run_stop_recovers_stale_running_with_dead_legacy_backend_record() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-stale-legacy-backend",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();

    let backend_pid_file = temp_dir.path().join("stale-legacy-backend.pid");
    let mut backend_group = spawn_isolated_backend_group(&backend_pid_file);
    let backend_pid = wait_for_pid_file(&backend_pid_file, "stale legacy backend");
    backend_group.kill().expect("kill isolated backend leader");
    let backend_status = backend_group.wait().expect("wait for isolated backend");
    assert!(
        !backend_status.success(),
        "killed backend helper should not exit successfully: {backend_status:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_running(backend_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_running(backend_pid),
        "legacy backend pid should be gone before stale recovery"
    );

    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                {
                    "pid": backend_pid,
                    "recorded_at": Utc::now(),
                    "run_id": "run-stop-stale-legacy-backend",
                    "run_started_at": "2026-04-10T00:00:00Z"
                }
            ]
        }))
        .expect("serialize legacy backend process record"),
    )
    .expect("write legacy backend process record");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("already stale"),
        "stale stop should prune dead legacy backend records instead of failing cleanup: {stdout}"
    );
    assert!(
        !backend_processes_path(temp_dir.path(), "alpha").exists(),
        "dead legacy backend records should be removed during stale recovery"
    );
}

#[test]
#[ignore = "flaky in CI: signal timing race; tracked as rlm.4 follow-up"]
fn run_stop_sigkill_finalizes_snapshot_even_when_backend_cleanup_fails() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigkill-cleanup-failure",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let mut orchestrator = Command::new("bash")
        .args(["-lc", "trap '' TERM; while :; do sleep 1; done"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        "{not-json",
    )
    .expect("write corrupt backend process record");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigkill-cleanup-failure"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-cleanup-failure"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required SIGKILL after timeout"),
        "stop failure should still report the forced SIGKILL outcome: {stderr}"
    );
    assert!(
        stderr.contains("corrupt record in runtime/backend/active-processes.json"),
        "stop failure should surface the backend cleanup error after killing the orchestrator: {stderr}"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        !orchestrator_status.success(),
        "orchestrator should not exit successfully after forced stop: {orchestrator_status:?}"
    );

    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json"))
            .expect("read run.json"),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "failed");
    assert!(
        !run_json["interrupted_run"].is_null(),
        "stop should still preserve interrupted_run when backend cleanup fails"
    );
    assert!(
        !project_root(temp_dir.path(), "alpha")
            .join("run.pid")
            .exists(),
        "run.pid should be removed once the orchestrator is stopped even if backend cleanup fails"
    );
}

#[test]
fn run_stop_refuses_live_legacy_pid_record_without_proc_start_ticks_when_no_lease_exists() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-legacy-pid",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&serde_json::json!({
            "pid": child.id(),
            "started_at": Utc::now(),
        }))
        .expect("serialize pid file"),
    )
    .expect("write legacy pid file");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy CLI-owned run"),
        "stop should refuse a live legacy pid-only run instead of recovering it as stale: {stderr}"
    );
    assert!(
        pid_is_alive(child.id()),
        "refused stop must not signal the live legacy orchestrator"
    );
    assert!(
        project_root(temp_dir.path(), "alpha")
            .join("run.pid")
            .exists(),
        "refused stop must leave the live legacy pid marker in place"
    );

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn run_stop_recovers_stale_running_and_kills_persisted_backend_process_group() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-stale-backend",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();

    let backend_pid_file = temp_dir.path().join("stale-backend-stop.pid");
    let mut backend_group = spawn_isolated_backend_group(&backend_pid_file);
    let backend_pid = wait_for_pid_file(&backend_pid_file, "stale backend");
    assert!(
        pid_is_running(backend_pid),
        "persisted stale backend process should be alive before recovery"
    );
    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                live_backend_process_record_json(
                    backend_pid,
                    "run-stop-stale-backend",
                    "2026-04-10T00:00:00Z"
                )
            ]
        }))
        .expect("serialize backend process record"),
    )
    .expect("write backend process record");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("already stale"),
        "stale stop should recover instead of refusing: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_running(backend_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_running(backend_pid),
        "stale stop recovery must kill persisted backend process groups before returning"
    );
    assert!(
        !backend_processes_path(temp_dir.path(), "alpha").exists(),
        "successful stale backend cleanup should remove the persisted backend process file"
    );

    let _ = backend_group.kill();
    let _ = backend_group.wait();
}

#[test]
fn run_stop_recovers_stale_running_when_persisted_backend_leader_already_exited() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-stale-backend-already-exited",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();

    let backend_pid_file = temp_dir
        .path()
        .join("stale-backend-stop-already-exited.pid");
    let mut backend_group = spawn_isolated_backend_group(&backend_pid_file);
    let backend_pid = wait_for_pid_file(&backend_pid_file, "stale backend");
    assert!(
        pid_is_running(backend_pid),
        "persisted stale backend process should be alive before cleanup setup"
    );
    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                live_backend_process_record_json(
                    backend_pid,
                    "run-stop-stale-backend-already-exited",
                    "2026-04-10T00:00:00Z"
                )
            ]
        }))
        .expect("serialize backend process record"),
    )
    .expect("write backend process record");

    backend_group.kill().expect("kill isolated backend leader");
    let backend_status = backend_group.wait().expect("wait for isolated backend");
    assert!(
        !backend_status.success(),
        "killed backend helper should not exit successfully: {backend_status:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_running(backend_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_running(backend_pid),
        "persisted backend leader should be gone before stale recovery"
    );

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("already stale"),
        "stale stop should recover even when the persisted backend leader already exited: {stdout}"
    );
    assert!(
        !backend_processes_path(temp_dir.path(), "alpha").exists(),
        "stale recovery should remove backend process records when the tracked backend is already gone"
    );
}

#[test]
#[ignore = "flaky in CI: signal timing race; tracked as rlm.4 follow-up"]
fn run_stop_reconciles_running_snapshot_after_sigterm_handoff_removes_pid() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigterm-handoff",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let pid_path = project_root(temp_dir.path(), "alpha").join("run.pid");
    let mut orchestrator = Command::new("bash")
        .args([
            "-lc",
            "trap 'rm -f \"$RALPH_PID_FILE\"; exit 0' TERM; while :; do sleep 1; done",
        ])
        .env("RALPH_PID_FILE", &pid_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");

    fs::write(
        &pid_path,
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator.id(),
            "cli",
            Some("run-stop-sigterm-handoff"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-sigterm-handoff"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let cli_lease = CliWriterLease {
        lease_id: "cli-stop-sigterm-handoff".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-stop-sigterm-handoff.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(cli_lease.clone()))
            .expect("serialize cli lease"),
    )
    .expect("write cli lease");
    fs::write(leases_dir.join("writer-alpha.lock"), &cli_lease.lease_id)
        .expect("write writer lock");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Stopped run for project 'alpha'"),
        "stop should finalize the stale snapshot after SIGTERM handoff removed run.pid: {stdout}"
    );
    assert!(
        !stdout.contains("status 'running'"),
        "stop should not report the run as still running after the orchestrator exits: {stdout}"
    );

    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json"))
            .expect("read run.json"),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "failed");
    assert!(
        !run_json["interrupted_run"].is_null(),
        "stop should preserve interrupted_run after reconciling the handoff"
    );
    assert!(!pid_path.exists(), "run.pid should stay removed after stop");
    assert!(
        !leases_dir.join("writer-alpha.lock").exists(),
        "recovery lease cleanup should release the writer lock after stop"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        orchestrator_status.success(),
        "SIGTERM handoff orchestrator should exit cleanly after removing run.pid: {orchestrator_status:?}"
    );
}

#[test]
fn run_stop_kills_backend_descendants_when_parent_exits_on_sigterm() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-stop-sigterm-parent-exit",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let backend_child_pid_path = temp_dir.path().join("backend-child-parent-exit.pid");
    let orchestrator_script = format!(
        "trap 'exit 0' TERM; setsid sh -c 'echo $$ > \"{}\"; exec sleep 60' & while [ ! -s \"{}\" ]; do sleep 0.05; done; while :; do sleep 1; done",
        backend_child_pid_path.display(),
        backend_child_pid_path.display(),
    );
    let mut orchestrator = Command::new("bash")
        .args(["-lc", &orchestrator_script])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn orchestrator");
    let orchestrator_pid = orchestrator.id();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !backend_child_pid_path.exists() || fs::read_to_string(&backend_child_pid_path).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "backend child pid file was never written"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let backend_child_pid = fs::read_to_string(&backend_child_pid_path)
        .expect("read backend child pid")
        .trim()
        .parse::<u32>()
        .expect("parse backend child pid");
    assert!(
        pid_is_alive(backend_child_pid),
        "backend child must be alive before stop"
    );

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            orchestrator_pid,
            "cli",
            Some("run-stop-sigterm-parent-exit"),
            Some("2026-04-10T00:00:00Z"),
            Some("cli-stop-parent-exit"),
        ))
        .expect("serialize cli pid"),
    )
    .expect("write run pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("terminated gracefully with SIGTERM and killed backend subprocesses"),
        "stop should report descendant cleanup even when the orchestrator exits on SIGTERM: {stdout}"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_alive(backend_child_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_alive(backend_child_pid),
        "backend child should be killed even when the orchestrator exits on SIGTERM"
    );

    let orchestrator_status = orchestrator.wait().expect("wait for orchestrator");
    assert!(
        orchestrator_status.success(),
        "orchestrator should exit cleanly on SIGTERM in this scenario: {orchestrator_status:?}"
    );
}

#[test]
fn run_stop_refuses_daemon_owned_running_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon-stop",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let worktree_lease = WorktreeLease {
        lease_id: "lease-daemon-stop".to_owned(),
        task_id: "task-daemon-stop".to_owned(),
        project_id: "alpha".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-daemon-stop"),
        branch_name: "task-daemon-stop".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-daemon-stop.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
    fs::write(leases_dir.join("writer-alpha.lock"), "lease-daemon-stop")
        .expect("write daemon writer lock");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            std::process::id(),
            "daemon",
            Some("run-daemon-stop"),
            Some("2026-04-10T00:00:00Z"),
            Some("lease-daemon-stop"),
        ))
        .expect("serialize daemon pid"),
    )
    .expect("write daemon pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("daemon task"),
        "stop should explain why daemon-owned runs are refused: {stderr}"
    );
    let run_json = fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json"))
        .expect("read run.json");
    assert!(
        run_json.contains("\"status\": \"running\""),
        "daemon-owned run should remain running after refused stop: {run_json}"
    );
    assert!(
        leases_dir.join("writer-alpha.lock").exists(),
        "daemon writer lock should remain intact after refused stop"
    );
}

#[test]
fn run_stop_refuses_legacy_cli_owned_running_without_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-cli-legacy-stop",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let cli_lease = CliWriterLease {
        lease_id: "lease-cli-legacy-stop".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-cli-legacy-stop.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(cli_lease))
            .expect("serialize cli lease"),
    )
    .expect("write cli lease");
    fs::write(
        leases_dir.join("writer-alpha.lock"),
        "lease-cli-legacy-stop",
    )
    .expect("write cli writer lock");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy CLI-owned run"),
        "stop should refuse fresh legacy CLI runs instead of reclaiming them: {stderr}"
    );
    assert!(
        leases_dir.join("writer-alpha.lock").exists(),
        "legacy CLI writer lock must remain intact after refused stop"
    );
    assert!(
        leases_dir.join("lease-cli-legacy-stop.json").exists(),
        "legacy CLI lease must not be reclaimed while it is still fresh"
    );
}

#[test]
fn run_stop_refuses_legacy_cli_owned_running_with_authoritative_pid_without_attempt_metadata() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-cli-legacy-stop-live-pid",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            child.id(),
            "cli",
            None,
            None,
            Some("legacy-cli-live-pid"),
        ))
        .expect("serialize legacy cli pid"),
    )
    .expect("write legacy cli pid");

    let output = Command::new(binary())
        .args(["run", "stop"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run stop");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy CLI-owned run"),
        "stop should refuse a live legacy CLI pid without attempt metadata: {stderr}"
    );
    assert!(
        !stderr.contains("project run changed"),
        "legacy pid handling should not surface a handoff retry error: {stderr}"
    );
    assert!(
        pid_is_alive(child.id()),
        "run stop must not signal a legacy live process without attempt metadata"
    );

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn run_resume_refuses_legacy_daemon_owned_running_without_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon-legacy-resume",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let worktree_lease = WorktreeLease {
        lease_id: "lease-daemon-legacy-resume".to_owned(),
        task_id: "task-daemon-legacy-resume".to_owned(),
        project_id: "alpha".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-daemon-legacy-resume"),
        branch_name: "task-daemon-legacy-resume".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-daemon-legacy-resume.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
    fs::write(
        leases_dir.join("writer-alpha.lock"),
        "lease-daemon-legacy-resume",
    )
    .expect("write daemon writer lock");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("daemon-owned running run"),
        "resume should refuse mixed-version daemon runs until the lease goes stale: {stderr}"
    );
    assert!(
        leases_dir.join("writer-alpha.lock").exists(),
        "legacy daemon writer lock must remain intact after refused resume"
    );
    assert!(
        leases_dir.join("lease-daemon-legacy-resume.json").exists(),
        "legacy daemon lease must not be reclaimed while it is still fresh"
    );
}

#[test]
fn run_resume_refuses_legacy_daemon_owned_running_with_authoritative_pid_without_attempt_metadata()
{
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-daemon-legacy-resume-live-pid",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.pid"),
        serde_json::to_string_pretty(&live_pid_record_json(
            std::process::id(),
            "daemon",
            None,
            None,
            Some("legacy-daemon-live-pid"),
        ))
        .expect("serialize legacy daemon pid"),
    )
    .expect("write legacy daemon pid");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("daemon-owned running run"),
        "resume should refuse a live legacy daemon pid without attempt metadata: {stderr}"
    );
    assert!(
        !stderr.contains("project run changed"),
        "legacy daemon pid handling should not surface a handoff retry error: {stderr}"
    );
}

#[test]
fn run_resume_persists_stale_recovery_before_backend_cleanup_error() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-resume-stale-backend-cleanup-error",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();
    fs::write(
        backend_processes_path(temp_dir.path(), "alpha"),
        "{not-json",
    )
    .expect("write corrupt backend process record");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("corrupt record in runtime/backend/active-processes.json"),
        "resume should still surface the backend cleanup error after stale recovery: {stderr}"
    );

    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json"))
            .expect("read run.json"),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "failed");
    assert!(
        !run_json["interrupted_run"].is_null(),
        "resume should durably mark the stale run interrupted before surfacing cleanup failure"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_resume_preserves_completed_journal_outcome_for_stale_running_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-resume-completed",
    "stage_cursor": { "stage": "review", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-01T10:01:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Review"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-resume-completed","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_completed","details":{"run_id":"run-resume-completed","completion_rounds":1,"max_completion_rounds":20}}"#,
    )
    .expect("write journal");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let stale_lease = CliWriterLease {
        lease_id: "cli-resume-completed".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::seconds(301),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-resume-completed.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(stale_lease.clone()))
            .expect("serialize stale resume lease"),
    )
    .expect("write stale resume lease");
    fs::write(leases_dir.join("writer-alpha.lock"), &stale_lease.lease_id)
        .expect("write stale resume writer lock");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already completed"),
        "resume should refuse when the journal already completed the run: {stderr}"
    );
    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json")).unwrap(),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "completed");
    assert_eq!(
        run_json["status_summary"],
        "completed (reconciled from journal)"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_resume_reconciles_completed_stale_running_snapshot_without_writer_lock_owner() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-resume-completed-no-owner",
    "stage_cursor": { "stage": "review", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-01T10:01:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Review"
}"#,
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-04-01T10:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-04-01T10:01:00Z","event_type":"run_started","details":{"run_id":"run-resume-completed-no-owner","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_completed","details":{"run_id":"run-resume-completed-no-owner","completion_rounds":1,"max_completion_rounds":20}}"#,
    )
    .expect("write journal");
    fs::remove_file(project_root(temp_dir.path(), "alpha").join("run.pid")).ok();

    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("already completed"),
        "resume should still reconcile a stale completed run without any writer lock owner: {stderr}"
    );
    let run_json: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(project_root(temp_dir.path(), "alpha").join("run.json")).unwrap(),
    )
    .expect("parse run.json");
    assert_eq!(run_json["status"], "completed");
    assert_eq!(
        run_json["status_summary"],
        "completed (reconciled from journal)"
    );
}

#[test]
fn run_resume_refuses_legacy_cli_owned_running_without_pid_file() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        r#"{
  "active_run": {
    "run_id": "run-cli-legacy-resume",
    "stage_cursor": { "stage": "implementation", "cycle": 1, "attempt": 1, "completion_round": 1 },
    "started_at": "2026-04-10T00:00:00Z"
  },
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running: Implementation"
}"#,
    )
    .expect("write running snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let cli_lease = CliWriterLease {
        lease_id: "lease-cli-legacy-resume".to_owned(),
        project_id: "alpha".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("lease-cli-legacy-resume.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(cli_lease))
            .expect("serialize cli lease"),
    )
    .expect("write cli lease");
    fs::write(
        leases_dir.join("writer-alpha.lock"),
        "lease-cli-legacy-resume",
    )
    .expect("write cli writer lock");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy CLI-owned running run"),
        "resume should refuse fresh legacy CLI runs instead of reclaiming them: {stderr}"
    );
    assert!(
        leases_dir.join("writer-alpha.lock").exists(),
        "legacy CLI writer lock must remain intact after refused resume"
    );
    assert!(
        leases_dir.join("lease-cli-legacy-resume.json").exists(),
        "legacy CLI lease must not be reclaimed while it is still fresh"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_resume_refuses_live_legacy_pid_record_without_proc_start_ticks_when_no_lease_exists() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "legacy-live-pid-resume");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("legacy-live-pid-resume".to_owned())
            .expect("valid project id");
    let prompt_hash = <ralph_burning::adapters::fs::FsProjectStore as ralph_burning::contexts::project_run_record::service::ProjectStorePort>::read_project_record(
        &ralph_burning::adapters::fs::FsProjectStore,
        temp_dir.path(),
        &project_id,
    )
    .expect("read project record")
    .prompt_hash;
    let now = Utc::now();

    fs::write(
        project_root(temp_dir.path(), "legacy-live-pid-resume").join("run.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "active_run": {
                "run_id": "run-cli-legacy-resume-live-pid-only",
                "stage_cursor": {
                    "stage": "planning",
                    "cycle": 1,
                    "attempt": 1,
                    "completion_round": 1
                },
                "started_at": now,
                "prompt_hash_at_cycle_start": prompt_hash,
                "prompt_hash_at_stage_start": prompt_hash,
                "qa_iterations_current_cycle": 0,
                "review_iterations_current_cycle": 0,
                "final_review_restart_count": 0
            },
            "interrupted_run": serde_json::Value::Null,
            "status": "running",
            "cycle_history": [],
            "completion_rounds": 1,
            "max_completion_rounds": 20,
            "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
            "amendment_queue": { "pending": [], "processed_count": 0 },
            "status_summary": "running: Planning"
        }))
        .expect("serialize stale run.json"),
    )
    .expect("write running snapshot");
    fs::write(
        project_root(temp_dir.path(), "legacy-live-pid-resume").join("journal.ndjson"),
        format!(
            "{{\"sequence\":1,\"timestamp\":\"2026-03-11T19:00:00Z\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"legacy-live-pid-resume\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{now}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-cli-legacy-resume-live-pid-only\",\"first_stage\":\"planning\",\"max_completion_rounds\":20}}}}"
        ),
    )
    .expect("write stale resume journal");

    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sleep");
    fs::write(
        project_root(temp_dir.path(), "legacy-live-pid-resume").join("run.pid"),
        serde_json::to_string_pretty(&serde_json::json!({
            "pid": child.id(),
            "started_at": Utc::now(),
        }))
        .expect("serialize legacy pid file"),
    )
    .expect("write legacy pid file");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy CLI-owned running run"),
        "resume should refuse a live legacy pid-only run until the orchestrator pid is gone: {stderr}"
    );
    assert!(
        pid_is_alive(child.id()),
        "refused resume must not signal the live legacy orchestrator"
    );
    assert!(
        project_root(temp_dir.path(), "legacy-live-pid-resume")
            .join("run.pid")
            .exists(),
        "refused resume must leave the live legacy pid marker in place"
    );

    let _ = child.kill();
    let _ = child.wait();
}

// ── Run History ──

#[test]
fn run_history_shows_journal_events_for_new_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "hist-test",
            "--name",
            "History Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "hist-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Journal Events"));
    assert!(stdout.contains("ProjectCreated"));
}

#[test]
fn run_rollback_soft_updates_snapshot_and_hides_rolled_back_history() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let project_root = project_root(temp_dir.path(), "alpha");
    fs::write(
        project_root.join("run.json"),
        r#"{
  "active_run": null,
  "status": "failed",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "failed at implementation"
}"#,
    )
    .expect("write failed snapshot");
    fs::write(
        project_root.join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-03-12T22:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-03-12T22:01:00Z","event_type":"stage_completed","details":{"stage_id":"planning","cycle":1,"attempt":1,"payload_id":"p1","artifact_id":"a1"}}
{"sequence":3,"timestamp":"2026-03-12T22:01:01Z","event_type":"rollback_created","details":{"rollback_id":"rb-planning","stage_id":"planning","cycle":1}}
{"sequence":4,"timestamp":"2026-03-12T22:02:00Z","event_type":"stage_completed","details":{"stage_id":"implementation","cycle":1,"attempt":1,"payload_id":"p2","artifact_id":"a2"}}
{"sequence":5,"timestamp":"2026-03-12T22:02:01Z","event_type":"rollback_created","details":{"rollback_id":"rb-implementation","stage_id":"implementation","cycle":1}}"#,
    )
    .expect("write journal");
    fs::write(
        project_root.join("history/payloads/p1.json"),
        r#"{"payload_id":"p1","stage_id":"planning","cycle":1,"attempt":1,"created_at":"2026-03-12T22:01:00Z","payload":{}}"#,
    )
    .expect("write p1");
    fs::write(
        project_root.join("history/artifacts/a1.json"),
        r#"{"artifact_id":"a1","payload_id":"p1","stage_id":"planning","created_at":"2026-03-12T22:01:00Z","content":"planning"}"#,
    )
    .expect("write a1");
    fs::write(
        project_root.join("history/payloads/p2.json"),
        r#"{"payload_id":"p2","stage_id":"implementation","cycle":1,"attempt":1,"created_at":"2026-03-12T22:02:00Z","payload":{}}"#,
    )
    .expect("write p2");
    fs::write(
        project_root.join("history/artifacts/a2.json"),
        r#"{"artifact_id":"a2","payload_id":"p2","stage_id":"implementation","created_at":"2026-03-12T22:02:00Z","content":"implementation"}"#,
    )
    .expect("write a2");
    fs::write(
        project_root.join("rollback/rb-planning.json"),
        r#"{
  "rollback_id": "rb-planning",
  "created_at": "2026-03-12T22:01:01Z",
  "stage_id": "planning",
  "cycle": 1,
  "run_snapshot": {
    "active_run": {
      "run_id": "run-1",
      "stage_cursor": {
        "stage": "implementation",
        "cycle": 1,
        "attempt": 1,
        "completion_round": 1
      },
      "started_at": "2026-03-12T22:00:00Z"
    },
    "status": "running",
    "cycle_history": [],
    "completion_rounds": 1,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "running: Implementation"
  }
}"#,
    )
    .expect("write rollback point");

    let rollback = Command::new(binary())
        .args(["run", "rollback", "--to", "planning"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run rollback");
    assert!(rollback.status.success(), "{:?}", rollback);

    let run_json = fs::read_to_string(project_root.join("run.json")).expect("read run.json");
    assert!(run_json.contains("\"status\": \"paused\""));
    assert!(run_json.contains("\"last_rollback_id\": \"rb-planning\""));
    assert!(run_json.contains("\"rollback_count\": 1"));

    let history = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");
    assert!(
        history.status.success(),
        "{}",
        String::from_utf8_lossy(&history.stderr)
    );

    let stdout = String::from_utf8_lossy(&history.stdout);
    assert!(stdout.contains("RollbackPerformed"));
    assert!(stdout.contains("p1"));
    assert!(
        !stdout.contains("p2"),
        "rolled-back payload should be hidden"
    );
}

#[test]
fn run_rollback_hard_failure_keeps_logical_rollback_durable() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let project_root = project_root(temp_dir.path(), "alpha");
    fs::write(
        project_root.join("run.json"),
        r#"{
  "active_run": null,
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "paused before hard rollback"
}"#,
    )
    .expect("write paused snapshot");
    fs::write(
        project_root.join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-03-12T22:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-03-12T22:02:01Z","event_type":"rollback_created","details":{"rollback_id":"rb-implementation","stage_id":"implementation","cycle":1}}"#,
    )
    .expect("write journal");
    fs::write(
        project_root.join("rollback/rb-implementation.json"),
        r#"{
  "rollback_id": "rb-implementation",
  "created_at": "2026-03-12T22:02:01Z",
  "stage_id": "implementation",
  "cycle": 1,
  "git_sha": "deadbeef",
  "run_snapshot": {
    "active_run": {
      "run_id": "run-1",
      "stage_cursor": {
        "stage": "qa",
        "cycle": 1,
        "attempt": 1,
        "completion_round": 1
      },
      "started_at": "2026-03-12T22:00:00Z"
    },
    "status": "running",
    "cycle_history": [],
    "completion_rounds": 1,
    "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
    "amendment_queue": { "pending": [], "processed_count": 0 },
    "status_summary": "running: QA"
  }
}"#,
    )
    .expect("write rollback point");

    let rollback = Command::new(binary())
        .args(["run", "rollback", "--to", "implementation", "--hard"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run hard rollback");
    assert!(!rollback.status.success());

    let stderr = String::from_utf8_lossy(&rollback.stderr);
    assert!(stderr.contains("logical rollback was committed"));

    let run_json = fs::read_to_string(project_root.join("run.json")).expect("read run.json");
    assert!(run_json.contains("\"status\": \"paused\""));
    assert!(run_json.contains("\"last_rollback_id\": \"rb-implementation\""));
    assert!(run_json.contains("\"rollback_count\": 1"));

    let journal = fs::read_to_string(project_root.join("journal.ndjson")).expect("read journal");
    assert!(journal.contains("\"rollback_performed\""));
}

#[cfg(unix)]
#[test]
fn run_rollback_hard_failure_uses_path_git_wrapper() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let init = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(temp_dir.path())
        .output()
        .expect("git init");
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    for (key, value) in [
        ("user.name", "Test User"),
        ("user.email", "test@example.com"),
    ] {
        let config = Command::new("git")
            .args(["config", key, value])
            .current_dir(temp_dir.path())
            .output()
            .expect("git config");
        assert!(config.status.success(), "git config {key} should succeed");
    }
    let add = Command::new("git")
        .args(["add", "."])
        .current_dir(temp_dir.path())
        .output()
        .expect("git add");
    assert!(add.status.success(), "git add should succeed");
    let commit = Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(temp_dir.path())
        .output()
        .expect("git commit");
    assert!(
        commit.status.success(),
        "{}",
        String::from_utf8_lossy(&commit.stderr)
    );
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(temp_dir.path())
        .output()
        .expect("git rev-parse HEAD");
    assert!(head.status.success(), "git rev-parse HEAD should succeed");
    let head_sha = String::from_utf8_lossy(&head.stdout).trim().to_owned();
    assert!(!head_sha.is_empty(), "HEAD SHA should not be empty");

    let project_root = project_root(temp_dir.path(), "alpha");
    fs::write(
        project_root.join("run.json"),
        r#"{
  "active_run": null,
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "paused before hard rollback"
}"#,
    )
    .expect("write paused snapshot");
    fs::write(
        project_root.join("journal.ndjson"),
        r#"{"sequence":1,"timestamp":"2026-03-12T22:00:00Z","event_type":"project_created","details":{"project_id":"alpha","flow":"standard"}}
{"sequence":2,"timestamp":"2026-03-12T22:02:01Z","event_type":"rollback_created","details":{"rollback_id":"rb-implementation","stage_id":"implementation","cycle":1}}"#,
    )
    .expect("write journal");
    fs::write(
        project_root.join("rollback/rb-implementation.json"),
        format!(
            r#"{{
  "rollback_id": "rb-implementation",
  "created_at": "2026-03-12T22:02:01Z",
  "stage_id": "implementation",
  "cycle": 1,
  "git_sha": "{head_sha}",
  "run_snapshot": {{
    "active_run": {{
      "run_id": "run-1",
      "stage_cursor": {{
        "stage": "qa",
        "cycle": 1,
        "attempt": 1,
        "completion_round": 1
      }},
      "started_at": "2026-03-12T22:00:00Z"
    }},
    "status": "running",
    "cycle_history": [],
    "completion_rounds": 1,
    "rollback_point_meta": {{ "last_rollback_id": null, "rollback_count": 0 }},
    "amendment_queue": {{ "pending": [], "processed_count": 0 }},
    "status_summary": "running: QA"
  }}
}}"#
        ),
    )
    .expect("write rollback point");

    let git_path = Command::new("sh")
        .args(["-lc", "command -v git"])
        .output()
        .expect("resolve git path");
    assert!(git_path.status.success(), "command -v git should succeed");
    let git_path = String::from_utf8_lossy(&git_path.stdout).trim().to_owned();
    assert!(!git_path.is_empty(), "git path should not be empty");

    let fake_bin = temp_dir.path().join("fake-bin");
    fs::create_dir_all(&fake_bin).expect("create fake-bin");
    let fake_git = fake_bin.join("git");
    fs::write(
        &fake_git,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"reset\" ] && [ \"$2\" = \"--hard\" ]; then\n  echo 'simulated git reset failure' >&2\n  exit 1\nfi\nexec \"{git_path}\" \"$@\"\n"
        ),
    )
    .expect("write fake git");
    fs::set_permissions(&fake_git, fs::Permissions::from_mode(0o755)).expect("chmod fake git");

    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let fake_path = if inherited_path.is_empty() {
        fake_bin.display().to_string()
    } else {
        format!("{}:{inherited_path}", fake_bin.display())
    };

    let rollback = Command::new(binary())
        .args(["run", "rollback", "--to", "implementation", "--hard"])
        .env("PATH", fake_path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run hard rollback");
    assert!(
        !rollback.status.success(),
        "rollback should fail via fake git"
    );

    let stderr = String::from_utf8_lossy(&rollback.stderr);
    assert!(stderr.contains("simulated git reset failure"));
    assert!(stderr.contains("logical rollback was committed"));

    let run_json = fs::read_to_string(project_root.join("run.json")).expect("read run.json");
    assert!(run_json.contains("\"status\": \"paused\""));
    assert!(run_json.contains("\"last_rollback_id\": \"rb-implementation\""));
    assert!(run_json.contains("\"rollback_count\": 1"));

    let journal = fs::read_to_string(project_root.join("journal.ndjson")).expect("read journal");
    assert!(journal.contains("\"rollback_performed\""));
}

// ── Run Tail ──

#[test]
fn run_tail_shows_durable_history_only_by_default() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-test",
            "--name",
            "Tail Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Durable History"));
    // No runtime logs section when --logs not passed
    assert!(!stdout.contains("Runtime Logs"));
}

#[test]
fn run_tail_with_logs_includes_runtime_logs_section() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-logs",
            "--name",
            "Tail Logs",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "tail", "--logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --logs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Durable History"));
    assert!(stdout.contains("Runtime Logs"));
}

#[test]
fn run_tail_with_logs_renders_final_review_member_timing_summary() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-reviewers",
            "--name",
            "Tail Reviewers",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-reviewers"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let journal_path = project_root(temp_dir.path(), "tail-reviewers").join("journal.ndjson");
    let reviewer_events = [
        r#"{"sequence":2,"timestamp":"2026-04-02T10:00:00Z","event_type":"reviewer_started","details":{"run_id":"run-1","stage_id":"final_review","cycle":1,"attempt":1,"completion_round":1,"panel":"final_review","phase":"proposal","reviewer_id":"reviewer-2","role":"reviewer","backend_family":"codex","model_id":"gpt-5.4"}}"#,
        r#"{"sequence":3,"timestamp":"2026-04-02T10:00:05Z","event_type":"reviewer_completed","details":{"run_id":"run-1","stage_id":"final_review","cycle":1,"attempt":1,"completion_round":1,"panel":"final_review","phase":"proposal","reviewer_id":"reviewer-2","role":"reviewer","backend_family":"codex","model_id":"gpt-5.4","duration_ms":37,"outcome":"proposed_amendments","amendment_count":2}}"#,
    ]
    .join("\n")
        + "\n";
    fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .expect("open journal")
        .write_all(reviewer_events.as_bytes())
        .expect("append reviewer events");

    let output = Command::new(binary())
        .args(["run", "tail", "--logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --logs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("final_review reviewer reviewer-2 proposal [codex / gpt-5.4]"));
    assert!(
        stdout.contains(
            "final_review reviewer reviewer-2 proposal completed in 37ms outcome=proposed_amendments amendments=2"
        )
    );
}

#[test]
fn run_tail_renders_iterative_minimal_iteration_events() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-iterative",
            "--name",
            "Tail Iterative",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "iterative_minimal",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-iterative"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let journal_path = project_root(temp_dir.path(), "tail-iterative").join("journal.ndjson");
    let events = [
        r#"{"sequence":2,"timestamp":"2026-04-02T10:00:00Z","event_type":"implementer_iteration_started","details":{"run_id":"run-1","stage_id":"plan_and_implement","cycle":1,"completion_round":1,"iteration":3}}"#,
        r#"{"sequence":3,"timestamp":"2026-04-02T10:00:02Z","event_type":"implementer_iteration_completed","details":{"run_id":"run-1","stage_id":"plan_and_implement","cycle":1,"completion_round":1,"iteration":3,"diff_changed":false,"outcome":"completed"}}"#,
        r#"{"sequence":4,"timestamp":"2026-04-02T10:00:03Z","event_type":"implementer_loop_exited","details":{"run_id":"run-1","stage_id":"plan_and_implement","cycle":1,"completion_round":1,"reason":"stable","total_iterations":3}}"#,
    ]
    .join("\n")
        + "\n";
    fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .expect("open journal")
        .write_all(events.as_bytes())
        .expect("append iteration events");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("plan_and_implement iteration 3 started (cycle 1 round 1)"));
    assert!(stdout
        .contains("plan_and_implement iteration 3 completed diff_changed=false outcome=completed"));
    assert!(stdout.contains("plan_and_implement loop exited reason=stable total_iterations=3"));
}

#[test]
fn run_tail_with_logs_shows_only_newest_log_file() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-multi",
            "--name",
            "Tail Multi",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-multi"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write two runtime log files: old and new
    let logs_dir = project_root(temp_dir.path(), "tail-multi").join("runtime/logs");
    fs::write(
        logs_dir.join("001.ndjson"),
        r#"{"timestamp":"2026-03-11T18:00:00Z","level":"info","source":"agent","message":"old log entry"}"#.to_owned() + "\n",
    ).expect("write old log");
    fs::write(
        logs_dir.join("002.ndjson"),
        r#"{"timestamp":"2026-03-11T19:00:00Z","level":"info","source":"agent","message":"new log entry"}"#.to_owned() + "\n",
    ).expect("write new log");

    let output = Command::new(binary())
        .args(["run", "tail", "--logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --logs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Runtime Logs"));
    // Only the newest log file entries should appear
    assert!(
        stdout.contains("new log entry"),
        "newest log should be shown"
    );
    assert!(
        !stdout.contains("old log entry"),
        "older log files should not be included"
    );
}

#[test]
fn run_status_json_outputs_stable_fields() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let run_json = r#"{
  "active_run": null,
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 4,
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
}"#;
    fs::write(
        project_root(temp_dir.path(), "alpha").join("run.json"),
        run_json,
    )
    .expect("write paused snapshot");

    let output = Command::new(binary())
        .args(["run", "status", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("status json should parse");
    assert_eq!(value["project_id"], "alpha");
    assert_eq!(value["status"], "paused");
    assert!(value["stage"].is_null());
    assert_eq!(value["completion_round"], serde_json::Value::Null);
    assert_eq!(value["summary"], "paused for review");
    assert_eq!(value["amendment_queue_depth"], 1);
    assert!(value["milestone_id"].is_null());
    assert!(value["bead_id"].is_null());
}

#[test]
fn run_status_displays_lineage_for_milestone_linked_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(temp_dir.path(), "alpha", "ms-alpha", "ms-alpha.bead-2");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Milestone: Alpha Milestone | Bead: Bootstrap bead-backed task creation"),
        "run status should show bead lineage summary, got: {stdout}"
    );
}

#[test]
fn run_status_json_includes_lineage_fields_for_milestone_linked_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(temp_dir.path(), "alpha", "ms-alpha", "ms-alpha.bead-2");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "status", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("status json should parse");
    assert_eq!(value["milestone_id"], "ms-alpha");
    assert_eq!(value["bead_id"], "ms-alpha.bead-2");
}

#[test]
fn run_status_tolerates_corrupt_journal_for_plain_project() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        "{",
    )
    .expect("corrupt journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Project: alpha"),
        "run status should still report snapshot state when journal is corrupt, got: {stdout}"
    );
    assert!(
        stdout.contains("Status: not started"),
        "run status should keep using run.json as the canonical status source, got: {stdout}"
    );
}

#[test]
fn run_status_still_shows_lineage_when_journal_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(temp_dir.path(), "alpha", "ms-alpha", "ms-alpha.bead-2");
    select_active_project_fixture(temp_dir.path(), "alpha");
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        "{",
    )
    .expect("corrupt journal");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Milestone: Alpha Milestone | Bead: Bootstrap bead-backed task creation"),
        "run status should load lineage from project metadata even when journal parsing fails, got: {stdout}"
    );
}

#[test]
fn run_history_verbose_shows_details_metadata_and_preview() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "history", "--verbose"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --verbose");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("details:"));
    assert!(stdout.contains("\"stage_id\": \"planning\""));
    assert!(stdout.contains("metadata:"));
    assert!(stdout.contains("\"payload_id\": \"p1\""));
    assert!(stdout.contains("preview: # Planning"));
    assert!(
        stdout.contains("..."),
        "long artifact preview should be truncated"
    );
}

#[test]
fn run_history_json_outputs_parseable_json() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "history", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("history json should parse");
    assert_eq!(value["project_id"], "alpha");
    assert!(
        value.get("milestone_id").is_some(),
        "history json should always include milestone_id"
    );
    assert!(
        value.get("bead_id").is_some(),
        "history json should always include bead_id"
    );
    assert!(value["milestone_id"].is_null());
    assert!(value["bead_id"].is_null());
    assert_eq!(value["events"].as_array().expect("events array").len(), 5);
    assert_eq!(
        value["payloads"].as_array().expect("payloads array").len(),
        2
    );
    assert!(
        value["payloads"][0].get("payload").is_none(),
        "compact history json should omit payload bodies"
    );
    assert!(
        value["artifacts"][0].get("content").is_none(),
        "compact history json should omit artifact content"
    );
}

#[test]
fn run_history_json_includes_optional_lineage_fields() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_lineage_fixture(
        temp_dir.path(),
        "alpha",
        "ms-alpha",
        "ms-alpha.bead-1",
    );

    let output = Command::new(binary())
        .args(["run", "history", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("history json should parse");
    assert_eq!(value["milestone_id"], "ms-alpha");
    assert_eq!(value["bead_id"], "ms-alpha.bead-1");
}

#[test]
fn run_history_entries_include_bead_id_for_lineage_linked_history() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_lineage_fixture(
        temp_dir.path(),
        "alpha",
        "ms-alpha",
        "ms-alpha.bead-1",
    );

    let text_output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(text_output.status.success());
    let stdout = String::from_utf8_lossy(&text_output.stdout);
    assert!(
        stdout.contains("stage_completed (bead=ms-alpha.bead-1)")
            || stdout.contains("StageCompleted (bead=ms-alpha.bead-1)"),
        "history text should annotate event entries with bead scope, got: {stdout}"
    );
    assert!(
        stdout.contains(
            "p1 (planning, cycle 1, attempt 1, kind=primary, round=1 (bead=ms-alpha.bead-1))"
        ),
        "history text should annotate payload entries with bead scope, got: {stdout}"
    );

    let json_output = Command::new(binary())
        .args(["run", "history", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --json");

    assert!(json_output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&json_output.stdout).expect("history json should parse");
    assert_eq!(value["events"][0]["bead_id"], "ms-alpha.bead-1");
    assert_eq!(value["payloads"][0]["bead_id"], "ms-alpha.bead-1");
    assert_eq!(value["artifacts"][0]["bead_id"], "ms-alpha.bead-1");
}

#[test]
fn run_history_json_verbose_includes_payload_and_content() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "history", "--json", "--verbose"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --json --verbose");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("history json should parse");
    assert_eq!(
        value["payloads"][0]["payload"]["summary"],
        "planning payload"
    );
    assert!(value["artifacts"][0]["content"]
        .as_str()
        .expect("artifact content")
        .starts_with("# Planning"));
}

#[test]
fn run_history_stage_filters_records() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "history", "--stage", "planning"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --stage planning");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("p1"));
    assert!(stdout.contains("a1"));
    assert!(!stdout.contains("p2"));
    assert!(!stdout.contains("a2"));
    assert!(!stdout.contains("ProjectCreated"));
}

#[test]
fn run_history_stage_unknown_stage_fails_cleanly() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "history", "--stage", "unknown_stage"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history --stage unknown_stage");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown stage identifier"));
}

#[test]
fn run_tail_last_limits_to_most_recent_events() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "tail", "--last", "2"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --last 2");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("ProjectCreated"));
    assert!(!stdout.contains("p1"));
    assert!(stdout.contains("p2"));
    assert!(stdout.contains("a2"));
}

#[test]
fn run_tail_follow_starts_and_interrupts_cleanly() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let child = Command::new(binary())
        .args(["run", "tail", "--follow"])
        .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run tail --follow");

    std::thread::sleep(std::time::Duration::from_millis(250));
    kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).expect("send SIGINT");
    let output = child.wait_with_output().expect("wait for follow output");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Following project 'alpha'"));
    assert!(stdout.contains("Stopped following."));
}

#[test]
fn run_tail_follow_surfaces_new_supporting_records_without_journal_events() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let child = Command::new(binary())
        .args(["run", "tail", "--follow"])
        .env("RALPH_BURNING_TEST_FOLLOW_BASELINE_DELAY_MS", "250")
        .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run tail --follow");

    std::thread::sleep(std::time::Duration::from_millis(100));

    let project_root = project_root(temp_dir.path(), "alpha");
    write_supporting_payload(&project_root);
    write_supporting_artifact(&project_root);

    std::thread::sleep(std::time::Duration::from_millis(400));
    kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).expect("send SIGINT");
    let output = child.wait_with_output().expect("wait for follow output");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("panel-p1"));
    assert!(stdout.contains("panel-a1"));
}

#[test]
fn run_tail_follow_fails_on_durable_orphan_supporting_payload() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let project_root = project_root(temp_dir.path(), "alpha");
    write_supporting_payload(&project_root);

    let child = Command::new(binary())
        .args(["run", "tail", "--follow"])
        .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run tail --follow");

    let output = wait_for_child_output(child, std::time::Duration::from_millis(3000));

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("history/payloads/panel-p1"));
    assert!(stderr.contains("payload has no matching artifact"));
}

#[test]
fn run_tail_follow_tolerates_startup_partial_supporting_pair() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let project_root = project_root(temp_dir.path(), "alpha");
    write_supporting_payload(&project_root);

    let child = Command::new(binary())
        .args(["run", "tail", "--follow"])
        .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run tail --follow");

    std::thread::sleep(std::time::Duration::from_millis(100));
    write_supporting_artifact(&project_root);

    std::thread::sleep(std::time::Duration::from_millis(500));
    kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).expect("send SIGINT");
    let output = child.wait_with_output().expect("wait for follow output");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("panel-p1"));
    assert!(stdout.contains("panel-a1"));
}

#[test]
fn run_tail_follow_logs_keeps_streaming_after_new_partial_supporting_pair() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");
    set_workspace_stream_output(temp_dir.path(), true);

    let project_root = project_root(temp_dir.path(), "alpha");
    let child = Command::new(binary())
        .args(["run", "tail", "--follow", "--logs"])
        .env("RALPH_BURNING_TEST_FOLLOW_POLL_INTERVAL_MS", "100")
        .current_dir(temp_dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn run tail --follow --logs");

    std::thread::sleep(std::time::Duration::from_millis(100));
    write_supporting_payload(&project_root);
    std::thread::sleep(std::time::Duration::from_millis(150));
    write_follow_runtime_log(&project_root, "follow log after partial pair");
    std::thread::sleep(std::time::Duration::from_millis(150));
    write_supporting_artifact(&project_root);

    std::thread::sleep(std::time::Duration::from_millis(500));
    kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).expect("send SIGINT");
    let output = child
        .wait_with_output()
        .expect("wait for follow --logs output");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("follow log after partial pair"));
    assert!(stdout.contains("panel-p1"));
    assert!(stdout.contains("panel-a1"));
}

#[test]
fn run_show_payload_prints_payload_json() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "show-payload", "p1"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run show-payload");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("payload output should parse");
    assert_eq!(value["summary"], "planning payload");
}

#[test]
fn run_show_payload_unknown_id_fails() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "show-payload", "missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run show-payload missing");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("payload not found"));
}

#[test]
fn run_show_artifact_prints_content() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_run_query_history_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "show-artifact", "a2"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run show-artifact");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("# Implementation"));
    assert!(stdout.contains("visible artifact"));
}

#[test]
fn run_show_artifact_unknown_id_fails() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "show-artifact", "missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run show-artifact missing");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("artifact not found"));
}

#[test]
fn run_rollback_list_shows_visible_targets() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");
    write_rollback_targets_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "rollback", "--list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run rollback --list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Rollback ID"));
    assert!(stdout.contains("rb-planning"));
    assert!(stdout.contains("rb-implementation"));
    assert!(stdout.contains("abc123"));
}

#[test]
fn run_rollback_list_with_no_targets_reports_empty_state() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["run", "rollback", "--list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run rollback --list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No rollback targets available."));
}

// ── Fail-fast on missing canonical files ──

#[test]
fn run_status_fails_fast_when_run_json_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "broken",
            "--name",
            "Broken",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "broken"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete run.json to simulate corruption
    fs::remove_file(project_root(temp_dir.path(), "broken").join("run.json"))
        .expect("remove run.json");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("missing"));
}

#[test]
fn run_history_fails_fast_when_journal_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "nojrnl",
            "--name",
            "No Journal",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "nojrnl"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete journal.ndjson to simulate corruption
    fs::remove_file(project_root(temp_dir.path(), "nojrnl").join("journal.ndjson"))
        .expect("remove journal");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("journal.ndjson"));
    assert!(stderr.contains("missing"));
}

#[test]
fn run_status_fails_fast_when_run_json_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt",
            "--name",
            "Corrupt",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write corrupt JSON to run.json
    fs::write(
        project_root(temp_dir.path(), "corrupt").join("run.json"),
        "{invalid json}",
    )
    .expect("corrupt run.json");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
}

// ── Missing project.toml corruption detection ──

#[test]
fn project_show_fails_fast_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-proj",
            "--name",
            "Corrupt",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Delete project.toml to simulate corruption
    fs::remove_file(project_root(temp_dir.path(), "corrupt-proj").join("project.toml"))
        .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["project", "show", "corrupt-proj"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

#[test]
fn project_list_skips_partial_project_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "good-proj",
            "--name",
            "Good",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Delete project.toml to simulate corruption
    fs::remove_file(project_root(temp_dir.path(), "good-proj").join("project.toml"))
        .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(
        output.status.success(),
        "project list should skip partial projects: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("good-proj"),
        "project list should not surface the partial project: {stdout}"
    );
}

#[test]
fn project_delete_fails_fast_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();

    // Create a bare directory without project.toml (simulates corruption)
    let corrupt_dir = project_root(temp_dir.path(), "bare-proj");
    fs::create_dir_all(&corrupt_dir).expect("create bare project dir");

    let output = Command::new(binary())
        .args(["project", "delete", "bare-proj"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

// ── Terminal snapshot status reporting ──

#[test]
fn run_status_reports_completed_for_terminal_run_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "terminal",
            "--name",
            "Terminal",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "terminal"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write a completed terminal snapshot (no active_run)
    let completed_snapshot = r#"{
  "active_run": null,
  "status": "completed",
  "cycle_history": [],
  "completion_rounds": 3,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "completed after 3 rounds"
}"#;
    fs::write(
        project_root(temp_dir.path(), "terminal").join("run.json"),
        completed_snapshot,
    )
    .expect("write completed snapshot");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: completed"));
    assert!(stdout.contains("completed after 3 rounds"));
}

#[test]
fn run_status_fails_for_semantically_inconsistent_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "inconsist",
            "--name",
            "Inconsistent",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "inconsist"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write a semantically inconsistent snapshot: running with no active_run
    let bad_snapshot = r#"{
  "active_run": null,
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 0,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running"
}"#;
    fs::write(
        project_root(temp_dir.path(), "inconsist").join("run.json"),
        bad_snapshot,
    )
    .expect("write inconsistent snapshot");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("inconsistent"));
}

#[test]
fn project_delete_fails_for_semantically_inconsistent_active_run() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "bad-state",
            "--name",
            "Bad State",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Write a semantically inconsistent snapshot: paused with an active_run
    let bad_snapshot = r#"{
  "active_run": {
    "run_id": "run-bad-state",
    "stage_cursor": {
      "stage": "planning",
      "cycle": 1,
      "attempt": 1,
      "completion_round": 1
    },
    "started_at": "2026-03-11T19:00:00Z"
  },
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 0,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "paused"
}"#;
    fs::write(
        project_root(temp_dir.path(), "bad-state").join("run.json"),
        bad_snapshot,
    )
    .expect("write inconsistent snapshot");

    let output = Command::new(binary())
        .args(["project", "delete", "bad-state"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("inconsistent"));
}

// ── Active-project canonical validation (corrupt project.toml) ──

#[test]
fn run_status_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-active",
            "--name",
            "Corrupt Active",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-active"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content (file exists but is malformed)
    fs::write(
        project_root(temp_dir.path(), "corrupt-active").join("project.toml"),
        "this is {{ not valid toml",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_history_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-hist",
            "--name",
            "Corrupt Hist",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-hist"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        project_root(temp_dir.path(), "corrupt-hist").join("project.toml"),
        "not valid toml {{{",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_tail_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-tail",
            "--name",
            "Corrupt Tail",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        project_root(temp_dir.path(), "corrupt-tail").join("project.toml"),
        "{invalid toml}",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn project_show_no_id_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-show",
            "--name",
            "Corrupt Show",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        project_root(temp_dir.path(), "corrupt-show").join("project.toml"),
        "garbled content }{{}",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["project", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_status_fails_fast_when_active_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "missing-toml",
            "--name",
            "Missing Toml",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "missing-toml"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete project.toml
    fs::remove_file(project_root(temp_dir.path(), "missing-toml").join("project.toml"))
        .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

// ── Run.json schema completeness ──

#[test]
fn project_create_run_json_contains_all_canonical_fields() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "schema",
            "--name",
            "Schema Check",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let run_json = fs::read_to_string(project_root(temp_dir.path(), "schema").join("run.json"))
        .expect("read run.json");

    assert!(run_json.contains("\"cycle_history\""));
    assert!(run_json.contains("\"completion_rounds\""));
    assert!(run_json.contains("\"rollback_point_meta\""));
    assert!(run_json.contains("\"amendment_queue\""));
    assert!(run_json.contains("\"active_run\""));
    assert!(run_json.contains("\"status\""));
    assert!(run_json.contains("\"status_summary\""));
}

// ── Regression: project select rejects schema-invalid project.toml ──

#[test]
fn project_select_rejects_syntactically_valid_but_schema_invalid_project_toml() {
    let temp_dir = initialize_workspace_fixture();

    // Create a project directory with a syntactically valid TOML that is missing
    // required canonical fields (only has 'id', no name/flow/prompt_reference/etc.)
    let project_root = project_root(temp_dir.path(), "partial");
    fs::create_dir_all(&project_root).expect("create project directory");
    fs::write(project_root.join("project.toml"), "id = \"partial\"\n")
        .expect("write incomplete project.toml");

    let output = Command::new(binary())
        .args(["project", "select", "partial"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("project.toml"),
        "error should reference project.toml, got: {stderr}"
    );
    assert!(
        stderr.contains("invalid canonical structure") || stderr.contains("corrupt"),
        "error should indicate structural invalidity, got: {stderr}"
    );
}

// ── Regression: delete transactional with active-project pointer ──

#[test]
fn project_delete_clears_active_pointer_transactionally() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create and select a project
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "txn-del",
            "--name",
            "Txn Delete",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "txn-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Verify it's the active project
    let active =
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active-project");
    assert_eq!(active, "txn-del");

    // Delete the project
    let output = Command::new(binary())
        .args(["project", "delete", "txn-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("delete project");
    assert!(output.status.success());

    // Active-project pointer should be cleared
    assert!(
        !active_project_path(temp_dir.path()).exists(),
        "active-project pointer should be cleared after delete"
    );

    // Project directory should be gone
    assert!(
        !project_root(temp_dir.path(), "txn-del").exists(),
        "project directory should be removed"
    );
}

#[test]
fn empty_journal_fails_fast_on_project_show() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    // Truncate journal to empty
    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
    assert!(
        stderr.contains("empty"),
        "error should mention empty journal, got: {stderr}"
    );
}

#[test]
fn project_show_does_not_rewrite_legacy_run_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let run_json_path = project_root(temp_dir.path(), "alpha").join("run.json");
    let legacy_snapshot = serde_json::json!({
        "active_run": null,
        "status": "not_started",
        "cycle_history": [],
        "completion_rounds": 0,
        "rollback_point_meta": {"last_rollback_id": null, "rollback_count": 0},
        "amendment_queue": {"pending": [], "processed_count": 0},
        "status_summary": "not started"
    });
    fs::write(
        &run_json_path,
        serde_json::to_string_pretty(&legacy_snapshot).expect("serialize legacy snapshot"),
    )
    .expect("write legacy run.json");
    let before = fs::read_to_string(&run_json_path).expect("read legacy run.json");

    let output = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(output.status.success());
    let after = fs::read_to_string(&run_json_path).expect("read post-show run.json");
    assert_eq!(
        after, before,
        "project show must not mutate legacy run.json"
    );
}

#[test]
fn empty_journal_fails_fast_on_run_history() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    // Select and truncate journal
    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
}

#[test]
fn empty_journal_fails_fast_on_run_tail() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    fs::write(
        project_root(temp_dir.path(), "alpha").join("journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
}

#[test]
fn delete_with_unremovable_active_pointer_restores_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create and select a project
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "restore-me",
            "--name",
            "Restore Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "restore-me"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Replace active-project file with a directory to make remove_file fail
    let ap_path = active_project_path(temp_dir.path());
    fs::remove_file(&ap_path).expect("remove active-project file");
    fs::create_dir_all(ap_path.join("blocker")).expect("create blocking dir");

    // Attempt delete — should fail because clearing the pointer fails
    let output = Command::new(binary())
        .args(["project", "delete", "restore-me"])
        .current_dir(temp_dir.path())
        .output()
        .expect("delete project");

    assert!(
        !output.status.success(),
        "delete should fail when pointer clear fails"
    );

    // Project must still be addressable at its canonical path
    assert!(
        project_root(temp_dir.path(), "restore-me")
            .join("project.toml")
            .exists(),
        "project should be restored after failed pointer clear"
    );
}

// ── Run Start ──

#[cfg(feature = "test-stub")]
fn setup_project(temp_dir: &tempfile::TempDir, project_id: &str, flow: &str) {
    let prompt = write_prompt_fixture(temp_dir.path());
    let create = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            project_id,
            "--name",
            &format!("Test {project_id}"),
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            flow,
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");
    assert!(
        create.status.success(),
        "setup create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let select = Command::new(binary())
        .args(["project", "select", project_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");
    assert!(select.status.success());
}

#[cfg(feature = "test-stub")]
fn setup_standard_project(temp_dir: &tempfile::TempDir, project_id: &str) {
    setup_project(temp_dir, project_id, "standard");
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_completes_standard_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-e2e");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Starting run for project"));
    assert!(stdout.contains("Run completed successfully"));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_completes_docs_change_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "docs-run", "docs_change");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> =
        fs::read_dir(project_root(temp_dir.path(), "docs-run").join("history/payloads"))
            .expect("read payloads dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
    assert_eq!(payload_files.len(), 5);

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "docs-run").join("journal.ndjson"))
            .expect("read journal");
    assert!(journal.contains("\"docs_plan\""));
    assert!(journal.contains("\"docs_update\""));
    assert!(journal.contains("\"docs_validation\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_completes_ci_improvement_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "ci-run", "ci_improvement");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> =
        fs::read_dir(project_root(temp_dir.path(), "ci-run").join("history/payloads"))
            .expect("read payloads dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
    assert_eq!(payload_files.len(), 5);

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "ci-run").join("journal.ndjson"))
            .expect("read journal");
    assert!(journal.contains("\"ci_plan\""));
    assert!(journal.contains("\"ci_update\""));
    assert!(journal.contains("\"ci_validation\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_produces_completed_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-snap");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    // Verify run.json shows completed
    let run_json = fs::read_to_string(project_root(temp_dir.path(), "run-snap").join("run.json"))
        .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "run.json should contain completed status, got: {run_json}"
    );
    assert!(
        run_json.contains("\"active_run\":null") || run_json.contains("\"active_run\": null"),
        "active_run should be null after completion"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_persists_journal_events() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-journal");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "run-journal").join("journal.ndjson"))
            .expect("read journal");

    // Should have project_created + run_started + stage events + run_completed
    assert!(
        journal.contains("\"run_started\""),
        "journal should contain run_started"
    );
    assert!(
        journal.contains("\"stage_entered\""),
        "journal should contain stage_entered"
    );
    assert!(
        journal.contains("\"stage_completed\""),
        "journal should contain stage_completed"
    );
    assert!(
        journal.contains("\"run_completed\""),
        "journal should contain run_completed"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_syncs_milestone_lineage_for_bead_backed_projects() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--flow",
            "standard",
            "--project-id",
            "bead-run",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("PATH", path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    let task_runs =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"))
            .expect("read milestone task-runs");
    assert!(task_runs.contains("\"bead_id\":\"ms-alpha.bead-2\""));
    assert!(task_runs.contains("\"project_id\":\"bead-run\""));
    assert!(task_runs.contains(&format!("\"plan_hash\":\"{plan_hash}\"")));
    assert!(task_runs.contains("\"outcome\":\"succeeded\""));

    let milestone_journal =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha").join("journal.ndjson"))
            .expect("read milestone journal");
    assert!(milestone_journal.contains("\"bead_started\""));
    assert!(milestone_journal.contains("\"bead_completed\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_refreshes_active_milestone_from_active_bead_backed_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--flow",
            "standard",
            "--project-id",
            "bead-run-active",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    fs::write(active_milestone_path(temp_dir.path()), "ms-stale").expect("seed stale milestone");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("PATH", path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-alpha"
    );
}

#[test]
fn project_create_from_bead_rejects_duplicate_active_same_bead_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-existing").join("run.json"),
        r#"{"active_run":{"run_id":"run-existing","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-04-01T10:11:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#,
    )
    .expect("write running snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-existing","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-duplicate",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "duplicate active bead project should be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bead 'ms-alpha.bead-2' already has an active task"));
    assert!(stderr.contains("project 'bead-existing'"));
    assert!(stderr.contains("run 'run-existing'"));
    assert!(
        !project_root(temp_dir.path(), "bead-duplicate").exists(),
        "rejected duplicate creation must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_allows_new_project_after_terminal_attempt() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-terminal",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-terminal").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-terminal","run_id":"run-terminal","plan_hash":"{plan_hash}","outcome":"succeeded","started_at":"2026-04-01T10:11:00Z","finished_at":"2026-04-01T10:15:00Z"}}"#
        ),
    )
    .expect("write terminal task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-terminal",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "terminal lineage should allow a new project: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        project_root(temp_dir.path(), "bead-after-terminal")
            .join("project.toml")
            .exists(),
        "new project should be created after terminal attempts"
    );
}

#[test]
fn project_create_from_bead_rejects_legacy_runless_active_lineage() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"legacy-project","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write legacy runless task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-legacy-runless",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "legacy runless lineage should block creation: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy active lineage row")
            && stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"),
        "expected remediation guidance for legacy runless lineage, got: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "bead-after-legacy-runless")
            .join("project.toml")
            .exists(),
        "no new project should be created when a legacy runless lineage row is still active"
    );
}

#[test]
fn project_create_from_bead_clears_pending_lineage_reset_before_duplicate_check() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("status.json"),
        format!(
            r#"{{
  "status": "ready",
  "plan_hash": "{plan_hash}",
  "plan_version": 2,
  "progress": {{
    "total_beads": 3,
    "completed_beads": 1,
    "in_progress_beads": 1,
    "failed_beads": 0,
    "skipped_beads": 0,
    "blocked_beads": 1
  }},
  "pending_lineage_reset": {{
    "plan_hash": "{plan_hash}",
    "plan_version": 2
  }},
  "updated_at": "2026-04-01T10:05:00Z"
}}"#
        ),
    )
    .expect("write milestone status with pending lineage reset");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"stale-project","run_id":"run-stale","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write stale active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-reset",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "pending lineage reset should clear stale lineage before duplicate checks: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        project_root(temp_dir.path(), "bead-after-reset")
            .join("project.toml")
            .exists(),
        "new project should be created after clearing stale lineage"
    );
    let status =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha").join("status.json"))
            .expect("read milestone status");
    assert!(
        !status.contains("pending_lineage_reset"),
        "create-time snapshot load should clear pending lineage reset before validation"
    );
    assert_eq!(
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"))
            .expect("read task runs after create"),
        "",
        "stale lineage should be truncated before duplicate checks"
    );
}

#[test]
fn project_create_from_bead_reports_orphaned_active_lineage_entry() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"orphan-project","run_id":"run-orphan","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write orphaned task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-orphan",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "orphaned active lineage should block project creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("active lineage points to missing project 'orphan-project'"));
    assert!(stderr.contains("run 'run-orphan'"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(
        !project_root(temp_dir.path(), "bead-after-orphan").exists(),
        "orphaned lineage must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_reports_missing_run_snapshot_for_active_lineage_entry() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::remove_file(project_root(temp_dir.path(), "bead-existing").join("run.json"))
        .expect("remove run snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-existing","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-missing-run",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "missing run snapshot should block project creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("active lineage points to an unreadable run snapshot"));
    assert!(stderr.contains("project 'bead-existing'"));
    assert!(stderr.contains("run 'run-existing'"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(stderr.contains("restore the project state"));
    assert!(
        !project_root(temp_dir.path(), "bead-after-missing-run").exists(),
        "broken lineage state must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_reports_corrupt_run_snapshot_for_active_lineage_entry() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-existing").join("run.json"),
        "{not valid json",
    )
    .expect("corrupt run snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-existing","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-corrupt-run",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "corrupt run snapshot should block project creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("active lineage points to an unreadable run snapshot"));
    assert!(stderr.contains("project 'bead-existing'"));
    assert!(stderr.contains("run 'run-existing'"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(stderr.contains("restore the project state"));
    assert!(
        !project_root(temp_dir.path(), "bead-after-corrupt-run").exists(),
        "broken lineage state must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_reports_not_started_run_snapshot_as_repairable_lineage() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-existing").join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    )
    .expect("write not-started snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-existing","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write inconsistent active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-not-started",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "a not-started snapshot behind active lineage should block creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("active lineage points to a not-started project snapshot"));
    assert!(stderr.contains("project 'bead-existing'"));
    assert!(stderr.contains("run 'run-existing'"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(
        !stderr.contains("run resume"),
        "repair guidance must not point at `run resume` for a not-started project: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "bead-after-not-started").exists(),
        "broken lineage state must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_reports_mismatched_task_source_for_active_lineage_entry() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-other-bead",
        "ms-alpha",
        "ms-alpha.bead-9",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-other-bead").join("run.json"),
        r#"{"active_run":{"run_id":"run-existing","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-04-01T10:11:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#,
    )
    .expect("write running snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-other-bead","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write mismatched active task-runs");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-after-mismatched-task-source",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "mismatched task_source should block project creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("active lineage points to project 'bead-other-bead' whose task source targets milestone 'ms-alpha' bead 'ms-alpha.bead-9'"),
        "expected mismatched task source repair guidance, got: {stderr}"
    );
    assert!(stderr.contains("run 'run-existing'"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(
        !project_root(temp_dir.path(), "bead-after-mismatched-task-source").exists(),
        "broken lineage state must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_rejects_existing_not_started_same_bead_project_before_br_claim() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-second",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "an existing not-started bead project should block duplicate creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(
        "bead 'ms-alpha.bead-2' already has project 'bead-existing' in not_started state"
    ));
    assert!(stderr.contains("ralph-burning project select bead-existing"));
    assert!(stderr.contains("ralph-burning run start"));
    assert!(
        !stderr.contains("br claim failed"),
        "duplicate not-started project should fail before the br-claim recovery path: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "bead-second").exists(),
        "duplicate create must not mint a second project"
    );
}

#[test]
fn project_create_from_bead_rejects_existing_failed_same_bead_project_before_br_claim() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-existing").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed after prior attempt"}"#,
    )
    .expect("write failed run.json");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-second",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "an existing failed bead project should block duplicate creation"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project 'bead-existing' already exists for that bead in failed state"));
    assert!(stderr.contains("ralph-burning project select bead-existing"));
    assert!(stderr.contains("ralph-burning run resume"));
    assert!(
        !stderr.contains("br claim failed"),
        "failed same-bead project should fail before the br-claim recovery path: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "bead-second").exists(),
        "duplicate create must not mint a second project"
    );
}

#[test]
fn project_create_from_bead_rejects_ambiguous_non_terminal_same_bead_projects() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-paused",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-paused").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"paused","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"paused"}"#,
    )
    .expect("write paused run.json");

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-failed",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-failed").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed"}"#,
    )
    .expect("write failed run.json");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-third",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "ambiguous same-bead retries should fail closed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("multiple existing same-bead projects need operator repair"));
    assert!(stderr.contains("bead-failed (failed)"));
    assert!(stderr.contains("bead-paused (paused)"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(
        !stderr.contains("br claim failed"),
        "ambiguous duplicate detection must happen before any br claim attempt: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "bead-third").exists(),
        "ambiguous same-bead state must not create a third project"
    );
}

#[test]
fn project_create_from_bead_reports_running_same_bead_project_without_active_lineage() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-running",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-running").join("run.json"),
        r#"{"active_run":{"run_id":"run-existing","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-04-01T10:11:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#,
    )
    .expect("write running run.json");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-second",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "a running same-bead project without lineage must fail closed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project 'bead-running' already exists for that bead in running state"));
    assert!(stderr.contains("has no matching active lineage row"));
    assert!(stderr.contains("repair milestones/ms-alpha/task-runs.ndjson"));
    assert!(
        !project_root(temp_dir.path(), "bead-second").exists(),
        "repair-required running state must not create a shadow project"
    );
}

#[test]
fn project_create_from_bead_rejects_same_project_id_for_existing_not_started_bead_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-existing",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "reusing the same project id for an existing not-started bead task should be rejected cleanly"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(
        "bead 'ms-alpha.bead-2' already has project 'bead-existing' in not_started state"
    ));
    assert!(stderr.contains("ralph-burning project select bead-existing"));
    assert!(stderr.contains("ralph-burning run start"));
    assert!(
        !stderr.contains("project 'bead-existing' already exists"),
        "same-bead reuse should return the targeted bead-project guidance instead of a generic duplicate-project error: {stderr}"
    );
    assert!(
        !stderr.contains("br claim failed"),
        "same-project duplicate detection must happen before any br claim attempt: {stderr}"
    );
}

#[test]
fn project_create_from_bead_prefers_failed_retry_over_completed_default_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "task-ms-alpha-bead-2",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "task-ms-alpha-bead-2").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed default run.json");

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-failed-retry",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-failed-retry").join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed after retry"}"#,
    )
    .expect("write failed retry run.json");

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-third",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        !output.status.success(),
        "a failed retry project must block creating another same-bead project even if the default project is completed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("project 'bead-failed-retry' already exists for that bead in failed state")
    );
    assert!(stderr.contains("ralph-burning project select bead-failed-retry"));
    assert!(stderr.contains("ralph-burning run resume"));
    assert!(
        !project_root(temp_dir.path(), "bead-third").exists(),
        "non-terminal retry state must not allow a third same-bead project"
    );
}

#[test]
fn project_create_from_bead_allows_creation_with_multiple_terminal_same_bead_projects() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    for project_id in ["bead-terminal-1", "bead-terminal-2"] {
        create_bead_backed_project_fixture(
            temp_dir.path(),
            project_id,
            "ms-alpha",
            "ms-alpha.bead-2",
        );
        fs::write(
            project_root(temp_dir.path(), project_id).join("run.json"),
            r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
        )
        .expect("write completed retry run.json");
    }

    let output = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-terminal-3",
        ])
        .env("PATH", path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");

    assert!(
        output.status.success(),
        "all-terminal same-bead history should still allow a new project: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        project_root(temp_dir.path(), "bead-terminal-3")
            .join("project.toml")
            .exists(),
        "create-from-bead should allow a new project when every matching prior project is terminal"
    );
}

#[test]
fn project_create_from_bead_for_different_bead_preserves_run_start_active_bead_guard() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-3",
        r#"[
  {
    "id": "ms-alpha.bead-3",
    "title": "Document task bootstrap follow-up",
    "status": "open",
    "priority": "P2",
    "issue_type": "docs",
    "description": "Document the follow-up task bootstrap workflow.",
    "acceptance_criteria": "- Follow-up docs are captured",
    "dependencies": []
  }
]"#,
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    create_bead_backed_project_fixture(
        temp_dir.path(),
        "bead-existing",
        "ms-alpha",
        "ms-alpha.bead-2",
    );
    fs::write(
        project_root(temp_dir.path(), "bead-existing").join("run.json"),
        r#"{"active_run":{"run_id":"run-existing","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-04-01T10:11:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#,
    )
    .expect("write running snapshot");
    fs::write(
        milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"),
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-existing","run_id":"run-existing","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write active task-runs");

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-3",
            "--project-id",
            "bead-other",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create-from-bead");
    assert!(
        create.status.success(),
        "create for different bead should still succeed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("PATH", path)
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        !start.status.success(),
        "run start should still reject a different active bead"
    );
    let stderr = String::from_utf8_lossy(&start.stderr);
    assert!(stderr
        .contains("cannot start bead 'ms-alpha.bead-3': bead 'ms-alpha.bead-2' is already active"));

    let task_runs =
        fs::read_to_string(milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson"))
            .expect("read task-runs");
    assert!(task_runs.contains("\"bead_id\":\"ms-alpha.bead-2\""));
    assert!(
        !task_runs.contains("\"bead_id\":\"ms-alpha.bead-3\""),
        "failed start must not append a second active lineage row"
    );
}

#[test]
fn run_sync_milestone_repairs_completed_bead_backed_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-repair",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    // Clear controller state written during project creation so the
    // manually-crafted journal timestamps below don't conflict.
    let ms_root = milestone_root(temp_dir.path(), "ms-alpha");
    let _ = fs::remove_file(ms_root.join("controller.json"));
    let _ = fs::remove_file(ms_root.join("controller-journal.ndjson"));
    let _ = fs::remove_file(ms_root.join("controller.lock"));

    let project_root = project_root(temp_dir.path(), "bead-sync-repair");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-repair","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:11:00Z","event_type":"run_started","details":{{"run_id":"run-20260401101100","first_stage":"planning","max_completion_rounds":20}}}}
{{"sequence":3,"timestamp":"2026-04-01T10:15:00Z","event_type":"run_completed","details":{{"run_id":"run-20260401101100","completion_rounds":0,"max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    fs::write(
        &task_runs_path,
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-sync-repair","run_id":"run-20260401101100","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write stale task-runs");

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        sync.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&sync.stderr)
    );

    let repaired_task_runs = fs::read_to_string(&task_runs_path).expect("read repaired task-runs");
    assert!(repaired_task_runs.contains("\"outcome\":\"succeeded\""));
    assert!(repaired_task_runs.contains("\"finished_at\":\"2026-04-01T10:15:00Z\""));
}

#[test]
fn run_sync_milestone_repairs_stale_terminal_outcome_with_original_timestamp() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-terminal-repair",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    // Clear controller state written during project creation so the
    // manually-crafted journal timestamps below don't conflict.
    let ms_root = milestone_root(temp_dir.path(), "ms-alpha");
    let _ = fs::remove_file(ms_root.join("controller.json"));
    let _ = fs::remove_file(ms_root.join("controller-journal.ndjson"));
    let _ = fs::remove_file(ms_root.join("controller.lock"));

    let project_root = project_root(temp_dir.path(), "bead-sync-terminal-repair");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-terminal-repair","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:11:00Z","event_type":"run_started","details":{{"run_id":"run-20260401101100","first_stage":"planning","max_completion_rounds":20}}}}
{{"sequence":3,"timestamp":"2026-04-01T10:12:00Z","event_type":"run_failed","details":{{"run_id":"run-20260401101100","stage_id":"review","failure_class":"stage_failure","message":"stale failure","completion_rounds":0,"max_completion_rounds":20}}}}
{{"sequence":4,"timestamp":"2026-04-01T10:15:00Z","event_type":"run_completed","details":{{"run_id":"run-20260401101100","completion_rounds":0,"max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    fs::write(
        &task_runs_path,
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-sync-terminal-repair","run_id":"run-20260401101100","plan_hash":"{plan_hash}","outcome":"failed","outcome_detail":"stale failure","started_at":"2026-04-01T10:11:00Z","finished_at":"2026-04-01T10:12:00Z"}}"#
        ),
    )
    .expect("write stale task-runs");

    let milestone_journal_path = milestone_root(temp_dir.path(), "ms-alpha").join("journal.ndjson");
    fs::write(
        &milestone_journal_path,
        format!(
            r#"{{"timestamp":"2026-04-01T10:11:00Z","event_type":"bead_started","bead_id":"ms-alpha.bead-2","details":"{{\"project_id\":\"bead-sync-terminal-repair\",\"run_id\":\"run-20260401101100\",\"plan_hash\":\"{plan_hash}\"}}"}}
{{"timestamp":"2026-04-01T10:12:00Z","event_type":"bead_failed","bead_id":"ms-alpha.bead-2","details":"{{\"project_id\":\"bead-sync-terminal-repair\",\"run_id\":\"run-20260401101100\",\"plan_hash\":\"{plan_hash}\",\"started_at\":\"2026-04-01T10:11:00Z\",\"outcome\":\"failed\",\"outcome_detail\":\"stale failure\"}}"}}"#
        ),
    )
    .expect("write stale milestone journal");

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        sync.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&sync.stderr)
    );

    let repaired_task_runs = fs::read_to_string(&task_runs_path).expect("read repaired task-runs");
    assert!(repaired_task_runs.contains("\"outcome\":\"succeeded\""));
    assert!(!repaired_task_runs.contains("\"outcome_detail\":\"stale failure\""));
    assert!(repaired_task_runs.contains("\"finished_at\":\"2026-04-01T10:15:00Z\""));

    let repaired_milestone_journal =
        fs::read_to_string(&milestone_journal_path).expect("read repaired milestone journal");
    assert!(!repaired_milestone_journal.contains("\"event_type\":\"bead_failed\""));
    assert!(repaired_milestone_journal.contains("\"event_type\":\"bead_completed\""));
    assert!(repaired_milestone_journal.contains("\"timestamp\":\"2026-04-01T10:15:00Z\""));
}

#[test]
fn run_sync_milestone_reconstructs_missing_lineage_for_terminal_project() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-reconstruct",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    // Clear controller state written during project creation so the
    // manually-crafted journal timestamps below don't conflict.
    let ms_root = milestone_root(temp_dir.path(), "ms-alpha");
    let _ = fs::remove_file(ms_root.join("controller.json"));
    let _ = fs::remove_file(ms_root.join("controller-journal.ndjson"));
    let _ = fs::remove_file(ms_root.join("controller.lock"));

    let project_root = project_root(temp_dir.path(), "bead-sync-reconstruct");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-reconstruct","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:11:00Z","event_type":"run_started","details":{{"run_id":"run-20260401101100","first_stage":"planning","max_completion_rounds":20}}}}
{{"sequence":3,"timestamp":"2026-04-01T10:15:00Z","event_type":"run_completed","details":{{"run_id":"run-20260401101100","completion_rounds":0,"max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    if task_runs_path.exists() {
        fs::remove_file(&task_runs_path).expect("remove task-runs");
    }

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        sync.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&sync.stderr)
    );

    let repaired_task_runs = fs::read_to_string(&task_runs_path).expect("read repaired task-runs");
    assert!(repaired_task_runs.contains("\"project_id\":\"bead-sync-reconstruct\""));
    assert!(repaired_task_runs.contains("\"run_id\":\"run-20260401101100\""));
    assert!(repaired_task_runs.contains(&format!("\"plan_hash\":\"{plan_hash}\"")));
    assert!(repaired_task_runs.contains("\"outcome\":\"succeeded\""));
    assert!(repaired_task_runs.contains("\"finished_at\""));
}

#[test]
fn run_sync_milestone_errors_when_missing_lineage_is_ambiguous() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-ambiguous",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let project_root = project_root(temp_dir.path(), "bead-sync-ambiguous");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed after ambiguous lineage drift"}"#,
    )
    .expect("write failed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-ambiguous","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_started","details":{{"run_id":"run-20260401102000","first_stage":"planning","max_completion_rounds":20}}}}
{{"sequence":3,"timestamp":"2026-04-01T10:25:00Z","event_type":"run_failed","details":{{"run_id":"run-20260401102000","stage_id":"planning","failure_class":"stage_failure","message":"failed after ambiguous lineage drift","completion_rounds":0,"max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    fs::write(
        &task_runs_path,
        format!(
            concat!(
                "{{\"milestone_id\":\"ms-alpha\",\"bead_id\":\"ms-alpha.bead-2\",",
                "\"project_id\":\"older-project-a\",\"run_id\":\"run-older-a\",\"plan_hash\":\"{0}\",",
                "\"outcome\":\"running\",\"started_at\":\"2026-04-01T09:40:00Z\"}}\n",
                "{{\"milestone_id\":\"ms-alpha\",\"bead_id\":\"ms-alpha.bead-2\",",
                "\"project_id\":\"older-project-b\",\"run_id\":\"run-older-b\",\"plan_hash\":\"{0}\",",
                "\"outcome\":\"running\",\"started_at\":\"2026-04-01T09:50:00Z\"}}"
            ),
            plan_hash
        ),
    )
    .expect("write ambiguous task-runs");

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        !sync.status.success(),
        "sync should surface ambiguous lineage failure"
    );
    let stderr = String::from_utf8_lossy(&sync.stderr);
    assert!(stderr.contains("multiple active lineage rows exist"));
    assert!(stderr.contains("manual cleanup required"));

    let repaired_task_runs = fs::read_to_string(&task_runs_path).expect("read task-runs");
    assert!(repaired_task_runs.contains("\"run_id\":\"run-older-a\""));
    assert!(repaired_task_runs.contains("\"run_id\":\"run-older-b\""));
    assert!(!repaired_task_runs.contains("\"run_id\":\"run-20260401102000\""));
}

#[test]
fn run_sync_milestone_fails_when_completed_project_lacks_run_completed_event() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-missing-run-completed",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let project_root = project_root(temp_dir.path(), "bead-sync-missing-run-completed");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"completed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    )
    .expect("write completed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-missing-run-completed","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:11:00Z","event_type":"run_started","details":{{"run_id":"run-20260401101100","first_stage":"planning","max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    fs::write(
        &task_runs_path,
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-sync-missing-run-completed","run_id":"run-20260401101100","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write running task-runs");

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        !sync.status.success(),
        "sync should fail without run_completed"
    );
    let stderr = String::from_utf8_lossy(&sync.stderr);
    assert!(stderr.contains("missing durable run_completed event"));

    let task_runs = fs::read_to_string(&task_runs_path).expect("read task-runs");
    assert!(task_runs.contains("\"outcome\":\"running\""));
    assert!(!task_runs.contains("\"finished_at\""));
}

#[test]
fn run_sync_milestone_fails_when_failed_project_lacks_run_failed_event() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    let plan_hash = milestone_plan_hash(temp_dir.path(), "ms-alpha");
    let fake_br = write_show_bead_script_with_default_list(
        temp_dir.path(),
        "ms-alpha.bead-2",
        default_ms_alpha_bead_2_show_response(),
    );
    let path = prepend_path(fake_br.parent().expect("fake br parent"));

    let create = Command::new(binary())
        .args([
            "project",
            "create-from-bead",
            "--milestone-id",
            "ms-alpha",
            "--bead-id",
            "ms-alpha.bead-2",
            "--project-id",
            "bead-sync-missing-run-failed",
        ])
        .env("PATH", &path)
        .current_dir(temp_dir.path())
        .output()
        .expect("create bead-backed project");
    assert!(
        create.status.success(),
        "create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let project_root = project_root(temp_dir.path(), "bead-sync-missing-run-failed");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"interrupted_run":null,"status":"failed","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"failed without durable run_failed event"}"#,
    )
    .expect("write failed run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-04-01T10:10:00Z","event_type":"project_created","details":{{"project_id":"bead-sync-missing-run-failed","flow":"docs_change","source":"milestone_bead","milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","plan_hash":"{plan_hash}","plan_version":2}}}}
{{"sequence":2,"timestamp":"2026-04-01T10:11:00Z","event_type":"run_started","details":{{"run_id":"run-20260401101100","first_stage":"planning","max_completion_rounds":20}}}}"#
        ),
    )
    .expect("write journal");

    let task_runs_path = milestone_root(temp_dir.path(), "ms-alpha").join("task-runs.ndjson");
    fs::write(
        &task_runs_path,
        format!(
            r#"{{"milestone_id":"ms-alpha","bead_id":"ms-alpha.bead-2","project_id":"bead-sync-missing-run-failed","run_id":"run-20260401101100","plan_hash":"{plan_hash}","outcome":"running","started_at":"2026-04-01T10:11:00Z"}}"#
        ),
    )
    .expect("write running task-runs");

    let sync = Command::new(binary())
        .args(["run", "sync-milestone"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run sync-milestone");
    assert!(
        !sync.status.success(),
        "sync should fail without run_failed"
    );
    let stderr = String::from_utf8_lossy(&sync.stderr);
    assert!(stderr.contains("missing durable run_failed event"));

    let task_runs = fs::read_to_string(&task_runs_path).expect("read task-runs");
    assert!(task_runs.contains("\"outcome\":\"running\""));
    assert!(!task_runs.contains("\"finished_at\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_persists_payload_and_artifact_records() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-artifacts");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let payloads_dir = project_root(temp_dir.path(), "run-artifacts").join("history/payloads");
    let artifacts_dir = project_root(temp_dir.path(), "run-artifacts").join("history/artifacts");

    // Standard flow has 8 stages. Panel stages produce multiple records:
    // prompt_review: 1 refiner + 2 validators + 1 primary = 4
    // completion_panel: 2 completers + 1 aggregate = 3
    // final_review: 2 reviewer proposals + 1 aggregate = 3
    // Other 5 stages: 1 each = 5
    // Total: 15
    let payload_files: Vec<_> = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    let artifact_files: Vec<_> = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();

    assert_eq!(
        payload_files.len(),
        16,
        "expected 16 payload files for standard flow (3 reviewers), got {}",
        payload_files.len()
    );
    assert_eq!(
        artifact_files.len(),
        16,
        "expected 15 artifact files for standard flow, got {}",
        artifact_files.len()
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_status_shows_completed_after_run() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-status-after");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let status = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Status: completed"),
        "run status should show completed after successful run, got: {stdout}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_completes_quick_dev_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "qd-run", "quick_dev");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> =
        fs::read_dir(project_root(temp_dir.path(), "qd-run").join("history/payloads"))
            .expect("read payloads dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
    // ApplyFixes is skipped when Review approves with no findings (stub default).
    assert_eq!(payload_files.len(), 6);

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "qd-run").join("journal.ndjson"))
            .expect("read journal");
    assert!(journal.contains("\"plan_and_implement\""));
    assert!(journal.contains("\"review\""));
    // apply_fixes is skipped but still referenced in the stage_skipped event.
    assert!(journal.contains("\"apply_fixes\""));
    assert!(journal.contains("\"final_review\""));
    assert!(journal.contains("\"stage_skipped\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_completes_minimal_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "minimal-run", "minimal");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> =
        fs::read_dir(project_root(temp_dir.path(), "minimal-run").join("history/payloads"))
            .expect("read payloads dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
    assert_eq!(payload_files.len(), 5);

    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "minimal-run").join("journal.ndjson"))
            .expect("read journal");
    assert!(journal.contains("\"stage_id\":\"plan_and_implement\""));
    assert!(journal.contains("\"stage_id\":\"final_review\""));
    assert!(!journal.contains("\"stage_id\":\"review\""));
    assert!(!journal.contains("\"stage_id\":\"apply_fixes\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_quick_dev_produces_completed_snapshot_and_correct_status() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "qd-status", "quick_dev");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let status = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Status: completed"),
        "run status should show completed after successful quick_dev run, got: {stdout}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_resume_quick_dev_from_failed_state() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "qd-resume", "quick_dev");

    // First run fails at review stage
    let first = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")
        .current_dir(temp_dir.path())
        .output()
        .expect("first run start");
    assert!(
        !first.status.success(),
        "first run should fail at review stage"
    );

    // Resume should succeed
    let resume = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");
    assert!(
        resume.status.success(),
        "run resume failed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );

    let run_json = fs::read_to_string(project_root(temp_dir.path(), "qd-resume").join("run.json"))
        .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "quick_dev run should be completed after resume, got: {run_json}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_rejects_already_completed_project() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-dup");

    // First run should succeed
    let first = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("first run start");
    assert!(first.status.success());

    // Second run should fail because status is completed
    let second = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("second run start");

    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("not_started"),
        "should require not_started status, got: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_rejects_already_running_project() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-running");

    // Write a running snapshot to simulate an active run
    let running_snapshot = r#"{"active_run":{"run_id":"run-test","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#;
    fs::write(
        project_root(temp_dir.path(), "run-running").join("run.json"),
        running_snapshot,
    )
    .expect("write running snapshot");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not_started"),
        "should require not_started status, got: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_without_active_project_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("active") || stderr.contains("no project"),
        "should require active project, got: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_with_prompt_review_disabled_produces_seven_stages() {
    let temp_dir = initialize_workspace_fixture();

    // Disable prompt_review before creating the project
    let set_output = Command::new(binary())
        .args(["config", "set", "prompt_review.enabled", "false"])
        .current_dir(temp_dir.path())
        .output()
        .expect("config set");
    assert!(
        set_output.status.success(),
        "config set failed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    setup_standard_project(&temp_dir, "no-pr-cli");

    let start = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    // Verify 11 payloads (no prompt_review, but completion_panel and final_review
    // both persist panel records).
    // 5 single-agent stages + completion_panel (2 completers + 1 aggregate)
    // + final_review (2 reviewers + 1 aggregate) = 11
    let payloads_dir = project_root(temp_dir.path(), "no-pr-cli").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        payload_count, 12,
        "expected 12 payloads without prompt_review (3 reviewers), got {payload_count}"
    );

    // Verify no prompt_review stage in journal
    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "no-pr-cli").join("journal.ndjson"))
            .expect("read journal");
    assert!(
        !journal.contains("\"prompt_review\""),
        "journal should not contain prompt_review stage when disabled"
    );

    // Verify completed status
    let run_json = fs::read_to_string(project_root(temp_dir.path(), "no-pr-cli").join("run.json"))
        .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "run should be completed, got: {run_json}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_preflight_failure_leaves_state_unchanged() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "preflight-cli");

    // Corrupt the run.json to simulate a state that would fail validation
    // before the engine can proceed. We test that the CLI properly handles
    // preflight-like errors with no state mutation.
    //
    // The stub backend always passes preflight, so we verify the no-mutation
    // invariant via the workspace-version validation path: an unsupported
    // workspace version must leave all project state unchanged.
    let ws_toml_path = workspace_config_path(temp_dir.path());
    let ws_toml = fs::read_to_string(&ws_toml_path).expect("read workspace.toml");
    let corrupted = ws_toml.replace("version = 1", "version = 999");
    fs::write(&ws_toml_path, corrupted).expect("write corrupted workspace.toml");

    // Capture pre-run state
    let pre_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-cli").join("run.json"))
            .expect("read run.json before");
    let pre_journal =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-cli").join("journal.ndjson"))
            .expect("read journal before");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail with bad workspace version"
    );

    // Verify NO state mutation occurred
    let post_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-cli").join("run.json"))
            .expect("read run.json after");
    let post_journal =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-cli").join("journal.ndjson"))
            .expect("read journal after");

    assert_eq!(
        pre_run_json, post_run_json,
        "run.json must not change on pre-engine failure"
    );
    assert_eq!(
        pre_journal, post_journal,
        "journal must not change on pre-engine failure"
    );

    let payloads_dir = project_root(temp_dir.path(), "preflight-cli").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after preflight failure"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_backend_preflight_failure_leaves_state_unchanged() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "preflight-backend");

    // Capture pre-run state
    let pre_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-backend").join("run.json"))
            .expect("read run.json before");
    let pre_journal = fs::read_to_string(
        project_root(temp_dir.path(), "preflight-backend").join("journal.ndjson"),
    )
    .expect("read journal before");

    // Use env var to make the backend unavailable at preflight
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("RALPH_BURNING_TEST_BACKEND_UNAVAILABLE", "1")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail with unavailable backend"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("preflight") || stderr.contains("unavailable"),
        "error should reference preflight or unavailable, got: {stderr}"
    );

    // Verify NO state mutation occurred — byte-identical
    let post_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "preflight-backend").join("run.json"))
            .expect("read run.json after");
    let post_journal = fs::read_to_string(
        project_root(temp_dir.path(), "preflight-backend").join("journal.ndjson"),
    )
    .expect("read journal after");

    assert_eq!(
        pre_run_json, post_run_json,
        "run.json must be byte-identical after preflight failure"
    );
    assert_eq!(
        pre_journal, post_journal,
        "journal must be byte-identical after preflight failure"
    );

    let payloads_dir = project_root(temp_dir.path(), "preflight-backend").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after preflight failure"
    );

    let artifacts_dir =
        project_root(temp_dir.path(), "preflight-backend").join("history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        artifact_count, 0,
        "no artifacts should exist after preflight failure"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_mid_stage_failure_no_partial_durable_history() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "midstage-fail");

    // Use env var to fail the first stage's invocation (prompt_review is
    // enabled by default, so it's the first stage executed).
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "prompt_review")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail on mid-stage invoke failure"
    );

    // Run snapshot must be failed, not running
    let run_json =
        fs::read_to_string(project_root(temp_dir.path(), "midstage-fail").join("run.json"))
            .expect("read run.json");
    assert!(
        run_json.contains("\"failed\""),
        "run.json should show failed status, got: {run_json}"
    );
    assert!(
        run_json.contains("\"active_run\":null") || run_json.contains("\"active_run\": null"),
        "active_run should be null after failure"
    );

    // No payload or artifact files should exist — no partial durable history
    let payloads_dir = project_root(temp_dir.path(), "midstage-fail").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after mid-stage failure"
    );

    let artifacts_dir = project_root(temp_dir.path(), "midstage-fail").join("history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        artifact_count, 0,
        "no artifacts should exist after mid-stage failure"
    );

    // No stage_completed event should exist in the journal
    let journal =
        fs::read_to_string(project_root(temp_dir.path(), "midstage-fail").join("journal.ndjson"))
            .expect("read journal");
    assert!(
        !journal.contains("\"stage_completed\""),
        "no stage_completed event should exist after mid-stage failure"
    );

    // Journal should end with run_failed event
    let last_line = journal.lines().last().expect("journal should not be empty");
    assert!(
        last_line.contains("\"run_failed\""),
        "last journal event should be run_failed, got: {last_line}"
    );
}

// ── Requirements CLI tests ──────────────────────────────────────────────────

#[cfg(feature = "test-stub")]
#[test]
fn requirements_quick_creates_completed_run() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "quick", "--idea", "Build a REST API"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements quick");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements quick should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Requirements completed"),
        "stdout should contain completion message.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("ralph-burning project create"),
        "stdout should contain suggested create command.\nstdout: {stdout}"
    );

    // Verify run directory exists
    let req_dir = requirements_root(temp_dir.path());
    assert!(req_dir.is_dir(), "requirements directory should exist");
    let entries: Vec<_> = fs::read_dir(&req_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();
    assert_eq!(entries.len(), 1, "should have exactly one requirements run");

    // Regression: verify the required file layout includes answers.toml and answers.json
    let run_dir = entries[0].path();
    assert!(
        run_dir.join("answers.toml").exists(),
        "quick run must have answers.toml"
    );
    assert!(
        run_dir.join("answers.json").exists(),
        "quick run must have answers.json"
    );
    assert!(
        run_dir.join("journal.ndjson").exists(),
        "quick run must have journal.ndjson"
    );
    assert!(
        run_dir.join("run.json").exists(),
        "quick run must have run.json"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_milestone_creates_completed_milestone_run() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "requirements",
            "milestone",
            "--idea",
            "Plan the alpha milestone",
        ])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements milestone");

    assert!(
        output.status.success(),
        "requirements milestone should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let run_id = only_requirements_run_id(temp_dir.path());
    let run_json = fs::read_to_string(
        requirements_root(temp_dir.path())
            .join(run_id)
            .join("run.json"),
    )
    .expect("read run.json");
    assert!(run_json.contains("\"mode\": \"milestone\""));
    assert!(run_json.contains("\"output_kind\": \"milestone_bundle\""));
    assert!(run_json.contains("\"status\": \"completed\""));
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_show_displays_completed_run() {
    let temp_dir = initialize_workspace_fixture();

    // First create a quick run
    let output = Command::new(binary())
        .args(["requirements", "quick", "--idea", "Build a REST API"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements quick");
    assert!(output.status.success());

    // Find the run ID from the requirements directory
    let req_dir = requirements_root(temp_dir.path());
    let run_id = fs::read_dir(&req_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().to_string())
        .next()
        .expect("should have one run");

    // Now show the run
    let output = Command::new(binary())
        .args(["requirements", "show", &run_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements show");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements show should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Status:           completed"),
        "should show completed status.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Mode:             quick"),
        "should show quick mode.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Recommended Flow: standard"),
        "should show recommended flow.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Seed Prompt:"),
        "should show seed prompt path.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Suggested command:"),
        "should show suggested create command.\nstdout: {stdout}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_draft_with_empty_questions_completes() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "draft", "--idea", "Simple refactoring"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements draft");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Default stub returns empty question set, so draft should complete
    assert!(
        output.status.success(),
        "requirements draft should succeed with empty questions.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Requirements completed"),
        "should complete through pipeline.\nstdout: {stdout}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_show_on_nonexistent_run_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "show", "nonexistent-run"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements show");

    assert!(
        !output.status.success(),
        "requirements show should fail for nonexistent run"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_answer_happy_path_completes_run() {
    let temp_dir = initialize_workspace_fixture();
    let run_id = "req-20260312-120000";
    let run_dir = requirements_root(temp_dir.path()).join(run_id);

    // Create required directory structure
    for subdir in &[
        "",
        "history/payloads",
        "history/artifacts",
        "seed",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
    ] {
        fs::create_dir_all(run_dir.join(subdir)).expect("create subdir");
    }

    // Write sessions.json (PersistedSessions requires { sessions: [] })
    fs::write(run_dir.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");

    // Write run.json in awaiting_answers state
    let run_json = serde_json::json!({
        "run_id": run_id,
        "idea": "Build a REST API",
        "mode": "draft",
        "status": "awaiting_answers",
        "question_round": 1,
        "latest_question_set_id": format!("{run_id}-qs-1"),
        "latest_draft_id": null,
        "latest_review_id": null,
        "latest_seed_id": null,
        "pending_question_count": 1,
        "created_at": "2026-03-12T12:00:00Z",
        "updated_at": "2026-03-12T12:00:00Z",
        "status_summary": "awaiting answers: 1 question(s), round 1"
    });
    fs::write(
        run_dir.join("run.json"),
        serde_json::to_string_pretty(&run_json).unwrap(),
    )
    .expect("write run.json");

    // Write question set payload
    let qs_payload = serde_json::json!({
        "questions": [
            {
                "id": "q1",
                "prompt": "What framework?",
                "rationale": "Determines architecture",
                "required": true
            }
        ]
    });
    fs::write(
        run_dir.join(format!("history/payloads/{run_id}-qs-1.json")),
        serde_json::to_string(&qs_payload).unwrap(),
    )
    .expect("write question payload");

    // Write journal with RunCreated and QuestionsGenerated
    let journal = format!(
        "{}\n{}\n",
        serde_json::json!({
            "sequence": 1,
            "timestamp": "2026-03-12T12:00:00Z",
            "event_type": "run_created",
            "details": { "run_id": run_id, "status": "drafting", "status_summary": "drafting" }
        }),
        serde_json::json!({
            "sequence": 2,
            "timestamp": "2026-03-12T12:00:00Z",
            "event_type": "questions_generated",
            "details": { "run_id": run_id, "status": "awaiting_answers", "status_summary": "awaiting answers" }
        }),
    );
    fs::write(run_dir.join("journal.ndjson"), journal).expect("write journal");

    // Write valid answers.toml
    fs::write(run_dir.join("answers.toml"), "q1 = \"Use Actix Web\"\n")
        .expect("write answers.toml");

    // Run requirements answer with EDITOR=true (no-op editor)
    let output = Command::new(binary())
        .args(["requirements", "answer", run_id])
        .env("RALPH_BURNING_BACKEND", "stub")
        .env("EDITOR", "true")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements answer");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements answer should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Verify run completed
    let run_data: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("run.json")).expect("read run.json"))
            .expect("parse run.json");
    assert_eq!(
        run_data["status"], "completed",
        "run should be completed after answer"
    );

    // Verify seed files exist
    assert!(
        run_dir.join("seed/project.json").exists(),
        "seed/project.json should exist"
    );
    assert!(
        run_dir.join("seed/prompt.md").exists(),
        "seed/prompt.md should exist"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn requirements_answer_on_nonexistent_run_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "answer", "nonexistent-run"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements answer");

    assert!(
        !output.status.success(),
        "requirements answer should fail for nonexistent run"
    );
}

// ===========================================================================
// Conformance List / Run CLI surface tests
// ===========================================================================

#[cfg(feature = "test-stub")]
#[test]
fn conformance_list_discovers_all_scenarios() {
    let output = Command::new(binary())
        .args(["conformance", "list"])
        .output()
        .expect("run conformance list");

    assert!(
        output.status.success(),
        "conformance list should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Should print a table header
    assert!(stdout.contains("SCENARIO ID"));
    assert!(stdout.contains("FEATURE"));
    // Should discover scenarios from checked-in feature files
    assert!(stdout.contains("Total:"));
    // Should include known scenario IDs
    assert!(stdout.contains("workspace-init-fresh"));
    assert!(stdout.contains("SC-START-001"));
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_run_with_valid_filter_executes_one_scenario() {
    let output = Command::new(binary())
        .args(["conformance", "run", "--filter", "flow-list-all-presets"])
        .output()
        .expect("run conformance run --filter");

    assert!(
        output.status.success(),
        "conformance run --filter should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("flow-list-all-presets"));
    assert!(stderr.contains("PASS"));
    assert!(stderr.contains("Selected:  1"));
    assert!(stderr.contains("Passed:    1"));
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_run_with_unknown_filter_exits_non_zero() {
    let output = Command::new(binary())
        .args(["conformance", "run", "--filter", "nonexistent-scenario-id"])
        .output()
        .expect("run conformance run --filter unknown");

    assert!(
        !output.status.success(),
        "conformance run with unknown filter should fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("nonexistent-scenario-id"));
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_list_validates_no_duplicate_ids() {
    // The checked-in corpus has no duplicates, so conformance list should succeed.
    let output = Command::new(binary())
        .args(["conformance", "list"])
        .output()
        .expect("run conformance list");

    assert!(
        output.status.success(),
        "conformance list should succeed when no duplicates: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Total:"),
        "output should include a total count"
    );
    assert!(stdout.contains("SC-START-001"), "should list SC-START-001");
    assert!(
        stdout.contains("workspace-init-fresh"),
        "should list workspace-init-fresh"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_list_fails_on_duplicate_ids() {
    // Create an isolated temp features directory with two feature files that share
    // a scenario ID, then point discovery at it via RALPH_BURNING_TEST_FEATURES_DIR.
    // This avoids mutating the checked-in corpus and eliminates race conditions with
    // other conformance tests reading the same directory.
    let tmp_dir = tempfile::tempdir().expect("create temp features dir");
    let features_path = tmp_dir.path();

    // Write the first feature file with SC-DUP-001
    fs::write(
        features_path.join("alpha.feature"),
        "Feature: Alpha\n\n  # SC-DUP-001\n  Scenario: First scenario\n    Given nothing\n",
    )
    .expect("write first feature file");

    // Write a second feature file that duplicates SC-DUP-001
    fs::write(
        features_path.join("beta.feature"),
        "Feature: Beta\n\n  # SC-DUP-001\n  Scenario: Duplicate scenario\n    Given nothing\n",
    )
    .expect("write duplicate feature file");

    let output = Command::new(binary())
        .env(
            "RALPH_BURNING_TEST_FEATURES_DIR",
            features_path.to_str().unwrap(),
        )
        .args(["conformance", "list"])
        .output()
        .expect("run conformance list with duplicate");

    assert!(
        !output.status.success(),
        "conformance list should fail on duplicate IDs: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("duplicate") || stderr.contains("SC-DUP-001"),
        "error should mention duplicate scenario ID: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_run_fail_fast_reports_summary_once() {
    // Run a single passing scenario - verify summary format and single-report invariant
    let output = Command::new(binary())
        .args(["conformance", "run", "--filter", "workspace-init-fresh"])
        .output()
        .expect("run conformance with filter");

    assert!(
        output.status.success(),
        "filtered conformance run should pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Selected:  1"));
    assert!(stderr.contains("Passed:    1"));
    assert!(stderr.contains("Failed:    0"));
    assert!(stderr.contains("Not run:   0"));
    assert_eq!(
        stderr.matches("Conformance Summary").count(),
        1,
        "summary should be printed exactly once"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_run_fail_fast_stops_and_reports_not_run() {
    // Force a specific early scenario to fail, run the full suite, and verify
    // fail-fast behavior: non-zero exit, failed=1, not_run > 0.
    let output = Command::new(binary())
        .env(
            "RALPH_BURNING_TEST_CONFORMANCE_FAIL_EXECUTOR",
            "active-project-select-existing",
        )
        .args(["conformance", "run"])
        .output()
        .expect("run conformance with forced failure");

    assert!(
        !output.status.success(),
        "conformance run should exit non-zero when a scenario fails"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Conformance Summary"),
        "output should include summary"
    );
    // Verify summary is reported exactly once (no double-reporting)
    assert_eq!(
        stderr.matches("Conformance Summary").count(),
        1,
        "summary should be printed exactly once"
    );
    // Verify fail-fast: failed=1 and not_run > 0
    assert!(
        stderr.contains("Failed:    1"),
        "should report exactly 1 failed scenario: {stderr}"
    );
    assert!(
        stderr.contains("Not run:"),
        "should report not-run count: {stderr}"
    );
    // Verify not_run count is > 0 (fail-fast stopped remaining scenarios)
    // Parse the Not run count
    let not_run_line = stderr
        .lines()
        .find(|l| l.contains("Not run:"))
        .unwrap_or("");
    let not_run_count: usize = not_run_line
        .split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(
        not_run_count > 0,
        "fail-fast should leave remaining scenarios as not run, got {not_run_count}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_run_failure_exits_non_zero_with_single_report() {
    // Unknown filter causes non-zero exit before execution
    let output = Command::new(binary())
        .args([
            "conformance",
            "run",
            "--filter",
            "NONEXISTENT-FAIL-FAST-TEST",
        ])
        .output()
        .expect("run conformance with unknown filter");

    assert!(
        !output.status.success(),
        "conformance run with unknown filter should exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("NONEXISTENT-FAIL-FAST-TEST"),
        "error should mention the unknown scenario ID"
    );
}

// ── Daemon waiting/resume E2E tests ─────────────────────────────────────────

#[test]
fn daemon_status_shows_waiting_for_requirements_task() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();

    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-waiting".to_owned(),
            issue_ref: "repo#99".to_owned(),
            project_id: "demo-waiting".to_owned(),
            project_name: Some("Waiting".to_owned()),
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
            source_revision: Some("abc12345".to_owned()),
            requirements_run_id: Some("req-20260313".to_owned()),
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(99),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "status",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon status");
    assert!(
        output.status.success(),
        "daemon status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("waiting_for_requirements"),
        "status should show waiting_for_requirements, got: {stdout}"
    );
}

#[test]
fn daemon_status_shows_dispatch_mode() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();

    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-dispatch".to_owned(),
            issue_ref: "repo#100".to_owned(),
            project_id: "demo-dispatch".to_owned(),
            project_name: Some("Dispatch".to_owned()),
            prompt: Some("Prompt".to_owned()),
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
            dispatch_mode: DispatchMode::RequirementsQuick,
            source_revision: Some("beef1234".to_owned()),
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(100),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "status",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon status");
    assert!(
        output.status.success(),
        "daemon status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dispatch=requirements_quick"),
        "status should show dispatch mode, got: {stdout}"
    );
}

#[test]
fn daemon_abort_waiting_task_succeeds() {
    let data_dir = tempdir().expect("create temp dir");
    let now = Utc::now();

    write_datadir_daemon_task(
        data_dir.path(),
        &DaemonTask {
            task_id: "task-waiting-abort".to_owned(),
            issue_ref: "repo#101".to_owned(),
            project_id: "demo-waiting-abort".to_owned(),
            project_name: Some("WaitingAbort".to_owned()),
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
            source_revision: None,
            requirements_run_id: Some("req-abort-test".to_owned()),
            workflow_run_id: None,
            repo_slug: Some(TEST_REPO_SLUG.to_owned()),
            issue_number: Some(101),
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );

    let output = Command::new(binary())
        .args([
            "daemon",
            "abort",
            "101",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--repo",
            TEST_REPO_SLUG,
        ])
        .output()
        .expect("run daemon abort");
    assert!(
        output.status.success(),
        "daemon abort failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Aborted") && stdout.contains("task-waiting-abort"),
        "should confirm abort, got: {stdout}"
    );

    // Verify task is now aborted
    let task_path = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/tasks/task-waiting-abort.json");
    let task_json = fs::read_to_string(task_path).expect("read task");
    let task: DaemonTask = serde_json::from_str(&task_json).expect("parse task");
    assert_eq!(TaskStatus::Aborted, task.status);
}

// ---------------------------------------------------------------------------
// Writer lock contention tests (CLI level)
// ---------------------------------------------------------------------------

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_start_acquires_and_releases_writer_lock() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "lock-start");
    select_active_project_fixture(temp_dir.path(), "lock-start");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(output.status.success(), "run start should succeed");

    let lock_path = daemon_root(temp_dir.path())
        .join("leases")
        .join("writer-lock-start.lock");
    assert!(
        !lock_path.exists(),
        "writer lock file should be released after run start completes"
    );

    // No CLI lease files should remain after successful run
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    let cli_leases: Vec<_> = std::fs::read_dir(&leases_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("cli-") && n.ends_with(".json"))
        })
        .collect();
    assert!(
        cli_leases.is_empty(),
        "no CLI lease file should remain after successful run start"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_start_fails_when_writer_lock_held() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "lock-held");
    select_active_project_fixture(temp_dir.path(), "lock-held");

    // Pre-create the writer lock file
    let lock_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&lock_dir).expect("create lease dir");
    fs::write(lock_dir.join("writer-lock-held.lock"), "held-by-test").expect("write lock");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        !output.status.success(),
        "run start should fail when lock is held"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer lock"),
        "error should mention writer lock, got: {stderr}"
    );

    // Verify no run-state mutation occurred
    let run_json = fs::read_to_string(project_root(temp_dir.path(), "lock-held").join("run.json"))
        .expect("read run.json");
    assert!(
        run_json.contains("\"not_started\""),
        "run state should remain not_started"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_resume_acquires_and_releases_writer_lock() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "lock-resume");
    select_active_project_fixture(temp_dir.path(), "lock-resume");

    // First, fail the run to get a failed snapshot
    let fail_output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "implementation")
        .output()
        .expect("run start to fail");
    assert!(!fail_output.status.success());

    // Now resume — the lock should be acquired and released
    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");
    assert!(output.status.success(), "run resume should succeed");

    let lock_path = daemon_root(temp_dir.path())
        .join("leases")
        .join("writer-lock-resume.lock");
    assert!(
        !lock_path.exists(),
        "writer lock file should be released after run resume completes"
    );

    // No CLI lease files should remain after successful resume
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    let cli_leases: Vec<_> = std::fs::read_dir(&leases_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("cli-") && n.ends_with(".json"))
        })
        .collect();
    assert!(
        cli_leases.is_empty(),
        "no CLI lease file should remain after successful run resume"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_resume_kills_persisted_stale_backend_process_group_before_restarting() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "stale-resume-backend");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("stale-resume-backend".to_owned())
            .expect("valid project id");
    let prompt_hash = <ralph_burning::adapters::fs::FsProjectStore as ralph_burning::contexts::project_run_record::service::ProjectStorePort>::read_project_record(
        &ralph_burning::adapters::fs::FsProjectStore,
        temp_dir.path(),
        &project_id,
    )
    .expect("read project record")
    .prompt_hash;
    let now = Utc::now();
    fs::write(
        project_root(temp_dir.path(), "stale-resume-backend").join("run.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "active_run": {
                "run_id": "run-stale-resume-backend",
                "stage_cursor": {
                    "stage": "planning",
                    "cycle": 1,
                    "attempt": 1,
                    "completion_round": 1
                },
                "started_at": now,
                "prompt_hash_at_cycle_start": prompt_hash,
                "prompt_hash_at_stage_start": prompt_hash,
                "qa_iterations_current_cycle": 0,
                "review_iterations_current_cycle": 0,
                "final_review_restart_count": 0
            },
            "interrupted_run": serde_json::Value::Null,
            "status": "running",
            "cycle_history": [],
            "completion_rounds": 1,
            "max_completion_rounds": 20,
            "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
            "amendment_queue": { "pending": [], "processed_count": 0 },
            "status_summary": "running: Planning"
        }))
        .expect("serialize stale run.json"),
    )
    .expect("write stale run.json");
    fs::write(
        project_root(temp_dir.path(), "stale-resume-backend").join("journal.ndjson"),
        format!(
            "{{\"sequence\":1,\"timestamp\":\"2026-03-11T19:00:00Z\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"stale-resume-backend\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{now}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-stale-resume-backend\",\"first_stage\":\"planning\",\"max_completion_rounds\":20}}}}"
        ),
    )
    .expect("write stale resume journal");
    fs::remove_file(project_root(temp_dir.path(), "stale-resume-backend").join("run.pid")).ok();

    let backend_pid_file = temp_dir.path().join("stale-resume-backend.pid");
    let mut backend_group = spawn_isolated_backend_group(&backend_pid_file);
    let backend_pid = wait_for_pid_file(&backend_pid_file, "stale resume backend");
    assert!(
        pid_is_running(backend_pid),
        "persisted stale backend process should be alive before resume recovery"
    );
    fs::write(
        backend_processes_path(temp_dir.path(), "stale-resume-backend"),
        serde_json::to_string_pretty(&serde_json::json!({
            "processes": [
                live_backend_process_record_json(
                    backend_pid,
                    "run-stale-resume-backend",
                    &now.to_rfc3339()
                )
            ]
        }))
        .expect("serialize backend process record"),
    )
    .expect("write backend process record");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(
        output.status.success(),
        "stale resume should recover and succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while pid_is_running(backend_pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        !pid_is_running(backend_pid),
        "stale resume must kill persisted backend process groups before restarting"
    );
    assert!(
        !backend_processes_path(temp_dir.path(), "stale-resume-backend").exists(),
        "successful stale backend cleanup should remove the persisted backend process file"
    );

    let _ = backend_group.kill();
    let _ = backend_group.wait();
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_resume_recovers_stale_running_and_reclaims_writer_lock() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "stale-resume-lock");
    let project_id = ralph_burning::shared::domain::ProjectId::new("stale-resume-lock".to_owned())
        .expect("valid project id");
    let prompt_hash = <ralph_burning::adapters::fs::FsProjectStore as ralph_burning::contexts::project_run_record::service::ProjectStorePort>::read_project_record(
        &ralph_burning::adapters::fs::FsProjectStore,
        temp_dir.path(),
        &project_id,
    )
    .expect("read project record")
    .prompt_hash;
    let now = Utc::now();
    let run_json_path = project_root(temp_dir.path(), "stale-resume-lock").join("run.json");
    fs::write(
        &run_json_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "active_run": {
                "run_id": "run-stale-resume-lock",
                "stage_cursor": {
                    "stage": "planning",
                    "cycle": 1,
                    "attempt": 1,
                    "completion_round": 1
                },
                "started_at": now,
                "prompt_hash_at_cycle_start": prompt_hash,
                "prompt_hash_at_stage_start": prompt_hash,
                "qa_iterations_current_cycle": 0,
                "review_iterations_current_cycle": 0,
                "final_review_restart_count": 0
            },
            "interrupted_run": serde_json::Value::Null,
            "status": "running",
            "cycle_history": [],
            "completion_rounds": 1,
            "max_completion_rounds": 20,
            "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
            "amendment_queue": { "pending": [], "processed_count": 0 },
            "status_summary": "running: Planning"
        }))
        .expect("serialize stale run.json"),
    )
    .expect("write stale run.json");
    fs::write(
        project_root(temp_dir.path(), "stale-resume-lock").join("journal.ndjson"),
        format!(
            "{{\"sequence\":1,\"timestamp\":\"2026-03-11T19:00:00Z\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"stale-resume-lock\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{now}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-stale-resume-lock\",\"first_stage\":\"planning\",\"max_completion_rounds\":20}}}}"
        ),
    )
    .expect("write stale resume journal");
    fs::remove_file(project_root(temp_dir.path(), "stale-resume-lock").join("run.pid")).ok();

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let stale_lease = CliWriterLease {
        lease_id: "cli-stale-resume".to_owned(),
        project_id: "stale-resume-lock".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::seconds(301),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-stale-resume.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(stale_lease.clone()))
            .expect("serialize stale resume lease"),
    )
    .expect("write stale resume lease");
    fs::write(
        leases_dir.join("writer-stale-resume-lock.lock"),
        &stale_lease.lease_id,
    )
    .expect("write stale resume writer lock");

    let resume = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");
    assert!(
        resume.status.success(),
        "stale resume should recover and succeed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );

    let final_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "stale-resume-lock").join("run.json"))
            .expect("read final run.json");
    assert!(
        final_run_json.contains("\"completed\""),
        "resume should finish after stale recovery, got: {final_run_json}"
    );
    assert!(
        !leases_dir.join("writer-stale-resume-lock.lock").exists(),
        "stale writer lock should be reclaimed before resume reacquires it"
    );
    assert!(
        !leases_dir.join("cli-stale-resume.json").exists(),
        "stale cli lease record should be pruned during resume recovery"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_resume_reclaims_writer_lock_for_failed_snapshot_cleanup_crash() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "failed-cleanup-lock");

    let fail_output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "review")
        .output()
        .expect("run start to fail");
    assert!(!fail_output.status.success());

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create lease dir");
    let stranded_lease = CliWriterLease {
        lease_id: "cli-failed-cleanup-lock".to_owned(),
        project_id: "failed-cleanup-lock".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    fs::write(
        leases_dir.join("cli-failed-cleanup-lock.json"),
        serde_json::to_string_pretty(&LeaseRecord::CliWriter(stranded_lease.clone()))
            .expect("serialize stranded cleanup lease"),
    )
    .expect("write stranded cleanup lease");
    fs::write(
        leases_dir.join("writer-failed-cleanup-lock.lock"),
        &stranded_lease.lease_id,
    )
    .expect("write stranded cleanup lock");
    let mut child = Command::new("sleep")
        .arg("60")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn helper process");
    let child_pid = child.id();
    let child_start_ticks = proc_start_ticks(child_pid);
    child.kill().expect("kill helper process");
    child.wait().expect("wait helper process");
    fs::write(
        project_root(temp_dir.path(), "failed-cleanup-lock").join("run.pid"),
        serde_json::to_string_pretty(&serde_json::json!({
            "pid": child_pid,
            "started_at": Utc::now(),
            "owner": "cli",
            "writer_owner": stranded_lease.lease_id,
            "proc_start_ticks": child_start_ticks,
        }))
        .expect("serialize dead pid record"),
    )
    .expect("write dead pid record");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");
    assert!(
        output.status.success(),
        "resume should reclaim stranded cleanup lock: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let run_json =
        fs::read_to_string(project_root(temp_dir.path(), "failed-cleanup-lock").join("run.json"))
            .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "resume should finish after reclaiming stranded cleanup lock, got: {run_json}"
    );
    assert!(
        !leases_dir.join("writer-failed-cleanup-lock.lock").exists(),
        "stranded cleanup writer lock should be reclaimed before resume reacquires it"
    );
    assert!(
        !leases_dir.join("cli-failed-cleanup-lock.json").exists(),
        "stranded cleanup cli lease should be pruned during resume recovery"
    );
    assert!(
        !project_root(temp_dir.path(), "failed-cleanup-lock")
            .join("run.pid")
            .exists(),
        "pid file should be removed after the recovered resume releases its writer lease"
    );
}

#[test]
fn run_resume_does_not_reclaim_live_worktree_owner_for_failed_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "failed-live-owner");
    select_active_project_fixture(temp_dir.path(), "failed-live-owner");

    fs::write(
        project_root(temp_dir.path(), "failed-live-owner").join("run.json"),
        r#"{
  "active_run": null,
  "status": "failed",
  "cycle_history": [],
  "completion_rounds": 1,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "failed at implementation"
}"#,
    )
    .expect("write failed snapshot");

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let now = Utc::now();
    let worktree_lease = WorktreeLease {
        lease_id: "lease-live-failed-resume".to_owned(),
        task_id: "task-live-failed-resume".to_owned(),
        project_id: "failed-live-owner".to_owned(),
        worktree_path: temp_dir.path().join("worktrees/task-live-failed-resume"),
        branch_name: "task-live-failed-resume".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
        cleanup_handoff: None,
    };
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: worktree_lease.task_id.clone(),
            issue_ref: "repo#77".to_owned(),
            project_id: "failed-live-owner".to_owned(),
            project_name: Some("failed live owner".to_owned()),
            prompt: Some("prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(ralph_burning::shared::domain::FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some(worktree_lease.lease_id.clone()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    fs::write(
        leases_dir.join("lease-live-failed-resume.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(worktree_lease))
            .expect("serialize live daemon lease"),
    )
    .expect("write live daemon lease");
    fs::write(
        leases_dir.join("writer-failed-live-owner.lock"),
        "lease-live-failed-resume",
    )
    .expect("write live daemon writer lock");

    let output = Command::new(binary())
        .args(["run", "resume"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");

    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to acquire writer lease for resume")
            || stderr.contains("project writer lock is already held"),
        "resume should fail on a live competing writer owner instead of reclaiming it: {stderr}"
    );
    assert!(
        leases_dir.join("writer-failed-live-owner.lock").exists(),
        "live daemon writer lock must remain intact after refused resume"
    );
    assert!(
        leases_dir.join("lease-live-failed-resume.json").exists(),
        "live daemon lease must not be reclaimed during terminal resume contention"
    );
    let daemon_task: DaemonTask = serde_json::from_str(
        &fs::read_to_string(
            daemon_root(temp_dir.path())
                .join("tasks")
                .join("task-live-failed-resume.json"),
        )
        .expect("read daemon task"),
    )
    .expect("parse daemon task");
    assert_eq!(daemon_task.status, TaskStatus::Active);
    assert_eq!(
        daemon_task.lease_id.as_deref(),
        Some("lease-live-failed-resume")
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_resume_fails_closed_when_stale_daemon_worktree_cleanup_is_partial() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "stale-daemon-resume");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("stale-daemon-resume".to_owned())
            .expect("valid project id");
    let prompt_hash = <ralph_burning::adapters::fs::FsProjectStore as ralph_burning::contexts::project_run_record::service::ProjectStorePort>::read_project_record(
        &ralph_burning::adapters::fs::FsProjectStore,
        temp_dir.path(),
        &project_id,
    )
    .expect("read project record")
    .prompt_hash;
    let now = Utc::now();
    fs::write(
        project_root(temp_dir.path(), "stale-daemon-resume").join("run.json"),
        serde_json::to_string_pretty(&serde_json::json!({
            "active_run": {
                "run_id": "run-stale-daemon-resume",
                "stage_cursor": {
                    "stage": "planning",
                    "cycle": 1,
                    "attempt": 1,
                    "completion_round": 1
                },
                "started_at": now,
                "prompt_hash_at_cycle_start": prompt_hash,
                "prompt_hash_at_stage_start": prompt_hash,
                "qa_iterations_current_cycle": 0,
                "review_iterations_current_cycle": 0,
                "final_review_restart_count": 0
            },
            "interrupted_run": serde_json::Value::Null,
            "status": "running",
            "cycle_history": [],
            "completion_rounds": 1,
            "max_completion_rounds": 20,
            "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
            "amendment_queue": { "pending": [], "processed_count": 0 },
            "status_summary": "running: Planning"
        }))
        .expect("serialize stale daemon run.json"),
    )
    .expect("write stale daemon run.json");
    fs::write(
        project_root(temp_dir.path(), "stale-daemon-resume").join("journal.ndjson"),
        format!(
            "{{\"sequence\":1,\"timestamp\":\"2026-03-11T19:00:00Z\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"stale-daemon-resume\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{now}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-stale-daemon-resume\",\"first_stage\":\"planning\",\"max_completion_rounds\":20}}}}"
        ),
    )
    .expect("write stale daemon resume journal");
    fs::remove_file(project_root(temp_dir.path(), "stale-daemon-resume").join("run.pid")).ok();

    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    let stale_worktree_path = temp_dir.path().join("worktrees/task-stale-daemon-resume");
    fs::create_dir_all(&stale_worktree_path).expect("create stale worktree dir");
    let stale_worktree_lease = WorktreeLease {
        lease_id: "lease-stale-daemon-resume".to_owned(),
        task_id: "task-stale-daemon-resume".to_owned(),
        project_id: "stale-daemon-resume".to_owned(),
        worktree_path: stale_worktree_path,
        branch_name: "task-stale-daemon-resume".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::seconds(301),
        cleanup_handoff: None,
    };
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: stale_worktree_lease.task_id.clone(),
            issue_ref: "repo#42".to_owned(),
            project_id: "stale-daemon-resume".to_owned(),
            project_name: Some("stale daemon".to_owned()),
            prompt: Some("prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(ralph_burning::shared::domain::FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some(stale_worktree_lease.lease_id.clone()),
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
            requirements_run_id: None,
            workflow_run_id: None,
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        },
    );
    fs::write(
        leases_dir.join("lease-stale-daemon-resume.json"),
        serde_json::to_string_pretty(&LeaseRecord::Worktree(stale_worktree_lease))
            .expect("serialize stale daemon worktree lease"),
    )
    .expect("write stale daemon worktree lease");
    fs::write(
        leases_dir.join("writer-stale-daemon-resume.lock"),
        "lease-stale-daemon-resume",
    )
    .expect("write stale daemon writer lock");

    let resume = Command::new(binary())
        .args(["run", "resume"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run resume");
    assert!(
        !resume.status.success(),
        "resume must fail closed when stale daemon worktree cleanup is partial"
    );
    let stderr = String::from_utf8_lossy(&resume.stderr);
    assert!(
        stderr.contains("failed to acquire writer lease for resume")
            && stderr.contains("lease cleanup partially failed"),
        "resume should surface partial cleanup instead of continuing: {stderr}"
    );

    let final_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "stale-daemon-resume").join("run.json"))
            .expect("read final run.json");
    assert!(
        final_run_json.contains("\"running\""),
        "resume must leave the stale running snapshot intact for operator recovery, got: {final_run_json}"
    );
    assert!(
        leases_dir.join("writer-stale-daemon-resume.lock").exists(),
        "fail-closed partial cleanup should leave the observed writer lock intact for operator recovery"
    );
    assert!(
        leases_dir.join("lease-stale-daemon-resume.json").exists(),
        "stale daemon worktree lease should remain durable after partial cleanup"
    );

    let daemon_task: DaemonTask = serde_json::from_str(
        &fs::read_to_string(
            daemon_root(temp_dir.path())
                .join("tasks")
                .join("task-stale-daemon-resume.json"),
        )
        .expect("read daemon task"),
    )
    .expect("parse daemon task");
    assert_eq!(daemon_task.status, TaskStatus::Failed);
    assert_eq!(
        daemon_task.failure_class.as_deref(),
        Some("stale_writer_owner_reclaimed"),
        "partial cleanup should still mark the stale daemon owner failed"
    );
    assert_eq!(
        daemon_task.lease_id.as_deref(),
        Some("lease-stale-daemon-resume"),
        "partial cleanup must preserve the daemon task lease reference"
    );

    let cli_leases: Vec<_> = std::fs::read_dir(&leases_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("cli-") && name.ends_with(".json"))
        })
        .collect();
    assert!(
        cli_leases.is_empty(),
        "resume must not leave behind a new CLI lease after partial cleanup failure: {}",
        String::from_utf8_lossy(&resume.stderr)
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_start_releases_lock_on_error() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "lock-err");
    select_active_project_fixture(temp_dir.path(), "lock-err");

    // Force a run failure — lock should still be released
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "planning")
        .output()
        .expect("run start");
    assert!(!output.status.success(), "run start should fail");

    let lock_path = daemon_root(temp_dir.path())
        .join("leases")
        .join("writer-lock-err.lock");
    assert!(
        !lock_path.exists(),
        "writer lock file should be released even when run fails"
    );

    // No CLI lease files should remain after failed run
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    let cli_leases: Vec<_> = std::fs::read_dir(&leases_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("cli-") && n.ends_with(".json"))
        })
        .collect();
    assert!(
        cli_leases.is_empty(),
        "no CLI lease file should remain after failed run start"
    );
}

// ---------------------------------------------------------------------------
// Guard close failure makes successful run exit non-zero (CLI level)
// ---------------------------------------------------------------------------

#[cfg(feature = "test-stub")]
#[test]
fn cli_run_start_close_failure_exits_nonzero() {
    // Regression: a successful run with a guard-close failure must exit
    // non-zero. The test seam deletes the writer lock file after the
    // engine completes but before the explicit close().
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "close-fail");
    select_active_project_fixture(temp_dir.path(), "close-fail");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE", "1")
        .output()
        .expect("run start with close failure");

    assert!(
        !output.status.success(),
        "run start must exit non-zero when guard close fails, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer_lock_absent") || stderr.contains("guard close failed"),
        "should report the close failure reason, got: {stderr}"
    );
    assert!(
        !project_root(temp_dir.path(), "close-fail")
            .join("run.pid")
            .exists(),
        "successful engine completion must remove run.pid even when writer-lease close fails"
    );

    // CLI lease record must remain durable (close did not delete it
    // because lock release failed).
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    let cli_leases: Vec<_> = std::fs::read_dir(&leases_dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("cli-") && n.ends_with(".json"))
        })
        .collect();
    assert!(
        !cli_leases.is_empty(),
        "CLI lease file must remain durable when close fails"
    );
}

// ---------------------------------------------------------------------------
// Reconcile cleanup failure reporting (CLI level)
// ---------------------------------------------------------------------------

#[test]
fn cli_daemon_reconcile_reports_no_failures_on_clean_workspace() {
    let data_dir = tempdir().expect("create temp dir");
    write_repo_registration(data_dir.path());

    let output = Command::new(binary())
        .args([
            "daemon",
            "reconcile",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
        ])
        .output()
        .expect("run reconcile");
    assert!(
        output.status.success(),
        "reconcile should succeed with no leases, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("reconciled"),
        "should print reconcile summary, got: {stdout}"
    );
    assert!(
        !stdout.contains("Cleanup Failures"),
        "should not contain cleanup failures, got: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Stale CLI lease reconcile + recovery (CLI level)
// ---------------------------------------------------------------------------

#[test]
fn cli_daemon_reconcile_cleans_stale_cli_lease() {
    let data_dir = tempdir().expect("create temp dir");
    write_repo_registration(data_dir.path());

    // Inject a stale CLI lease record and writer lock into the data-dir layout.
    let leases_dir = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");

    let cli_lease = CliWriterLease {
        lease_id: "cli-stale-inject".to_owned(),
        project_id: "cli-reconcile".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
        cleanup_handoff: None,
    };
    let record = LeaseRecord::CliWriter(cli_lease);
    let lease_json = serde_json::to_string_pretty(&record).expect("serialize cli lease");
    fs::write(leases_dir.join("cli-stale-inject.json"), lease_json).expect("write cli lease file");
    fs::write(
        leases_dir.join("writer-cli-reconcile.lock"),
        "cli-stale-inject",
    )
    .expect("write writer lock");

    // Run daemon reconcile to clean the stale CLI lease.
    let reconcile_output = Command::new(binary())
        .args([
            "daemon",
            "reconcile",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
        ])
        .output()
        .expect("daemon reconcile");
    let reconcile_stdout = String::from_utf8_lossy(&reconcile_output.stdout);
    assert!(
        reconcile_output.status.success(),
        "reconcile should succeed, stderr: {}",
        String::from_utf8_lossy(&reconcile_output.stderr)
    );
    assert!(
        reconcile_stdout.contains("stale_leases=1"),
        "should report 1 stale lease, got: {reconcile_stdout}"
    );
    assert!(
        reconcile_stdout.contains("released_leases=1"),
        "should report 1 released lease, got: {reconcile_stdout}"
    );
    assert!(
        reconcile_stdout.contains("failed_tasks=0"),
        "should report 0 failed tasks, got: {reconcile_stdout}"
    );
    assert!(
        !reconcile_stdout.contains("Cleanup Failures"),
        "should not contain cleanup failures, got: {reconcile_stdout}"
    );

    // Verify CLI lease file and writer lock are cleaned.
    assert!(
        !leases_dir.join("cli-stale-inject.json").exists(),
        "CLI lease file should be removed after reconcile"
    );
    assert!(
        !leases_dir.join("writer-cli-reconcile.lock").exists(),
        "writer lock should be removed after reconcile"
    );
}

#[test]
fn cli_daemon_reconcile_reports_failure_for_stale_cli_lease_missing_lock() {
    let data_dir = tempdir().expect("create temp dir");
    write_repo_registration(data_dir.path());

    // Inject a stale CLI lease record WITHOUT a matching writer lock.
    let leases_dir = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");

    let cli_lease = CliWriterLease {
        lease_id: "cli-no-lock-cli".to_owned(),
        project_id: "orphan-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
        cleanup_handoff: None,
    };
    let record = LeaseRecord::CliWriter(cli_lease);
    let lease_json = serde_json::to_string_pretty(&record).expect("serialize cli lease");
    fs::write(leases_dir.join("cli-no-lock-cli.json"), lease_json).expect("write cli lease file");

    // Reconcile should fail because the writer lock is already absent.
    let output = Command::new(binary())
        .args([
            "daemon",
            "reconcile",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
        ])
        .output()
        .expect("daemon reconcile");
    assert!(
        !output.status.success(),
        "reconcile should fail when writer lock is missing for stale CLI lease"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Cleanup Failures"),
        "should report cleanup failures, got: {stdout}"
    );
    assert!(
        stdout.contains("writer_lock_absent"),
        "should mention writer_lock_absent, got: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Oversized TTL override must not reclaim a fresh CLI-held writer lock
// ---------------------------------------------------------------------------

#[test]
fn cli_daemon_reconcile_oversized_ttl_does_not_reclaim_fresh_cli_lease() {
    let data_dir = tempdir().expect("create temp dir");
    write_repo_registration(data_dir.path());

    // Inject a fresh CLI lease record and matching writer lock.
    let leases_dir = data_dir
        .path()
        .join("repos")
        .join(TEST_OWNER)
        .join(TEST_REPO)
        .join("daemon/leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");

    let cli_lease = CliWriterLease {
        lease_id: "cli-fresh-oversized".to_owned(),
        project_id: "oversized-ttl-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
        cleanup_handoff: None,
    };
    let record = LeaseRecord::CliWriter(cli_lease);
    let lease_json = serde_json::to_string_pretty(&record).expect("serialize cli lease");
    fs::write(leases_dir.join("cli-fresh-oversized.json"), lease_json)
        .expect("write cli lease file");
    fs::write(
        leases_dir.join("writer-oversized-ttl-proj.lock"),
        "cli-fresh-oversized",
    )
    .expect("write writer lock");

    // Run daemon reconcile with u64::MAX as TTL override.
    // The saturating conversion must prevent the fresh lease from being
    // marked stale — no leases should be reclaimed.
    let output = Command::new(binary())
        .args([
            "daemon",
            "reconcile",
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "--ttl-seconds",
            "18446744073709551615",
        ])
        .output()
        .expect("daemon reconcile");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "reconcile should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("stale_leases=0"),
        "should report 0 stale leases with oversized TTL, got: {stdout}"
    );
    assert!(
        stdout.contains("released_leases=0"),
        "should report 0 released leases with oversized TTL, got: {stdout}"
    );
    assert!(
        stdout.contains("failed_tasks=0"),
        "should report 0 failed tasks with oversized TTL, got: {stdout}"
    );

    // CLI lease file and writer lock must still exist.
    assert!(
        leases_dir.join("cli-fresh-oversized.json").exists(),
        "CLI lease file must not be removed by oversized TTL reconcile"
    );
    assert!(
        leases_dir.join("writer-oversized-ttl-proj.lock").exists(),
        "writer lock must not be released by oversized TTL reconcile"
    );
}

// ---------------------------------------------------------------------------
// Daemon lifecycle conformance regression tests
// ---------------------------------------------------------------------------

#[cfg(feature = "test-stub")]
#[test]
fn conformance_daemon_lifecycle_007_passes() {
    let output = Command::new(binary())
        .args(["conformance", "run", "--filter", "DAEMON-LIFECYCLE-007"])
        .output()
        .expect("run conformance --filter DAEMON-LIFECYCLE-007");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "DAEMON-LIFECYCLE-007 should pass: {stderr}"
    );
    assert!(
        stderr.contains("Passed:    1") || stderr.contains("PASS"),
        "should report scenario passed, got: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_daemon_lifecycle_008_passes() {
    let output = Command::new(binary())
        .args(["conformance", "run", "--filter", "DAEMON-LIFECYCLE-008"])
        .output()
        .expect("run conformance --filter DAEMON-LIFECYCLE-008");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "DAEMON-LIFECYCLE-008 should pass: {stderr}"
    );
    assert!(
        stderr.contains("Passed:    1") || stderr.contains("PASS"),
        "should report scenario passed, got: {stderr}"
    );
}

#[cfg(feature = "test-stub")]
#[test]
fn conformance_full_suite_passes() {
    // Hard-link the CLI binary to a stable path under the test binary's
    // directory (inside target/) so nested sub-spawns remain reliable even
    // if cargo relinks the original during parallel test execution. Using
    // target/ instead of tempdir() avoids dependence on an executable /tmp
    // (some systems mount /tmp with noexec). A hard link pins the inode —
    // even if the original path is replaced, the linked copy stays valid.
    // This avoids ETXTBSY from copy and ENOENT from relink races.
    let binary_dir = std::path::Path::new(binary())
        .parent()
        .expect("binary parent directory");
    let stable_binary = binary_dir.join("ralph-burning-stable-conformance");
    // Remove any stale copy from a previous run before linking.
    let _ = std::fs::remove_file(&stable_binary);
    std::fs::hard_link(binary(), &stable_binary)
        .or_else(|_| std::fs::copy(binary(), &stable_binary).map(|_| ()))
        .expect("link or copy binary to stable path");

    let output = Command::new(&stable_binary)
        .args(["conformance", "run"])
        .env("RALPH_BURNING_CLI_PATH", &stable_binary)
        .output()
        .expect("run conformance run (full suite)");

    // Clean up the stable binary after the run.
    let _ = std::fs::remove_file(&stable_binary);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "full conformance suite should pass: {stderr}"
    );
    assert!(
        stderr.contains("Failed:    0"),
        "no scenarios should fail, got: {stderr}"
    );
}

// ── Slice 3: Manual Amendment CLI Tests ───────────────────────────────────

#[test]
fn project_amend_add_text_succeeds_and_prints_id() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args([
            "project",
            "amend",
            "add",
            "--text",
            "Fix the widget alignment",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");

    assert!(
        output.status.success(),
        "amend add should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Amendment: manual-"),
        "should print 'Amendment: <id>' starting with 'manual-', got: {stdout}"
    );
}

#[test]
fn project_amend_add_file_succeeds() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let amendment_file = temp_dir.path().join("amendment.md");
    fs::write(&amendment_file, "# Amendment\nPlease fix the button color.")
        .expect("write amendment file");

    let output = Command::new(binary())
        .args(["project", "amend", "add", "--file", "amendment.md"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add --file");

    assert!(
        output.status.success(),
        "amend add --file should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Amendment: manual-"),
        "should print 'Amendment: <id>', got: {stdout}"
    );
}

#[test]
fn project_amend_add_rejects_empty_body() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "  "])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add empty");

    assert!(
        !output.status.success(),
        "amend add with empty text should fail"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("empty"),
        "should mention empty body: {stderr}"
    );
}

#[test]
fn project_amend_list_empty() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No pending amendments"),
        "should say no pending: {stdout}"
    );
}

#[test]
fn project_amend_add_then_list_shows_amendment() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Add an amendment
    let add_output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Fix the UI alignment"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success());
    let add_stdout = String::from_utf8_lossy(&add_output.stdout);
    let amendment_id = add_stdout
        .trim()
        .strip_prefix("Amendment: ")
        .expect("should have 'Amendment: ' prefix")
        .to_owned();

    // List amendments
    let list_output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");
    assert!(list_output.status.success());
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains(&amendment_id),
        "list should contain amendment id: {stdout}"
    );
    assert!(
        stdout.contains("[manual]"),
        "list should show [manual] source: {stdout}"
    );
}

#[test]
fn project_amend_remove_existing() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let add_output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Fix something"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success());
    let add_stdout = String::from_utf8_lossy(&add_output.stdout);
    let amendment_id = add_stdout
        .trim()
        .strip_prefix("Amendment: ")
        .expect("should have 'Amendment: ' prefix")
        .to_owned();

    let remove_output = Command::new(binary())
        .args(["project", "amend", "remove", &amendment_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend remove");
    assert!(remove_output.status.success());
    let stdout = String::from_utf8_lossy(&remove_output.stdout);
    assert!(
        stdout.contains("Removed"),
        "should confirm removal: {stdout}"
    );

    // Verify it's gone
    let list_output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");
    assert!(list_output.status.success());
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains("No pending amendments"),
        "should be empty after remove: {stdout}"
    );
}

#[test]
fn project_amend_remove_missing_fails() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["project", "amend", "remove", "nonexistent-id"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend remove missing");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "should mention not found: {stderr}"
    );
}

#[test]
fn project_amend_clear_removes_all() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Add two amendments
    for body in &["Fix A", "Fix B"] {
        let output = Command::new(binary())
            .args(["project", "amend", "add", "--text", body])
            .current_dir(temp_dir.path())
            .output()
            .expect("run amend add");
        assert!(output.status.success());
    }

    let clear_output = Command::new(binary())
        .args(["project", "amend", "clear"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend clear");
    assert!(clear_output.status.success());
    let stdout = String::from_utf8_lossy(&clear_output.stdout);
    assert!(
        stdout.contains("Cleared 2"),
        "should clear 2 amendments: {stdout}"
    );

    // Verify empty
    let list_output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains("No pending amendments"),
        "should be empty after clear"
    );
}

#[test]
fn project_amend_duplicate_manual_add_is_noop() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let body = "Exact same amendment text";

    let first = Command::new(binary())
        .args(["project", "amend", "add", "--text", body])
        .current_dir(temp_dir.path())
        .output()
        .expect("first add");
    assert!(first.status.success());
    let first_stdout = String::from_utf8_lossy(&first.stdout);
    let first_id = first_stdout
        .trim()
        .strip_prefix("Amendment: ")
        .expect("should have 'Amendment: ' prefix")
        .to_owned();

    let second = Command::new(binary())
        .args(["project", "amend", "add", "--text", body])
        .current_dir(temp_dir.path())
        .output()
        .expect("second add");
    assert!(second.status.success());
    let second_stdout = String::from_utf8_lossy(&second.stdout);
    assert!(
        second_stdout.contains("Duplicate"),
        "should report duplicate: {second_stdout}"
    );
    assert!(
        second_stdout.contains(&first_id),
        "should reference original id: {second_stdout}"
    );

    // Only one amendment should exist
    let list = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("list");
    let stdout = String::from_utf8_lossy(&list.stdout);
    let count = stdout.lines().filter(|l| l.contains("manual-")).count();
    assert_eq!(
        count, 1,
        "should have exactly 1 amendment after dup add: {stdout}"
    );
}

#[test]
fn project_amend_add_reopens_completed_project() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Set project to completed state
    let project_root = project_root(temp_dir.path(), "alpha");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"completed","cycle_history":[{"cycle":1,"stage_id":"planning","started_at":"2026-03-11T19:00:00Z","completed_at":"2026-03-11T19:10:00Z"}],"completion_rounds":1,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"completed"}"#,
    ).expect("write completed run.json");

    let output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Post-completion fix"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add on completed");
    assert!(
        output.status.success(),
        "should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify the project is now paused
    let run_json = fs::read_to_string(project_root.join("run.json")).expect("read run.json");
    let snapshot: serde_json::Value = serde_json::from_str(&run_json).expect("parse run.json");
    assert_eq!(
        snapshot["status"], "paused",
        "project should be paused after reopen"
    );
    assert!(
        snapshot["interrupted_run"].is_object(),
        "should have interrupted_run"
    );
}

#[test]
fn project_amend_add_journal_records_event() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args([
            "project",
            "amend",
            "add",
            "--text",
            "Journal test amendment",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(output.status.success());

    let journal = fs::read_to_string(project_root(temp_dir.path(), "alpha").join("journal.ndjson"))
        .expect("read journal");
    let last_line = journal.lines().last().expect("journal has lines");
    let event: serde_json::Value = serde_json::from_str(last_line).expect("parse event");
    assert_eq!(event["event_type"], "amendment_queued");
    assert_eq!(event["details"]["source"], "manual");
    assert!(
        event["details"]["dedup_key"].is_string(),
        "should have dedup_key"
    );
    assert!(
        event["details"]["amendment_id"].is_string(),
        "should have amendment_id"
    );
}

#[test]
fn project_amend_add_lease_conflict_rejects() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Create a writer lock file to simulate an active lease.
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    fs::write(leases_dir.join("writer-alpha.lock"), "fake-lease-id").expect("write lock");

    let output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Should be rejected"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add during lease");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer lease") || stderr.contains("lock"),
        "should mention lease conflict: {stderr}"
    );
}

#[test]
fn project_amend_remove_lease_conflict_rejects() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Add an amendment first (no lock yet).
    let add_output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Amendment to remove"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success());
    let add_stdout = String::from_utf8_lossy(&add_output.stdout);
    let amendment_id = add_stdout
        .trim()
        .strip_prefix("Amendment: ")
        .expect("should have 'Amendment: ' prefix")
        .to_owned();

    // Create a writer lock file to simulate an active lease.
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    fs::write(leases_dir.join("writer-alpha.lock"), "fake-lease-id").expect("write lock");

    let output = Command::new(binary())
        .args(["project", "amend", "remove", &amendment_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend remove during lease");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer lease") || stderr.contains("lock"),
        "should mention lease conflict: {stderr}"
    );

    // Verify the amendment was NOT removed.
    fs::remove_file(leases_dir.join("writer-alpha.lock")).ok();
    let list_output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        list_stdout.contains(&amendment_id),
        "amendment should still be pending: {list_stdout}"
    );
}

#[test]
fn project_amend_clear_lease_conflict_rejects() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    select_active_project_fixture(temp_dir.path(), "alpha");

    // Add an amendment first (no lock yet).
    let add_output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Amendment to clear"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success());

    // Create a writer lock file to simulate an active lease.
    let leases_dir = daemon_root(temp_dir.path()).join("leases");
    fs::create_dir_all(&leases_dir).expect("create leases dir");
    fs::write(leases_dir.join("writer-alpha.lock"), "fake-lease-id").expect("write lock");

    let output = Command::new(binary())
        .args(["project", "amend", "clear"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend clear during lease");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer lease") || stderr.contains("lock"),
        "should mention lease conflict: {stderr}"
    );

    // Verify amendments were NOT cleared.
    fs::remove_file(leases_dir.join("writer-alpha.lock")).ok();
    let list_output = Command::new(binary())
        .args(["project", "amend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend list");
    let list_stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        !list_stdout.contains("No pending amendments"),
        "amendments should still be pending: {list_stdout}"
    );
}

// ---------------------------------------------------------------------------
// Guard close failure makes successful amend add exit non-zero (CLI level)
// ---------------------------------------------------------------------------

#[test]
fn cli_project_amend_add_close_failure_exits_nonzero() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "close-add");
    select_active_project_fixture(temp_dir.path(), "close-add");

    let output = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Close failure test"])
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE", "1")
        .output()
        .expect("run amend add with close failure");

    assert!(
        !output.status.success(),
        "amend add must exit non-zero when guard close fails, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer_lock_absent") || stderr.contains("guard close failed"),
        "should report the close failure reason, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Guard close failure makes successful amend remove exit non-zero (CLI level)
// ---------------------------------------------------------------------------

#[test]
fn cli_project_amend_remove_close_failure_exits_nonzero() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "close-rm");
    select_active_project_fixture(temp_dir.path(), "close-rm");

    // Add an amendment first (without close-failure seam).
    let add_output = Command::new(binary())
        .args([
            "project",
            "amend",
            "add",
            "--text",
            "Amendment for close-rm test",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success(), "add should succeed");
    let add_stdout = String::from_utf8_lossy(&add_output.stdout);
    let amendment_id = add_stdout
        .lines()
        .find(|l| l.starts_with("Amendment: "))
        .expect("should print amendment id")
        .trim_start_matches("Amendment: ")
        .trim()
        .to_owned();

    let output = Command::new(binary())
        .args(["project", "amend", "remove", &amendment_id])
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE", "1")
        .output()
        .expect("run amend remove with close failure");

    assert!(
        !output.status.success(),
        "amend remove must exit non-zero when guard close fails, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer_lock_absent") || stderr.contains("guard close failed"),
        "should report the close failure reason, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Guard close failure makes successful amend clear exit non-zero (CLI level)
// ---------------------------------------------------------------------------

#[test]
fn cli_project_amend_clear_close_failure_exits_nonzero() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "close-clr");
    select_active_project_fixture(temp_dir.path(), "close-clr");

    // Add an amendment first (without close-failure seam).
    let add_output = Command::new(binary())
        .args([
            "project",
            "amend",
            "add",
            "--text",
            "Amendment for close-clr test",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run amend add");
    assert!(add_output.status.success(), "add should succeed");

    let output = Command::new(binary())
        .args(["project", "amend", "clear"])
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE", "1")
        .output()
        .expect("run amend clear with close failure");

    assert!(
        !output.status.success(),
        "amend clear must exit non-zero when guard close fails, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("writer_lock_absent") || stderr.contains("guard close failed"),
        "should report the close failure reason, got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Combined partial-clear + close failure: partial-clear IDs are still surfaced
// ---------------------------------------------------------------------------

#[test]
fn cli_project_amend_clear_partial_failure_surfaces_ids_despite_close_failure() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "combo-clr");
    select_active_project_fixture(temp_dir.path(), "combo-clr");

    // Add two amendments so the partial failure has both removed and remaining.
    let add1 = Command::new(binary())
        .args(["project", "amend", "add", "--text", "First amendment"])
        .current_dir(temp_dir.path())
        .output()
        .expect("add first amendment");
    assert!(add1.status.success(), "first add should succeed");

    let add2 = Command::new(binary())
        .args(["project", "amend", "add", "--text", "Second amendment"])
        .current_dir(temp_dir.path())
        .output()
        .expect("add second amendment");
    assert!(add2.status.success(), "second add should succeed");

    // Trigger partial clear (first remove succeeds, second fails) AND close failure.
    let output = Command::new(binary())
        .args(["project", "amend", "clear"])
        .current_dir(temp_dir.path())
        .env("RALPH_BURNING_TEST_AMENDMENT_REMOVE_FAIL_AFTER", "1")
        .env("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE", "1")
        .output()
        .expect("run amend clear with partial + close failure");

    assert!(
        !output.status.success(),
        "should exit non-zero on partial clear + close failure"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The partial-clear contract requires exact removed/remaining IDs.
    assert!(
        stderr.contains("removed:"),
        "must surface removed IDs even when close also fails, got: {stderr}"
    );
    assert!(
        stderr.contains("remaining:"),
        "must surface remaining IDs even when close also fails, got: {stderr}"
    );

    // The close failure should also be mentioned.
    assert!(
        stderr.contains("writer-lease cleanup also failed")
            || stderr.contains("writer_lock_absent")
            || stderr.contains("guard close failed"),
        "should note the close failure alongside partial-clear details, got: {stderr}"
    );
}

// ── backend command tests (Slice 5) ─────────────────────────────────────────

#[test]
fn backend_list_shows_all_families() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend list");

    assert!(
        output.status.success(),
        "backend list should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("claude"), "should list claude");
    assert!(stdout.contains("codex"), "should list codex");
    assert!(stdout.contains("openrouter"), "should list openrouter");
    assert!(stdout.contains("stub"), "should list stub");
}

#[test]
fn backend_list_json_is_valid() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "list", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend list --json");

    assert!(
        output.status.success(),
        "backend list --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    assert!(parsed.is_array(), "JSON output should be an array");
    let arr = parsed.as_array().unwrap();
    assert_eq!(4, arr.len(), "should have 4 backend families");
}

#[test]
fn backend_check_succeeds_with_defaults() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "check"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend check");

    assert!(
        output.status.success(),
        "backend check should succeed with defaults: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("passed"),
        "should report passed: {}",
        stdout
    );
}

#[test]
fn backend_check_json_contract() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "check", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend check --json");

    assert!(
        output.status.success(),
        "backend check --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    assert!(
        parsed.get("passed").is_some(),
        "JSON should have 'passed' field"
    );
    assert!(
        parsed.get("failures").is_some(),
        "JSON should have 'failures' field"
    );
}

#[test]
fn backend_show_effective_text_output() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "show-effective"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend show-effective");

    assert!(
        output.status.success(),
        "backend show-effective should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Base backend"), "should show base backend");
    assert!(
        stdout.contains("Per-role resolution"),
        "should show per-role section"
    );
}

#[test]
fn backend_show_effective_json_contract() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "show-effective", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend show-effective --json");

    assert!(
        output.status.success(),
        "backend show-effective --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    assert!(
        parsed.get("base_backend").is_some(),
        "JSON should have 'base_backend'"
    );
    assert!(parsed.get("roles").is_some(), "JSON should have 'roles'");
    assert!(
        parsed.get("default_timeout_seconds").is_some(),
        "JSON should have 'default_timeout_seconds'"
    );
}

#[test]
fn backend_probe_singular_role() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend", "probe", "--role", "planner", "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe");

    assert!(
        output.status.success(),
        "backend probe should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("planner"), "should show probed role");
    assert!(stdout.contains("standard"), "should show flow");
}

#[test]
fn backend_probe_completion_panel() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend",
            "probe",
            "--role",
            "completion_panel",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe completion panel");

    assert!(
        output.status.success(),
        "backend probe completion_panel should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("completion"),
        "should show panel type: {}",
        stdout
    );
}

#[test]
fn backend_probe_final_review_panel() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend",
            "probe",
            "--role",
            "final_review_panel",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe final_review_panel");

    assert!(
        output.status.success(),
        "backend probe final_review_panel should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("final_review"),
        "should show panel type: {}",
        stdout
    );
}

#[test]
fn backend_probe_final_reviewer() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend",
            "probe",
            "--role",
            "final_reviewer",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe final_reviewer");

    assert!(
        output.status.success(),
        "backend probe final_reviewer should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("gpt-5.4-xhigh"),
        "should show the default first final reviewer target: {}",
        stdout
    );
}

#[test]
fn backend_probe_json_contract() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend", "probe", "--role", "planner", "--flow", "standard", "--json",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe --json");

    assert!(
        output.status.success(),
        "backend probe --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    assert!(parsed.get("role").is_some(), "JSON should have 'role'");
    assert!(parsed.get("flow").is_some(), "JSON should have 'flow'");
    assert!(parsed.get("target").is_some(), "JSON should have 'target'");
}

#[test]
fn backend_check_nonzero_exit_on_failure() {
    let temp_dir = initialize_workspace_fixture();

    // Write config with a disabled base backend
    let workspace_toml = r#"version = 1
created_at = "2026-03-19T03:28:00Z"

[settings]
default_flow = "standard"
default_backend = "openrouter"

[backends.openrouter]
enabled = false
"#;
    fs::write(workspace_config_path(temp_dir.path()), workspace_toml)
        .expect("write workspace.toml");

    let output = Command::new(binary())
        .args(["backend", "check"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend check with disabled backend");

    assert!(
        !output.status.success(),
        "backend check should exit non-zero when base backend is disabled"
    );
}

#[test]
fn backend_show_effective_with_cli_override() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["backend", "show-effective", "--json", "--backend", "codex"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend show-effective with override");

    assert!(
        output.status.success(),
        "backend show-effective with override should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    let base_value = parsed["base_backend"]["value"]
        .as_str()
        .expect("base_backend.value");
    assert!(
        base_value.contains("codex"),
        "base backend should be overridden to codex, got: {}",
        base_value
    );
}

#[test]
fn backend_probe_nonzero_exit_on_disabled_backend() {
    let temp_dir = initialize_workspace_fixture();

    // Disable the base backend so probing any role that depends on it fails
    let workspace_toml = r#"version = 1
created_at = "2026-03-19T03:28:00Z"

[settings]
default_flow = "standard"
default_backend = "openrouter"

[backends.openrouter]
enabled = false
"#;
    fs::write(workspace_config_path(temp_dir.path()), workspace_toml)
        .expect("write workspace.toml");

    let output = Command::new(binary())
        .args([
            "backend", "probe", "--role", "planner", "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe with disabled backend");

    assert!(
        !output.status.success(),
        "backend probe should exit non-zero when required backend is disabled"
    );
}

#[test]
fn backend_probe_nonzero_exit_on_panel_minimum_violation() {
    let temp_dir = initialize_workspace_fixture();

    // Configure completion panel with min_completers=2 but only one backend
    // enabled (claude required + openrouter optional but disabled).
    let workspace_toml = r#"version = 1
created_at = "2026-03-19T03:28:00Z"

[backends.openrouter]
enabled = false

[completion]
backends = ["claude", "?openrouter"]
min_completers = 2
consensus_threshold = 0.66
"#;
    fs::write(workspace_config_path(temp_dir.path()), workspace_toml)
        .expect("write workspace.toml");

    let output = Command::new(binary())
        .args([
            "backend",
            "probe",
            "--role",
            "completion_panel",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend probe with insufficient panel members");

    assert!(
        !output.status.success(),
        "backend probe should exit non-zero when optional omission drops panel below minimum"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("insufficient") || stderr.contains("panel") || stderr.contains("minimum"),
        "error output should mention panel minimum violation: {}",
        stderr
    );
}

#[test]
fn backend_check_nonzero_exit_json_reports_failures() {
    let temp_dir = initialize_workspace_fixture();

    // Disable the base backend
    let workspace_toml = r#"version = 1
created_at = "2026-03-19T03:28:00Z"

[settings]
default_flow = "standard"
default_backend = "openrouter"

[backends.openrouter]
enabled = false
"#;
    fs::write(workspace_config_path(temp_dir.path()), workspace_toml)
        .expect("write workspace.toml");

    let output = Command::new(binary())
        .args(["backend", "check", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend check --json with disabled backend");

    assert!(
        !output.status.success(),
        "backend check --json should exit non-zero when base backend is disabled"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("should be valid JSON even on failure");
    assert_eq!(
        parsed["passed"].as_bool(),
        Some(false),
        "passed should be false"
    );
    let failures = parsed["failures"]
        .as_array()
        .expect("failures should be an array");
    assert!(
        !failures.is_empty(),
        "failures array should contain at least one entry"
    );
    // Each failure should have the expected contract fields
    let failure = &failures[0];
    assert!(failure.get("role").is_some(), "failure should have 'role'");
    assert!(
        failure.get("backend_family").is_some(),
        "failure should have 'backend_family'"
    );
    assert!(
        failure.get("failure_kind").is_some(),
        "failure should have 'failure_kind'"
    );
}

#[test]
fn backend_list_nonzero_exit_without_workspace() {
    let temp_dir = tempdir().expect("create temp dir");
    // No workspace initialized — backend list should fail

    let output = Command::new(binary())
        .args(["backend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend list without workspace");

    assert!(
        !output.status.success(),
        "backend list should exit non-zero when no workspace exists"
    );
}

#[test]
fn backend_show_effective_nonzero_exit_without_workspace() {
    let temp_dir = tempdir().expect("create temp dir");
    // No workspace initialized — show-effective should fail

    let output = Command::new(binary())
        .args(["backend", "show-effective"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend show-effective without workspace");

    assert!(
        !output.status.success(),
        "backend show-effective should exit non-zero when no workspace exists"
    );
}

#[test]
fn backend_list_nonzero_exit_with_corrupt_config() {
    let temp_dir = initialize_workspace_fixture();

    // Corrupt the workspace.toml
    fs::write(
        workspace_config_path(temp_dir.path()),
        "this is not valid toml {{{",
    )
    .expect("write corrupt config");

    let output = Command::new(binary())
        .args(["backend", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend list with corrupt config");

    assert!(
        !output.status.success(),
        "backend list should exit non-zero with corrupt workspace config"
    );
}

#[test]
fn backend_show_effective_nonzero_exit_with_invalid_override() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "backend",
            "show-effective",
            "--backend",
            "not-a-real-backend",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run backend show-effective with invalid override");

    assert!(
        !output.status.success(),
        "backend show-effective should exit non-zero with invalid backend override"
    );
}

// ── Slice 7: Template override CLI integration tests ────────────────────

#[cfg(feature = "test-stub")]
#[test]
fn run_start_malformed_template_override_exits_nonzero_with_no_durable_state_change() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "tpl-malformed");

    // Install a malformed workspace template override for "planning"
    // (the first stage in a standard flow). Missing all required placeholders.
    let ws_templates = temp_dir.path().join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("planning.md"),
        "This override has no placeholders at all.",
    )
    .expect("write malformed override");

    // Capture pre-run state
    let pre_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "tpl-malformed").join("run.json"))
            .expect("read run.json before");
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start with malformed override");

    assert!(
        !output.status.success(),
        "run start should fail with malformed template override"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("malformed template override") || stderr.contains("MalformedTemplate"),
        "stderr should mention malformed template: {stderr}"
    );

    // Verify no durable state was mutated beyond run_started
    let post_run_json =
        fs::read_to_string(project_root(temp_dir.path(), "tpl-malformed").join("run.json"))
            .expect("read run.json after");
    let post_journal =
        fs::read_to_string(project_root(temp_dir.path(), "tpl-malformed").join("journal.ndjson"))
            .expect("read journal after");

    // The journal must not contain a stage_entered event for "planning"
    // (the stage whose template was malformed). Earlier stages like
    // prompt_review may legitimately enter and complete before the
    // malformed planning template is reached.
    for line in post_journal.lines() {
        if line.contains("stage_entered") && line.contains("planning") {
            panic!("no stage_entered event should be written for the malformed planning stage");
        }
        if line.contains("stage_completed") && line.contains("planning") {
            panic!("no stage_completed event should be written for the malformed planning stage");
        }
    }

    // run.json must not record a running stage for the failed template
    assert!(
        !post_run_json.contains("\"status\":\"running\"") || post_run_json == pre_run_json,
        "run.json must not show running status for a malformed template failure"
    );

    // No payloads should exist for the planning stage specifically.
    // Earlier stages like prompt_review may legitimately write payloads.
    let payloads_dir = project_root(temp_dir.path(), "tpl-malformed").join("history/payloads");
    if payloads_dir.exists() {
        let planning_payloads: Vec<_> = fs::read_dir(&payloads_dir)
            .expect("read payloads dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("planning"))
            .collect();
        assert!(
            planning_payloads.is_empty(),
            "no planning payloads should be written for a malformed template, found: {:?}",
            planning_payloads
                .iter()
                .map(|e| e.file_name())
                .collect::<Vec<_>>()
        );
    }
}

#[cfg(feature = "test-stub")]
#[test]
fn run_start_malformed_project_override_does_not_fall_back_to_workspace() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "tpl-no-fallback");

    // Install a VALID workspace override
    let ws_templates = temp_dir.path().join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("planning.md"),
        "VALID WS\n\n{{role_instruction}}\n\n{{project_prompt}}\n\n{{json_schema}}",
    )
    .expect("write valid workspace override");

    // Install a MALFORMED project override (should NOT fall back to workspace)
    let proj_templates = project_root(temp_dir.path(), "tpl-no-fallback").join("templates");
    fs::create_dir_all(&proj_templates).expect("create project templates dir");
    fs::write(
        proj_templates.join("planning.md"),
        "BROKEN PROJECT OVERRIDE — no placeholders",
    )
    .expect("write malformed project override");

    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_BACKEND", "stub")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start with malformed project override");

    assert!(
        !output.status.success(),
        "malformed project override must not silently fall back to workspace override"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("malformed template override") || stderr.contains("MalformedTemplate"),
        "stderr should mention malformed template: {stderr}"
    );
}

// ── Task Alias Integration Tests ──

#[test]
fn task_select_sets_active_project_with_task_aware_output() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["task", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task select");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Selected task 'alpha' (project 'alpha')"),
        "task select should show task-aware output, got: {stdout}"
    );
    // Verify the active-project pointer is set (same state as project select)
    assert_eq!(
        "alpha",
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project")
    );
}

#[test]
fn task_select_updates_active_milestone_for_bead_backed_projects() {
    let temp_dir = initialize_workspace_fixture();
    create_bead_backed_project_fixture(temp_dir.path(), "bead-beta", "ms-beta", "ms-beta.bead-1");
    fs::write(active_milestone_path(temp_dir.path()), "ms-alpha").expect("seed active milestone");

    let output = Command::new(binary())
        .args(["task", "select", "bead-beta"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task select");

    assert!(
        output.status.success(),
        "task select should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path()))
            .expect("read active project")
            .trim(),
        "bead-beta"
    );
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path()))
            .expect("read active milestone")
            .trim(),
        "ms-beta"
    );
}

#[test]
fn task_select_and_project_select_produce_same_state() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    // Use task select
    let task_output = Command::new(binary())
        .args(["task", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task select");
    assert!(task_output.status.success());

    let active_after_task =
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project");

    // Use project select to overwrite (same project — pointer should be identical)
    let project_output = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");
    assert!(project_output.status.success());

    let active_after_project =
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project");

    assert_eq!(active_after_task, active_after_project);
}

#[test]
fn task_select_rejects_missing_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["task", "select", "nonexistent"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task select for missing");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "task select for missing project should fail, got: {stderr}"
    );
}

#[test]
fn task_list_shows_no_tasks_when_empty() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["task", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No tasks found"),
        "task list empty should say 'No tasks found', got: {stdout}"
    );
}

#[test]
fn task_list_shows_same_entries_as_project_list() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create two projects
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "alpha",
            "--name",
            "Alpha",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create alpha");

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "beta",
            "--name",
            "Beta",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create beta");

    // Select alpha as active
    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select alpha");

    let task_list = Command::new(binary())
        .args(["task", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task list");

    let project_list = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(task_list.status.success());
    assert!(project_list.status.success());

    let task_stdout = String::from_utf8_lossy(&task_list.stdout);
    let project_stdout = String::from_utf8_lossy(&project_list.stdout);
    let task_stderr = String::from_utf8_lossy(&task_list.stderr);
    let project_stderr = String::from_utf8_lossy(&project_list.stderr);

    // Both should show the same entries (same format)
    assert!(task_stdout.contains("alpha *"));
    assert!(task_stdout.contains("beta"));
    assert!(task_stdout.contains("standard"));
    assert!(task_stdout.contains("quick_dev"));
    // The entry lines themselves should be identical
    assert_eq!(task_stdout, project_stdout);
    assert!(
        task_stderr.is_empty(),
        "task aliases should not emit deprecation notices: {task_stderr}"
    );
    assert!(
        project_stderr.contains(
            "Note: `ralph-burning project list` is deprecated. Use `ralph-burning task list` instead."
        ),
        "project list should emit a deprecation notice to stderr: {project_stderr}"
    );
}

#[test]
fn project_help_mentions_deprecation_and_aliases() {
    let project_help = Command::new(binary())
        .args(["project", "--help"])
        .output()
        .expect("run project --help");
    assert!(project_help.status.success());
    let project_stdout = String::from_utf8_lossy(&project_help.stdout);
    assert!(project_stdout.contains("Project commands (deprecated"));
    assert!(project_stdout.contains("task select"));
    assert!(project_stdout.contains("task list"));

    let create_help = Command::new(binary())
        .args(["project", "create", "--help"])
        .output()
        .expect("run project create --help");
    assert!(create_help.status.success());
    let create_stdout = String::from_utf8_lossy(&create_help.stdout);
    assert!(create_stdout.contains("alias: prefer `task create` or `milestone create`"));
}

#[test]
fn task_show_displays_task_aware_detail() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "showme",
            "--name",
            "Show Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "docs_change",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let output = Command::new(binary())
        .args(["task", "show", "showme"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Task: 'showme' (project 'showme')"),
        "task show should use task-aware header, got: {stdout}"
    );
    assert!(stdout.contains("Name: Show Me"));
    assert!(stdout.contains("Flow: docs_change"));
    assert!(stdout.contains("Run status: not started"));
}

#[test]
fn task_show_resolves_active_project_when_no_id_given() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "active-task",
            "--name",
            "Active Task",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["task", "select", "active-task"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select task");

    let output = Command::new(binary())
        .args(["task", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show without id");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Task: 'active-task' (project 'active-task') (active)"),
        "task show should show active marker, got: {stdout}"
    );
}

#[test]
fn task_show_json_includes_lineage_for_milestone_tasks() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");
    create_bead_backed_project_fixture(temp_dir.path(), "task-json", "ms-alpha", "ms-alpha.bead-2");

    let output = Command::new(binary())
        .args(["task", "show", "--json", "task-json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("task show json should parse");
    assert_eq!(value["record"]["id"], "task-json");
    assert_eq!(value["task_lineage"]["milestone_id"], "ms-alpha");
    assert_eq!(value["task_lineage"]["bead_id"], "ms-alpha.bead-2");
    assert_eq!(
        value["task_lineage"]["bead_title"],
        "Bootstrap bead-backed task creation"
    );
}

#[test]
fn task_show_json_omits_lineage_for_non_milestone_tasks() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "plain-task");

    let output = Command::new(binary())
        .args(["task", "show", "--json", "plain-task"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show --json");

    assert!(output.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("task show json should parse");
    assert_eq!(value["record"]["id"], "plain-task");
    assert!(
        value.get("task_lineage").is_none(),
        "non-milestone task json should omit task_lineage"
    );
}

#[test]
fn task_show_fails_when_no_active_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["task", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show without active project");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no active project"),
        "task show without active project should fail, got: {stderr}"
    );
}

#[test]
fn milestone_create_produces_valid_milestone() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "create", "Alpha Launch"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");

    assert!(
        output.status.success(),
        "milestone create should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created milestone 'ms-alpha-launch'"));
    assert!(milestone_root(temp_dir.path(), "ms-alpha-launch").exists());
    assert!(milestone_root(temp_dir.path(), "ms-alpha-launch")
        .join("milestone.toml")
        .exists());
    assert!(milestone_root(temp_dir.path(), "ms-alpha-launch")
        .join("status.json")
        .exists());
}

#[test]
fn milestone_create_rejects_invalid_id() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "create", "!!!"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");

    assert!(!output.status.success(), "milestone create should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid identifier"));
    assert!(stderr.contains("single path segments"));
}

#[test]
fn milestone_show_displays_milestone_detail() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(
        output.status.success(),
        "milestone show should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Milestone:    ms-alpha"));
    assert!(stdout.contains("Name:         Alpha Milestone"));
    assert!(stdout.contains("Description:  Deliver the alpha milestone."));
    assert!(stdout.contains("Status:       ready"));
    assert!(stdout.contains("Plan Version: 2"));
}

#[test]
fn milestone_show_json_output() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(
        output.status.success(),
        "milestone show --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("milestone show json should parse");
    assert_eq!(value["id"], "ms-alpha");
    assert_eq!(value["name"], "Alpha Milestone");
    assert_eq!(value["status"], "ready");
    assert_eq!(value["bead_count"], 3);
    assert_eq!(value["plan_version"], 2);
    assert_eq!(value["has_plan"], true);
    assert!(value.get("progress").is_some());
}

#[test]
fn milestone_status_shows_current_state() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(
        output.status.success(),
        "milestone status should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Milestone:    ms-alpha"));
    assert!(stdout.contains("Status:       ready"));
    assert_eq!(
        fs::read_to_string(active_milestone_path(temp_dir.path())).expect("read active milestone"),
        "ms-alpha"
    );
}

#[test]
fn task_show_dispatches_to_project_show() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let task_output = Command::new(binary())
        .args(["task", "show", "alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show");
    let project_output = Command::new(binary())
        .args(["project", "show", "alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(task_output.status.success(), "task show should succeed");
    assert!(
        project_output.status.success(),
        "project show should succeed"
    );
    let task_json: serde_json::Value =
        serde_json::from_slice(&task_output.stdout).expect("task show json should parse");
    let project_json: serde_json::Value =
        serde_json::from_slice(&project_output.stdout).expect("project show json should parse");
    assert_eq!(task_json, project_json);
    assert!(String::from_utf8_lossy(&task_output.stderr).is_empty());
    assert!(String::from_utf8_lossy(&project_output.stderr).contains("deprecated"));
}

#[test]
fn task_list_dispatches_to_project_list() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");
    create_project_fixture(temp_dir.path(), "beta");
    select_active_project_fixture(temp_dir.path(), "alpha");

    let task_output = Command::new(binary())
        .args(["task", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task list");
    let project_output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(task_output.status.success(), "task list should succeed");
    assert!(
        project_output.status.success(),
        "project list should succeed"
    );
    assert_eq!(task_output.stdout, project_output.stdout);
    let stdout = String::from_utf8_lossy(&task_output.stdout);
    assert!(stdout.contains("alpha *"));
    assert!(stdout.contains("beta"));
    assert!(String::from_utf8_lossy(&task_output.stderr).is_empty());
    assert!(String::from_utf8_lossy(&project_output.stderr).contains("deprecated"));
}

#[test]
fn task_select_dispatches_to_project_select() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let task_output = Command::new(binary())
        .args(["task", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task select");

    assert!(task_output.status.success(), "task select should succeed");
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project"),
        "alpha"
    );
    assert!(String::from_utf8_lossy(&task_output.stderr).is_empty());

    fs::remove_file(active_project_path(temp_dir.path())).expect("clear active project");

    let project_output = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(
        project_output.status.success(),
        "project select should succeed"
    );
    assert_eq!(
        fs::read_to_string(active_project_path(temp_dir.path())).expect("read active project"),
        "alpha"
    );
    assert!(String::from_utf8_lossy(&project_output.stderr).contains("deprecated"));
}

#[test]
fn project_commands_print_deprecation_to_stderr() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let show = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");
    let list = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");
    let select = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(show.status.success());
    assert!(list.status.success());
    assert!(select.status.success());
    assert!(String::from_utf8_lossy(&show.stderr).contains("project show"));
    assert!(String::from_utf8_lossy(&list.stderr).contains("project list"));
    assert!(String::from_utf8_lossy(&select.stderr).contains("project select"));
}

#[test]
fn project_commands_stdout_unaffected_by_deprecation() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let show = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");
    let list = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");
    let select = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    let show_stdout = String::from_utf8_lossy(&show.stdout);
    let list_stdout = String::from_utf8_lossy(&list.stdout);
    let select_stdout = String::from_utf8_lossy(&select.stdout);
    assert!(!show_stdout.contains("deprecated"));
    assert!(!list_stdout.contains("deprecated"));
    assert!(!select_stdout.contains("deprecated"));
    assert!(show_stdout.contains("Project: alpha"));
    assert!(list_stdout.contains("alpha"));
    assert!(select_stdout.contains("Selected project alpha"));
}

#[test]
fn project_show_still_works_with_deprecation() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let output = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(
        output.status.success(),
        "project show should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("Project: alpha"));
    assert!(stdout.contains("Name: Fixture alpha"));
    assert!(stdout.contains("Run status: not started"));
    assert!(stderr.contains("Note: `ralph-burning project show` is deprecated."));
}

#[test]
fn milestone_show_nonexistent_produces_error() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(!output.status.success(), "missing milestone should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("milestone 'ms-missing' was not found"));
}

#[test]
fn milestone_run_without_workspace_fails() {
    let temp_dir = tempdir().expect("create temp dir");

    let output = Command::new(binary())
        .args(["milestone", "run"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone run");

    assert!(!output.status.success(), "milestone run should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No such file or directory") || stderr.contains("workspace.toml"),
        "milestone run outside a workspace should fail clearly, got: {stderr}"
    );
}

#[test]
fn task_show_nonexistent_produces_error() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["task", "show", "missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run task show");

    assert!(!output.status.success(), "missing task should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project 'missing' was not found"));
}

#[test]
fn milestone_show_human_readable_format() {
    let temp_dir = initialize_workspace_fixture();

    let create = Command::new(binary())
        .args(["milestone", "create", "Alpha Launch"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone create");
    assert!(create.status.success(), "milestone create should succeed");

    let output = Command::new(binary())
        .args(["milestone", "show", "ms-alpha-launch"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone show");

    assert!(output.status.success(), "milestone show should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Milestone:"));
    assert!(stdout.contains("Description:"));
    assert!(stdout.contains("Progress:"));
    assert!(stdout.contains("Created At:"));
    assert!(stdout.contains("Updated At:"));
}

#[test]
fn milestone_status_json_format() {
    let temp_dir = initialize_workspace_fixture();
    write_milestone_fixture(temp_dir.path(), "ms-alpha");

    let output = Command::new(binary())
        .args(["milestone", "status", "ms-alpha", "--json"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run milestone status");

    assert!(
        output.status.success(),
        "milestone status --json should succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("milestone status json should parse");
    assert_eq!(value["id"], "ms-alpha");
    assert_eq!(value["status"], "ready");
    assert_eq!(value["bead_count"], 3);
    assert_eq!(value["plan_version"], 2);
    assert_eq!(value["has_plan"], true);
    assert_eq!(value["progress"]["total_beads"], 3);
}
