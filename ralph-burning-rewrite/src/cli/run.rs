use std::sync::Arc;

use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore, FsRollbackPointStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogStore, FsRuntimeLogWriteStore,
    FsSessionStore,
};
use crate::adapters::process_backend::ProcessBackendAdapter;
use crate::adapters::stub_backend::StubBackendAdapter;
use crate::adapters::worktree::WorktreeAdapter;
use crate::adapters::BackendAdapter;
use crate::contexts::agent_execution::service::AgentExecutionService;
use crate::contexts::automation_runtime::cli_writer_lease::{
    CliWriterLeaseGuard, CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};
use crate::contexts::project_run_record::model::RunStatus;
use crate::contexts::project_run_record::service::{self, ProjectStorePort, RunSnapshotPort};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{BackendSelection, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct RunCommand {
    #[command(subcommand)]
    pub command: RunSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RunSubcommand {
    Start(RunBackendOverrideArgs),
    Resume(RunBackendOverrideArgs),
    Status,
    History,
    Tail {
        #[arg(long)]
        logs: bool,
    },
    Rollback {
        #[arg(long)]
        to: String,
        #[arg(long)]
        hard: bool,
    },
}

#[derive(Debug, Args, Clone, Default)]
pub struct RunBackendOverrideArgs {
    #[arg(long = "backend")]
    pub backend: Option<String>,
    #[arg(long = "planner-backend")]
    pub planner_backend: Option<String>,
    #[arg(long = "implementer-backend")]
    pub implementer_backend: Option<String>,
    #[arg(long = "reviewer-backend")]
    pub reviewer_backend: Option<String>,
    #[arg(long = "qa-backend")]
    pub qa_backend: Option<String>,
}

pub async fn handle(command: RunCommand) -> AppResult<()> {
    match command.command {
        RunSubcommand::Status => handle_status().await,
        RunSubcommand::History => handle_history().await,
        RunSubcommand::Tail { logs } => handle_tail(logs).await,
        RunSubcommand::Start(args) => handle_start(args).await,
        RunSubcommand::Resume(args) => handle_resume(args).await,
        RunSubcommand::Rollback { to, hard } => handle_rollback(to, hard).await,
    }
}

pub fn build_agent_execution_service(
) -> AppResult<AgentExecutionService<BackendAdapter, FsRawOutputStore, FsSessionStore>> {
    let backend_selector = match std::env::var("RALPH_BURNING_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "process".to_owned(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(AppError::InvalidConfigValue {
                key: "RALPH_BURNING_BACKEND".to_owned(),
                value: "<non-unicode>".to_owned(),
                reason: "expected one of stub, process".to_owned(),
            });
        }
    };

    let adapter = match backend_selector.as_str() {
        "stub" => BackendAdapter::Stub(build_stub_backend_adapter()),
        "process" => BackendAdapter::Process(ProcessBackendAdapter::new()),
        other => {
            return Err(AppError::InvalidConfigValue {
                key: "RALPH_BURNING_BACKEND".to_owned(),
                value: other.to_owned(),
                reason: "expected one of stub, process".to_owned(),
            });
        }
    };

    Ok(AgentExecutionService::new(
        adapter,
        FsRawOutputStore,
        FsSessionStore,
    ))
}

fn build_stub_backend_adapter() -> StubBackendAdapter {
    let mut adapter = StubBackendAdapter::default();

    // Test-only injection seam: environment variables configure the stub backend
    // to simulate failure modes that aren't reachable through normal CLI usage.
    if std::env::var("RALPH_BURNING_TEST_BACKEND_UNAVAILABLE").is_ok() {
        adapter = adapter.unavailable();
    }
    if let Ok(stage_str) = std::env::var("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE") {
        if let Ok(stage_id) = stage_str.parse::<StageId>() {
            adapter = adapter.with_invoke_failure(stage_id);
        }
    }
    // Test-only seam: configure a stage to fail the first N invocations, then succeed.
    // Format: "stage_id:count" e.g. "implementation:1"
    if let Ok(spec) = std::env::var("RALPH_BURNING_TEST_TRANSIENT_FAILURE") {
        if let Some((stage_str, count_str)) = spec.split_once(':') {
            if let (Ok(stage_id), Ok(count)) =
                (stage_str.parse::<StageId>(), count_str.parse::<u32>())
            {
                adapter = adapter.with_transient_failure(stage_id, count);
            }
        }
    }
    // Test-only seam: JSON map from stage-id string to payload JSON.
    // Values may be a single object or an array of objects (payload sequence).
    // Example: {"completion_panel": {"outcome":"conditionally_approved",...}}
    // Example sequence: {"qa": [{"outcome":"request_changes",...}, {"outcome":"approved",...}]}
    if let Ok(overrides_json) = std::env::var("RALPH_BURNING_TEST_STAGE_OVERRIDES") {
        if let Ok(overrides) = serde_json::from_str::<
            std::collections::HashMap<String, serde_json::Value>,
        >(&overrides_json)
        {
            for (stage_str, payload) in overrides {
                if let Ok(stage_id) = stage_str.parse::<StageId>() {
                    if let Some(arr) = payload.as_array() {
                        adapter = adapter.with_stage_payload_sequence(stage_id, arr.clone());
                    } else {
                        adapter = adapter.with_stage_payload(stage_id, payload);
                    }
                }
            }
        }
    }

    adapter
}

async fn handle_start(overrides: RunBackendOverrideArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace version
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Resolve active project
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record
    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;

    // Validate run snapshot integrity
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;

    // Check preconditions before engine call (fail fast with clear errors)
    match run_snapshot.status {
        RunStatus::NotStarted => {}
        RunStatus::Failed | RunStatus::Paused => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; use `ralph-burning run resume`",
                    run_snapshot.status
                ),
            });
        }
        status => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; run start requires 'not_started'",
                    status
                ),
            });
        }
    }
    if run_snapshot.has_active_run() {
        return Err(AppError::RunStartFailed {
            reason: "project already has an active run".to_owned(),
        });
    }

    // Acquire per-project writer lock with lease record before any run-state mutation
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )?;

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;

    let agent_service = build_agent_execution_service()?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Starting run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::execute_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &current_dir,
        &project_id,
        project_record.flow,
        &effective_config,
    )
    .await?;

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = current_dir.join(format!(
            ".ralph-burning/daemon/leases/writer-{}.lock",
            project_id.as_str()
        ));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }
    Ok(())
}

async fn handle_resume(overrides: RunBackendOverrideArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match run_snapshot.status {
        RunStatus::Failed | RunStatus::Paused => {}
        RunStatus::NotStarted => {
            return Err(AppError::ResumeFailed {
                reason: "project has not started a run yet; use `ralph-burning run start`"
                    .to_owned(),
            });
        }
        RunStatus::Running => {
            return Err(AppError::ResumeFailed {
                reason: "project already has a running run; `run resume` only works from failed or paused snapshots".to_owned(),
            });
        }
        RunStatus::Completed => {
            return Err(AppError::ResumeFailed {
                reason: "project is already completed; there is nothing to resume".to_owned(),
            });
        }
    }
    if run_snapshot.has_active_run() {
        return Err(AppError::ResumeFailed {
            reason: "failed or paused snapshots must not retain an active run".to_owned(),
        });
    }

    // Acquire per-project writer lock with lease record before any run-state mutation
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )?;

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;
    let agent_service = build_agent_execution_service()?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Resuming run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::resume_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &FsArtifactStore,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &current_dir,
        &project_id,
        project_record.flow,
        &effective_config,
    )
    .await?;

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }

    Ok(())
}

async fn handle_status() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record before proceeding with run queries
    let project_store = FsProjectStore;
    let _ = project_store.read_project_record(&current_dir, &project_id)?;

    let run_store = FsRunSnapshotStore;
    let status = service::run_status(&run_store, &current_dir, &project_id)?;

    println!("Project: {}", status.project_id);
    println!("Status: {}", status.status);
    if let Some(ref stage) = status.stage {
        println!("Stage: {}", stage);
    }
    if let Some(cycle) = status.cycle {
        println!("Cycle: {}", cycle);
    }
    if let Some(round) = status.completion_round {
        println!("Completion round: {}", round);
    }
    println!("Summary: {}", status.summary);

    Ok(())
}

async fn handle_history() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record before proceeding with run queries
    let project_store = FsProjectStore;
    let _ = project_store.read_project_record(&current_dir, &project_id)?;

    let journal_store = FsJournalStore;
    let artifact_store = FsArtifactStore;

    let history = service::run_history(&journal_store, &artifact_store, &current_dir, &project_id)?;

    println!("Project: {}", history.project_id);
    println!("--- Journal Events ---");
    for event in &history.events {
        println!(
            "  [{}] {} - {:?}",
            event.sequence, event.timestamp, event.event_type
        );
    }

    if !history.payloads.is_empty() {
        println!("--- Payloads ---");
        for payload in &history.payloads {
            println!(
                "  {} ({}, cycle {}, attempt {})",
                payload.payload_id, payload.stage_id, payload.cycle, payload.attempt
            );
        }
    }

    if !history.artifacts.is_empty() {
        println!("--- Artifacts ---");
        for artifact in &history.artifacts {
            println!(
                "  {} (payload: {}, stage: {})",
                artifact.artifact_id, artifact.payload_id, artifact.stage_id
            );
        }
    }

    Ok(())
}

async fn handle_tail(include_logs: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record before proceeding with run queries
    let project_store = FsProjectStore;
    let _ = project_store.read_project_record(&current_dir, &project_id)?;

    let journal_store = FsJournalStore;
    let artifact_store = FsArtifactStore;
    let log_store = FsRuntimeLogStore;

    let tail = service::run_tail(
        &journal_store,
        &artifact_store,
        &log_store,
        &current_dir,
        &project_id,
        include_logs,
    )?;

    println!("Project: {}", tail.project_id);
    println!("--- Durable History ---");
    for event in &tail.events {
        println!(
            "  [{}] {} - {:?}",
            event.sequence, event.timestamp, event.event_type
        );
    }

    if !tail.payloads.is_empty() {
        println!("--- Payloads ---");
        for payload in &tail.payloads {
            println!(
                "  {} ({}, cycle {}, attempt {})",
                payload.payload_id, payload.stage_id, payload.cycle, payload.attempt
            );
        }
    }

    if !tail.artifacts.is_empty() {
        println!("--- Artifacts ---");
        for artifact in &tail.artifacts {
            println!(
                "  {} (payload: {}, stage: {})",
                artifact.artifact_id, artifact.payload_id, artifact.stage_id
            );
        }
    }

    if let Some(ref logs) = tail.runtime_logs {
        println!("--- Runtime Logs ---");
        if logs.is_empty() {
            println!("  (no runtime logs)");
        } else {
            for log in logs {
                println!(
                    "  [{}] {:?} [{}] {}",
                    log.timestamp, log.level, log.source, log.message
                );
            }
        }
    }

    Ok(())
}

async fn handle_rollback(target: String, hard: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;

    let target_stage = match target.parse::<StageId>() {
        Ok(stage_id) => stage_id,
        Err(_) => {
            return Err(AppError::RollbackStageNotInFlow {
                project_id: project_id.to_string(),
                stage_id: target,
                flow: project_record.flow.to_string(),
            });
        }
    };

    let rollback_point = service::perform_rollback(
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsRollbackPointStore,
        Some(&WorktreeAdapter),
        &current_dir,
        &project_id,
        project_record.flow,
        target_stage,
        hard,
    )?;

    println!(
        "Rollback complete: project '{}' paused at {} cycle {}.",
        project_id, rollback_point.stage_id, rollback_point.cycle
    );
    if hard {
        println!("Repository reset to recorded git SHA.");
    }

    Ok(())
}

fn parse_cli_backend_overrides(args: &RunBackendOverrideArgs) -> AppResult<CliBackendOverrides> {
    Ok(CliBackendOverrides {
        backend: parse_backend_selection_arg("backend", args.backend.as_deref())?,
        planner_backend: parse_backend_selection_arg(
            "planner_backend",
            args.planner_backend.as_deref(),
        )?,
        implementer_backend: parse_backend_selection_arg(
            "implementer_backend",
            args.implementer_backend.as_deref(),
        )?,
        reviewer_backend: parse_backend_selection_arg(
            "reviewer_backend",
            args.reviewer_backend.as_deref(),
        )?,
        qa_backend: parse_backend_selection_arg("qa_backend", args.qa_backend.as_deref())?,
    })
}

fn parse_backend_selection_arg(
    _key: &str,
    raw: Option<&str>,
) -> AppResult<Option<BackendSelection>> {
    raw.map(BackendSelection::from_backend_name).transpose()
}
