use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore,
    FsProjectStore, FsRawOutputStore, FsRunSnapshotStore, FsRunSnapshotWriteStore,
    FsRuntimeLogStore, FsRuntimeLogWriteStore, FsSessionStore,
};
use crate::adapters::stub_backend::StubBackendAdapter;
use crate::contexts::agent_execution::service::AgentExecutionService;
use crate::contexts::project_run_record::model::RunStatus;
use crate::contexts::project_run_record::service::{self, ProjectStorePort, RunSnapshotPort};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{FlowPreset, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct RunCommand {
    #[command(subcommand)]
    pub command: RunSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RunSubcommand {
    Start,
    Resume,
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

pub async fn handle(command: RunCommand) -> AppResult<()> {
    match command.command {
        RunSubcommand::Status => handle_status().await,
        RunSubcommand::History => handle_history().await,
        RunSubcommand::Tail { logs } => handle_tail(logs).await,
        RunSubcommand::Start => handle_start().await,
        RunSubcommand::Resume => handle_resume().await,
        RunSubcommand::Rollback { .. } => Err(AppError::NotYetImplemented {
            command: "run rollback".to_owned(),
        }),
    }
}

pub fn build_agent_execution_service(
) -> AgentExecutionService<StubBackendAdapter, FsRawOutputStore, FsSessionStore> {
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

    AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore)
}

async fn handle_start() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace version
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Resolve active project
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record
    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;

    // Only standard flow is supported in this slice
    if project_record.flow != FlowPreset::Standard {
        return Err(AppError::UnsupportedFlow {
            flow_id: project_record.flow.as_str().to_owned(),
        });
    }

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

    let effective_config = EffectiveConfig::load(&current_dir)?;

    let agent_service = build_agent_execution_service();
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Starting run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::execute_standard_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &current_dir,
        &project_id,
        &effective_config,
    )
    .await?;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }
    Ok(())
}

async fn handle_resume() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;
    if project_record.flow != FlowPreset::Standard {
        return Err(AppError::UnsupportedFlow {
            flow_id: project_record.flow.as_str().to_owned(),
        });
    }

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

    let effective_config = EffectiveConfig::load(&current_dir)?;
    let agent_service = build_agent_execution_service();
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Resuming run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::resume_standard_run(
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
        &effective_config,
    )
    .await?;

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
