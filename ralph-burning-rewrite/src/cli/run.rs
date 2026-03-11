use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FsArtifactStore, FsJournalStore, FsProjectStore, FsRunSnapshotStore, FsRuntimeLogStore,
};
use crate::contexts::project_run_record::service::{self, ProjectStorePort};
use crate::contexts::workspace_governance;
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
        RunSubcommand::Start => Err(AppError::NotYetImplemented {
            command: "run start".to_owned(),
        }),
        RunSubcommand::Resume => Err(AppError::NotYetImplemented {
            command: "run resume".to_owned(),
        }),
        RunSubcommand::Rollback { .. } => Err(AppError::NotYetImplemented {
            command: "run rollback".to_owned(),
        }),
    }
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

    let history = service::run_history(
        &journal_store,
        &artifact_store,
        &current_dir,
        &project_id,
    )?;

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
