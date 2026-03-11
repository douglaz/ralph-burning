use std::path::PathBuf;

use chrono::Utc;
use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FileSystem, FsActiveProjectStore, FsJournalStore, FsProjectStore, FsRunSnapshotStore,
};
use crate::contexts::project_run_record::model::ProjectStatusSummary;
use crate::contexts::project_run_record::service::{self, CreateProjectInput};
use crate::contexts::workspace_governance;
use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct ProjectCommand {
    #[command(subcommand)]
    pub command: ProjectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ProjectSubcommand {
    Create(ProjectCreateArgs),
    Select { id: String },
    List,
    Show { id: Option<String> },
    Delete { id: String },
}

#[derive(Debug, Args)]
pub struct ProjectCreateArgs {
    #[arg(long)]
    pub id: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub prompt: PathBuf,
    #[arg(long)]
    pub flow: String,
}

pub async fn handle(command: ProjectCommand) -> AppResult<()> {
    match command.command {
        ProjectSubcommand::Select { id } => {
            let current_dir = std::env::current_dir()?;
            let project_id = ProjectId::new(id)?;
            workspace_governance::set_active_project(&current_dir, &project_id)?;
            println!("Selected project {}", project_id);
            Ok(())
        }
        ProjectSubcommand::Create(args) => handle_create(args).await,
        ProjectSubcommand::List => handle_list().await,
        ProjectSubcommand::Show { id } => handle_show(id).await,
        ProjectSubcommand::Delete { id } => handle_delete(id).await,
    }
}

async fn handle_create(args: ProjectCreateArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace version
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Validate project ID
    let project_id = ProjectId::new(args.id)?;

    // Validate flow preset
    let flow: FlowPreset = args.flow.parse()?;

    // Validate prompt file is readable
    let prompt_path = if args.prompt.is_absolute() {
        args.prompt.clone()
    } else {
        current_dir.join(&args.prompt)
    };

    let prompt_contents =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::InvalidPrompt {
            path: args.prompt.display().to_string(),
            reason: e.to_string(),
        })?;

    if prompt_contents.trim().is_empty() {
        return Err(AppError::InvalidPrompt {
            path: args.prompt.display().to_string(),
            reason: "prompt file is empty".to_owned(),
        });
    }

    let prompt_hash = FileSystem::prompt_hash(&prompt_contents);

    let store = FsProjectStore;
    let journal_store = FsJournalStore;

    let input = CreateProjectInput {
        id: project_id,
        name: args.name,
        flow,
        prompt_path: args.prompt.display().to_string(),
        prompt_contents,
        prompt_hash,
        created_at: Utc::now(),
    };

    let record = service::create_project(&store, &journal_store, &current_dir, input)?;

    println!(
        "Created project '{}' with flow '{}'",
        record.id, record.flow
    );
    Ok(())
}

async fn handle_list() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsProjectStore;
    let active_store = FsActiveProjectStore;

    let entries = service::list_projects(&store, &active_store, &current_dir)?;

    if entries.is_empty() {
        println!("No projects found.");
        return Ok(());
    }

    for entry in &entries {
        let active_marker = if entry.is_active { " *" } else { "" };
        let status = match entry.status_summary {
            ProjectStatusSummary::Created => "created",
            ProjectStatusSummary::Active => "active",
            ProjectStatusSummary::Completed => "completed",
            ProjectStatusSummary::Failed => "failed",
        };
        println!(
            "  {}{} ({}) [{}] - {}",
            entry.id, active_marker, entry.flow, status, entry.name
        );
    }

    Ok(())
}

async fn handle_show(id: Option<String>) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Resolve project ID: explicit or active project
    let project_id = match id {
        Some(raw) => ProjectId::new(raw)?,
        None => workspace_governance::resolve_active_project(&current_dir)?,
    };

    let store = FsProjectStore;
    let run_store = FsRunSnapshotStore;
    let journal_store = FsJournalStore;
    let active_store = FsActiveProjectStore;

    let detail = service::show_project(
        &store,
        &run_store,
        &journal_store,
        &active_store,
        &current_dir,
        &project_id,
    )?;

    let active_label = if detail.is_active { " (active)" } else { "" };
    println!("Project: {}{}", detail.record.id, active_label);
    println!("Name: {}", detail.record.name);
    println!("Flow: {}", detail.record.flow);
    println!("Prompt reference: {}", detail.record.prompt_reference);
    println!("Prompt hash: {}", detail.record.prompt_hash);
    println!("Created: {}", detail.record.created_at);
    println!("Run status: {}", detail.run_snapshot.status_summary);
    println!("Journal events: {}", detail.journal_event_count);
    println!("Rollback points: {}", detail.rollback_count);

    Ok(())
}

async fn handle_delete(id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = ProjectId::new(id)?;

    let store = FsProjectStore;
    let run_store = FsRunSnapshotStore;
    let active_store = FsActiveProjectStore;

    service::delete_project(&store, &run_store, &active_store, &current_dir, &project_id)?;

    println!("Deleted project '{}'", project_id);
    Ok(())
}
