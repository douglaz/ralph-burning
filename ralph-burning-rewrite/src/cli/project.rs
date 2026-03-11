use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::contexts::workspace_governance;
use crate::shared::domain::ProjectId;
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
        ProjectSubcommand::Create(_) => Err(AppError::NotYetImplemented {
            command: "project create".to_owned(),
        }),
        ProjectSubcommand::List => Err(AppError::NotYetImplemented {
            command: "project list".to_owned(),
        }),
        ProjectSubcommand::Show { .. } => Err(AppError::NotYetImplemented {
            command: "project show".to_owned(),
        }),
        ProjectSubcommand::Delete { .. } => Err(AppError::NotYetImplemented {
            command: "project delete".to_owned(),
        }),
    }
}
