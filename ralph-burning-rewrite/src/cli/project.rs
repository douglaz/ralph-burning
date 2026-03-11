use std::path::PathBuf;

use clap::{Args, Subcommand};

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
    let command_name = match command.command {
        ProjectSubcommand::Create(_) => "project create",
        ProjectSubcommand::Select { .. } => "project select",
        ProjectSubcommand::List => "project list",
        ProjectSubcommand::Show { .. } => "project show",
        ProjectSubcommand::Delete { .. } => "project delete",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
