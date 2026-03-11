use clap::{Args, Subcommand};

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
    let command_name = match command.command {
        RunSubcommand::Start => "run start",
        RunSubcommand::Resume => "run resume",
        RunSubcommand::Status => "run status",
        RunSubcommand::History => "run history",
        RunSubcommand::Tail { .. } => "run tail",
        RunSubcommand::Rollback { .. } => "run rollback",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
