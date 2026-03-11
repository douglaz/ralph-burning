use clap::{Args, Subcommand};

use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct ConformanceCommand {
    #[command(subcommand)]
    pub command: ConformanceSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ConformanceSubcommand {
    List,
    Run {
        #[arg(long)]
        filter: Option<String>,
    },
}

pub async fn handle(command: ConformanceCommand) -> AppResult<()> {
    let command_name = match command.command {
        ConformanceSubcommand::List => "conformance list",
        ConformanceSubcommand::Run { .. } => "conformance run",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
