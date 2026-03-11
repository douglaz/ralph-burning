use clap::{Args, Subcommand};

use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct RequirementsCommand {
    #[command(subcommand)]
    pub command: RequirementsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RequirementsSubcommand {
    Draft {
        #[arg(long)]
        idea: String,
    },
    Quick {
        #[arg(long)]
        idea: String,
    },
    Show {
        run_id: String,
    },
    Answer {
        run_id: String,
    },
}

pub async fn handle(command: RequirementsCommand) -> AppResult<()> {
    let command_name = match command.command {
        RequirementsSubcommand::Draft { .. } => "requirements draft",
        RequirementsSubcommand::Quick { .. } => "requirements quick",
        RequirementsSubcommand::Show { .. } => "requirements show",
        RequirementsSubcommand::Answer { .. } => "requirements answer",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
