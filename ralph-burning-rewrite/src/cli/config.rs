use clap::{Args, Subcommand};

use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub command: ConfigSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ConfigSubcommand {
    Show,
    Get { key: String },
    Set { key: String, value: String },
    Edit,
}

pub async fn handle(command: ConfigCommand) -> AppResult<()> {
    let command_name = match command.command {
        ConfigSubcommand::Show => "config show",
        ConfigSubcommand::Get { .. } => "config get",
        ConfigSubcommand::Set { .. } => "config set",
        ConfigSubcommand::Edit => "config edit",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
