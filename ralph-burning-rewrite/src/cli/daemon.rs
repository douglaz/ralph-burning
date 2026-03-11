use clap::{Args, Subcommand};

use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct DaemonCommand {
    #[command(subcommand)]
    pub command: DaemonSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonSubcommand {
    Start,
    Status,
    Abort { task_id: String },
    Retry { task_id: String },
    Reconcile,
}

pub async fn handle(command: DaemonCommand) -> AppResult<()> {
    let command_name = match command.command {
        DaemonSubcommand::Start => "daemon start",
        DaemonSubcommand::Status => "daemon status",
        DaemonSubcommand::Abort { .. } => "daemon abort",
        DaemonSubcommand::Retry { .. } => "daemon retry",
        DaemonSubcommand::Reconcile => "daemon reconcile",
    };

    Err(AppError::NotYetImplemented {
        command: command_name.to_owned(),
    })
}
