use clap::{Args, Subcommand};

use crate::adapters::br_process::{BrAdapter, OsProcessRunner};
use crate::adapters::fs::{FsJournalStore, FsProjectStore, FsRunSnapshotStore};
use crate::contexts::bead_workflow::pr_open::{
    open_pr_for_completed_run, PrOpenError, PrOpenRequest, PrOpenStores, ProcessPrToolPort,
};
use crate::contexts::workspace_governance;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
#[command(about = "Pull request automation for completed runs.")]
pub struct PrCommand {
    #[command(subcommand)]
    pub command: PrSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum PrSubcommand {
    #[command(about = "Squash checkpoint commits, push, and open a PR for a completed run.")]
    Open(PrOpenArgs),
}

#[derive(Debug, Args)]
pub struct PrOpenArgs {
    #[arg(long = "bead-id")]
    pub bead_id: Option<String>,
    #[arg(long)]
    pub skip_gates: bool,
}

pub async fn handle(command: PrCommand) -> AppResult<()> {
    match command.command {
        PrSubcommand::Open(args) => handle_open(args).await,
    }
}

async fn handle_open(args: PrOpenArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;
    let br = BrAdapter::with_runner(OsProcessRunner::new()).with_working_dir(current_dir.clone());
    let output = open_pr_for_completed_run(
        PrOpenRequest {
            base_dir: &current_dir,
            project_id: &project_id,
            bead_id_override: args.bead_id.as_deref(),
            skip_gates: args.skip_gates,
        },
        PrOpenStores {
            project_store: &FsProjectStore,
            run_store: &FsRunSnapshotStore,
            journal_store: &FsJournalStore,
        },
        &br,
        &ProcessPrToolPort,
    )
    .await
    .map_err(map_pr_open_error)?;

    for warning in output.warnings {
        eprintln!("warning: {warning}");
    }
    println!("{}", output.pr_url);
    Ok(())
}

fn map_pr_open_error(error: PrOpenError) -> AppError {
    AppError::PrOpenFailed {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, Commands};

    use super::*;

    #[test]
    fn pr_open_parses_options() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "pr",
            "open",
            "--bead-id",
            "2qlo",
            "--skip-gates",
        ]);
        let Commands::Pr(command) = cli.command else {
            panic!("expected pr command");
        };
        let PrSubcommand::Open(args) = command.command;
        assert_eq!(args.bead_id.as_deref(), Some("2qlo"));
        assert!(args.skip_gates);
    }
}
