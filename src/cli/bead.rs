use chrono::Utc;
use clap::{Args, Subcommand};
use std::path::PathBuf;

use crate::adapters::fs::{FsJournalStore, FsProjectStore};
use crate::contexts::bead_workflow::create_project::{
    create_project_from_bead, BeadProjectCreationError, CreateProjectFromBeadInput,
    GitFeatureBranchPort, ProcessBeadProjectBrPort,
};
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
#[command(
    about = "Manage bead-backed project creation.",
    long_about = "Manage bead-backed project creation for drain-loop workflows."
)]
pub struct BeadCommand {
    #[arg(long = "br-path", value_name = "PATH", global = true)]
    pub br_path: Option<PathBuf>,
    #[command(subcommand)]
    pub command: BeadSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum BeadSubcommand {
    #[command(
        about = "Create a Ralph project from a bead id without starting a run.",
        long_about = "Create a Ralph project from a bead id without starting a run.\n\nExample: ralph-burning bead create-project d31l --flow minimal\nExample: ralph-burning bead create-project d31l --branch"
    )]
    CreateProject(BeadCreateProjectArgs),
}

#[derive(Debug, Args)]
pub struct BeadCreateProjectArgs {
    pub bead_id: String,
    #[arg(long)]
    pub flow: Option<String>,
    #[arg(
        long,
        value_name = "NAME",
        num_args = 0..=1,
        default_missing_value = "",
        help = "Create a feature branch. Omit NAME to derive feat/<bead-id>-<short-slug>."
    )]
    pub branch: Option<String>,
}

pub async fn handle(command: BeadCommand) -> AppResult<()> {
    let br_path = crate::cli::resolve_br_path_for_command(command.br_path.as_deref())?;
    match command.command {
        BeadSubcommand::CreateProject(args) => handle_create_project(args, br_path).await,
    }
}

async fn handle_create_project(args: BeadCreateProjectArgs, br_path: PathBuf) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let effective_config = EffectiveConfig::load(&current_dir)?;
    let flow = args
        .flow
        .as_deref()
        .map(str::parse)
        .transpose()?
        .unwrap_or_else(|| effective_config.default_flow());

    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    let br = ProcessBeadProjectBrPort::with_br_binary(current_dir.clone(), br_path);
    let branch_port = GitFeatureBranchPort;
    let output = create_project_from_bead(
        &store,
        &journal_store,
        &br,
        &branch_port,
        &current_dir,
        CreateProjectFromBeadInput {
            bead_id: args.bead_id.clone(),
            flow,
            branch: args.branch,
            created_at: Utc::now(),
            prior_failure_context: None,
        },
    )
    .await
    .map_err(map_bead_project_error)?;

    println!(
        "Created project '{}' from bead '{}'",
        output.project.id, args.bead_id
    );
    if let Some(branch_name) = output.branch_name {
        println!("Created branch '{branch_name}'");
    }
    Ok(())
}

fn map_bead_project_error(error: BeadProjectCreationError) -> AppError {
    AppError::BeadProjectCreationFailed {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use crate::cli::{Cli, Commands};

    use super::*;

    #[test]
    fn bead_create_project_parses_bead_id_and_options() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "bead",
            "create-project",
            "d31l",
            "--branch",
            "feat/custom",
            "--flow",
            "standard",
        ]);
        let Commands::Bead(command) = cli.command else {
            panic!("expected bead command");
        };
        let BeadSubcommand::CreateProject(args) = command.command;
        assert!(command.br_path.is_none());
        assert_eq!(args.bead_id, "d31l");
        assert_eq!(args.branch.as_deref(), Some("feat/custom"));
        assert_eq!(args.flow.as_deref(), Some("standard"));
    }

    #[test]
    fn bead_global_br_path_parses_before_subcommand() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "bead",
            "--br-path",
            "/opt/beads/bin/br",
            "create-project",
            "d31l",
        ]);
        let Commands::Bead(command) = cli.command else {
            panic!("expected bead command");
        };
        assert_eq!(
            command.br_path.as_deref(),
            Some(std::path::Path::new("/opt/beads/bin/br"))
        );
    }

    #[test]
    fn bead_create_project_help_includes_usage_example() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("bead"));

        let mut command = Cli::command();
        let help = command
            .find_subcommand_mut("bead")
            .and_then(|bead| bead.find_subcommand_mut("create-project"))
            .expect("create-project subcommand")
            .render_long_help()
            .to_string();
        assert!(help.contains("ralph-burning bead create-project d31l --flow minimal"));
    }
}
