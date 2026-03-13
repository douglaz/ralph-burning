pub mod config;
pub mod conformance;
pub mod daemon;
pub mod flow;
pub mod init;
pub mod project;
pub mod requirements;
pub mod run;

use clap::{Parser, Subcommand};

use crate::shared::error::AppResult;

#[derive(Debug, Parser)]
#[command(
    name = "ralph-burning",
    version,
    about = "Bootstrap and inspect ralph-burning workspaces and built-in flows."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Init(init::InitCommand),
    Flow(flow::FlowCommand),
    Config(config::ConfigCommand),
    Project(project::ProjectCommand),
    Run(run::RunCommand),
    Requirements(requirements::RequirementsCommand),
    Daemon(daemon::DaemonCommand),
    Conformance(conformance::ConformanceCommand),
}

pub async fn run(cli: Cli) -> AppResult<()> {
    match cli.command {
        Commands::Init(command) => init::handle(command).await,
        Commands::Flow(command) => flow::handle(command).await,
        Commands::Config(command) => config::handle(command).await,
        Commands::Project(command) => project::handle(command).await,
        Commands::Run(command) => run::handle(command).await,
        Commands::Requirements(command) => requirements::handle(command).await,
        Commands::Daemon(command) => daemon::handle(command).await,
        Commands::Conformance(command) => conformance::handle(command).await,
    }
}
