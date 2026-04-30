pub mod backend;
pub mod bead;
pub mod config;
pub mod conformance;
pub mod daemon;
pub mod flow;
pub mod init;
pub mod milestone;
pub mod pr;
pub mod project;
pub mod requirements;
pub mod run;
pub mod task;

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
    Milestone(milestone::MilestoneCommand),
    Pr(pr::PrCommand),
    Project(project::ProjectCommand),
    Run(run::RunCommand),
    Requirements(requirements::RequirementsCommand),
    Daemon(daemon::DaemonCommand),
    Backend(backend::BackendCommand),
    Conformance(conformance::ConformanceCommand),
    Task(task::TaskCommand),
    Bead(bead::BeadCommand),
}

pub async fn run(cli: Cli) -> AppResult<()> {
    match cli.command {
        Commands::Init(command) => init::handle(command).await,
        Commands::Flow(command) => flow::handle(command).await,
        Commands::Config(command) => config::handle(command).await,
        Commands::Milestone(command) => milestone::handle(command).await,
        Commands::Pr(command) => pr::handle(command).await,
        Commands::Project(command) => project::handle(command).await,
        Commands::Run(command) => run::handle(command).await,
        Commands::Requirements(command) => requirements::handle(command).await,
        Commands::Daemon(command) => daemon::handle(command).await,
        Commands::Backend(command) => backend::handle(command).await,
        Commands::Conformance(command) => conformance::handle(command).await,
        Commands::Task(command) => task::handle(command).await,
        Commands::Bead(command) => bead::handle(command).await,
    }
}
