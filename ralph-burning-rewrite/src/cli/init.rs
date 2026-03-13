use chrono::Utc;
use clap::Args;

use crate::contexts::workspace_governance;
use crate::shared::error::AppResult;

#[derive(Debug, Args, Default)]
pub struct InitCommand {}

pub async fn handle(_command: InitCommand) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let initialization = workspace_governance::initialize_workspace(&current_dir, Utc::now())?;
    println!(
        "Initialized workspace at {}",
        initialization.workspace_root.display()
    );
    Ok(())
}
