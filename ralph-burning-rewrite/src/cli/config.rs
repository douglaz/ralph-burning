use clap::{Args, Subcommand};

use crate::adapters::fs::FileSystem;
use crate::contexts::workspace_governance::{self, EffectiveConfig};
use crate::shared::error::AppResult;

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
    let current_dir = std::env::current_dir()?;

    match command.command {
        ConfigSubcommand::Show => {
            let config = EffectiveConfig::load(&current_dir)?;
            println!("[settings]");
            for entry in config.entries() {
                println!(
                    "{} = {} # source: {}",
                    entry.key,
                    entry.value.toml_like_value(),
                    entry.source
                );
            }
            Ok(())
        }
        ConfigSubcommand::Get { key } => {
            let entry = EffectiveConfig::load(&current_dir)?.get(&key)?;
            println!("{}", entry.value.display_value());
            Ok(())
        }
        ConfigSubcommand::Set { key, value } => {
            let entry = EffectiveConfig::set(&current_dir, &key, &value)?;
            println!(
                "Updated {} = {} in workspace.toml",
                entry.key,
                entry.value.display_value()
            );
            Ok(())
        }
        ConfigSubcommand::Edit => {
            let _ = EffectiveConfig::load(&current_dir)?;
            let config_path = current_dir
                .join(workspace_governance::WORKSPACE_DIR)
                .join(workspace_governance::WORKSPACE_CONFIG_FILE);
            FileSystem::open_editor(&config_path)?;
            match EffectiveConfig::load(&current_dir) {
                Ok(_) => {
                    println!("Validated workspace.toml");
                    Ok(())
                }
                Err(error) => {
                    eprintln!(
                        "workspace.toml is invalid after editing: {error}. Fix the file manually."
                    );
                    Err(error)
                }
            }
        }
    }
}
