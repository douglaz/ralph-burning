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
    Show {
        #[arg(long)]
        project: bool,
    },
    Get {
        key: String,
        #[arg(long)]
        project: bool,
    },
    Set {
        key: String,
        value: String,
        #[arg(long)]
        project: bool,
    },
    Edit {
        #[arg(long)]
        project: bool,
    },
}

pub async fn handle(command: ConfigCommand) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    match command.command {
        ConfigSubcommand::Show { project } => {
            let config = load_effective_config(&current_dir, project)?;
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
        ConfigSubcommand::Get { key, project } => {
            let entry = if project {
                let project_id = workspace_governance::resolve_active_project(&current_dir)?;
                EffectiveConfig::get_project(&current_dir, &project_id, &key)?
            } else {
                EffectiveConfig::load(&current_dir)?.get(&key)?
            };
            println!("{}", entry.value.display_value());
            Ok(())
        }
        ConfigSubcommand::Set {
            key,
            value,
            project,
        } => {
            let entry = if project {
                let project_id = workspace_governance::resolve_active_project(&current_dir)?;
                EffectiveConfig::set_project(&current_dir, &project_id, &key, &value)?
            } else {
                EffectiveConfig::set(&current_dir, &key, &value)?
            };
            println!(
                "Updated {} = {} in {}",
                entry.key,
                entry.value.display_value(),
                if project { "project config.toml" } else { "workspace.toml" }
            );
            Ok(())
        }
        ConfigSubcommand::Edit { project } => {
            let _ = load_effective_config(&current_dir, project)?;
            let config_path = if project {
                let project_id = workspace_governance::resolve_active_project(&current_dir)?;
                let existing =
                    crate::adapters::fs::FileSystem::read_project_config(&current_dir, &project_id)?;
                crate::adapters::fs::FileSystem::write_project_config(
                    &current_dir,
                    &project_id,
                    &existing,
                )?;
                crate::adapters::fs::FileSystem::project_policy_config_path(&current_dir, &project_id)
            } else {
                current_dir
                    .join(workspace_governance::WORKSPACE_DIR)
                    .join(workspace_governance::WORKSPACE_CONFIG_FILE)
            };
            FileSystem::open_editor(&config_path)?;
            match load_effective_config(&current_dir, project) {
                Ok(_) => {
                    println!(
                        "Validated {}",
                        if project { "project config.toml" } else { "workspace.toml" }
                    );
                    Ok(())
                }
                Err(error) => {
                    eprintln!(
                        "{} is invalid after editing: {error}. Fix the file manually.",
                        if project { "project config.toml" } else { "workspace.toml" }
                    );
                    Err(error)
                }
            }
        }
    }
}

fn load_effective_config(base_dir: &std::path::Path, project: bool) -> AppResult<EffectiveConfig> {
    if project {
        let project_id = workspace_governance::resolve_active_project(base_dir)?;
        EffectiveConfig::load_for_project(
            base_dir,
            Some(&project_id),
            crate::contexts::workspace_governance::config::CliBackendOverrides::default(),
        )
    } else {
        EffectiveConfig::load(base_dir)
    }
}
