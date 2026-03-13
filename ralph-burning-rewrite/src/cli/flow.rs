use clap::{Args, Subcommand};

use crate::contexts::workflow_composition::{built_in_flows, flow_definition_by_id};
use crate::shared::error::AppResult;

#[derive(Debug, Args)]
pub struct FlowCommand {
    #[command(subcommand)]
    pub command: FlowSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum FlowSubcommand {
    List,
    Show { flow_id: String },
}

pub async fn handle(command: FlowCommand) -> AppResult<()> {
    match command.command {
        FlowSubcommand::List => {
            for definition in built_in_flows() {
                println!("{} - {}", definition.preset, definition.description);
            }
            Ok(())
        }
        FlowSubcommand::Show { flow_id } => {
            let definition = flow_definition_by_id(&flow_id)?;
            println!("Flow: {}", definition.preset);
            println!("Description: {}", definition.description);
            println!(
                "Validation profile: {} ({})",
                definition.validation_profile.name, definition.validation_profile.summary
            );
            println!(
                "Final review enabled: {}",
                if definition.validation_profile.final_review_enabled {
                    "yes"
                } else {
                    "no"
                }
            );
            println!("Stage count: {}", definition.stages.len());
            println!();
            println!("Stages:");
            for (index, stage) in definition.stages.iter().enumerate() {
                println!("{}. {}", index + 1, stage);
            }
            Ok(())
        }
    }
}
