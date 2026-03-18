use clap::{Args, Subcommand};

use crate::composition::agent_execution_builder;
use crate::contexts::workspace_governance::EffectiveConfig;
use crate::shared::error::AppResult;

#[derive(Debug, Args)]
pub struct RequirementsCommand {
    #[command(subcommand)]
    pub command: RequirementsSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RequirementsSubcommand {
    Draft {
        #[arg(long)]
        idea: String,
    },
    Quick {
        #[arg(long)]
        idea: String,
    },
    Show {
        run_id: String,
    },
    Answer {
        run_id: String,
    },
}

pub async fn handle(command: RequirementsCommand) -> AppResult<()> {
    let base_dir = std::env::current_dir()?;

    // Ensure workspace exists
    let _ = crate::contexts::workspace_governance::load_workspace_config(&base_dir)?;

    // Load effective config for workspace backend/model defaults
    let effective_config = EffectiveConfig::load(&base_dir)?;

    let service = agent_execution_builder::build_requirements_service(&effective_config)?;

    match command.command {
        RequirementsSubcommand::Draft { idea } => {
            let now = chrono::Utc::now();
            let run_id = service.draft(&base_dir, &idea, now).await?;
            println!("Requirements run created: {run_id}");
        }
        RequirementsSubcommand::Quick { idea } => {
            let now = chrono::Utc::now();
            let run_id = service.quick(&base_dir, &idea, now).await?;
            println!("Requirements run completed: {run_id}");
        }
        RequirementsSubcommand::Show { run_id } => {
            let result = service.show(&base_dir, &run_id)?;
            println!("Run ID:           {}", result.run.run_id);
            println!("Mode:             {}", result.run.mode);
            println!("Status:           {}", result.run.status);
            println!("Question Round:   {}", result.run.question_round);

            // Stage-aware progress for full mode
            if let Some(ref stage) = result.run.current_stage {
                println!("Current Stage:    {}", stage.display_name());
            }
            if !result.run.committed_stages.is_empty() {
                let stages: Vec<&str> = result.run.committed_stages.keys().map(|s| s.as_str()).collect();
                println!("Completed Stages: {}", stages.join(", "));
            }
            if result.run.quick_revision_count > 0 {
                println!("Quick Revisions:  {}", result.run.quick_revision_count);
            }
            if result.run.last_transition_cached {
                println!("Last Transition:  cached (reused)");
            }

            if let Some(ref summary) = result.failure_summary {
                println!("Last Failure:     {summary}");
            }
            if let Some(count) = result.pending_question_count {
                println!("Pending Questions: {count}");
            }
            if let Some(flow) = result.recommended_flow {
                println!("Recommended Flow: {flow}");
            }
            if let Some(ref path) = result.seed_prompt_path {
                println!("Seed Prompt:      {}", path.display());
                // Read seed/project.json for the suggested create command
                let seed_project_path = path.parent().unwrap().join("project.json");
                if let Ok(raw) = std::fs::read_to_string(&seed_project_path) {
                    if let Ok(seed) = serde_json::from_str::<
                        crate::contexts::requirements_drafting::model::ProjectSeedPayload,
                    >(&raw)
                    {
                        println!();
                        println!("Suggested command:");
                        println!(
                            "  ralph-burning project create --id {} --name \"{}\" --flow {} --prompt {}",
                            seed.project_id,
                            seed.project_name,
                            seed.flow,
                            path.display()
                        );
                    }
                }
            }
        }
        RequirementsSubcommand::Answer { run_id } => {
            service.answer(&base_dir, &run_id).await?;
            println!("Answers submitted for run: {run_id}");
        }
    }

    Ok(())
}
