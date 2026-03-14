use clap::{Args, Subcommand};

use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
use crate::adapters::stub_backend::StubBackendAdapter;
use crate::contexts::agent_execution::service::{AgentExecutionService, BackendSelectionConfig};
use crate::contexts::requirements_drafting::service::RequirementsService;
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
    let workspace_defaults = BackendSelectionConfig::from_effective_config(&effective_config)?;

    let mut adapter = StubBackendAdapter::default();

    // Test-only seam: JSON map from label string to payload JSON for requirements contracts.
    // Example: {"question_set": {"questions": [{"id":"q1","prompt":"...","required":true}]}}
    if let Ok(overrides_json) = std::env::var("RALPH_BURNING_TEST_LABEL_OVERRIDES") {
        if let Ok(overrides) = serde_json::from_str::<
            std::collections::HashMap<String, serde_json::Value>,
        >(&overrides_json)
        {
            for (label, payload) in overrides {
                // Support both short labels ("question_set") and full labels
                // ("requirements:question_set"). The invocation contract uses the
                // full "requirements:<stage>" form internally.
                let full_label = if label.starts_with("requirements:") {
                    label
                } else {
                    format!("requirements:{label}")
                };
                adapter = adapter.with_label_payload(full_label, payload);
            }
        }
    }

    let raw_output_store = FsRawOutputStore;
    let session_store = FsSessionStore;
    let agent_service = AgentExecutionService::new(adapter, raw_output_store, session_store);
    let requirements_store = FsRequirementsStore;
    let service = RequirementsService::new(agent_service, requirements_store)
        .with_workspace_defaults(workspace_defaults);

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
