use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use clap::{ArgGroup, Args, Subcommand};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::adapters::br_health::{beads_health_failure_details, check_beads_health};
use crate::adapters::br_models::{BeadDetail, BeadStatus, BeadSummary, DependencyKind};
use crate::adapters::br_process::{
    BrAdapter, BrCommand, BrError, BrMutationAdapter, SyncIfDirtyHealthError,
};
use crate::adapters::fs::{
    FileSystem, FsActiveProjectStore, FsAmendmentQueueStore, FsDaemonStore, FsJournalStore,
    FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
    FsMilestoneSnapshotStore, FsMilestoneStore, FsPayloadArtifactWriteStore, FsProjectStore,
    FsRequirementsStore, FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
};
use crate::composition::agent_execution_builder;
use crate::contexts::automation_runtime::cli_writer_lease::{
    CliWriterLeaseGuard, CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};
use crate::contexts::milestone_record::bead_refs::{
    br_show_output_indicates_missing, milestone_bead_refs_match,
};
use crate::contexts::milestone_record::bundle::{bead_matches_implicit_slot, MilestoneBundle};
use crate::contexts::milestone_record::controller::{
    self as milestone_controller, MilestoneControllerState,
};
use crate::contexts::milestone_record::model::{MilestoneId, MilestoneStatus};
use crate::contexts::milestone_record::service::{
    self as milestone_service, MilestonePlanPort, MilestoneSnapshotPort, MilestoneStorePort,
};
use crate::contexts::project_run_record::model::{ProjectDetail, ProjectStatusSummary, RunStatus};
use crate::contexts::project_run_record::service::{
    self, default_project_id_for_bead, BeadDependencyPromptContext, BeadProjectContext,
    CreateProjectFromBeadContextInput, CreateProjectInput, PlannedElsewherePromptContext,
    ProjectStorePort, RunSnapshotPort,
};
use crate::contexts::requirements_drafting::model::{
    ProjectSeedPayload, RequirementsOutputKind, RequirementsStatus, SUPPORTED_SEED_VERSIONS,
};
use crate::contexts::requirements_drafting::service::{
    self as requirements_service, MilestoneBundleHandoff, RequirementsStorePort, SeedHandoff,
};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::{AppError, AppResult};
use crate::shared::text::truncate_with_ascii_ellipsis;

const PLANNED_ELSEWHERE_MAX_ITEMS: usize = 6;
const PLANNED_ELSEWHERE_MAX_BYTES: usize = 1536;
const PLANNED_ELSEWHERE_SUMMARY_MAX_BYTES: usize = 240;

#[derive(Debug, Args)]
pub struct ProjectCommand {
    #[command(subcommand)]
    pub command: ProjectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ProjectSubcommand {
    Create(ProjectCreateArgs),
    CreateFromBead(CreateFromBeadArgs),
    Bootstrap(BootstrapArgs),
    Select { id: String },
    List,
    Show { id: Option<String> },
    Delete { id: String },
    Amend(AmendCommand),
}

#[derive(Debug, Args)]
pub struct AmendCommand {
    #[command(subcommand)]
    pub command: AmendSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum AmendSubcommand {
    Add(AmendAddArgs),
    List,
    Remove { id: String },
    Clear,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("amend_input")
        .required(true)
        .multiple(false)
        .args(["text", "file"])
))]
pub struct AmendAddArgs {
    #[arg(long, group = "amend_input")]
    pub text: Option<String>,
    #[arg(long, group = "amend_input")]
    pub file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ProjectCreateArgs {
    #[arg(long, required_unless_present = "from_requirements")]
    pub id: Option<String>,
    #[arg(long, required_unless_present = "from_requirements")]
    pub name: Option<String>,
    #[arg(long, required_unless_present = "from_requirements")]
    pub prompt: Option<PathBuf>,
    #[arg(long)]
    pub flow: Option<String>,
    #[arg(
        long = "from-requirements",
        conflicts_with_all = ["id", "name", "prompt", "flow"]
    )]
    pub from_requirements: Option<String>,
}

#[derive(Debug, Args)]
pub struct CreateFromBeadArgs {
    #[arg(long = "milestone-id")]
    pub milestone_id: String,
    #[arg(long = "bead-id")]
    pub bead_id: String,
    #[arg(long = "project-id")]
    pub project_id: Option<String>,
    #[arg(long = "prompt-file")]
    pub prompt_file: Option<PathBuf>,
    #[arg(long)]
    pub flow: Option<String>,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("bootstrap_input")
        .required(true)
        .multiple(false)
        .args(["idea", "from_file", "from_seed"])
))]
pub struct BootstrapArgs {
    #[arg(long, group = "bootstrap_input")]
    pub idea: Option<String>,
    #[arg(long = "from-file", group = "bootstrap_input")]
    pub from_file: Option<PathBuf>,
    /// Path to a JSON project seed file (ProjectSeedPayload). Skips quick-requirements
    /// and creates the project directly from the seed.
    #[arg(long = "from-seed", group = "bootstrap_input")]
    pub from_seed: Option<PathBuf>,
    #[arg(long)]
    pub flow: Option<String>,
    #[arg(long)]
    pub start: bool,
    /// Enable the requirements review step during bootstrap. By default, the
    /// review is skipped and drafting proceeds directly to project creation.
    #[arg(long = "enable-review")]
    pub enable_review: bool,
}

pub async fn handle(command: ProjectCommand) -> AppResult<()> {
    match command.command {
        ProjectSubcommand::Select { id } => {
            let current_dir = std::env::current_dir()?;
            let project_id = ProjectId::new(id)?;
            workspace_governance::set_active_project(&current_dir, &project_id)?;
            let project_record = FsProjectStore.read_project_record(&current_dir, &project_id)?;
            workspace_governance::sync_active_milestone_from_project_record(
                &current_dir,
                &project_record,
            )?;
            println!("Selected project {}", project_id);
            Ok(())
        }
        ProjectSubcommand::Create(args) => {
            if let Some(run_id) = args.from_requirements {
                handle_create_from_requirements(run_id).await
            } else {
                handle_create(args).await
            }
        }
        ProjectSubcommand::CreateFromBead(args) => handle_create_from_bead(args).await,
        ProjectSubcommand::Bootstrap(args) => handle_bootstrap(args).await,
        ProjectSubcommand::List => handle_list().await,
        ProjectSubcommand::Show { id } => handle_show(id).await,
        ProjectSubcommand::Delete { id } => handle_delete(id).await,
        ProjectSubcommand::Amend(amend) => handle_amend(amend).await,
    }
}

async fn handle_create(args: ProjectCreateArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace version
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Validate project ID
    let project_id = ProjectId::new(args.id.expect("clap should require --id"))?;

    let flow = args
        .flow
        .as_deref()
        .map(str::parse)
        .transpose()?
        .unwrap_or(FlowPreset::Minimal);

    let prompt_arg = args.prompt.expect("clap should require --prompt");
    let prompt_path = if prompt_arg.is_absolute() {
        prompt_arg.clone()
    } else {
        current_dir.join(&prompt_arg)
    };

    let prompt_contents =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::InvalidPrompt {
            path: prompt_arg.display().to_string(),
            reason: e.to_string(),
        })?;

    if prompt_contents.trim().is_empty() {
        return Err(AppError::InvalidPrompt {
            path: prompt_arg.display().to_string(),
            reason: "prompt file is empty".to_owned(),
        });
    }

    let prompt_hash = FileSystem::prompt_hash(&prompt_contents);

    let store = FsProjectStore;
    let journal_store = FsJournalStore;

    let input = CreateProjectInput {
        id: project_id,
        name: args.name.expect("clap should require --name"),
        flow,
        prompt_path: prompt_arg.display().to_string(),
        prompt_contents,
        prompt_hash,
        created_at: Utc::now(),
        task_source: None,
    };

    let record = service::create_project(&store, &journal_store, &current_dir, input)?;

    println!(
        "Created project '{}' with flow '{}'",
        record.id, record.flow
    );
    Ok(())
}

async fn handle_create_from_requirements(run_id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    match load_requirements_handoff(&current_dir, &run_id)? {
        RequirementsCreateHandoff::ProjectSeed(handoff) => {
            let store = FsProjectStore;
            let journal_store = FsJournalStore;
            let record = service::create_project_from_seed(
                &store,
                &journal_store,
                &current_dir,
                handoff,
                None,
                Utc::now(),
            )
            .map_err(|error| map_requirements_project_error(error, &run_id))?;

            set_active_project_after_create(&current_dir, &record.id)?;
            let detail = load_project_detail(&current_dir, &record.id)?;
            print_project_detail(&detail);
        }
        RequirementsCreateHandoff::MilestoneBundle(handoff) => {
            let milestone_store = FsMilestoneStore;
            let snapshot_store = FsMilestoneSnapshotStore;
            let journal_store = FsMilestoneJournalStore;
            let plan_store = FsMilestonePlanStore;
            let record = milestone_service::materialize_bundle(
                &milestone_store,
                &snapshot_store,
                &journal_store,
                &plan_store,
                &current_dir,
                &handoff.bundle,
                Utc::now(),
            )?;
            set_active_milestone_after_command(&current_dir, &record.id)?;
            println!(
                "Created milestone '{}' from requirements run '{}'",
                record.id, handoff.requirements_run_id
            );
            println!(
                "Plan: {}",
                current_dir
                    .join(".ralph-burning/milestones")
                    .join(record.id.as_str())
                    .join("plan.json")
                    .display()
            );
        }
    }
    Ok(())
}

/// Core logic for `project create-from-bead` / `task create`.
///
/// Performs all side effects (bead claiming, project creation, milestone controller
/// linking, active-project selection) and returns the created project's ID.
/// Callers are responsible for loading the detail and formatting output.
pub(crate) async fn execute_create_from_bead(args: CreateFromBeadArgs) -> AppResult<ProjectId> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let milestone_store = FsMilestoneStore;
    let plan_store = FsMilestonePlanStore;
    let snapshot_store = FsMilestoneSnapshotStore;
    let milestone_id = MilestoneId::new(args.milestone_id)?;
    ensure_milestone_exists(&milestone_store, &current_dir, &milestone_id)?;
    let milestone = milestone_store.read_milestone_record(&current_dir, &milestone_id)?;
    let milestone_snapshot = snapshot_store.read_snapshot(&current_dir, &milestone_id)?;
    let milestone_bundle = load_milestone_bundle(&plan_store, &current_dir, &milestone_id)?;
    let bead = load_bead_detail(&current_dir, &milestone_id, &args.bead_id).await?;
    let flow_override = parse_flow_override(args.flow.as_deref())?;
    ensure_bead_belongs_to_milestone(&milestone_id, &bead)?;
    ensure_bead_creation_targets_are_actionable(&milestone_id, milestone_snapshot.status, &bead)?;
    let bead_plan = resolve_bead_plan(&milestone_bundle.bundle, &milestone_id, &bead)?;
    let confirmed_plan_version = if bead_plan.membership_confirmed {
        validate_milestone_plan_snapshot(
            &milestone_id,
            milestone_snapshot.plan_hash.as_deref(),
            milestone_snapshot.plan_version,
            &milestone_bundle.plan_hash,
        )?
    } else {
        None
    };
    let prompt_override = load_optional_prompt_override(&current_dir, args.prompt_file.as_deref())?;
    let flow = flow_override
        .or(bead_plan.flow_override)
        .unwrap_or(milestone_bundle.bundle.default_flow);
    let plan_hash = bead_plan
        .membership_confirmed
        .then(|| milestone_bundle.plan_hash.clone());
    let plan_version = bead_plan
        .membership_confirmed
        .then_some(confirmed_plan_version)
        .flatten();
    let (upstream_dependencies, downstream_dependents, planned_elsewhere) =
        if prompt_override.is_some() {
            (Vec::new(), Vec::new(), Vec::new())
        } else {
            let bead_summaries = match load_bead_summaries(&current_dir).await {
                Ok(summaries) => summaries,
                Err(error @ AppError::CorruptRecord { .. }) => return Err(error),
                Err(AppError::Io(_)) => BTreeMap::new(),
                Err(other) => return Err(other),
            };

            (
                build_dependency_prompt_context(&bead.dependencies, &bead_summaries),
                build_dependent_prompt_context(&bead.dependents, &bead_summaries),
                build_planned_elsewhere_context(
                    &milestone_bundle.bundle,
                    &milestone_id,
                    &bead,
                    &bead_plan,
                    &bead_summaries,
                ),
            )
        };

    let context = BeadProjectContext {
        milestone_id: milestone.id.to_string(),
        milestone_name: milestone.name.clone(),
        milestone_description: milestone.description.clone(),
        milestone_summary: Some(milestone_bundle.bundle.executive_summary.clone()),
        milestone_status: milestone_snapshot.status,
        milestone_progress: milestone_snapshot.progress.clone(),
        milestone_goals: milestone_bundle.bundle.goals.clone(),
        milestone_non_goals: milestone_bundle.bundle.non_goals.clone(),
        milestone_constraints: milestone_bundle.bundle.constraints.clone(),
        agents_guidance: milestone_bundle.bundle.agents_guidance.clone(),
        bead_id: bead.id.clone(),
        bead_title: bead.title.clone(),
        bead_description: bead.description.clone(),
        bead_acceptance_criteria: bead.acceptance_criteria.clone(),
        upstream_dependencies,
        downstream_dependents,
        planned_elsewhere,
        review_policy:
            crate::contexts::project_run_record::task_prompt_contract::default_review_policy(),
        parent_epic_id: infer_parent_epic_id(&bead),
        flow,
        plan_hash,
        plan_version,
    };

    // Validate project_id before any external side effects so that a
    // malformed --project-id doesn't leave the bead claimed with no task.
    let project_id = args.project_id.map(ProjectId::new).transpose()?;

    // Resolve the effective project ID (explicit or auto-generated) and check
    // for duplicates before claiming the bead. A collision after claiming would
    // leave the bead in_progress with no linked task, requiring manual recovery.
    let effective_project_id = match &project_id {
        Some(id) => id.clone(),
        None => default_project_id_for_bead(&milestone_id.to_string(), &bead.id)?,
    };
    if FsProjectStore.project_exists(&current_dir, &effective_project_id)? {
        return Err(AppError::DuplicateProject {
            project_id: effective_project_id.as_str().to_owned(),
        });
    }

    // Claim the bead in br before creating the project. If the claim
    // fails, transition the milestone controller to needs_operator so
    // the operator can investigate before any Ralph task is created.
    if let Err(claim_error) =
        claim_bead_in_br(&current_dir, &bead.id, effective_project_id.as_str()).await
    {
        let now = Utc::now();
        let reason = format!("br claim failed for bead '{}': {claim_error}", bead.id);
        if let Err(controller_error) = milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            &current_dir,
            &milestone_id,
            milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::NeedsOperator,
                &reason,
            )
            .with_bead(&bead.id),
            now,
        ) {
            tracing::warn!(
                bead_id = %bead.id,
                %controller_error,
                "failed to transition controller to needs_operator after br claim failure"
            );
        }
        return Err(AppError::Io(std::io::Error::other(reason)));
    }

    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    let now = Utc::now();
    let record = match service::create_project_from_bead_context(
        &store,
        &journal_store,
        &current_dir,
        CreateProjectFromBeadContextInput {
            project_id,
            prompt_override,
            created_at: now,
            context,
        },
    ) {
        Ok(record) => record,
        Err(create_error) => {
            // Bead was already claimed in br but project creation failed.
            // Transition controller to needs_operator so the dangling
            // in_progress bead gets operator attention.
            let reason = format!(
                "project creation failed after bead '{}' was claimed in br: {create_error}",
                bead.id
            );
            if let Err(controller_error) = milestone_controller::sync_controller_state(
                &FsMilestoneControllerStore,
                &current_dir,
                &milestone_id,
                milestone_controller::ControllerTransitionRequest::new(
                    MilestoneControllerState::NeedsOperator,
                    &reason,
                )
                .with_bead(&bead.id),
                now,
            ) {
                tracing::warn!(
                    bead_id = %bead.id,
                    %controller_error,
                    "failed to transition controller to needs_operator after project creation failure"
                );
            }
            return Err(create_error);
        }
    };

    // Record the linked task/project ID in the milestone controller
    // so the controller tracks which project owns this bead.
    if let Err(link_error) = milestone_controller::sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        &current_dir,
        &milestone_id,
        &bead.id,
        record.id.as_str(),
        "bead claimed in br and Ralph project created",
        now,
    ) {
        // Bead is claimed in br and the project exists, but we cannot
        // record the link in the controller. Transition to needs_operator
        // so the dangling pair gets attention.
        let reason = format!(
            "controller failed to record task link for bead '{}' / project '{}': {link_error}",
            bead.id, record.id
        );
        if let Err(controller_error) = milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            &current_dir,
            &milestone_id,
            milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::NeedsOperator,
                &reason,
            )
            .with_bead(&bead.id),
            now,
        ) {
            tracing::warn!(
                bead_id = %bead.id,
                project_id = %record.id,
                %controller_error,
                "failed to transition controller to needs_operator after task-link failure"
            );
        }
        return Err(link_error);
    }

    set_active_milestone_after_command(&current_dir, &milestone_id)?;
    set_active_project_after_create(&current_dir, &record.id)?;
    Ok(record.id)
}

pub(crate) fn find_existing_bead_project(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<Option<ProjectId>> {
    let default_project_id = default_project_id_for_bead(&milestone_id.to_string(), bead_id)?;
    let mut matching_task_source_projects = Vec::new();
    let mut default_project_record = None;

    for project_id in FsProjectStore.list_project_ids(base_dir)? {
        let record = FsProjectStore.read_project_record(base_dir, &project_id)?;
        if project_id == default_project_id {
            default_project_record = Some(record.clone());
        }

        let Some(task_source) = record.task_source.as_ref() else {
            continue;
        };
        if task_source.milestone_id == milestone_id.as_str()
            && milestone_bead_refs_match(milestone_id, &task_source.bead_id, bead_id)
        {
            matching_task_source_projects.push(record.id);
        }
    }

    if let Some(project_id) = matching_task_source_projects
        .iter()
        .find(|project_id| **project_id == default_project_id)
        .cloned()
    {
        return Ok(Some(project_id));
    }

    match matching_task_source_projects.len() {
        0 => {}
        1 => return Ok(matching_task_source_projects.into_iter().next()),
        _ => {
            let project_ids = matching_task_source_projects
                .iter()
                .map(|project_id| project_id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(AppError::Io(std::io::Error::other(format!(
                "multiple projects already reference bead '{}' in milestone '{}': {}",
                bead_id, milestone_id, project_ids
            ))));
        }
    }

    let Some(default_record) = default_project_record else {
        return Ok(None);
    };

    if default_record.task_source.is_none() {
        return Ok(Some(default_record.id));
    }

    Ok(None)
}

async fn handle_create_from_bead(args: CreateFromBeadArgs) -> AppResult<()> {
    let project_id = execute_create_from_bead(args).await?;
    let current_dir = std::env::current_dir()?;
    let detail = load_project_detail(&current_dir, &project_id)?;
    print_project_detail(&detail);
    Ok(())
}

async fn handle_bootstrap(args: BootstrapArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let flow_override = parse_flow_override(args.flow.as_deref())?;

    let (handoff, handoff_ref) = if let Some(ref seed_path) = args.from_seed {
        // --from-seed: skip quick-requirements, load the seed directly.
        let handoff = load_seed_from_file(&current_dir, seed_path)?;
        let ref_label = seed_path.display().to_string();
        (handoff, ref_label)
    } else {
        // --idea or --from-file: run quick-requirements pipeline.
        let idea = read_bootstrap_idea(&current_dir, &args)?;
        let effective_config = EffectiveConfig::load(&current_dir)?;
        let requirements_cli_service =
            agent_execution_builder::build_requirements_service(&effective_config)?;
        let run_id = requirements_cli_service
            .quick(&current_dir, &idea, Utc::now(), None, args.enable_review)
            .await?;
        let handoff = requirements_service::extract_seed_handoff(
            &FsRequirementsStore,
            &current_dir,
            &run_id,
        )?;
        (handoff, run_id)
    };

    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    let record = service::create_project_from_seed(
        &store,
        &journal_store,
        &current_dir,
        handoff,
        flow_override,
        Utc::now(),
    )
    .map_err(|error| map_requirements_project_error(error, &handoff_ref))?;

    set_active_project_after_create(&current_dir, &record.id)?;

    if args.start {
        let start_result = start_created_project(&current_dir, &record.id).await;
        match &start_result {
            Ok(()) => {
                let detail = load_project_detail(&current_dir, &record.id)?;
                print_project_detail(&detail);
            }
            Err(_) => {
                if let Ok(detail) = load_project_detail(&current_dir, &record.id) {
                    print_project_detail(&detail);
                }
            }
        }
        start_result?;
    } else {
        let detail = load_project_detail(&current_dir, &record.id)?;
        print_project_detail(&detail);
    }

    Ok(())
}

async fn handle_list() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsProjectStore;
    let active_store = FsActiveProjectStore;

    let entries = service::list_projects(&store, &active_store, &current_dir)?;

    if entries.is_empty() {
        println!("No projects found.");
        return Ok(());
    }

    for entry in &entries {
        let active_marker = if entry.is_active { " *" } else { "" };
        let status = match entry.status_summary {
            ProjectStatusSummary::Created => "created",
            ProjectStatusSummary::Active => "active",
            ProjectStatusSummary::Completed => "completed",
            ProjectStatusSummary::Failed => "failed",
        };
        println!(
            "  {}{} ({}) [{}] - {}",
            entry.id, active_marker, entry.flow, status, entry.name
        );
    }

    Ok(())
}

async fn handle_show(id: Option<String>) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Resolve project ID: explicit or active project
    let project_id = match id {
        Some(raw) => ProjectId::new(raw)?,
        None => workspace_governance::resolve_active_project(&current_dir)?,
    };

    let detail = load_project_detail(&current_dir, &project_id)?;
    print_project_detail(&detail);
    Ok(())
}

async fn handle_delete(id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = ProjectId::new(id)?;

    let store = FsProjectStore;
    let run_store = FsRunSnapshotStore;
    let active_store = FsActiveProjectStore;

    service::delete_project(&store, &run_store, &active_store, &current_dir, &project_id)?;

    println!("Deleted project '{}'", project_id);
    Ok(())
}

pub(crate) fn load_project_detail(
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<ProjectDetail> {
    let store = FsProjectStore;
    let run_store = FsRunSnapshotStore;
    let journal_store = FsJournalStore;
    let active_store = FsActiveProjectStore;
    let milestone_store = FsMilestoneStore;
    let plan_store = FsMilestonePlanStore;

    service::show_project(
        &store,
        &run_store,
        &journal_store,
        &active_store,
        &milestone_store,
        &plan_store,
        base_dir,
        project_id,
    )
}

fn print_project_detail(detail: &ProjectDetail) {
    let active_label = if detail.is_active { " (active)" } else { "" };
    println!("Project: {}{}", detail.record.id, active_label);
    println!("Name: {}", detail.record.name);
    println!("Flow: {}", detail.record.flow);
    println!("Prompt reference: {}", detail.record.prompt_reference);
    println!("Prompt hash: {}", detail.record.prompt_hash);
    println!("Created: {}", detail.record.created_at);
    println!("Run status: {}", detail.run_snapshot.status_summary);
    println!("Journal events: {}", detail.journal_event_count);
    println!("Rollback points: {}", detail.rollback_count);
    if let Some(lineage) = &detail.task_lineage {
        println!(
            "Milestone: {} ({})",
            lineage.milestone_name, lineage.milestone_id
        );
        println!(
            "Bead: {} ({})",
            lineage.bead_title.as_deref().unwrap_or("<unknown>"),
            lineage.bead_id
        );
        if !lineage.acceptance_criteria.is_empty() {
            println!("Acceptance criteria:");
            for criterion in &lineage.acceptance_criteria {
                println!("  - {criterion}");
            }
        }
    }
}

fn ensure_milestone_exists(
    store: &dyn MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<()> {
    if store.milestone_exists(base_dir, milestone_id)? {
        return Ok(());
    }

    Err(AppError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("milestone '{}' not found", milestone_id),
    )))
}

#[derive(Debug)]
struct LoadedMilestoneBundle {
    bundle: MilestoneBundle,
    plan_hash: String,
}

fn load_milestone_bundle(
    store: &dyn MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<LoadedMilestoneBundle> {
    let raw = store.read_plan_json(base_dir, milestone_id)?;
    let mut bundle: MilestoneBundle =
        serde_json::from_str(&raw).map_err(|error| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: error.to_string(),
        })?;

    if bundle.identity.id != milestone_id.as_str() {
        return Err(AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: format!(
                "bundle identity '{}' does not match milestone '{}'",
                bundle.identity.id, milestone_id
            ),
        });
    }

    backfill_legacy_explicit_bead_flags(&mut bundle, milestone_id);
    bundle
        .validate()
        .map_err(|errors| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: errors.join("; "),
        })?;

    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());

    Ok(LoadedMilestoneBundle {
        bundle,
        plan_hash: format!("{:x}", hasher.finalize()),
    })
}

fn backfill_legacy_explicit_bead_flags(bundle: &mut MilestoneBundle, milestone_id: &MilestoneId) {
    let mut next_implicit_bead = 1usize;

    for workstream in &mut bundle.workstreams {
        for proposal in &mut workstream.beads {
            let implicit_bead_id = format!("{}.bead-{}", milestone_id.as_str(), next_implicit_bead);
            next_implicit_bead += 1;

            if proposal.explicit_id.is_some() {
                continue;
            }

            if let Some(candidate) = proposal.bead_id.as_deref() {
                proposal.explicit_id = Some(!bead_matches_implicit_slot(
                    candidate,
                    milestone_id.as_str(),
                    &implicit_bead_id,
                ));
            }
        }
    }
}

fn validate_milestone_plan_snapshot(
    milestone_id: &MilestoneId,
    snapshot_plan_hash: Option<&str>,
    snapshot_plan_version: u32,
    bundle_plan_hash: &str,
) -> AppResult<Option<u32>> {
    let status_file = format!("milestones/{}/status.json", milestone_id);
    match snapshot_plan_hash {
        Some(snapshot_plan_hash) => {
            if snapshot_plan_version == 0 {
                return Err(AppError::CorruptRecord {
                    file: status_file,
                    details: "status snapshot has plan_hash but plan_version is 0".to_owned(),
                });
            }
            if snapshot_plan_hash != bundle_plan_hash {
                return Err(AppError::CorruptRecord {
                    file: status_file,
                    details: format!(
                        "plan metadata is stale: status.json hash '{}' does not match plan.json hash '{}'",
                        snapshot_plan_hash, bundle_plan_hash
                    ),
                });
            }
        }
        None if snapshot_plan_version > 0 => {
            return Err(AppError::CorruptRecord {
                file: status_file,
                details: "status snapshot is missing plan_hash for the current plan.json"
                    .to_owned(),
            });
        }
        None => return Ok(None),
    }

    Ok((snapshot_plan_version > 0).then_some(snapshot_plan_version))
}

fn ensure_bead_creation_targets_are_actionable(
    milestone_id: &MilestoneId,
    milestone_status: MilestoneStatus,
    bead: &BeadDetail,
) -> AppResult<()> {
    if milestone_status.is_terminal() {
        return Err(AppError::InvalidConfigValue {
            key: "milestone_status".to_owned(),
            value: milestone_status.to_string(),
            reason: format!(
                "cannot create project from bead '{}': milestone '{}' is already {}",
                bead.id, milestone_id, milestone_status
            ),
        });
    }

    match bead.status {
        BeadStatus::Open | BeadStatus::InProgress => Ok(()),
        BeadStatus::Closed | BeadStatus::Deferred => Err(AppError::InvalidConfigValue {
            key: "bead_status".to_owned(),
            value: bead.status.to_string(),
            reason: format!(
                "cannot create project from bead '{}': bead is already {}",
                bead.id, bead.status
            ),
        }),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum BrShowResponse {
    Single(BeadDetail),
    Many(Vec<BeadDetail>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BrListResponse {
    Envelope { issues: Vec<BeadSummary> },
    Many(Vec<BeadSummary>),
}

/// Claim a bead in `br` by setting its status to `in_progress` and flushing.
///
/// Recovered local pending mutations are flushed first so a retry after a
/// failed `br sync --flush-only` can publish the earlier local claim before
/// attempting a new `br update`. The recovered-flush short-circuit is only
/// taken when the replayed `update_bead_status(<bead>, "in_progress")` record
/// carries the same stable claim-owner token and a post-flush `br show`
/// confirms the bead is still `in_progress`, so another process's recovered
/// claim or later local edits cannot be mistaken for the current caller's
/// success.
async fn claim_bead_in_br(base_dir: &Path, bead_id: &str, claim_owner: &str) -> AppResult<()> {
    ensure_beads_claim_health(base_dir, bead_id)?;

    let claim_owner_token = claim_owner_token(claim_owner, bead_id);
    let br = BrMutationAdapter::with_adapter_id(
        BrAdapter::new().with_working_dir(base_dir.to_path_buf()),
        claim_owner_token.clone(),
    );
    let recovered_flush = match br.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(outcome) => outcome,
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            return Err(AppError::Io(std::io::Error::other(format!(
                "refusing to replay a previously successful local bead claim for '{bead_id}' \
                 because bead state is unsafe: {details}"
            ))));
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            return Err(AppError::Io(std::io::Error::other(format!(
                "failed to replay a previously successful local bead claim for '{bead_id}' via \
                 br sync --flush-only: {error}"
            ))));
        }
    };
    if !recovered_flush.is_clean() {
        tracing::info!(
            bead_id = bead_id,
            flushed_mutations = recovered_flush.flushed_mutations(),
            "flushed recovered local bead mutations before issuing an explicit claim update"
        );
    }
    if recovered_flush.includes_owned_update_status(&claim_owner_token, bead_id, "in_progress") {
        match br
            .inner()
            .exec_json::<BeadDetail>(&BrCommand::show(bead_id.to_owned()))
            .await
        {
            Ok(detail) if detail.status == BeadStatus::InProgress => {
                ensure_beads_claim_post_flush_health(base_dir, bead_id)?;
                tracing::info!(
                    bead_id = bead_id,
                    claim_owner = claim_owner,
                    "recovered flush replayed this bead claim and br show confirmed the bead is still in_progress; skipping duplicate br update"
                );
                return Ok(());
            }
            Ok(detail) => {
                tracing::warn!(
                    bead_id = bead_id,
                    claim_owner = claim_owner,
                    current_status = %detail.status,
                    "recovered claim flush completed but the bead is no longer in_progress; issuing an explicit br update"
                );
            }
            Err(error) => {
                tracing::warn!(
                    bead_id = bead_id,
                    claim_owner = claim_owner,
                    %error,
                    "failed to verify recovered claim status after flush; issuing an explicit br update"
                );
            }
        }
    }

    ensure_beads_claim_health(base_dir, bead_id)?;
    br.update_bead_status(bead_id, "in_progress")
        .await
        .map_err(|update_error| {
            AppError::Io(std::io::Error::other(format!(
                "failed to claim bead '{bead_id}' via br update --status=in_progress: \
                 {update_error}"
            )))
        })?;
    match br.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(_) => {}
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            return Err(AppError::Io(std::io::Error::other(format!(
                "bead '{bead_id}' was locally claimed (status set to in_progress) but bead \
                 state became unsafe before br sync --flush-only: {details}. The bead remains \
                 locally claimed in br; resolve the bead-state issue and rerun \
                 `br sync --flush-only`."
            ))));
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            return Err(AppError::Io(std::io::Error::other(format!(
                "bead '{bead_id}' was locally claimed (status set to in_progress) but sync \
                 to remote storage failed: {error}. The bead remains locally claimed in br; \
                 a subsequent `br sync --flush-only` will retry the remote push."
            ))));
        }
    }
    Ok(())
}

fn claim_owner_token(claim_owner: &str, bead_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"project-claim:");
    hasher.update(claim_owner.as_bytes());
    hasher.update(b":");
    hasher.update(bead_id.as_bytes());
    format!("project-claim-{:x}", hasher.finalize())
}

fn ensure_beads_claim_health(base_dir: &Path, bead_id: &str) -> AppResult<()> {
    if let Some(details) = beads_health_failure_details(&check_beads_health(base_dir)) {
        return Err(AppError::Io(std::io::Error::other(format!(
            "refusing to claim bead '{bead_id}' because bead state is unsafe: {details}"
        ))));
    }

    Ok(())
}

fn ensure_beads_claim_post_flush_health(base_dir: &Path, bead_id: &str) -> AppResult<()> {
    if let Some(details) = beads_health_failure_details(&check_beads_health(base_dir)) {
        return Err(AppError::Io(std::io::Error::other(format!(
            "bead '{bead_id}' was locally claimed by replaying a recovered br sync --flush-only, \
             but bead state is still unsafe: {details}. Resolve the bead-state issue before \
             continuing with milestone work."
        ))));
    }

    Ok(())
}

async fn load_bead_detail(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<BeadDetail> {
    let response: BrShowResponse = BrAdapter::new()
        .with_working_dir(base_dir.to_path_buf())
        .exec_json(&BrCommand::show(bead_id))
        .await
        .map_err(|error| match error {
            BrError::BrExitError { stderr, stdout, .. }
                if br_show_output_indicates_missing(&stderr, &stdout) =>
            {
                AppError::Io(std::io::Error::other(format!(
                    "failed to load bead '{bead_id}': bead not found"
                )))
            }
            BrError::BrExitError { stderr, .. } => AppError::Io(std::io::Error::other(format!(
                "failed to load bead '{bead_id}': {stderr}"
            ))),
            other => AppError::Io(std::io::Error::other(format!(
                "failed to load bead '{bead_id}': {other}"
            ))),
        })?;

    match response {
        BrShowResponse::Single(bead) => {
            if bead_id.contains('.') {
                if bead.id != bead_id {
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "failed to load bead '{bead_id}': br show returned bead '{}'",
                        bead.id
                    ))));
                }
                return Ok(bead);
            }

            if milestone_bead_refs_match(milestone_id, &bead.id, bead_id) {
                return Ok(bead);
            }

            Err(AppError::Io(std::io::Error::other(format!(
                "failed to load bead '{bead_id}': br show returned bead '{}'",
                bead.id
            ))))
        }
        BrShowResponse::Many(beads) => {
            let mut matches = beads.into_iter().filter(|bead| {
                if bead_id.contains('.') {
                    bead.id == bead_id
                } else {
                    milestone_bead_refs_match(milestone_id, &bead.id, bead_id)
                }
            });
            let bead = matches.next().ok_or_else(|| {
                let detail = if bead_id.contains('.') {
                    "br show returned no matching bead".to_owned()
                } else {
                    format!(
                        "br show returned no matching bead in milestone '{}'",
                        milestone_id
                    )
                };
                AppError::Io(std::io::Error::other(format!(
                    "failed to load bead '{bead_id}': {detail}"
                )))
            })?;
            if matches.next().is_some() {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "failed to load bead '{bead_id}': br show returned multiple matching beads"
                ))));
            }
            Ok(bead)
        }
    }
}

async fn load_bead_summaries(base_dir: &Path) -> AppResult<BTreeMap<String, BeadSummary>> {
    let response: BrListResponse = BrAdapter::new()
        .with_working_dir(base_dir.to_path_buf())
        .exec_json(&BrCommand::list_all())
        .await
        .map_err(map_br_list_error)?;
    let summaries = match response {
        BrListResponse::Envelope { issues } => issues,
        BrListResponse::Many(issues) => issues,
    };

    Ok(BTreeMap::from_iter(
        summaries
            .into_iter()
            .map(|summary| (summary.id.clone(), summary)),
    ))
}

fn map_br_list_error(error: BrError) -> AppError {
    match error {
        BrError::BrExitError { stderr, .. } if br_list_exit_error_looks_corrupt(&stderr) => {
            AppError::CorruptRecord {
                file: ".beads/issues.jsonl".to_owned(),
                details: format!(
                    "`br list --all --deferred --limit=0 --json` reported corrupt bead data: {stderr}"
                ),
            }
        }
        BrError::BrExitError { stderr, .. } => AppError::Io(std::io::Error::other(format!(
            "failed to load bead summaries: {stderr}"
        ))),
        BrError::BrParseError { details, .. } => AppError::CorruptRecord {
            file: ".beads/issues.jsonl".to_owned(),
            details: format!(
                "failed to parse `br list --all --deferred --limit=0 --json` output: {details}"
            ),
        },
        other => AppError::Io(std::io::Error::other(format!(
            "failed to load bead summaries: {other}"
        ))),
    }
}

fn br_list_exit_error_looks_corrupt(stderr: &str) -> bool {
    let normalized = stderr.to_ascii_lowercase();
    [
        "corrupt",
        "failed to parse",
        "parse error",
        "invalid json",
        "malformed json",
        "json parse",
        "decode error",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn load_optional_prompt_override(
    base_dir: &Path,
    path: Option<&Path>,
) -> AppResult<Option<String>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    let prompt = std::fs::read_to_string(&resolved).map_err(|error| AppError::InvalidPrompt {
        path: resolved.display().to_string(),
        reason: error.to_string(),
    })?;
    if prompt.trim().is_empty() {
        return Err(AppError::InvalidPrompt {
            path: resolved.display().to_string(),
            reason: "prompt file is empty".to_owned(),
        });
    }
    Ok(Some(prompt))
}

fn infer_parent_epic_id(bead: &BeadDetail) -> Option<String> {
    bead.dependencies
        .iter()
        .find(|dependency| dependency.kind == DependencyKind::ParentChild)
        .map(|dependency| dependency.id.clone())
}

#[derive(Debug, Clone, Copy, Default)]
struct ResolvedBeadPlan {
    flow_override: Option<FlowPreset>,
    membership_confirmed: bool,
    matched_workstream_index: Option<usize>,
    matched_bead_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PlannedElsewherePriority {
    DirectDependent,
    SharedAcceptance,
    AdjacentNeighbor,
    UpstreamNeighbor,
}

#[derive(Debug, Clone)]
struct PlannedElsewhereCandidate {
    item: PlannedElsewherePromptContext,
    priority: PlannedElsewherePriority,
}

fn ensure_bead_belongs_to_milestone(
    milestone_id: &MilestoneId,
    bead: &BeadDetail,
) -> AppResult<()> {
    let expected_prefix = format!("{}.", milestone_id.as_str());
    if bead.id.starts_with(&expected_prefix) {
        return Ok(());
    }

    Err(AppError::InvalidConfigValue {
        key: "bead_id".to_owned(),
        value: bead.id.clone(),
        reason: format!(
            "expected bead id to belong to milestone '{}' (prefix '{}')",
            milestone_id, expected_prefix
        ),
    })
}

fn resolve_bead_plan(
    bundle: &MilestoneBundle,
    milestone_id: &MilestoneId,
    bead: &BeadDetail,
) -> AppResult<ResolvedBeadPlan> {
    ensure_bead_belongs_to_milestone(milestone_id, bead)?;

    let mut next_implicit_bead = 1usize;
    let mut matching_by_id = Vec::new();
    let mut matching_by_title = Vec::new();
    let mut authoritative_implicit_match = None;

    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        for (bead_index, proposal) in workstream.beads.iter().enumerate() {
            let implicit_bead_id = format!("{}.bead-{}", milestone_id.as_str(), next_implicit_bead);
            next_implicit_bead += 1;

            if proposal_matches_bead_id(proposal, milestone_id, bead) {
                matching_by_id.push((workstream_index, bead_index, proposal));
            }
            if proposal_is_title_fallback_candidate(proposal, milestone_id, &implicit_bead_id)
                && proposal.title == bead.title
            {
                if bead_matches_implicit_slot(&bead.id, milestone_id.as_str(), &implicit_bead_id) {
                    authoritative_implicit_match = Some((workstream_index, bead_index, proposal));
                }
                matching_by_title.push((workstream_index, bead_index, proposal));
            }
        }
    }

    match matching_by_id.as_slice() {
        [(workstream_index, bead_index, proposal)] => {
            return Ok(ResolvedBeadPlan {
                flow_override: proposal.flow_override,
                membership_confirmed: true,
                matched_workstream_index: Some(*workstream_index),
                matched_bead_index: Some(*bead_index),
            });
        }
        [] => {}
        _ => {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/plan.json", milestone_id),
                details: format!(
                    "multiple bead proposals resolve to bead '{}'; cannot resolve bead metadata",
                    bead.id
                ),
            });
        }
    }

    if let Some((workstream_index, bead_index, proposal)) = authoritative_implicit_match {
        return Ok(ResolvedBeadPlan {
            flow_override: proposal.flow_override,
            membership_confirmed: true,
            matched_workstream_index: Some(workstream_index),
            matched_bead_index: Some(bead_index),
        });
    }

    match matching_by_title.as_slice() {
        [(workstream_index, bead_index, proposal)] => Ok(ResolvedBeadPlan {
            flow_override: proposal.flow_override,
            membership_confirmed: false,
            matched_workstream_index: Some(*workstream_index),
            matched_bead_index: Some(*bead_index),
        }),
        [] => Ok(ResolvedBeadPlan {
            flow_override: None,
            membership_confirmed: false,
            matched_workstream_index: None,
            matched_bead_index: None,
        }),
        _ => Err(AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: format!(
                "multiple bead proposals named '{}' match bead '{}'; cannot resolve bead metadata",
                bead.title, bead.id
            ),
        }),
    }
}

fn proposal_matches_bead_id(
    proposal: &crate::contexts::milestone_record::bundle::BeadProposal,
    milestone_id: &MilestoneId,
    bead: &BeadDetail,
) -> bool {
    if proposal.explicit_id != Some(true) {
        return false;
    }
    let Some(candidate) = proposal.bead_id.as_deref() else {
        return false;
    };
    let expected_suffix = bead
        .id
        .strip_prefix(&format!("{}.", milestone_id.as_str()))
        .unwrap_or(bead.id.as_str());
    candidate == bead.id || candidate == expected_suffix
}

fn proposal_is_title_fallback_candidate(
    proposal: &crate::contexts::milestone_record::bundle::BeadProposal,
    milestone_id: &MilestoneId,
    implicit_bead_id: &str,
) -> bool {
    if proposal.explicit_id.is_none() {
        return match proposal.bead_id.as_deref() {
            None => true,
            Some(candidate) => {
                bead_matches_implicit_slot(candidate, milestone_id.as_str(), implicit_bead_id)
            }
        };
    }

    proposal.explicit_id == Some(false)
        && proposal.bead_id.as_deref().is_some_and(|candidate| {
            bead_matches_implicit_slot(candidate, milestone_id.as_str(), implicit_bead_id)
        })
}

fn canonical_proposal_id(
    milestone_id: &MilestoneId,
    proposal: &crate::contexts::milestone_record::bundle::BeadProposal,
    implicit_index: usize,
) -> String {
    match proposal.bead_id.as_deref() {
        Some(candidate) if candidate.starts_with(&format!("{}.", milestone_id.as_str())) => {
            candidate.to_owned()
        }
        Some(candidate) => format!("{}.{}", milestone_id.as_str(), candidate),
        None => format!("{}.bead-{}", milestone_id.as_str(), implicit_index),
    }
}

fn canonicalize_bundle_bead_ref(milestone_id: &MilestoneId, raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with(&format!("{}.", milestone_id.as_str())) {
        trimmed.to_owned()
    } else {
        format!("{}.{}", milestone_id.as_str(), trimmed)
    }
}

fn opening_fence_delimiter(line: &str) -> Option<(char, usize)> {
    let trimmed = line.trim();
    let marker = trimmed.chars().next()?;
    if marker != '`' && marker != '~' {
        return None;
    }

    let count = trimmed.chars().take_while(|ch| *ch == marker).count();
    if count < 3 {
        return None;
    }

    Some((marker, count))
}

fn closes_fence(line: &str, opening: (char, usize)) -> bool {
    let Some(candidate) = opening_fence_delimiter(line) else {
        return false;
    };

    if candidate.0 != opening.0 || candidate.1 < opening.1 {
        return false;
    }

    let trimmed = line.trim();
    trimmed[candidate.1..].trim().is_empty()
}

fn markdown_heading_title(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
    if !(1..=6).contains(&hash_count) {
        return None;
    }
    let rest = &trimmed[hash_count..];
    if !rest.is_empty()
        && !rest
            .chars()
            .next()
            .map(char::is_whitespace)
            .unwrap_or(false)
    {
        return None;
    }
    Some(rest.trim().trim_end_matches('#').trim())
}

fn normalized_summary_label(label: &str) -> String {
    label
        .trim()
        .trim_end_matches(':')
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_planned_elsewhere_scope_label(label: &str) -> bool {
    matches!(
        normalized_summary_label(label).as_str(),
        "goal"
            | "goals"
            | "scope"
            | "summary"
            | "details"
            | "detail"
            | "overview"
            | "context"
            | "objective"
            | "objectives"
            | "description"
            | "nongoals"
            | "acceptancecriteria"
    )
}

fn strip_markdown_list_marker(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if let Some(item) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
    {
        return Some(item);
    }

    let bytes = trimmed.as_bytes();
    let mut marker_len = 0usize;
    while marker_len < bytes.len() && bytes[marker_len].is_ascii_digit() {
        marker_len += 1;
    }

    if marker_len == 0 || marker_len + 1 >= bytes.len() {
        return None;
    }

    if matches!(bytes[marker_len], b'.' | b')') && bytes[marker_len + 1] == b' ' {
        Some(&trimmed[marker_len + 2..])
    } else {
        None
    }
}

fn compact_planned_elsewhere_summary(value: Option<&str>) -> Option<String> {
    value.and_then(|raw| {
        let mut active_fence = None;
        let mut lines = Vec::new();

        for line in raw.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                if lines.is_empty() {
                    continue;
                }
                break;
            }

            if let Some(opening) = active_fence {
                if closes_fence(trimmed, opening) {
                    active_fence = None;
                }
                continue;
            }

            if let Some(opening) = opening_fence_delimiter(trimmed) {
                if lines.is_empty() {
                    active_fence = Some(opening);
                    continue;
                }
                break;
            }

            if markdown_heading_title(trimmed).is_some() {
                if lines.is_empty() {
                    continue;
                }
                break;
            }

            if let Some((label, rest)) = trimmed.split_once(':') {
                if is_planned_elsewhere_scope_label(label) {
                    if !rest.trim().is_empty() {
                        lines.push(rest.trim().to_owned());
                    }
                    continue;
                }
            }

            if let Some(item) = strip_markdown_list_marker(trimmed) {
                if lines.is_empty() {
                    lines.push(item.trim().to_owned());
                    continue;
                }
                break;
            }

            lines.push(trimmed.to_owned());
        }

        if lines.is_empty() {
            None
        } else {
            Some(lines.join("\n"))
        }
    })
}

fn bead_status_label(status: &BeadStatus) -> &'static str {
    match status {
        BeadStatus::Open => "open",
        BeadStatus::InProgress => "in_progress",
        BeadStatus::Closed => "closed",
        BeadStatus::Deferred => "deferred",
    }
}

fn bead_status_outcome(status: &BeadStatus) -> &'static str {
    match status {
        BeadStatus::Open => "pending",
        BeadStatus::InProgress => "active",
        BeadStatus::Closed => "completed",
        BeadStatus::Deferred => "deferred",
    }
}

fn prompt_bead_status(summary: Option<&BeadSummary>, fallback: Option<&BeadStatus>) -> String {
    summary
        .map(|entry| &entry.status)
        .or(fallback)
        .map(|status| bead_status_label(status).to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn prompt_bead_outcome(summary: Option<&BeadSummary>, fallback: Option<&BeadStatus>) -> String {
    summary
        .map(|entry| &entry.status)
        .or(fallback)
        .map(|status| bead_status_outcome(status).to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn relationship_label(kind: &DependencyKind, upstream: bool) -> &'static str {
    match (kind, upstream) {
        (DependencyKind::Blocks, true) => "blocking dependency",
        (DependencyKind::Blocks, false) => "downstream dependent",
        (DependencyKind::ParentChild, true) => "parent epic",
        (DependencyKind::ParentChild, false) => "child bead",
    }
}

fn build_dependency_prompt_context(
    relations: &[crate::adapters::br_models::DependencyRef],
    bead_summaries: &BTreeMap<String, BeadSummary>,
) -> Vec<BeadDependencyPromptContext> {
    let mut items: Vec<_> = relations
        .iter()
        .map(|relation| {
            let summary = bead_summaries.get(&relation.id);
            BeadDependencyPromptContext {
                id: relation.id.clone(),
                title: summary
                    .map(|entry| entry.title.clone())
                    .or_else(|| relation.title.clone()),
                relationship: relationship_label(&relation.kind, true).to_owned(),
                status: Some(prompt_bead_status(summary, relation.status.as_ref())),
                outcome: Some(prompt_bead_outcome(summary, relation.status.as_ref())),
            }
        })
        .collect();
    items.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.title.cmp(&right.title))
    });
    items
}

fn build_dependent_prompt_context(
    relations: &[crate::adapters::br_models::DependencyRef],
    bead_summaries: &BTreeMap<String, BeadSummary>,
) -> Vec<BeadDependencyPromptContext> {
    let mut items: Vec<_> = relations
        .iter()
        .map(|relation| {
            let summary = bead_summaries.get(&relation.id);
            BeadDependencyPromptContext {
                id: relation.id.clone(),
                title: summary
                    .map(|entry| entry.title.clone())
                    .or_else(|| relation.title.clone()),
                relationship: relationship_label(&relation.kind, false).to_owned(),
                status: Some(prompt_bead_status(summary, relation.status.as_ref())),
                outcome: Some(prompt_bead_outcome(summary, relation.status.as_ref())),
            }
        })
        .collect();
    items.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.title.cmp(&right.title))
    });
    items
}

fn infer_implicit_slot_hint(
    bundle: &MilestoneBundle,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> Option<(usize, usize)> {
    let mut next_implicit_bead = 1usize;
    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        for (bead_index, proposal) in workstream.beads.iter().enumerate() {
            let implicit_bead_id = format!("{}.bead-{}", milestone_id.as_str(), next_implicit_bead);
            next_implicit_bead += 1;
            if bead_matches_implicit_slot(bead_id, milestone_id.as_str(), &implicit_bead_id)
                && proposal_is_title_fallback_candidate(proposal, milestone_id, &implicit_bead_id)
            {
                return Some((workstream_index, bead_index));
            }
        }
    }
    None
}

fn build_planned_elsewhere_context(
    bundle: &MilestoneBundle,
    milestone_id: &MilestoneId,
    bead: &BeadDetail,
    bead_plan: &ResolvedBeadPlan,
    bead_summaries: &BTreeMap<String, BeadSummary>,
) -> Vec<PlannedElsewherePromptContext> {
    let allow_plan_derived_enrichment = bead_plan.membership_confirmed;
    let dependent_ids = BTreeSet::from_iter(bead.dependents.iter().map(|item| item.id.clone()));
    let upstream_ids = BTreeSet::from_iter(bead.dependencies.iter().map(|item| item.id.clone()));
    let mut items = BTreeMap::new();

    let mut add_item = |item: PlannedElsewherePromptContext, priority: PlannedElsewherePriority| {
        items
            .entry(item.id.clone())
            .or_insert(PlannedElsewhereCandidate { item, priority });
    };

    let mut next_implicit_bead = 1usize;
    let mut proposal_lookup = BTreeMap::new();
    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        for (bead_index, proposal) in workstream.beads.iter().enumerate() {
            let proposal_id = canonical_proposal_id(milestone_id, proposal, next_implicit_bead);
            next_implicit_bead += 1;
            proposal_lookup.insert(
                proposal_id,
                (
                    workstream_index,
                    bead_index,
                    workstream.name.as_str(),
                    proposal,
                ),
            );
        }
    }

    let mut shared_acceptance_owners = BTreeMap::<String, Vec<String>>::new();
    if allow_plan_derived_enrichment {
        for criterion in &bundle.acceptance_map {
            let covered_by = criterion
                .covered_by
                .iter()
                .map(|bead_ref| canonicalize_bundle_bead_ref(milestone_id, bead_ref))
                .collect::<Vec<_>>();
            if !covered_by.iter().any(|covered_id| covered_id == &bead.id) {
                continue;
            }

            for related_bead_id in covered_by {
                if related_bead_id == bead.id
                    || upstream_ids.contains(&related_bead_id)
                    || dependent_ids.contains(&related_bead_id)
                {
                    continue;
                }

                shared_acceptance_owners
                    .entry(related_bead_id)
                    .or_default()
                    .push(criterion.id.clone());
            }
        }
    }

    for dependent in &bead.dependents {
        let summary = bead_summaries.get(&dependent.id);
        let plan_summary = allow_plan_derived_enrichment
            .then(|| proposal_lookup.get(&dependent.id))
            .flatten()
            .and_then(|(_, _, _, proposal)| {
                compact_planned_elsewhere_summary(proposal.description.as_deref())
            });
        add_item(
            PlannedElsewherePromptContext {
                id: dependent.id.clone(),
                title: summary
                    .map(|entry| entry.title.clone())
                    .or_else(|| dependent.title.clone())
                    .unwrap_or_else(|| dependent.id.clone()),
                relationship: relationship_label(&dependent.kind, false).to_owned(),
                status: Some(prompt_bead_status(summary, dependent.status.as_ref())),
                summary: plan_summary,
            },
            PlannedElsewherePriority::DirectDependent,
        );
    }

    for (related_bead_id, mut criterion_ids) in shared_acceptance_owners {
        criterion_ids.sort();
        criterion_ids.dedup();
        let summary = bead_summaries.get(&related_bead_id);
        let proposal_entry = proposal_lookup.get(&related_bead_id);
        let plan_summary = proposal_entry.and_then(|(_, _, _, proposal)| {
            compact_planned_elsewhere_summary(proposal.description.as_deref())
        });
        let workstream_name = proposal_entry.map(|(_, _, workstream_name, _)| *workstream_name);
        let criteria_label = criterion_ids.join(", ");
        let relationship = match workstream_name {
            Some(workstream_name) => format!(
                "shared milestone acceptance ownership in {workstream_name} ({criteria_label})"
            ),
            None => format!("shared milestone acceptance ownership ({criteria_label})"),
        };
        add_item(
            PlannedElsewherePromptContext {
                id: related_bead_id.clone(),
                title: summary
                    .map(|entry| entry.title.clone())
                    .or_else(|| proposal_entry.map(|(_, _, _, proposal)| proposal.title.clone()))
                    .unwrap_or_else(|| related_bead_id.clone()),
                relationship,
                status: Some(prompt_bead_status(summary, None)),
                summary: plan_summary,
            },
            PlannedElsewherePriority::SharedAcceptance,
        );
    }

    let location_hint = bead_plan
        .membership_confirmed
        .then_some(())
        .and(
            bead_plan
                .matched_workstream_index
                .zip(bead_plan.matched_bead_index),
        )
        .or_else(|| {
            bead_plan
                .membership_confirmed
                .then(|| infer_implicit_slot_hint(bundle, milestone_id, &bead.id))
                .flatten()
        });
    let Some((workstream_index, current_bead_index)) = location_hint else {
        return apply_planned_elsewhere_budget(items.into_values().collect());
    };

    let workstream = &bundle.workstreams[workstream_index];
    let neighbor_range_start = current_bead_index.saturating_sub(1);
    let neighbor_range_end = usize::min(workstream.beads.len(), current_bead_index + 2);

    let mut implicit_index = 1usize;
    for (candidate_workstream_index, candidate_workstream) in bundle.workstreams.iter().enumerate()
    {
        for (candidate_bead_index, proposal) in candidate_workstream.beads.iter().enumerate() {
            let proposal_id = canonical_proposal_id(milestone_id, proposal, implicit_index);
            implicit_index += 1;

            if candidate_workstream_index != workstream_index
                || candidate_bead_index == current_bead_index
                || candidate_bead_index < neighbor_range_start
                || candidate_bead_index >= neighbor_range_end
                || proposal_id == bead.id
            {
                continue;
            }

            let (relation, priority) = if dependent_ids.contains(&proposal_id) {
                (
                    "downstream dependent already planned elsewhere",
                    PlannedElsewherePriority::DirectDependent,
                )
            } else if upstream_ids.contains(&proposal_id) {
                (
                    "upstream dependency already planned elsewhere",
                    PlannedElsewherePriority::UpstreamNeighbor,
                )
            } else {
                (
                    "adjacent same-workstream bead",
                    PlannedElsewherePriority::AdjacentNeighbor,
                )
            };
            let summary = bead_summaries.get(&proposal_id);
            add_item(
                PlannedElsewherePromptContext {
                    id: proposal_id.clone(),
                    title: summary
                        .map(|entry| entry.title.clone())
                        .unwrap_or_else(|| proposal.title.clone()),
                    relationship: format!("{relation} in {}", workstream.name),
                    status: Some(prompt_bead_status(summary, None)),
                    summary: compact_planned_elsewhere_summary(proposal.description.as_deref()),
                },
                priority,
            );
        }
    }

    apply_planned_elsewhere_budget(items.into_values().collect())
}

fn apply_planned_elsewhere_budget(
    items: Vec<PlannedElsewhereCandidate>,
) -> Vec<PlannedElsewherePromptContext> {
    let mut ranked_items = items;
    ranked_items.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.item.id.cmp(&right.item.id))
            .then_with(|| left.item.title.cmp(&right.item.title))
    });

    let mut selected = Vec::new();
    let mut used_bytes = 0usize;
    for item in ranked_items {
        if selected.len() >= PLANNED_ELSEWHERE_MAX_ITEMS
            || used_bytes >= PLANNED_ELSEWHERE_MAX_BYTES
        {
            break;
        }

        let separator_bytes = usize::from(!selected.is_empty());
        let remaining_bytes = PLANNED_ELSEWHERE_MAX_BYTES
            .saturating_sub(used_bytes)
            .saturating_sub(separator_bytes);
        let Some(item) = fit_planned_elsewhere_item_to_budget(&item.item, remaining_bytes) else {
            continue;
        };
        used_bytes += separator_bytes + planned_elsewhere_serialized_bytes(&item);
        selected.push(item);
    }

    selected.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.title.cmp(&right.title))
    });
    selected
}

fn fit_planned_elsewhere_item_to_budget(
    item: &PlannedElsewherePromptContext,
    remaining_bytes: usize,
) -> Option<PlannedElsewherePromptContext> {
    let mut fitted = item.clone();
    fitted.summary = fitted.summary.as_deref().and_then(|summary| {
        truncate_with_ascii_ellipsis(summary, PLANNED_ELSEWHERE_SUMMARY_MAX_BYTES)
    });

    let mut fitted = fit_planned_elsewhere_base_to_budget(&fitted, remaining_bytes)?;
    let Some(summary) = fitted.summary.clone() else {
        return Some(fitted);
    };

    if planned_elsewhere_serialized_bytes(&fitted) <= remaining_bytes {
        return Some(fitted);
    }

    let mut best_fit = None;
    let mut low = 0usize;
    let mut high = summary.len();
    while low <= high {
        let mid = low + (high - low) / 2;
        let mut candidate = fitted.clone();
        candidate.summary = truncate_with_ascii_ellipsis(&summary, mid);
        if planned_elsewhere_serialized_bytes(&candidate) <= remaining_bytes {
            best_fit = Some(candidate);
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }

    best_fit.or_else(|| {
        fitted.summary = None;
        (planned_elsewhere_serialized_bytes(&fitted) <= remaining_bytes).then_some(fitted)
    })
}

fn fit_planned_elsewhere_base_to_budget(
    item: &PlannedElsewherePromptContext,
    remaining_bytes: usize,
) -> Option<PlannedElsewherePromptContext> {
    let mut base_only = PlannedElsewherePromptContext {
        summary: None,
        ..item.clone()
    };
    if planned_elsewhere_serialized_bytes_without_summary(&base_only) <= remaining_bytes {
        return Some(item.clone());
    }

    let mut best_fit = base_only.clone();
    best_fit.title = "...".to_owned();
    best_fit.relationship = "...".to_owned();
    if planned_elsewhere_serialized_bytes_without_summary(&best_fit) > remaining_bytes {
        return None;
    }

    let mut low = 1usize;
    let mut high = item.relationship.len();
    while low <= high {
        let mid = low + (high - low) / 2;
        let Some(truncated_relationship) = truncate_with_ascii_ellipsis(&item.relationship, mid)
        else {
            break;
        };
        let mut candidate = best_fit.clone();
        candidate.relationship = truncated_relationship;
        if planned_elsewhere_serialized_bytes_without_summary(&candidate) <= remaining_bytes {
            best_fit = candidate;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }

    base_only = best_fit.clone();
    let mut low = 1usize;
    let mut high = item.title.len();
    while low <= high {
        let mid = low + (high - low) / 2;
        let Some(truncated_title) = truncate_with_ascii_ellipsis(&item.title, mid) else {
            break;
        };
        let mut candidate = base_only.clone();
        candidate.title = truncated_title;
        if planned_elsewhere_serialized_bytes_without_summary(&candidate) <= remaining_bytes {
            best_fit = candidate;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }

    let mut output = item.clone();
    output.title = best_fit.title;
    output.relationship = best_fit.relationship;
    Some(output)
}

fn planned_elsewhere_serialized_bytes(item: &PlannedElsewherePromptContext) -> usize {
    planned_elsewhere_rendered_bytes(&planned_elsewhere_item_body(item))
}

fn planned_elsewhere_serialized_bytes_without_summary(
    item: &PlannedElsewherePromptContext,
) -> usize {
    planned_elsewhere_rendered_bytes(&planned_elsewhere_item_body_without_summary(item))
}

fn planned_elsewhere_item_body(item: &PlannedElsewherePromptContext) -> String {
    let mut line = planned_elsewhere_item_body_without_summary(item);
    if let Some(summary) = item.summary.as_deref() {
        line.push_str("\nSummary:\n");
        line.push_str(summary);
    }
    line
}

fn planned_elsewhere_item_body_without_summary(item: &PlannedElsewherePromptContext) -> String {
    let mut line = format!("{} ({}) - {}", item.id, item.title, item.relationship);
    if let Some(status) = item.status.as_deref() {
        line.push_str(&format!("; status: {status}"));
    }
    line
}

fn planned_elsewhere_rendered_bytes(item_body: &str) -> usize {
    let mut lines = item_body.lines();
    let first_line = lines.next().unwrap_or_default();
    let continuation_indent = "- ".len().max(4);
    let mut bytes = if !first_line.is_empty() && opening_fence_delimiter(first_line).is_some() {
        "-".len() + 1 + continuation_indent + first_line.len()
    } else {
        "- ".len() + first_line.len()
    };

    for line in lines {
        bytes += 1 + continuation_indent + line.len();
    }

    bytes
}

fn parse_flow_override(raw: Option<&str>) -> AppResult<Option<FlowPreset>> {
    raw.map(str::parse).transpose()
}

fn read_bootstrap_idea(base_dir: &Path, args: &BootstrapArgs) -> AppResult<String> {
    let idea = match (&args.idea, &args.from_file) {
        (Some(idea), None) => idea.clone(),
        (None, Some(path)) => {
            let resolved = if path.is_absolute() {
                path.clone()
            } else {
                base_dir.join(path)
            };
            std::fs::read_to_string(&resolved).map_err(|error| {
                AppError::Io(std::io::Error::new(
                    error.kind(),
                    format!(
                        "failed to read bootstrap input file '{}': {}",
                        resolved.display(),
                        error
                    ),
                ))
            })?
        }
        _ => unreachable!("clap should enforce exactly one bootstrap input"),
    };

    if idea.trim().is_empty() {
        return Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "bootstrap idea input is empty",
        )));
    }

    Ok(idea)
}

enum RequirementsCreateHandoff {
    ProjectSeed(SeedHandoff),
    MilestoneBundle(MilestoneBundleHandoff),
}

fn load_requirements_handoff(
    base_dir: &Path,
    run_id: &str,
) -> AppResult<RequirementsCreateHandoff> {
    let store = FsRequirementsStore;
    let run_ids = store.list_requirements_run_ids(base_dir)?;
    if !run_ids.iter().any(|candidate| candidate == run_id) {
        return Err(AppError::InvalidRequirementsState {
            run_id: run_id.to_owned(),
            details: "requirements run not found".to_owned(),
        });
    }

    let run = requirements_service::read_requirements_run_status(&store, base_dir, run_id)?;
    if run.status != RequirementsStatus::Completed {
        return Err(AppError::RequirementsHandoffFailed {
            task_id: run_id.to_owned(),
            details: format!(
                "requirements run is in '{}' status, expected 'completed'",
                run.status
            ),
        });
    }

    match run.output_kind {
        RequirementsOutputKind::ProjectSeed => {
            requirements_service::extract_seed_handoff(&store, base_dir, run_id)
                .map(RequirementsCreateHandoff::ProjectSeed)
        }
        RequirementsOutputKind::MilestoneBundle => {
            requirements_service::extract_milestone_bundle_handoff(&store, base_dir, run_id)
                .map(RequirementsCreateHandoff::MilestoneBundle)
        }
    }
}

/// Load a `SeedHandoff` directly from a JSON project seed file, bypassing the
/// requirements pipeline. This is used by `project bootstrap --from-seed` for
/// backends where the quick-requirements pipeline cannot complete (e.g. model
/// behaviour prevents approval within the revision limit).
fn load_seed_from_file(
    base_dir: &Path,
    seed_path: &Path,
) -> AppResult<requirements_service::SeedHandoff> {
    let resolved = if seed_path.is_absolute() {
        seed_path.to_path_buf()
    } else {
        base_dir.join(seed_path)
    };

    let raw = std::fs::read_to_string(&resolved).map_err(|error| {
        AppError::Io(std::io::Error::new(
            error.kind(),
            format!(
                "failed to read seed file '{}': {}",
                resolved.display(),
                error
            ),
        ))
    })?;

    let seed: ProjectSeedPayload =
        serde_json::from_str(&raw).map_err(|error| AppError::RequirementsHandoffFailed {
            task_id: resolved.display().to_string(),
            details: format!("invalid project seed JSON: {error}"),
        })?;

    if !SUPPORTED_SEED_VERSIONS.contains(&seed.version) {
        return Err(AppError::RequirementsHandoffFailed {
            task_id: resolved.display().to_string(),
            details: format!(
                "unsupported seed version {} (supported: {:?})",
                seed.version, SUPPORTED_SEED_VERSIONS
            ),
        });
    }

    Ok(requirements_service::SeedHandoff {
        requirements_run_id: format!("seed-file:{}", resolved.display()),
        project_id: seed.project_id,
        project_name: seed.project_name,
        flow: seed.flow,
        prompt_body: seed.prompt_body,
        prompt_path: resolved,
        recommended_flow: None,
    })
}

fn map_requirements_project_error(error: AppError, run_id: &str) -> AppError {
    match error {
        AppError::DuplicateProject { project_id } => AppError::Io(std::io::Error::other(
            format!(
                "requirements run '{}' resolves to project '{}', but that project already exists. Use `ralph-burning project select {}` to work with the existing project.",
                run_id, project_id, project_id
            ),
        )),
        other => other,
    }
}

fn set_active_project_after_create(base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
    workspace_governance::set_active_project(base_dir, project_id).map_err(|error| {
        AppError::Io(std::io::Error::other(format!(
            "Project '{}' was created successfully but could not be selected as active: {}. Use `ralph-burning project select {}` to select it manually.",
            project_id, error, project_id
        )))
    })
}

fn set_active_milestone_after_command(
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<()> {
    workspace_governance::set_active_milestone(base_dir, milestone_id).map_err(|error| {
        AppError::Io(std::io::Error::other(format!(
            "The command succeeded but could not record milestone '{}' as active: {}. Run `ralph-burning milestone next {}` or `ralph-burning milestone run {}` with the explicit milestone ID.",
            milestone_id, error, milestone_id, milestone_id
        )))
    })
}

async fn start_created_project(base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(base_dir, project_id)?;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;

    match run_snapshot.status {
        RunStatus::NotStarted => {}
        RunStatus::Failed | RunStatus::Paused => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; use `ralph-burning run resume`",
                    run_snapshot.status
                ),
            });
        }
        status => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; run start requires 'not_started'",
                    status
                ),
            });
        }
    }
    if run_snapshot.has_active_run() {
        return Err(AppError::RunStartFailed {
            reason: "project already has an active run".to_owned(),
        });
    }

    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        base_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )?;

    let effective_config = EffectiveConfig::load_for_project(
        base_dir,
        Some(project_id),
        CliBackendOverrides::default(),
    )?;
    let agent_service =
        agent_execution_builder::build_agent_execution_service_for_config(&effective_config)?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;
    let amendment_queue = FsAmendmentQueueStore;

    println!("Starting run for project '{}'...", project_id);

    let result = engine::execute_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        base_dir,
        project_id,
        project_record.flow,
        &effective_config,
    )
    .await;

    match result {
        Ok(()) => {
            lock_guard.close()?;

            let final_snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;
            match final_snapshot.status {
                RunStatus::Completed => println!("Run completed successfully."),
                RunStatus::Paused => println!("{}", final_snapshot.status_summary),
                status => println!("Run finished with status '{}'.", status),
            }
            Ok(())
        }
        Err(error) => {
            let snapshot_after_error = run_snapshot_read
                .read_run_snapshot(base_dir, project_id)
                .ok();
            let close_error = lock_guard.close().err();

            if snapshot_after_error
                .as_ref()
                .is_some_and(|snapshot| snapshot.status == RunStatus::NotStarted)
            {
                let mut message = format!(
                    "Project '{}' created successfully but run failed to start: {}. Use `ralph-burning run start` to retry.",
                    project_id, error
                );
                if let Some(close_error) = close_error {
                    message.push_str(&format!(
                        " Writer lease cleanup also failed: {close_error}."
                    ));
                }
                return Err(AppError::Io(std::io::Error::other(message)));
            }

            Err(error)
        }
    }
}

async fn handle_amend(amend: AmendCommand) -> AppResult<()> {
    match amend.command {
        AmendSubcommand::Add(args) => handle_amend_add(args).await,
        AmendSubcommand::List => handle_amend_list().await,
        AmendSubcommand::Remove { id } => handle_amend_remove(id).await,
        AmendSubcommand::Clear => handle_amend_clear().await,
    }
}

async fn handle_amend_add(args: AmendAddArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let body = match (&args.text, &args.file) {
        (Some(text), None) => text.clone(),
        (None, Some(path)) => {
            let resolved = if path.is_absolute() {
                path.clone()
            } else {
                current_dir.join(path)
            };
            std::fs::read_to_string(&resolved).map_err(|error| {
                AppError::Io(std::io::Error::new(
                    error.kind(),
                    format!(
                        "failed to read amendment file '{}': {}",
                        resolved.display(),
                        error
                    ),
                ))
            })?
        }
        _ => unreachable!("clap should enforce exactly one input"),
    };

    if body.trim().is_empty() {
        return Err(AppError::AmendmentQueueError {
            details: "amendment body is empty".to_owned(),
        });
    }

    // Acquire an RAII writer lease to prevent races between the lease check
    // and the actual mutation.
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = match CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    ) {
        Ok(guard) => guard,
        Err(AppError::ProjectWriterLockHeld { .. })
        | Err(AppError::AcquisitionRollbackFailed { .. }) => {
            return Err(AppError::AmendmentLeaseConflict {
                project_id: project_id.to_string(),
            });
        }
        Err(other) => return Err(other),
    };

    let amendment_queue = FsAmendmentQueueStore;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let project_store = FsProjectStore;

    let result = service::add_manual_amendment(
        &amendment_queue,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &project_store,
        &current_dir,
        &project_id,
        &body,
    )?;

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = FileSystem::live_workspace_root_path(&current_dir)
            .join("daemon/leases")
            .join(format!("writer-{}.lock", project_id.as_str()));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    match result {
        service::AmendmentAddResult::Created { amendment_id } => {
            println!("Amendment: {}", amendment_id);
        }
        service::AmendmentAddResult::Duplicate { amendment_id } => {
            println!(
                "Duplicate amendment: existing amendment '{}' has the same content",
                amendment_id
            );
        }
    }

    Ok(())
}

async fn handle_amend_list() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let run_snapshot_read = FsRunSnapshotStore;
    let amendments = service::list_amendments(&run_snapshot_read, &current_dir, &project_id)?;

    if amendments.is_empty() {
        println!("No pending amendments.");
        return Ok(());
    }

    for amendment in &amendments {
        let body_preview = truncate_utf8(&amendment.body, 80);
        println!(
            "  {} [{}] dedup={} {}",
            amendment.amendment_id, amendment.source, amendment.dedup_key, body_preview
        );
    }

    Ok(())
}

async fn handle_amend_remove(id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Acquire an RAII writer lease to prevent races with daemon/CLI writers.
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = match CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    ) {
        Ok(guard) => guard,
        Err(AppError::ProjectWriterLockHeld { .. })
        | Err(AppError::AcquisitionRollbackFailed { .. }) => {
            return Err(AppError::AmendmentLeaseConflict {
                project_id: project_id.to_string(),
            });
        }
        Err(other) => return Err(other),
    };

    let amendment_queue = FsAmendmentQueueStore;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    service::remove_amendment(
        &amendment_queue,
        &run_snapshot_read,
        &run_snapshot_write,
        &current_dir,
        &project_id,
        &id,
    )?;

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = FileSystem::live_workspace_root_path(&current_dir)
            .join("daemon/leases")
            .join(format!("writer-{}.lock", project_id.as_str()));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    println!("Removed amendment '{}'", id);
    Ok(())
}

async fn handle_amend_clear() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Acquire an RAII writer lease to prevent races with daemon/CLI writers.
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = match CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    ) {
        Ok(guard) => guard,
        Err(AppError::ProjectWriterLockHeld { .. })
        | Err(AppError::AcquisitionRollbackFailed { .. }) => {
            return Err(AppError::AmendmentLeaseConflict {
                project_id: project_id.to_string(),
            });
        }
        Err(other) => return Err(other),
    };

    let amendment_queue = FsAmendmentQueueStore;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let result = service::clear_amendments(
        &amendment_queue,
        &run_snapshot_read,
        &run_snapshot_write,
        &current_dir,
        &project_id,
    );

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = FileSystem::live_workspace_root_path(&current_dir)
            .join("daemon/leases")
            .join(format!("writer-{}.lock", project_id.as_str()));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Capture close result but don't propagate yet — the partial-clear
    // contract requires surfacing removed/remaining IDs even if lease
    // cleanup also fails.
    let close_result = lock_guard.close();

    match result {
        Ok(removed) => {
            // On successful clear, propagate any close failure.
            close_result?;
            if removed.is_empty() {
                println!("No pending amendments to clear.");
            } else {
                println!("Cleared {} amendment(s).", removed.len());
                for id in &removed {
                    println!("  removed: {}", id);
                }
            }
        }
        Err(AppError::AmendmentClearPartial {
            removed, remaining, ..
        }) => {
            // Always surface partial-clear IDs, even if close also failed.
            eprintln!("Partial clear failure:");
            for id in &removed {
                eprintln!("  removed: {}", id);
            }
            for id in &remaining {
                eprintln!("  remaining: {}", id);
            }
            if let Err(close_err) = close_result {
                eprintln!("  (writer-lease cleanup also failed: {close_err})");
            }
            return Err(AppError::AmendmentClearPartial {
                removed_count: removed.len(),
                total: removed.len() + remaining.len(),
                removed,
                remaining,
            });
        }
        Err(other) => return Err(other),
    }

    Ok(())
}

/// UTF-8-safe body truncation. Truncates at a char boundary and appends "..."
/// if the body is longer than `max_chars`.
fn truncate_utf8(s: &str, max_chars: usize) -> String {
    let char_count = s.chars().count();
    if char_count <= max_chars {
        s.to_owned()
    } else {
        let truncated: String = s.chars().take(max_chars.saturating_sub(3)).collect();
        format!("{truncated}...")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_planned_elsewhere_budget, backfill_legacy_explicit_bead_flags,
        build_dependency_prompt_context, build_dependent_prompt_context,
        build_planned_elsewhere_context, compact_planned_elsewhere_summary,
        ensure_bead_belongs_to_milestone, ensure_bead_creation_targets_are_actionable,
        infer_parent_epic_id, load_milestone_bundle, map_br_list_error,
        planned_elsewhere_serialized_bytes, planned_elsewhere_serialized_bytes_without_summary,
        resolve_bead_plan, validate_milestone_plan_snapshot, PlannedElsewhereCandidate,
        PlannedElsewherePriority, PLANNED_ELSEWHERE_MAX_BYTES, PLANNED_ELSEWHERE_SUMMARY_MAX_BYTES,
    };
    use std::collections::BTreeMap;
    use std::path::Path;

    use crate::adapters::br_models::{
        BeadDetail, BeadPriority, BeadStatus, BeadSummary, BeadType, DependencyKind, DependencyRef,
    };
    use crate::adapters::br_process::BrError;
    use crate::adapters::fs::FsMilestonePlanStore;
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::model::{MilestoneId, MilestoneStatus};
    use crate::contexts::project_run_record::service::PlannedElsewherePromptContext;
    use crate::shared::domain::FlowPreset;
    use crate::shared::error::AppError;

    fn setup_milestone_workspace(dir: &Path, milestone_id: &str) {
        std::fs::create_dir_all(dir.join(".ralph-burning/milestones").join(milestone_id))
            .expect("create milestone workspace");
    }

    fn sample_bundle() -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
            },
            executive_summary: "Summary".to_owned(),
            goals: vec!["Goal".to_owned()],
            non_goals: Vec::new(),
            constraints: Vec::new(),
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Criterion".to_owned(),
                covered_by: vec!["bead-2".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Creation".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Bootstrap bead-backed task creation".to_owned(),
                    description: None,
                    bead_type: Some("feature".to_owned()),
                    priority: Some(1),
                    labels: Vec::new(),
                    depends_on: vec!["bead-1".to_owned()],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: Some(FlowPreset::DocsChange),
                }],
            }],
            default_flow: FlowPreset::QuickDev,
            agents_guidance: None,
        }
    }

    fn sample_two_bead_bundle() -> MilestoneBundle {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads.insert(
            0,
            BeadProposal {
                bead_id: None,
                explicit_id: None,
                title: "Define task-source metadata".to_owned(),
                description: None,
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: Vec::new(),
                depends_on: Vec::new(),
                acceptance_criteria: Vec::new(),
                flow_override: None,
            },
        );
        bundle
    }

    fn sample_three_bead_bundle() -> MilestoneBundle {
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Document task bootstrap follow-up".to_owned(),
            description: Some(
                "Capture the operator-facing workflow once project creation is stable.".to_owned(),
            ),
            bead_type: Some("docs".to_owned()),
            priority: Some(2),
            labels: Vec::new(),
            depends_on: vec!["bead-2".to_owned()],
            acceptance_criteria: Vec::new(),
            flow_override: None,
        });
        bundle
    }

    #[test]
    fn load_milestone_bundle_rejects_invalid_bundle_semantics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        setup_milestone_workspace(tmp.path(), milestone_id.as_str());

        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads[1].bead_id = Some("bead-1".to_owned());
        let raw = serde_json::to_string_pretty(&bundle).expect("serialize bundle");
        std::fs::write(
            tmp.path()
                .join(".ralph-burning/milestones")
                .join(milestone_id.as_str())
                .join("plan.json"),
            raw,
        )
        .expect("write plan.json");

        let error = load_milestone_bundle(&FsMilestonePlanStore, tmp.path(), &milestone_id)
            .expect_err("invalid plan bundle should fail");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error.to_string().contains("duplicate bead identifier"));
    }

    fn sample_bead() -> BeadDetail {
        BeadDetail {
            id: "ms-alpha.bead-2".to_owned(),
            title: "Bootstrap bead-backed task creation".to_owned(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Feature,
            labels: Vec::new(),
            description: None,
            acceptance_criteria: vec!["Ship it".to_owned()],
            dependencies: vec![DependencyRef {
                id: "ms-alpha.epic-1".to_owned(),
                kind: DependencyKind::ParentChild,
                title: Some("Parent".to_owned()),
                status: None,
            }],
            dependents: Vec::new(),
            owner: None,
            created_at: None,
            updated_at: None,
        }
    }

    fn render_planned_elsewhere_item(item: &PlannedElsewherePromptContext) -> String {
        let mut line = format!("{} ({}) - {}", item.id, item.title, item.relationship);
        if let Some(status) = item.status.as_deref() {
            line.push_str(&format!("; status: {status}"));
        }
        if let Some(summary) = item.summary.as_deref() {
            line.push_str("\nSummary:\n");
            line.push_str(summary);
        }
        let mut lines = line.lines();
        let first_line = lines.next().unwrap_or_default();
        let mut rendered = format!("- {first_line}");
        for continuation in lines {
            rendered.push('\n');
            rendered.push_str("    ");
            rendered.push_str(continuation);
        }
        rendered
    }

    fn render_planned_elsewhere_block(items: &[PlannedElsewherePromptContext]) -> String {
        items
            .iter()
            .map(render_planned_elsewhere_item)
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn infer_parent_epic_id_ignores_child_edges_from_dependents() {
        let mut bead = sample_bead();
        bead.dependencies.clear();
        bead.dependents.push(DependencyRef {
            id: "ms-alpha.bead-3".to_owned(),
            kind: DependencyKind::ParentChild,
            title: Some("Child bead".to_owned()),
            status: None,
        });

        assert_eq!(infer_parent_epic_id(&bead), None);
    }

    #[test]
    fn resolve_bead_plan_returns_per_bead_flow_override() {
        let bundle = sample_two_bead_bundle();
        let bead = sample_bead();
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, Some(FlowPreset::DocsChange));
        assert!(resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_does_not_confirm_title_fallback_against_mismatched_explicit_bead_id() {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("ms-alpha.bead-200".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        let bead = sample_bead();
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, None);
        assert!(!resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_confirms_legacy_canonical_bead_ids_after_backfill() {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("ms-alpha.bead-2".to_owned());
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        backfill_legacy_explicit_bead_flags(&mut bundle, &milestone_id);
        let bead = sample_bead();

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, Some(FlowPreset::DocsChange));
        assert!(resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_treats_legacy_short_canonical_slot_ids_as_implicit_after_backfill() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads[1].bead_id = Some("bead-2".to_owned());
        backfill_legacy_explicit_bead_flags(&mut bundle, &milestone_id);

        let mut bead = sample_bead();
        bead.title = "Renamed live bead".to_owned();

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, None);
        assert!(!resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_treats_legacy_qualified_canonical_slot_ids_as_implicit_after_backfill() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads[1].bead_id = Some("ms-alpha.bead-2".to_owned());
        backfill_legacy_explicit_bead_flags(&mut bundle, &milestone_id);

        let mut bead = sample_bead();
        bead.title = "Renamed live bead".to_owned();

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, None);
        assert!(!resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_falls_back_when_live_title_drifted() {
        let bundle = sample_bundle();
        let mut bead = sample_bead();
        bead.title = "Renamed live bead".to_owned();
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, None);
        assert!(!resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_does_not_confirm_reordered_implicit_proposal_by_title_alone() {
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads.swap(0, 1);
        let bead = sample_bead();
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        assert_eq!(resolved.flow_override, Some(FlowPreset::DocsChange));
        assert!(!resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_uses_current_implicit_slot_to_break_duplicate_title_ties() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads[0].title = "Bootstrap bead-backed task creation".to_owned();
        bundle.workstreams[0].beads[0].flow_override = Some(FlowPreset::QuickDev);
        bundle.workstreams[0].beads[1].flow_override = Some(FlowPreset::DocsChange);

        let resolved =
            resolve_bead_plan(&bundle, &milestone_id, &sample_bead()).expect("resolve bead");

        assert_eq!(resolved.flow_override, Some(FlowPreset::DocsChange));
        assert!(resolved.membership_confirmed);
    }

    #[test]
    fn resolve_bead_plan_rejects_cross_milestone_bead_ids() {
        let bundle = sample_bundle();
        let mut bead = sample_bead();
        bead.id = "other-ms.bead-2".to_owned();
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let error = resolve_bead_plan(&bundle, &milestone_id, &bead)
            .expect_err("cross-milestone bead should fail");

        assert!(matches!(error, AppError::InvalidConfigValue { .. }));
    }

    #[test]
    fn build_planned_elsewhere_context_skips_neighbors_when_membership_is_unconfirmed() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads[0].description =
            Some("Define metadata before project creation.".to_owned());
        let mut bead = sample_bead();
        bead.title = "Renamed live bead".to_owned();
        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &BTreeMap::new(),
        );

        assert!(planned_elsewhere.is_empty());
    }

    #[test]
    fn build_planned_elsewhere_context_skips_shared_acceptance_owners_when_membership_is_unconfirmed(
    ) {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.acceptance_map[0].covered_by = vec!["bead-2".to_owned(), "bead-4".to_owned()];
        bundle.workstreams.push(Workstream {
            name: "Validation".to_owned(),
            description: Some("Confirm task bootstrap behavior.".to_owned()),
            beads: vec![BeadProposal {
                bead_id: Some("bead-4".to_owned()),
                explicit_id: Some(true),
                title: "Validate task bootstrap follow-up".to_owned(),
                description: Some(
                    "Confirm the shared acceptance outcome without expanding the current bead."
                        .to_owned(),
                ),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: Vec::new(),
                depends_on: Vec::new(),
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: None,
            }],
        });
        let mut bead = sample_bead();
        bead.title = "Renamed live bead".to_owned();
        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");
        let bead_summaries = BTreeMap::from([(
            "ms-alpha.bead-4".to_owned(),
            BeadSummary {
                id: "ms-alpha.bead-4".to_owned(),
                title: "Validate task bootstrap follow-up".to_owned(),
                status: BeadStatus::Open,
                priority: BeadPriority::new(1),
                bead_type: BeadType::Task,
                labels: Vec::new(),
            },
        )]);

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &bead_summaries,
        );

        assert!(planned_elsewhere.is_empty());
    }

    #[test]
    fn build_planned_elsewhere_context_ignores_unconfirmed_title_fallback_location() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.workstreams[0].beads.swap(0, 1);
        bundle.workstreams[0].beads[0].description =
            Some("This proposal title still matches the live bead.".to_owned());
        bundle.workstreams[0].beads[1].description =
            Some("This is the real adjacent slot neighbor.".to_owned());
        let bead = sample_bead();
        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &BTreeMap::new(),
        );

        assert!(planned_elsewhere.is_empty());
    }

    #[test]
    fn build_planned_elsewhere_context_drops_implicit_slot_hint_when_slot_was_reassigned() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_three_bead_bundle();
        bundle.workstreams[0].beads[1].bead_id = Some("ms-alpha.bead-200".to_owned());
        bundle.workstreams[0].beads[1].explicit_id = Some(true);
        let bead = sample_bead();
        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &BTreeMap::new(),
        );

        assert!(planned_elsewhere.is_empty());
    }

    #[test]
    fn build_dependency_prompt_context_sorts_items_by_id_and_title() {
        let relations = vec![
            DependencyRef {
                id: "ms-alpha.bead-20".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Zulu".to_owned()),
                status: None,
            },
            DependencyRef {
                id: "ms-alpha.bead-3".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Bravo".to_owned()),
                status: None,
            },
            DependencyRef {
                id: "ms-alpha.bead-11".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Alpha".to_owned()),
                status: None,
            },
        ];

        let prompt_context = build_dependency_prompt_context(&relations, &BTreeMap::new());

        assert_eq!(
            prompt_context
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ms-alpha.bead-11", "ms-alpha.bead-20", "ms-alpha.bead-3"]
        );
    }

    #[test]
    fn build_dependent_prompt_context_sorts_items_by_id_and_title() {
        let relations = vec![
            DependencyRef {
                id: "ms-alpha.bead-9".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Zulu".to_owned()),
                status: None,
            },
            DependencyRef {
                id: "ms-alpha.bead-1".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Alpha".to_owned()),
                status: None,
            },
        ];

        let prompt_context = build_dependent_prompt_context(&relations, &BTreeMap::new());

        assert_eq!(
            prompt_context
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ms-alpha.bead-1", "ms-alpha.bead-9"]
        );
    }

    #[test]
    fn build_dependency_prompt_context_uses_relation_status_when_summary_missing() {
        let relations = vec![DependencyRef {
            id: "ms-alpha.bead-1".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Define task-source metadata".to_owned()),
            status: Some(BeadStatus::Closed),
        }];

        let prompt_context = build_dependency_prompt_context(&relations, &BTreeMap::new());

        assert_eq!(prompt_context[0].status.as_deref(), Some("closed"));
        assert_eq!(prompt_context[0].outcome.as_deref(), Some("completed"));
    }

    #[test]
    fn build_dependent_prompt_context_uses_relation_status_when_summary_missing() {
        let relations = vec![DependencyRef {
            id: "ms-alpha.bead-3".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Document task bootstrap follow-up".to_owned()),
            status: Some(BeadStatus::InProgress),
        }];

        let prompt_context = build_dependent_prompt_context(&relations, &BTreeMap::new());

        assert_eq!(prompt_context[0].status.as_deref(), Some("in_progress"));
        assert_eq!(prompt_context[0].outcome.as_deref(), Some("active"));
    }

    #[test]
    fn build_dependency_prompt_context_uses_unknown_status_when_all_status_sources_are_missing() {
        let relations = vec![DependencyRef {
            id: "ms-alpha.bead-1".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Define task-source metadata".to_owned()),
            status: None,
        }];

        let prompt_context = build_dependency_prompt_context(&relations, &BTreeMap::new());

        assert_eq!(prompt_context[0].status.as_deref(), Some("unknown"));
        assert_eq!(prompt_context[0].outcome.as_deref(), Some("unknown"));
    }

    #[test]
    fn build_planned_elsewhere_context_sorts_explicit_and_neighbor_items_by_id() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bundle = sample_three_bead_bundle();
        let mut bead = sample_bead();
        bead.dependencies = vec![
            DependencyRef {
                id: "ms-alpha.bead-3".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Document task bootstrap follow-up".to_owned()),
                status: None,
            },
            DependencyRef {
                id: "ms-alpha.bead-1".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Define task-source metadata".to_owned()),
                status: None,
            },
        ];
        bead.dependents = vec![
            DependencyRef {
                id: "ms-alpha.bead-9".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Later dependent".to_owned()),
                status: None,
            },
            DependencyRef {
                id: "ms-alpha.bead-4".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Sooner dependent".to_owned()),
                status: None,
            },
        ];
        let resolved = resolve_bead_plan(&bundle, &milestone_id, &bead).expect("resolve bead");

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &BTreeMap::new(),
        );

        assert_eq!(
            planned_elsewhere
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "ms-alpha.bead-1",
                "ms-alpha.bead-3",
                "ms-alpha.bead-4",
                "ms-alpha.bead-9",
            ]
        );
    }

    #[test]
    fn build_planned_elsewhere_context_includes_shared_acceptance_owners_from_other_workstreams() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.acceptance_map[0].covered_by = vec![
            "bead-2".to_owned(),
            "bead-4".to_owned(),
            "bead-4".to_owned(),
        ];
        bundle.workstreams.push(Workstream {
            name: "Validation".to_owned(),
            description: Some("Confirm task bootstrap behavior.".to_owned()),
            beads: vec![BeadProposal {
                bead_id: Some("bead-4".to_owned()),
                explicit_id: Some(true),
                title: "Validate task bootstrap follow-up".to_owned(),
                description: Some(
                    "Confirm the shared acceptance outcome without expanding the current bead."
                        .to_owned(),
                ),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: Vec::new(),
                depends_on: Vec::new(),
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: None,
            }],
        });
        let resolved =
            resolve_bead_plan(&bundle, &milestone_id, &sample_bead()).expect("resolve bead");
        let bead_summaries = BTreeMap::from([(
            "ms-alpha.bead-4".to_owned(),
            BeadSummary {
                id: "ms-alpha.bead-4".to_owned(),
                title: "Validate task bootstrap follow-up".to_owned(),
                status: BeadStatus::Open,
                priority: BeadPriority::new(1),
                bead_type: BeadType::Task,
                labels: Vec::new(),
            },
        )]);

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &sample_bead(),
            &resolved,
            &bead_summaries,
        );

        assert!(planned_elsewhere.iter().any(|item| {
            item.id == "ms-alpha.bead-4"
                && item
                    .relationship
                    .contains("shared milestone acceptance ownership in Validation (AC-1)")
                && item.status.as_deref() == Some("open")
                && item.summary.as_deref()
                    == Some(
                        "Confirm the shared acceptance outcome without expanding the current bead.",
                    )
        }));
    }

    #[test]
    fn build_planned_elsewhere_context_excludes_upstream_dependencies_from_shared_acceptance_owners(
    ) {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_two_bead_bundle();
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];
        let resolved =
            resolve_bead_plan(&bundle, &milestone_id, &sample_bead()).expect("resolve bead");
        let bead_summaries = BTreeMap::from([(
            "ms-alpha.bead-1".to_owned(),
            BeadSummary {
                id: "ms-alpha.bead-1".to_owned(),
                title: "Define task-source metadata".to_owned(),
                status: BeadStatus::Closed,
                priority: BeadPriority::new(1),
                bead_type: BeadType::Task,
                labels: Vec::new(),
            },
        )]);
        let mut bead = sample_bead();
        bead.dependencies.push(DependencyRef {
            id: "ms-alpha.bead-1".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Define task-source metadata".to_owned()),
            status: Some(BeadStatus::Closed),
        });

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &bead_summaries,
        );

        assert!(planned_elsewhere.iter().all(|item| {
            item.id != "ms-alpha.bead-1"
                || !item
                    .relationship
                    .contains("shared milestone acceptance ownership")
        }));
    }

    #[test]
    fn build_planned_elsewhere_context_prefers_downstream_dependent_relationship_over_shared_acceptance_ownership(
    ) {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let mut bundle = sample_three_bead_bundle();
        bundle.acceptance_map[0].covered_by = vec!["bead-2".to_owned(), "bead-3".to_owned()];
        let resolved =
            resolve_bead_plan(&bundle, &milestone_id, &sample_bead()).expect("resolve bead");
        let bead_summaries = BTreeMap::from([(
            "ms-alpha.bead-3".to_owned(),
            BeadSummary {
                id: "ms-alpha.bead-3".to_owned(),
                title: "Document task bootstrap follow-up".to_owned(),
                status: BeadStatus::Open,
                priority: BeadPriority::new(2),
                bead_type: BeadType::Docs,
                labels: Vec::new(),
            },
        )]);
        let mut bead = sample_bead();
        bead.dependents.push(DependencyRef {
            id: "ms-alpha.bead-3".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Document task bootstrap follow-up".to_owned()),
            status: Some(BeadStatus::Open),
        });

        let planned_elsewhere = build_planned_elsewhere_context(
            &bundle,
            &milestone_id,
            &bead,
            &resolved,
            &bead_summaries,
        );

        let matching = planned_elsewhere
            .iter()
            .filter(|item| item.id == "ms-alpha.bead-3")
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].relationship, "downstream dependent");
        assert!(!matching[0]
            .relationship
            .contains("shared milestone acceptance ownership"));
    }

    #[test]
    fn compact_planned_elsewhere_summary_skips_fenced_openers() {
        let summary = compact_planned_elsewhere_summary(Some(
            "```md\n## Review Policy\nKeep this example fenced.\n```\nCapture the real follow-up text.",
        ));

        assert_eq!(summary.as_deref(), Some("Capture the real follow-up text."));
    }

    #[test]
    fn apply_planned_elsewhere_budget_prefers_high_priority_items_and_caps_bytes() {
        let long_summary = "Capture deterministic scope context. ".repeat(32);
        let budgeted = apply_planned_elsewhere_budget(vec![
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-10".to_owned(),
                    title: "Direct dependent follow-up alpha".to_owned(),
                    relationship: "downstream dependent".to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(long_summary.clone()),
                },
                priority: PlannedElsewherePriority::DirectDependent,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-11".to_owned(),
                    title: "Direct dependent follow-up beta".to_owned(),
                    relationship: "downstream dependent".to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(long_summary.clone()),
                },
                priority: PlannedElsewherePriority::DirectDependent,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-20".to_owned(),
                    title: "Shared acceptance validation alpha".to_owned(),
                    relationship:
                        "shared milestone acceptance ownership in Validation (AC-1, AC-2)"
                            .to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(long_summary.clone()),
                },
                priority: PlannedElsewherePriority::SharedAcceptance,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-21".to_owned(),
                    title: "Shared acceptance validation beta".to_owned(),
                    relationship:
                        "shared milestone acceptance ownership in Validation (AC-3, AC-4)"
                            .to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(long_summary.clone()),
                },
                priority: PlannedElsewherePriority::SharedAcceptance,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-30".to_owned(),
                    title: "Adjacent same-workstream follow-up".to_owned(),
                    relationship: "adjacent same-workstream bead in Task Substrate".to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(long_summary.clone()),
                },
                priority: PlannedElsewherePriority::AdjacentNeighbor,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-40".to_owned(),
                    title: "Upstream blocker context".to_owned(),
                    relationship: "upstream dependency already planned elsewhere in Task Substrate"
                        .to_owned(),
                    status: Some("closed".to_owned()),
                    summary: Some(long_summary),
                },
                priority: PlannedElsewherePriority::UpstreamNeighbor,
            },
        ]);

        assert_eq!(
            budgeted
                .iter()
                .filter(|item| {
                    matches!(
                        item.id.as_str(),
                        "ms-alpha.bead-10"
                            | "ms-alpha.bead-11"
                            | "ms-alpha.bead-20"
                            | "ms-alpha.bead-21"
                    )
                })
                .count(),
            4
        );
        assert!(budgeted.iter().all(|item| {
            item.summary
                .as_ref()
                .map(|summary| summary.len() <= PLANNED_ELSEWHERE_SUMMARY_MAX_BYTES)
                .unwrap_or(true)
        }));
        assert!(
            budgeted
                .iter()
                .map(planned_elsewhere_serialized_bytes)
                .sum::<usize>()
                <= PLANNED_ELSEWHERE_MAX_BYTES
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_skips_scope_labels_and_returns_body_text() {
        let summary =
            compact_planned_elsewhere_summary(Some("Goal:\nKeep project creation deterministic."));

        assert_eq!(
            summary.as_deref(),
            Some("Keep project creation deterministic.")
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_preserves_continuation_lines_in_same_paragraph() {
        let summary = compact_planned_elsewhere_summary(Some(
            "Goal:\nCapture the operator-facing workflow once project creation is stable,\nincluding the follow-up validation handoff.\n\nNon-goals:\nLeave execution wiring unchanged.",
        ));

        assert_eq!(
            summary.as_deref(),
            Some(
                "Capture the operator-facing workflow once project creation is stable,\nincluding the follow-up validation handoff."
            )
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_uses_inline_scope_label_body() {
        let summary = compact_planned_elsewhere_summary(Some(
            "Scope: Capture the operator-facing workflow once project creation is stable.",
        ));

        assert_eq!(
            summary.as_deref(),
            Some("Capture the operator-facing workflow once project creation is stable.")
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_skips_level_one_heading() {
        let summary = compact_planned_elsewhere_summary(Some(
            "# Planned Follow-up\nCapture the operator-facing workflow once project creation is stable.",
        ));

        assert_eq!(
            summary.as_deref(),
            Some("Capture the operator-facing workflow once project creation is stable.")
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_skips_non_goal_section_labels() {
        let summary = compact_planned_elsewhere_summary(Some(
            "Non-goals:\nLeave the current bead scoped to prompt generation.",
        ));

        assert_eq!(
            summary.as_deref(),
            Some("Leave the current bead scoped to prompt generation.")
        );
    }

    #[test]
    fn compact_planned_elsewhere_summary_skips_acceptance_criteria_labels() {
        let summary = compact_planned_elsewhere_summary(Some(
            "Acceptance Criteria:\nCapture the operator-facing handoff after prompt generation lands.",
        ));

        assert_eq!(
            summary.as_deref(),
            Some("Capture the operator-facing handoff after prompt generation lands.")
        );
    }

    #[test]
    fn planned_elsewhere_serialized_bytes_match_rendered_bullet_output() {
        let item = PlannedElsewherePromptContext {
            id: "ms-alpha.bead-3".to_owned(),
            title: "Document milestone bootstrap flow".to_owned(),
            relationship: "adjacent same-workstream bead in Task Substrate".to_owned(),
            status: Some("open".to_owned()),
            summary: Some(
                "Capture the operator-facing workflow once project creation is stable,\nincluding the follow-up validation handoff.".to_owned(),
            ),
        };

        let without_summary = PlannedElsewherePromptContext {
            summary: None,
            ..item.clone()
        };

        assert_eq!(
            planned_elsewhere_serialized_bytes_without_summary(&item),
            render_planned_elsewhere_item(&without_summary).len()
        );
        assert_eq!(
            planned_elsewhere_serialized_bytes(&item),
            render_planned_elsewhere_item(&item).len()
        );
    }

    #[test]
    fn apply_planned_elsewhere_budget_accounts_for_rendered_multiline_overhead() {
        let multiline_summary =
            "Capture deterministic scope context for execution.\nKeep the related validation handoff nearby without absorbing it."
                .repeat(8);
        let budgeted = apply_planned_elsewhere_budget(vec![
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-10".to_owned(),
                    title: "Direct dependent follow-up alpha".to_owned(),
                    relationship: "downstream dependent".to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(multiline_summary.clone()),
                },
                priority: PlannedElsewherePriority::DirectDependent,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-11".to_owned(),
                    title: "Direct dependent follow-up beta".to_owned(),
                    relationship: "downstream dependent".to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(multiline_summary.clone()),
                },
                priority: PlannedElsewherePriority::DirectDependent,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-20".to_owned(),
                    title: "Shared acceptance validation alpha".to_owned(),
                    relationship:
                        "shared milestone acceptance ownership in Validation (AC-1, AC-2)"
                            .to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(multiline_summary.clone()),
                },
                priority: PlannedElsewherePriority::SharedAcceptance,
            },
            PlannedElsewhereCandidate {
                item: PlannedElsewherePromptContext {
                    id: "ms-alpha.bead-21".to_owned(),
                    title: "Shared acceptance validation beta".to_owned(),
                    relationship:
                        "shared milestone acceptance ownership in Validation (AC-3, AC-4)"
                            .to_owned(),
                    status: Some("open".to_owned()),
                    summary: Some(multiline_summary),
                },
                priority: PlannedElsewherePriority::SharedAcceptance,
            },
        ]);

        assert!(
            render_planned_elsewhere_block(&budgeted).len() <= PLANNED_ELSEWHERE_MAX_BYTES,
            "rendered bytes: {}",
            render_planned_elsewhere_block(&budgeted).len()
        );
    }

    #[test]
    fn apply_planned_elsewhere_budget_keeps_truncated_base_line_when_item_exceeds_budget() {
        let oversized = "oversized context ".repeat(220);
        let budgeted = apply_planned_elsewhere_budget(vec![PlannedElsewhereCandidate {
            item: PlannedElsewherePromptContext {
                id: "ms-alpha.bead-10".to_owned(),
                title: format!("Direct dependent {oversized}"),
                relationship: format!(
                    "shared milestone acceptance ownership in Validation ({oversized})"
                ),
                status: Some("open".to_owned()),
                summary: Some(oversized),
            },
            priority: PlannedElsewherePriority::DirectDependent,
        }]);

        assert_eq!(budgeted.len(), 1);
        assert_eq!(budgeted[0].id, "ms-alpha.bead-10");
        assert_eq!(budgeted[0].status.as_deref(), Some("open"));
        assert!(render_planned_elsewhere_block(&budgeted).contains("ms-alpha.bead-10"));
        assert!(render_planned_elsewhere_block(&budgeted).contains("status: open"));
        assert!(
            render_planned_elsewhere_block(&budgeted).len() <= PLANNED_ELSEWHERE_MAX_BYTES,
            "rendered bytes: {}",
            render_planned_elsewhere_block(&budgeted).len()
        );
    }

    #[test]
    fn map_br_list_error_marks_corrupt_exit_output_as_corrupt_record() {
        let error = map_br_list_error(BrError::BrExitError {
            exit_code: 1,
            stdout: String::new(),
            stderr: "failed to parse .beads/issues.jsonl: corrupt json".to_owned(),
            command: "br list --json".to_owned(),
        });

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error.to_string().contains(".beads/issues.jsonl"));
    }

    #[test]
    fn map_br_list_error_keeps_missing_issues_file_failures_degradable() {
        let error = map_br_list_error(BrError::BrExitError {
            exit_code: 1,
            stdout: String::new(),
            stderr: "failed to read .beads/issues.jsonl: No such file or directory".to_owned(),
            command: "br list --json".to_owned(),
        });

        assert!(matches!(error, AppError::Io(_)));
    }

    #[test]
    fn map_br_list_error_keeps_generic_exit_failures_degradable() {
        let error = map_br_list_error(BrError::BrExitError {
            exit_code: 1,
            stdout: String::new(),
            stderr: "simulated br list failure".to_owned(),
            command: "br list --json".to_owned(),
        });

        assert!(matches!(error, AppError::Io(_)));
    }

    #[test]
    fn ensure_bead_belongs_to_milestone_accepts_matching_prefix() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        ensure_bead_belongs_to_milestone(&milestone_id, &sample_bead())
            .expect("matching bead should pass");
    }

    #[test]
    fn validate_milestone_plan_snapshot_rejects_stale_hash() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let error =
            validate_milestone_plan_snapshot(&milestone_id, Some("status-hash"), 2, "plan-hash")
                .expect_err("stale status hash should fail");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error.to_string().contains("plan metadata is stale"));
    }

    #[test]
    fn validate_milestone_plan_snapshot_allows_legacy_missing_metadata() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let version = validate_milestone_plan_snapshot(&milestone_id, None, 0, "plan-hash")
            .expect("legacy metadata should be accepted");

        assert_eq!(version, None);
    }

    #[test]
    fn validate_milestone_plan_snapshot_rejects_hash_without_version() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        let error =
            validate_milestone_plan_snapshot(&milestone_id, Some("plan-hash"), 0, "plan-hash")
                .expect_err("hash without version should fail");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error.to_string().contains("plan_version is 0"));
    }

    #[test]
    fn ensure_bead_creation_targets_are_actionable_rejects_terminal_milestone() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead = sample_bead();

        let error = ensure_bead_creation_targets_are_actionable(
            &milestone_id,
            MilestoneStatus::Completed,
            &bead,
        )
        .expect_err("completed milestone should be rejected");

        assert!(matches!(error, AppError::InvalidConfigValue { .. }));
        assert!(error
            .to_string()
            .contains("milestone 'ms-alpha' is already completed"));
    }

    #[test]
    fn ensure_bead_creation_targets_are_actionable_rejects_non_actionable_bead_statuses() {
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        for status in [BeadStatus::Closed, BeadStatus::Deferred] {
            let mut bead = sample_bead();
            bead.status = status.clone();

            let error = ensure_bead_creation_targets_are_actionable(
                &milestone_id,
                MilestoneStatus::Ready,
                &bead,
            )
            .expect_err("non-actionable bead status should be rejected");

            assert!(matches!(error, AppError::InvalidConfigValue { .. }));
            assert!(error
                .to_string()
                .contains(&format!("bead is already {status}")));
        }
    }

    // ── claim_bead_in_br tests ────────────────────────────────────────────

    #[cfg(unix)]
    mod claim_bead_tests {
        use std::os::unix::fs::PermissionsExt;

        use super::*;
        use crate::adapters::fs::FsMilestoneControllerStore;
        use crate::contexts::milestone_record::controller::{
            self as milestone_controller, MilestoneControllerState,
        };
        use crate::test_support::env::{lock_path_mutex, PathGuard};

        fn write_beads_export(base_dir: &std::path::Path, contents: &str) {
            let beads_dir = base_dir.join(".beads");
            std::fs::create_dir_all(&beads_dir).expect("create .beads dir");
            std::fs::write(beads_dir.join("issues.jsonl"), contents).expect("write issues.jsonl");
        }

        fn claim_owner() -> &'static str {
            "project-claim-test"
        }

        fn other_claim_owner() -> &'static str {
            "other-project-claim-test"
        }

        /// Install a fake `br` script that succeeds on `update` and `sync`
        /// subcommands and returns bead detail JSON on `show`.
        fn install_fake_br_claim_success(base_dir: &std::path::Path, bead_id: &str) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  update)
    echo "Updated {bead_id}"
    exit 0
    ;;
  sync)
    echo "Synced"
    exit 0
    ;;
  show)
    cat <<'BEAD_JSON'
{{"id":"{bead_id}","title":"Test bead","status":"in_progress","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}}
BEAD_JSON
    exit 0
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` that fails on `update` (non-zero exit).
        fn install_fake_br_claim_failure(base_dir: &std::path::Path) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = r#"#!/bin/sh
case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  update)
    echo "update failed: permission denied" >&2
    exit 1
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#;
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` that fails the first `sync --flush-only` after a
        /// successful claim update, then succeeds on retry while tolerating an
        /// idempotent second `update --status=in_progress`.
        fn install_fake_br_claim_retry_after_sync_failure(
            base_dir: &std::path::Path,
            bead_id: &str,
        ) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
set -eu

case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  update)
    count=0
    if [ -f .beads/update-count ]; then
      count=$(cat .beads/update-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/update-count
    echo "in_progress" > .beads/{bead_id}.status
    echo "Updated {bead_id}"
    exit 0
    ;;
  sync)
    count=0
    if [ -f .beads/sync-count ]; then
      count=$(cat .beads/sync-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/sync-count
    if [ "$count" -eq 1 ]; then
      echo "transient sync failure" >&2
      exit 1
    fi
    echo "Synced"
    exit 0
    ;;
  show)
    status="open"
    if [ -f .beads/{bead_id}.status ]; then
      status=$(cat .beads/{bead_id}.status)
    fi
    cat <<BEAD_JSON
{{"id":"{bead_id}","title":"Test bead","status":"$status","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}}
BEAD_JSON
    exit 0
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` that models a backend where replaying the same
        /// claim twice is rejected. The retry only succeeds when
        /// `claim_bead_in_br()` recognizes that the recovered flush already
        /// replayed `update --status=in_progress` and skips the duplicate
        /// update.
        fn install_fake_br_claim_retry_requires_recovered_claim_short_circuit(
            base_dir: &std::path::Path,
            bead_id: &str,
        ) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
set -eu

case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  update)
    count=0
    if [ -f .beads/update-count ]; then
      count=$(cat .beads/update-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/update-count
    if [ "$count" -gt 1 ]; then
      echo "{bead_id} already in progress" >&2
      exit 1
    fi
    echo "in_progress" > .beads/{bead_id}.status
    echo "Updated {bead_id}"
    exit 0
    ;;
  sync)
    count=0
    if [ -f .beads/sync-count ]; then
      count=$(cat .beads/sync-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/sync-count
    if [ "$count" -eq 1 ]; then
      echo "transient sync failure" >&2
      exit 1
    fi
    echo "Synced"
    exit 0
    ;;
  show)
    status="open"
    if [ -f .beads/{bead_id}.status ]; then
      status=$(cat .beads/{bead_id}.status)
    fi
    cat <<BEAD_JSON
{{"id":"{bead_id}","title":"Test bead","status":"$status","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}}
BEAD_JSON
    exit 0
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` where a recovered dirty repo can be flushed, but
        /// a new explicit claim still fails because another operator already
        /// owns the bead.
        fn install_fake_br_claim_after_unrelated_recovered_flush_failure(
            base_dir: &std::path::Path,
            bead_id: &str,
        ) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
set -eu

case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  sync)
    count=0
    if [ -f .beads/sync-count ]; then
      count=$(cat .beads/sync-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/sync-count
    echo "Synced"
    exit 0
    ;;
  show)
    status="open"
    if [ -f .beads/{bead_id}.status ]; then
      status=$(cat .beads/{bead_id}.status)
    fi
    cat <<BEAD_JSON
{{"id":"{bead_id}","title":"Test bead","status":"$status","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}}
BEAD_JSON
    exit 0
    ;;
  update)
    count=0
    if [ -f .beads/update-count ]; then
      count=$(cat .beads/update-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/update-count
    echo "{bead_id} already claimed by another operator" >&2
    exit 1
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` that flushes recovered work and then leaves the
        /// bead export conflicted so the explicit claim is blocked before the
        /// follow-up `br update`.
        fn install_fake_br_claim_conflict_after_recovered_flush(
            base_dir: &std::path::Path,
            bead_id: &str,
        ) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
set -eu

case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  sync)
    count=0
    if [ -f .beads/sync-count ]; then
      count=$(cat .beads/sync-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/sync-count
    cat <<'CONFLICT' > .beads/issues.jsonl
<<<<<<< HEAD
{{"id":"bead-a"}}
=======
{{"id":"bead-b"}}
>>>>>>> branch
CONFLICT
    echo "Synced"
    exit 0
    ;;
  update)
    count=0
    if [ -f .beads/update-count ]; then
      count=$(cat .beads/update-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/update-count
    echo "Updated {bead_id}"
    exit 0
    ;;
  show)
    status="in_progress"
    if [ -f .beads/{bead_id}.status ]; then
      status=$(cat .beads/{bead_id}.status)
    fi
    cat <<BEAD_JSON
{{"id":"{bead_id}","title":"Test bead","status":"$status","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}}
BEAD_JSON
    exit 0
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Install a fake `br` that succeeds on `update`, then corrupts the
        /// bead export so the final `br sync --flush-only` must be blocked.
        fn install_fake_br_claim_conflict_before_final_sync(
            base_dir: &std::path::Path,
            bead_id: &str,
        ) {
            write_beads_export(base_dir, "{\"id\":\"seed-bead\"}\n");
            let fake_bin = base_dir.join("fake-bin");
            std::fs::create_dir_all(&fake_bin).expect("create fake bin dir");
            let script = format!(
                r#"#!/bin/sh
set -eu

case "$1" in
  --version)
    echo "br test stub"
    exit 0
    ;;
  update)
    count=0
    if [ -f .beads/update-count ]; then
      count=$(cat .beads/update-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/update-count
    echo "in_progress" > .beads/{bead_id}.status
    cat <<'CONFLICT' > .beads/issues.jsonl
<<<<<<< HEAD
{{"id":"bead-a"}}
=======
{{"id":"bead-b"}}
>>>>>>> branch
CONFLICT
    echo "Updated {bead_id}"
    exit 0
    ;;
  sync)
    count=0
    if [ -f .beads/sync-count ]; then
      count=$(cat .beads/sync-count)
    fi
    count=$((count + 1))
    echo "$count" > .beads/sync-count
    echo "Synced"
    exit 0
    ;;
  *)
    echo "unexpected: $*" >&2
    exit 1
    ;;
esac
"#
            );
            let br_path = fake_bin.join("br");
            std::fs::write(&br_path, script).expect("write fake br");
            std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake br");
        }

        /// Set up milestone directory so the controller store can write files.
        fn ensure_milestone_dir(base_dir: &std::path::Path, milestone_id: &MilestoneId) {
            let milestone_dir = base_dir
                .join(".ralph-burning/milestones")
                .join(milestone_id.as_str());
            std::fs::create_dir_all(milestone_dir).expect("create milestone dir");
        }

        #[tokio::test]
        async fn claim_bead_in_br_succeeds_on_update_and_sync(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_success(base_dir, "bead-1");
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await?;
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rejects_conflicted_beads_jsonl(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();
            write_beads_export(
                base_dir,
                r#"<<<<<<< HEAD
{"id":"bead-1"}
=======
{"id":"bead-2"}
>>>>>>> branch
"#,
            );

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;

            assert!(result.is_err());
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("refusing to claim bead 'bead-1'"),
                "error should mention the rejected claim: {error}"
            );
            assert!(
                error.contains("conflict"),
                "error should mention conflict resolution: {error}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rejects_malformed_beads_jsonl(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();
            write_beads_export(base_dir, "{\"id\":\"bead-1\"}\n{\"id\": }\n");

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;

            assert!(result.is_err());
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("refusing to claim bead 'bead-1'"),
                "error should mention the rejected claim: {error}"
            );
            assert!(
                error.contains("malformed .beads/issues.jsonl line 2"),
                "error should mention malformed JSONL: {error}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_fails_when_update_exits_nonzero(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_failure(base_dir);
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(result.is_err());
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains("failed to claim bead 'bead-1'"),
                "error should mention the bead id: {error}"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rechecks_health_after_recovered_flush_before_update(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_conflict_after_recovered_flush(base_dir, "bead-1");
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/unrelated.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                format!(
                    r#"{{"adapter_id":"{}","operation":"comment_bead","bead_id":"bead-2","status":null}}"#,
                    super::super::claim_owner_token(claim_owner(), "bead-1")
                ),
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "conflicted export after recovered flush should block the explicit claim update"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("refusing to claim bead 'bead-1'"),
                "error should describe the blocked explicit claim: {error}"
            );
            assert!(
                error.contains("resolve the conflict"),
                "error should direct the operator to resolve the conflict: {error}"
            );
            let sync_count = std::fs::read_to_string(base_dir.join(".beads/sync-count"))?;
            assert_eq!(
                sync_count.trim(),
                "1",
                "recovered flush should still happen once"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "the explicit update must not run once the second health check sees a conflict"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rechecks_health_after_owned_recovered_claim_flush_before_short_circuit(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_conflict_after_recovered_flush(base_dir, "bead-1");
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/recovered.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                format!(
                    r#"{{"adapter_id":"{}","operation":"update_bead_status","bead_id":"bead-1","status":"in_progress"}}"#,
                    super::super::claim_owner_token(claim_owner(), "bead-1")
                ),
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "unsafe export after recovered claim flush should block the short-circuit success path"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("locally claimed by replaying a recovered br sync --flush-only"),
                "error should explain that the recovered flush replayed the claim: {error}"
            );
            assert!(
                error.contains("resolve the conflict"),
                "error should direct the operator to resolve the conflict: {error}"
            );
            let sync_count = std::fs::read_to_string(base_dir.join(".beads/sync-count"))?;
            assert_eq!(
                sync_count.trim(),
                "1",
                "recovered claim flush should still run once"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "the explicit update must stay skipped on the recovered-claim short-circuit path"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_does_not_short_circuit_on_other_claim_owners_recovered_update(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_after_unrelated_recovered_flush_failure(base_dir, "bead-1");
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/recovered.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                format!(
                    r#"{{"adapter_id":"{}","operation":"update_bead_status","bead_id":"bead-1","status":"in_progress"}}"#,
                    super::super::claim_owner_token(other_claim_owner(), "bead-1")
                ),
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "another claim owner's recovered update must block the retry before it can flush foreign work"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("another local bead workflow still has pending `update_bead_status`"),
                "the foreign recovered update should block the claim before any replay flush: {error}"
            );
            assert!(
                !base_dir.join(".beads/sync-count").exists(),
                "the claim must not flush another claim owner's recovered update"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "the claim must not issue its own update after the foreign replay is rejected"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_retries_by_flushing_recovered_pending_claim(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_retry_requires_recovered_claim_short_circuit(base_dir, "bead-1");
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let first_attempt =
                super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(first_attempt.is_err(), "first sync should fail");

            super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await?;

            let update_count = std::fs::read_to_string(base_dir.join(".beads/update-count"))?;
            assert_eq!(
                update_count.trim(),
                "1",
                "retry should not re-issue br update once the recovered flush already replayed the claim"
            );
            let sync_count = std::fs::read_to_string(base_dir.join(".beads/sync-count"))?;
            assert_eq!(
                sync_count.trim(),
                "2",
                "retry should only perform the original failed sync and the recovered flush"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rechecks_actual_bead_status_after_owned_recovered_flush(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_retry_after_sync_failure(base_dir, "bead-1");
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let first_attempt =
                super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(first_attempt.is_err(), "first sync should fail");

            std::fs::write(base_dir.join(".beads/bead-1.status"), "open\n")?;

            super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await?;

            let update_count = std::fs::read_to_string(base_dir.join(".beads/update-count"))?;
            assert_eq!(
                update_count.trim(),
                "2",
                "retry must re-issue br update when the post-flush bead state is no longer in_progress"
            );
            let sync_count = std::fs::read_to_string(base_dir.join(".beads/sync-count"))?;
            assert_eq!(
                sync_count.trim(),
                "3",
                "retry should perform the recovered flush and then the final publish sync"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rejects_foreign_pending_mutation_replay(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_retry_after_sync_failure(base_dir, "bead-1");
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/foreign.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                r#"{"adapter_id":"other-workflow","operation":"create_bead","bead_id":"bead-2","status":null}"#,
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "claim should fail closed instead of flushing another workflow's pending mutation"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("another local bead workflow still has pending `create_bead`"),
                "error should explain why the foreign pending mutation blocked the claim: {error}"
            );
            assert!(
                !base_dir.join(".beads/sync-count").exists(),
                "claim must not publish the foreign pending mutation"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "claim must not issue its own update after the foreign pending mutation blocks replay"
            );
            assert!(
                journal_path.exists(),
                "blocking the claim must leave the foreign pending journal in place"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_rechecks_health_before_final_sync(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_conflict_before_final_sync(base_dir, "bead-1");
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "conflicted export after update should block the final sync"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("bead 'bead-1' was locally claimed"),
                "error should report the already-applied local claim: {error}"
            );
            assert!(
                error.contains("resolve the conflict"),
                "error should direct the operator to resolve the conflict: {error}"
            );
            let update_count = std::fs::read_to_string(base_dir.join(".beads/update-count"))?;
            assert_eq!(
                update_count.trim(),
                "1",
                "the explicit update should still run"
            );
            assert!(
                !base_dir.join(".beads/sync-count").exists(),
                "the final sync must not run once the pre-sync health check fails"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_confirms_legacy_recovered_claim_after_flush(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_retry_after_sync_failure(base_dir, "bead-1");
            std::fs::write(base_dir.join(".beads/.br-unsynced-mutations"), "pending\n")?;
            std::fs::write(base_dir.join(".beads/bead-1.status"), "in_progress\n")?;
            std::fs::write(base_dir.join(".beads/sync-count"), "1\n")?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await?;

            let update_count = std::fs::read_to_string(base_dir.join(".beads/update-count"))?;
            assert_eq!(
                update_count.trim(),
                "1",
                "legacy markers without a journaled status update should fall through to an explicit claim"
            );
            let sync_count = std::fs::read_to_string(base_dir.join(".beads/sync-count"))?;
            assert_eq!(
                sync_count.trim(),
                "3",
                "legacy markers should still flush once before the explicit claim sync"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_does_not_treat_unrelated_recovered_flush_as_our_claim(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_after_unrelated_recovered_flush_failure(base_dir, "bead-1");
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/unrelated.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                r#"{"adapter_id":"other","operation":"comment_bead","bead_id":"bead-2","status":null}"#,
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "foreign recovered mutations must block the retry instead of being flushed"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("another local bead workflow still has pending `comment_bead`"),
                "claim should fail closed before flushing unrelated work: {error}"
            );
            assert!(
                !base_dir.join(".beads/sync-count").exists(),
                "the claim must not flush unrelated recovered work"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "the claim must not proceed to its own update after the foreign replay is rejected"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_bead_in_br_does_not_treat_other_operator_status_after_unrelated_flush_as_our_claim(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            install_fake_br_claim_after_unrelated_recovered_flush_failure(base_dir, "bead-1");
            std::fs::write(base_dir.join(".beads/bead-1.status"), "in_progress\n")?;
            let journal_path = base_dir.join(".beads/.br-unsynced-mutations.d/unrelated.json");
            std::fs::create_dir_all(
                journal_path
                    .parent()
                    .expect("journal path must have parent"),
            )?;
            std::fs::write(
                &journal_path,
                r#"{"adapter_id":"other","operation":"comment_bead","bead_id":"bead-2","status":null}"#,
            )?;
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "another operator's in-progress status plus foreign recovered work must block the retry"
            );
            let error = result.expect_err("claim should fail").to_string();
            assert!(
                error.contains("another local bead workflow still has pending `comment_bead`"),
                "claim should fail closed before flushing unrelated recovered work: {error}"
            );
            assert!(
                !base_dir.join(".beads/sync-count").exists(),
                "the claim must not flush unrelated recovered work"
            );
            assert!(
                !base_dir.join(".beads/update-count").exists(),
                "the claim must not issue an explicit update after the foreign replay is rejected"
            );
            Ok(())
        }

        /// Simulates the race condition: our controller is in Claimed state
        /// (selection set it before any br update), but another operator
        /// claimed the bead in br between our selection and our update.
        /// The br update fails and must NOT be retried as idempotent — the
        /// claim must fail so the caller transitions to needs_operator.
        #[tokio::test]
        async fn claim_fails_even_when_controller_already_claimed_the_bead(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();
            let milestone_id = MilestoneId::new("ms-race")?;
            ensure_milestone_dir(base_dir, &milestone_id);

            // Controller already selected this bead (Claimed state), but
            // another operator raced ahead and claimed it in br.
            milestone_controller::initialize_controller_with_request(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                milestone_controller::ControllerTransitionRequest::new(
                    MilestoneControllerState::Claimed,
                    "selection picked bead-1",
                )
                .with_bead("bead-1"),
                chrono::Utc::now(),
            )?;

            // br update fails (another process already set it to in_progress)
            install_fake_br_claim_failure(base_dir);
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            let result = super::super::claim_bead_in_br(base_dir, "bead-1", claim_owner()).await;
            assert!(
                result.is_err(),
                "claim must fail even though controller has the bead selected — \
                 local selection does not prove a prior br update succeeded"
            );
            Ok(())
        }

        #[tokio::test]
        async fn claim_failure_transitions_controller_to_needs_operator(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            let milestone_id = MilestoneId::new("ms-claim-test")?;
            ensure_milestone_dir(base_dir, &milestone_id);

            milestone_controller::initialize_controller_with_request(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                milestone_controller::ControllerTransitionRequest::new(
                    MilestoneControllerState::Claimed,
                    "selection picked bead-claim-1",
                )
                .with_bead("bead-claim-1"),
                chrono::Utc::now(),
            )?;

            install_fake_br_claim_failure(base_dir);
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            // Claim should fail
            let claim_result =
                super::super::claim_bead_in_br(base_dir, "bead-claim-1", claim_owner()).await;
            assert!(claim_result.is_err());

            // Simulate the transition that handle_create_from_bead would do
            let reason = format!(
                "br claim failed for bead 'bead-claim-1': {}",
                claim_result.unwrap_err()
            );
            milestone_controller::sync_controller_state(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                milestone_controller::ControllerTransitionRequest::new(
                    MilestoneControllerState::NeedsOperator,
                    &reason,
                )
                .with_bead("bead-claim-1"),
                chrono::Utc::now(),
            )?;

            // Verify controller is now in NeedsOperator
            let controller = milestone_controller::load_controller(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
            )?
            .expect("controller should exist");
            assert_eq!(controller.state, MilestoneControllerState::NeedsOperator);
            assert!(
                controller
                    .last_transition_reason
                    .as_deref()
                    .unwrap_or("")
                    .contains("br claim failed"),
                "transition reason should mention br claim failure: {:?}",
                controller.last_transition_reason
            );
            Ok(())
        }

        #[tokio::test]
        async fn successful_claim_records_task_id_in_controller(
        ) -> Result<(), Box<dyn std::error::Error>> {
            let _path_lock = lock_path_mutex();
            let temp_dir = tempfile::tempdir()?;
            let base_dir = temp_dir.path();

            let milestone_id = MilestoneId::new("ms-link-test")?;
            ensure_milestone_dir(base_dir, &milestone_id);

            milestone_controller::initialize_controller_with_request(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                milestone_controller::ControllerTransitionRequest::new(
                    MilestoneControllerState::Claimed,
                    "selection picked bead-link-1",
                )
                .with_bead("bead-link-1"),
                chrono::Utc::now(),
            )?;

            install_fake_br_claim_success(base_dir, "bead-link-1");
            let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

            // Claim succeeds
            super::super::claim_bead_in_br(base_dir, "bead-link-1", claim_owner()).await?;

            // Record the linked task/project ID
            let now = chrono::Utc::now();
            let claimed = milestone_controller::sync_controller_task_claimed(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                "bead-link-1",
                "project-from-bead",
                "bead claimed in br and Ralph project created",
                now,
            )?;
            // The returned record should already have the task_id
            assert_eq!(
                claimed.active_task_id.as_deref(),
                Some("project-from-bead"),
                "sync_controller_task_claimed return value should have task_id"
            );

            // Verify controller persisted with both bead_id and task_id
            let controller = milestone_controller::load_controller(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
            )?
            .expect("controller should exist");
            assert_eq!(controller.state, MilestoneControllerState::Claimed);
            assert_eq!(controller.active_bead_id.as_deref(), Some("bead-link-1"));
            assert_eq!(
                controller.active_task_id.as_deref(),
                Some("project-from-bead")
            );
            Ok(())
        }
    }
}
