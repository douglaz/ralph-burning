use std::io::ErrorKind;

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::adapters::br_models::{BeadDetail, BeadStatus, ReadyBead};
use crate::adapters::br_process::BrAdapter;
use crate::adapters::bv_process::BvAdapter;
use crate::adapters::fs::{
    FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
    FsMilestoneSnapshotStore, FsMilestoneStore, FsProjectStore, FsRequirementsStore,
    FsRunSnapshotStore, FsTaskRunLineageStore,
};
use crate::cli::{project, run};
use crate::composition::agent_execution_builder;
use crate::contexts::milestone_record::bead_refs::{
    br_show_output_indicates_missing, milestone_bead_refs_match,
};
use crate::contexts::milestone_record::bundle::MilestoneBundle;
use crate::contexts::milestone_record::controller::{
    self as milestone_controller, ControllerBeadStatus, ControllerTaskStatus,
    MilestoneControllerResumePort, MilestoneControllerState,
};
use crate::contexts::milestone_record::model::{
    MilestoneId, MilestoneProgress, MilestoneRecord, MilestoneSnapshot, MilestoneStatus,
};
use crate::contexts::milestone_record::queries::{BeadExecutionHistoryView, MilestoneTaskListView};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CreateMilestoneInput, MilestonePlanPort, MilestoneSnapshotPort,
    MilestoneStorePort,
};
use crate::contexts::project_run_record::model::{ProjectStatusSummary, RunStatus};
use crate::contexts::project_run_record::service::{ProjectStorePort, RunSnapshotPort};
use crate::contexts::requirements_drafting::model::{
    RequirementsMode, RequirementsOutputKind, RequirementsRun, RequirementsStatus,
};
use crate::contexts::requirements_drafting::service as requirements_service;
use crate::contexts::requirements_drafting::service::RequirementsStorePort;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::error::{AppError, AppResult};

const PENDING_REQUIREMENTS_START_PREFIX: &str = "__starting__:";
const PENDING_REQUIREMENTS_START_STALE_AFTER_SECONDS: i64 = 30;
const PENDING_REQUIREMENTS_DRAFTING_STALE_AFTER_SECONDS: i64 = 300;
const MAX_MILESTONE_RUN_STEPS: usize = 256;

#[derive(Debug, Args)]
pub struct MilestoneCommand {
    #[command(subcommand)]
    pub command: MilestoneSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum MilestoneSubcommand {
    Create(MilestoneCreateArgs),
    Plan {
        milestone_id: String,
    },
    Next {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Run {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Show {
        milestone_id: String,
        #[arg(long)]
        json: bool,
    },
    BeadHistory {
        milestone_id: String,
        bead_id: String,
        #[arg(long)]
        json: bool,
    },
    Status {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Tasks {
        milestone_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Args)]
pub struct MilestoneCreateArgs {
    pub name: String,
    #[arg(long = "from-idea")]
    pub from_idea: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MilestoneSummaryView {
    id: String,
    name: String,
    status: String,
    bead_count: u32,
    progress: MilestoneProgress,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_bead: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_requirements: Option<PendingRequirementsView>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct PendingRequirementsView {
    run_id: String,
    status: String,
    status_summary: String,
}

#[derive(Debug, Serialize)]
struct MilestoneDetailView {
    id: String,
    name: String,
    description: String,
    status: String,
    bead_count: u32,
    progress: MilestoneProgress,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_bead: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_requirements: Option<PendingRequirementsView>,
    plan_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_hash: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    has_plan: bool,
}

#[derive(Debug, Serialize)]
struct MilestoneListView {
    milestones: Vec<MilestoneSummaryView>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum MilestoneCommandStatus {
    Success,
    Blocked,
    NeedsOperator,
    Completed,
}

#[derive(Debug, Clone, Serialize)]
struct MilestoneBeadView {
    id: String,
    title: String,
    priority: String,
    readiness: String,
}

#[derive(Debug, Clone, Serialize)]
struct MilestoneNextView {
    milestone_id: String,
    status: MilestoneCommandStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bead: Option<MilestoneBeadView>,
}

#[derive(Debug, Clone, Serialize)]
struct MilestoneRunView {
    milestone_id: String,
    status: MilestoneCommandStatus,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bead: Option<MilestoneBeadView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum BrShowResponse {
    Single(BeadDetail),
    Many(Vec<BeadDetail>),
}

pub async fn handle(command: MilestoneCommand) -> AppResult<()> {
    match command.command {
        MilestoneSubcommand::Create(args) => handle_create(args).await,
        MilestoneSubcommand::Plan { milestone_id } => handle_plan(milestone_id).await,
        MilestoneSubcommand::Next { milestone_id, json } => handle_next(milestone_id, json).await,
        MilestoneSubcommand::Run { milestone_id, json } => handle_run(milestone_id, json).await,
        MilestoneSubcommand::Show { milestone_id, json } => handle_show(milestone_id, json).await,
        MilestoneSubcommand::BeadHistory {
            milestone_id,
            bead_id,
            json,
        } => handle_bead_history(milestone_id, bead_id, json).await,
        MilestoneSubcommand::Status { milestone_id, json } => {
            handle_status(milestone_id, json).await
        }
        MilestoneSubcommand::Tasks { milestone_id, json } => handle_tasks(milestone_id, json).await,
    }
}

async fn handle_create(args: MilestoneCreateArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let milestone_id = derive_milestone_id(&args.name)?;
    let description = args
        .from_idea
        .unwrap_or_else(|| default_planning_idea(&args.name));
    let store = FsMilestoneStore;
    let record = milestone_service::create_milestone(
        &store,
        &current_dir,
        CreateMilestoneInput {
            id: milestone_id.clone(),
            name: args.name,
            description,
        },
        Utc::now(),
    )
    .map_err(|error| map_create_error(&milestone_id, error))?;
    workspace_governance::set_active_milestone(&current_dir, &record.id)?;

    println!("Created milestone '{}' ({})", record.id, record.name);
    Ok(())
}

async fn handle_plan(milestone_id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let snapshot_store = FsMilestoneSnapshotStore;
    let journal_store = FsMilestoneJournalStore;
    let plan_store = FsMilestonePlanStore;
    let requirements_store = FsRequirementsStore;

    let milestone_id = MilestoneId::new(milestone_id)?;
    let record = load_existing_milestone(&store, &current_dir, &milestone_id)?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    let (run_id, run) = load_or_start_milestone_requirements_run(
        &snapshot_store,
        &requirements_store,
        &current_dir,
        &milestone_id,
        &record,
    )
    .await?;

    match run.status {
        RequirementsStatus::Completed => {}
        RequirementsStatus::AwaitingAnswers => {
            println!(
                "Milestone '{}' planning is awaiting answers in requirements run '{}'. Complete `ralph-burning requirements answer {}` and rerun `ralph-burning milestone plan {}`.",
                milestone_id, run_id, run_id, milestone_id
            );
            return Ok(());
        }
        RequirementsStatus::Failed => {
            clear_pending_requirements_run(
                &snapshot_store,
                &current_dir,
                &milestone_id,
                Some(&run_id),
            )?;
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "planning".to_owned(),
                details: format!(
                    "requirements run '{}' failed: {}",
                    run_id, run.status_summary
                ),
            });
        }
        status => {
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "planning".to_owned(),
                details: format!(
                    "requirements run '{}' ended in '{}' status before producing a milestone bundle: {}",
                    run_id, status, run.status_summary
                ),
            });
        }
    }
    let handoff = match requirements_service::extract_milestone_bundle_handoff(
        &requirements_store,
        &current_dir,
        &run_id,
    ) {
        Ok(handoff) => handoff,
        Err(error) => {
            clear_pending_requirements_run(
                &snapshot_store,
                &current_dir,
                &milestone_id,
                Some(&run_id),
            )?;
            return Err(planning_error(
                &milestone_id,
                format!(
                    "requirements run '{}' did not produce a usable milestone bundle: {}",
                    run_id, error
                ),
            ));
        }
    };

    let mut bundle = handoff.bundle;
    retarget_bundle(&mut bundle, &milestone_id, &record.name);

    milestone_service::materialize_bundle(
        &store,
        &snapshot_store,
        &journal_store,
        &plan_store,
        &current_dir,
        &bundle,
        Utc::now(),
    )
    .map_err(|error| AppError::MilestoneOperationFailed {
        milestone_id: milestone_id.to_string(),
        action: "planning".to_owned(),
        details: error.to_string(),
    })?;
    clear_pending_requirements_run(&snapshot_store, &current_dir, &milestone_id, Some(&run_id))?;

    let detail = load_milestone_detail(
        &store,
        &snapshot_store,
        &plan_store,
        &requirements_store,
        &current_dir,
        &milestone_id,
    )?;
    println!(
        "Planned milestone '{}' from requirements run '{}' ({} beads, status: {})",
        detail.id, run_id, detail.bead_count, detail.status
    );
    Ok(())
}

async fn handle_next(milestone_id: Option<String>, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let milestone_id = resolve_requested_milestone(&store, &current_dir, milestone_id)?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;

    let outcome = inspect_next_milestone_action(&current_dir, &milestone_id).await?;
    let failure =
        milestone_command_failure(&milestone_id, "next", outcome.status, &outcome.message);

    if json {
        print_json(&outcome)?;
    } else {
        print_milestone_next(&outcome);
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn handle_run(milestone_id: Option<String>, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let milestone_id = resolve_requested_milestone(&store, &current_dir, milestone_id)?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;

    let outcome = execute_milestone_run(&current_dir, &milestone_id).await?;
    let failure = milestone_command_failure(&milestone_id, "run", outcome.status, &outcome.message);

    if json {
        print_json(&outcome)?;
    } else {
        print_milestone_run(&outcome);
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn handle_show(milestone_id: String, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let snapshot_store = FsMilestoneSnapshotStore;
    let plan_store = FsMilestonePlanStore;
    let requirements_store = FsRequirementsStore;
    let milestone_id = MilestoneId::new(milestone_id)?;
    let detail = load_milestone_detail(
        &store,
        &snapshot_store,
        &plan_store,
        &requirements_store,
        &current_dir,
        &milestone_id,
    )?;

    if json {
        print_json(&detail)?;
    } else {
        print_milestone_detail(&detail);
    }
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    Ok(())
}

async fn handle_bead_history(milestone_id: String, bead_id: String, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let milestone_id = MilestoneId::new(milestone_id)?;
    load_existing_milestone(&store, &current_dir, &milestone_id)?;
    let history = load_bead_execution_history(&current_dir, &milestone_id, &bead_id)?;

    if json {
        print_json(&history)?;
    } else {
        print_bead_execution_history(&history);
    }

    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    Ok(())
}

async fn handle_status(milestone_id: Option<String>, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let snapshot_store = FsMilestoneSnapshotStore;
    let plan_store = FsMilestonePlanStore;
    let requirements_store = FsRequirementsStore;

    if let Some(milestone_id) = milestone_id {
        let milestone_id = MilestoneId::new(milestone_id)?;
        load_existing_milestone(&store, &current_dir, &milestone_id)?;
        workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
        let detail = load_milestone_detail(
            &store,
            &snapshot_store,
            &plan_store,
            &requirements_store,
            &current_dir,
            &milestone_id,
        )?;
        if json {
            print_json(&detail)?;
        } else {
            print_milestone_detail(&detail);
        }
        return Ok(());
    }

    let milestones = milestone_service::list_milestones(&store, &current_dir)?
        .into_iter()
        .map(|milestone_id| {
            load_milestone_summary(
                &store,
                &snapshot_store,
                &plan_store,
                &requirements_store,
                &current_dir,
                &milestone_id,
            )
        })
        .collect::<AppResult<Vec<_>>>()?;

    if json {
        print_json(&MilestoneListView {
            milestones: milestones.clone(),
        })?;
    } else if milestones.is_empty() {
        println!("No milestones found.");
    } else {
        for milestone in milestones {
            println!(
                "{}\t{}\t{}",
                milestone.id,
                milestone.status,
                format_progress_line(milestone.bead_count, &milestone.progress)
            );
        }
    }
    Ok(())
}

async fn handle_tasks(milestone_id: String, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let milestone_id = MilestoneId::new(milestone_id)?;
    load_existing_milestone(&store, &current_dir, &milestone_id)?;
    let tasks = load_milestone_task_list(&current_dir, &milestone_id)?;

    if json {
        print_json(&tasks)?;
    } else {
        print_milestone_task_list(&tasks);
    }

    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    Ok(())
}

fn resolve_requested_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &std::path::Path,
    milestone_id: Option<String>,
) -> AppResult<MilestoneId> {
    match milestone_id {
        Some(milestone_id) => {
            let milestone_id = MilestoneId::new(milestone_id)?;
            load_existing_milestone(store, base_dir, &milestone_id)?;
            Ok(milestone_id)
        }
        None => resolve_active_milestone(store, base_dir),
    }
}

fn resolve_active_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &std::path::Path,
) -> AppResult<MilestoneId> {
    if let Some(raw) = workspace_governance::read_active_milestone(base_dir)? {
        if let Ok(milestone_id) = MilestoneId::new(raw) {
            match load_existing_milestone(store, base_dir, &milestone_id) {
                Ok(_) => return Ok(milestone_id),
                Err(AppError::MilestoneNotFound { .. }) => {}
                Err(error) => return Err(error),
            }
        }
    }

    let milestone_id = workspace_governance::active_project_milestone_id(base_dir)?
        .ok_or(AppError::NoActiveMilestone)?;
    load_existing_milestone(store, base_dir, &milestone_id)?;
    Ok(milestone_id)
}

async fn inspect_next_milestone_action(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneNextView> {
    let controller = resolve_controller_for_next(base_dir, milestone_id).await?;
    let milestone_id_text = milestone_id.to_string();

    match controller.state {
        MilestoneControllerState::Claimed
        | MilestoneControllerState::Running
        | MilestoneControllerState::Reconciling => {
            let bead_id = controller.active_bead_id.as_deref().ok_or_else(|| {
                AppError::MilestoneOperationFailed {
                    milestone_id: milestone_id_text.clone(),
                    action: "next".to_owned(),
                    details: format!(
                        "controller state '{}' is missing an active bead identifier",
                        controller_state_label(controller.state)
                    ),
                }
            })?;
            let bead = load_bead_view(base_dir, milestone_id, bead_id, controller.state, "next")?;
            Ok(MilestoneNextView {
                milestone_id: milestone_id_text,
                status: MilestoneCommandStatus::Success,
                message: format!(
                    "next bead is '{}' ({})",
                    bead.id,
                    controller_state_readiness(controller.state)
                ),
                bead: Some(bead),
            })
        }
        MilestoneControllerState::Completed => Ok(MilestoneNextView {
            milestone_id: milestone_id_text,
            status: MilestoneCommandStatus::Completed,
            message: "milestone is already completed".to_owned(),
            bead: None,
        }),
        MilestoneControllerState::Blocked => Ok(MilestoneNextView {
            milestone_id: milestone_id_text,
            status: MilestoneCommandStatus::Blocked,
            message: controller
                .last_transition_reason
                .unwrap_or_else(|| "milestone has no ready beads to execute".to_owned()),
            bead: None,
        }),
        MilestoneControllerState::NeedsOperator => Ok(MilestoneNextView {
            milestone_id: milestone_id_text,
            status: MilestoneCommandStatus::NeedsOperator,
            message: controller.last_transition_reason.unwrap_or_else(|| {
                "milestone controller requires operator intervention".to_owned()
            }),
            bead: None,
        }),
        MilestoneControllerState::Idle | MilestoneControllerState::Selecting => {
            Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id_text,
                action: "next".to_owned(),
                details: format!(
                    "controller remained in '{}' after next-bead selection",
                    controller_state_label(controller.state)
                ),
            })
        }
    }
}

async fn resolve_controller_for_next(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<milestone_controller::MilestoneControllerRecord> {
    let runtime = MilestoneCommandControllerRuntime {
        base_dir,
        milestone_id,
    };
    let now = Utc::now();
    let controller = milestone_controller::resume_controller(
        &FsMilestoneControllerStore,
        &runtime,
        base_dir,
        milestone_id,
        now,
    )?;

    if matches!(
        controller.state,
        MilestoneControllerState::Idle
            | MilestoneControllerState::Selecting
            | MilestoneControllerState::Blocked
    ) {
        let br = BrAdapter::new().with_working_dir(base_dir.to_path_buf());
        let bv = BvAdapter::new().with_working_dir(base_dir.to_path_buf());
        return run::select_next_milestone_bead(base_dir, milestone_id, &br, &bv, now).await;
    }

    Ok(controller)
}

async fn execute_milestone_run(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneRunView> {
    for _step in 0..MAX_MILESTONE_RUN_STEPS {
        let controller = resolve_controller_for_next(base_dir, milestone_id).await?;
        match controller.state {
            MilestoneControllerState::Completed => {
                return Ok(MilestoneRunView {
                    milestone_id: milestone_id.to_string(),
                    status: MilestoneCommandStatus::Completed,
                    message: "milestone execution completed".to_owned(),
                    bead: None,
                    project_id: None,
                });
            }
            MilestoneControllerState::Blocked => {
                return Ok(MilestoneRunView {
                    milestone_id: milestone_id.to_string(),
                    status: MilestoneCommandStatus::Blocked,
                    message: controller
                        .last_transition_reason
                        .unwrap_or_else(|| "milestone has no ready beads to execute".to_owned()),
                    bead: None,
                    project_id: None,
                });
            }
            MilestoneControllerState::NeedsOperator => {
                return Ok(MilestoneRunView {
                    milestone_id: milestone_id.to_string(),
                    status: MilestoneCommandStatus::NeedsOperator,
                    message: controller.last_transition_reason.unwrap_or_else(|| {
                        "milestone controller requires operator intervention".to_owned()
                    }),
                    bead: None,
                    project_id: None,
                });
            }
            MilestoneControllerState::Claimed
            | MilestoneControllerState::Running
            | MilestoneControllerState::Reconciling => {
                let bead_id = controller.active_bead_id.as_deref().ok_or_else(|| {
                    AppError::MilestoneOperationFailed {
                        milestone_id: milestone_id.to_string(),
                        action: "run".to_owned(),
                        details: format!(
                            "controller state '{}' is missing an active bead identifier",
                            controller_state_label(controller.state)
                        ),
                    }
                })?;
                let bead =
                    load_bead_view(base_dir, milestone_id, bead_id, controller.state, "run")?;
                let project_id =
                    ensure_project_for_controller(base_dir, milestone_id, &controller).await?;
                workspace_governance::set_active_project(base_dir, &project_id)?;

                match load_run_action(base_dir, &project_id)? {
                    MilestoneRunAction::SyncMilestone => {
                        run::execute_sync_milestone(false).await?;
                    }
                    MilestoneRunAction::Start => {
                        run::execute_start(run::RunBackendOverrideArgs::default(), false).await?;
                    }
                    MilestoneRunAction::Resume => {
                        run::execute_resume(run::RunBackendOverrideArgs::default(), false).await?;
                    }
                }

                let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, &project_id)?;
                match snapshot.status {
                    RunStatus::Completed => continue,
                    RunStatus::Paused => {
                        return Ok(MilestoneRunView {
                            milestone_id: milestone_id.to_string(),
                            status: MilestoneCommandStatus::Blocked,
                            message: snapshot.status_summary,
                            bead: Some(bead),
                            project_id: Some(project_id.to_string()),
                        });
                    }
                    RunStatus::Failed => {
                        return Ok(MilestoneRunView {
                            milestone_id: milestone_id.to_string(),
                            status: MilestoneCommandStatus::NeedsOperator,
                            message: snapshot.status_summary,
                            bead: Some(bead),
                            project_id: Some(project_id.to_string()),
                        });
                    }
                    RunStatus::NotStarted | RunStatus::Running => {
                        return Ok(MilestoneRunView {
                            milestone_id: milestone_id.to_string(),
                            status: MilestoneCommandStatus::NeedsOperator,
                            message: format!(
                                "run command left project '{}' in unexpected '{}' state",
                                project_id, snapshot.status
                            ),
                            bead: Some(bead),
                            project_id: Some(project_id.to_string()),
                        });
                    }
                }
            }
            MilestoneControllerState::Idle | MilestoneControllerState::Selecting => {
                return Err(AppError::MilestoneOperationFailed {
                    milestone_id: milestone_id.to_string(),
                    action: "run".to_owned(),
                    details: format!(
                        "controller remained in '{}' while preparing milestone execution",
                        controller_state_label(controller.state)
                    ),
                });
            }
        }
    }

    Err(AppError::MilestoneOperationFailed {
        milestone_id: milestone_id.to_string(),
        action: "run".to_owned(),
        details: format!(
            "aborted after {} milestone execution steps to avoid an infinite loop",
            MAX_MILESTONE_RUN_STEPS
        ),
    })
}

async fn ensure_project_for_controller(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    controller: &milestone_controller::MilestoneControllerRecord,
) -> AppResult<crate::shared::domain::ProjectId> {
    let bead_id =
        controller
            .active_bead_id
            .as_deref()
            .ok_or_else(|| AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "run".to_owned(),
                details: "controller is missing an active bead identifier".to_owned(),
            })?;

    if let Some(task_id) = controller.active_task_id.as_deref() {
        let project_id = crate::shared::domain::ProjectId::new(task_id.to_owned())?;
        if !FsProjectStore.project_exists(base_dir, &project_id)? {
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "run".to_owned(),
                details: format!(
                    "controller references project '{}' for bead '{}', but that project does not exist",
                    project_id, bead_id
                ),
            });
        }
        return Ok(project_id);
    }

    if let Some(project_id) = project::find_existing_bead_project(base_dir, milestone_id, bead_id)?
    {
        milestone_controller::sync_controller_task_claimed(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
            bead_id,
            project_id.as_str(),
            "adopted existing bead-backed project",
            Utc::now(),
        )?;
        return Ok(project_id);
    }

    match project::execute_create_from_bead(project::CreateFromBeadArgs {
        milestone_id: milestone_id.to_string(),
        bead_id: bead_id.to_owned(),
        project_id: None,
        prompt_file: None,
        flow: None,
    })
    .await
    {
        Ok(project_id) => Ok(project_id),
        Err(AppError::DuplicateProject { .. }) => {
            if let Some(project_id) =
                project::find_existing_bead_project(base_dir, milestone_id, bead_id)?
            {
                milestone_controller::sync_controller_task_claimed(
                    &FsMilestoneControllerStore,
                    base_dir,
                    milestone_id,
                    bead_id,
                    project_id.as_str(),
                    "adopted existing bead-backed project after duplicate project detection",
                    Utc::now(),
                )?;
                Ok(project_id)
            } else {
                Err(AppError::MilestoneOperationFailed {
                    milestone_id: milestone_id.to_string(),
                    action: "run".to_owned(),
                    details: format!(
                        "controller could not recover the existing project for bead '{}'",
                        bead_id
                    ),
                })
            }
        }
        Err(error) => Err(error),
    }
}

enum MilestoneRunAction {
    Start,
    Resume,
    SyncMilestone,
}

fn load_run_action(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
) -> AppResult<MilestoneRunAction> {
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, project_id)?;
    Ok(match snapshot.status {
        RunStatus::NotStarted => MilestoneRunAction::Start,
        RunStatus::Completed => MilestoneRunAction::SyncMilestone,
        RunStatus::Paused | RunStatus::Failed | RunStatus::Running => MilestoneRunAction::Resume,
    })
}

fn milestone_command_failure(
    milestone_id: &MilestoneId,
    action: &str,
    status: MilestoneCommandStatus,
    message: &str,
) -> Option<AppError> {
    match status {
        MilestoneCommandStatus::Success | MilestoneCommandStatus::Completed => None,
        MilestoneCommandStatus::Blocked | MilestoneCommandStatus::NeedsOperator => {
            Some(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: action.to_owned(),
                details: message.to_owned(),
            })
        }
    }
}

fn print_milestone_next(outcome: &MilestoneNextView) {
    match outcome.status {
        MilestoneCommandStatus::Success => {
            if let Some(bead) = &outcome.bead {
                println!(
                    "Next bead for milestone '{}': {} ({}, {}) - {}",
                    outcome.milestone_id, bead.id, bead.priority, bead.readiness, bead.title
                );
            } else {
                println!("Milestone '{}': {}", outcome.milestone_id, outcome.message);
            }
        }
        MilestoneCommandStatus::Completed => {
            println!("Milestone '{}': {}", outcome.milestone_id, outcome.message);
        }
        MilestoneCommandStatus::Blocked => {
            println!(
                "Milestone '{}' is blocked: {}",
                outcome.milestone_id, outcome.message
            );
        }
        MilestoneCommandStatus::NeedsOperator => {
            println!(
                "Milestone '{}' needs operator intervention: {}",
                outcome.milestone_id, outcome.message
            );
        }
    }
}

fn print_milestone_run(outcome: &MilestoneRunView) {
    match outcome.status {
        MilestoneCommandStatus::Completed => {
            println!("Milestone '{}': {}", outcome.milestone_id, outcome.message);
        }
        MilestoneCommandStatus::Success => {
            let project_suffix = outcome
                .project_id
                .as_deref()
                .map(|project_id| format!(" via project '{project_id}'"))
                .unwrap_or_default();
            println!(
                "Milestone '{}' advanced{}: {}",
                outcome.milestone_id, project_suffix, outcome.message
            );
        }
        MilestoneCommandStatus::Blocked => {
            println!(
                "Milestone '{}' is blocked: {}",
                outcome.milestone_id, outcome.message
            );
        }
        MilestoneCommandStatus::NeedsOperator => {
            println!(
                "Milestone '{}' needs operator intervention: {}",
                outcome.milestone_id, outcome.message
            );
        }
    }
}

fn print_bead_execution_history(history: &BeadExecutionHistoryView) {
    println!(
        "Milestone: {} ({})",
        history.lineage.milestone_name, history.lineage.milestone_id
    );
    println!(
        "Bead: {} ({})",
        history.lineage.bead_title.as_deref().unwrap_or("<unknown>"),
        history.lineage.bead_id
    );
    if !history.lineage.acceptance_criteria.is_empty() {
        println!("Acceptance criteria:");
        for criterion in &history.lineage.acceptance_criteria {
            println!("  - {criterion}");
        }
    }

    if history.runs.is_empty() {
        println!("Runs: none");
        return;
    }

    println!("Runs:");
    println!("project_id\trun_id\toutcome\tstarted_at\tfinished_at\tduration_ms\ttask_id");
    for run in &history.runs {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            run.project_id,
            run.run_id.as_deref().unwrap_or("-"),
            run.outcome,
            run.started_at.to_rfc3339(),
            run.finished_at
                .as_ref()
                .map(DateTime::<Utc>::to_rfc3339)
                .unwrap_or_else(|| "-".to_owned()),
            run.duration_ms
                .map(|duration_ms| duration_ms.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            run.task_id.as_deref().unwrap_or("-"),
        );
    }
}

fn print_milestone_task_list(tasks: &MilestoneTaskListView) {
    println!(
        "Milestone: {} ({})",
        tasks.milestone_name, tasks.milestone_id
    );
    if tasks.tasks.is_empty() {
        println!("Tasks: none");
        return;
    }

    println!("project_id\tbead_id\tstatus\tcreated_at");
    for task in &tasks.tasks {
        println!(
            "{}\t{}\t{}\t{}",
            task.project_id,
            task.bead_id,
            project_status_summary_label(task.status_summary),
            task.created_at.to_rfc3339(),
        );
    }
}

fn project_status_summary_label(status: ProjectStatusSummary) -> &'static str {
    match status {
        ProjectStatusSummary::Created => "created",
        ProjectStatusSummary::Active => "active",
        ProjectStatusSummary::Completed => "completed",
        ProjectStatusSummary::Failed => "failed",
    }
}

fn controller_state_label(state: MilestoneControllerState) -> &'static str {
    match state {
        MilestoneControllerState::Idle => "idle",
        MilestoneControllerState::Selecting => "selecting",
        MilestoneControllerState::Claimed => "claimed",
        MilestoneControllerState::Running => "running",
        MilestoneControllerState::Reconciling => "reconciling",
        MilestoneControllerState::Blocked => "blocked",
        MilestoneControllerState::NeedsOperator => "needs_operator",
        MilestoneControllerState::Completed => "completed",
    }
}

fn controller_state_readiness(state: MilestoneControllerState) -> &'static str {
    match state {
        MilestoneControllerState::Claimed => "ready",
        MilestoneControllerState::Running => "running",
        MilestoneControllerState::Reconciling => "reconciling",
        MilestoneControllerState::Idle
        | MilestoneControllerState::Selecting
        | MilestoneControllerState::Blocked
        | MilestoneControllerState::NeedsOperator
        | MilestoneControllerState::Completed => "unknown",
    }
}

fn load_bead_view(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    controller_state: MilestoneControllerState,
    action: &str,
) -> AppResult<MilestoneBeadView> {
    let detail = load_bead_detail_from_br(base_dir, milestone_id, bead_id)?.ok_or_else(|| {
        AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: format!("active bead '{bead_id}' could not be loaded from br"),
        }
    })?;
    if !bead_belongs_to_milestone(milestone_id, &detail.id) {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: format!(
                "active bead '{}' is not part of milestone '{}'",
                detail.id, milestone_id
            ),
        });
    }
    Ok(MilestoneBeadView {
        id: detail.id,
        title: detail.title,
        priority: detail.priority.to_string(),
        readiness: controller_state_readiness(controller_state).to_owned(),
    })
}

fn bead_belongs_to_milestone(milestone_id: &MilestoneId, bead_id: &str) -> bool {
    bead_id == milestone_id.as_str()
        || bead_id.starts_with(&format!("{}.", milestone_id.as_str()))
        || !bead_id.contains('.')
}

fn load_bead_detail_from_br(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<Option<BeadDetail>> {
    let output = std::process::Command::new("br")
        .args(["show", bead_id, "--json"])
        .current_dir(base_dir)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if br_show_output_indicates_missing(&stderr, &stdout) {
            return Ok(None);
        }
        let details = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("exit status {}", output.status)
        };
        return Err(AppError::Io(std::io::Error::other(format!(
            "br show {bead_id} --json failed: {details}"
        ))));
    }

    let response: BrShowResponse =
        serde_json::from_slice(&output.stdout).map_err(|error| AppError::CorruptRecord {
            file: format!("br show {bead_id} --json"),
            details: format!("failed to parse bead JSON: {error}"),
        })?;
    match response {
        BrShowResponse::Single(detail) => {
            if bead_id.contains('.') {
                if detail.id != bead_id {
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "br show {bead_id} --json returned bead '{}'",
                        detail.id
                    ))));
                }
                Ok(Some(detail))
            } else if milestone_bead_refs_match(milestone_id, &detail.id, bead_id) {
                Ok(Some(detail))
            } else {
                Err(AppError::Io(std::io::Error::other(format!(
                    "br show {bead_id} --json returned bead '{}'",
                    detail.id
                ))))
            }
        }
        BrShowResponse::Many(details) => {
            let mut matches = details.into_iter().filter(|detail| {
                if bead_id.contains('.') {
                    detail.id == bead_id
                } else {
                    milestone_bead_refs_match(milestone_id, &detail.id, bead_id)
                }
            });
            let Some(detail) = matches.next() else {
                return Ok(None);
            };
            if matches.next().is_some() {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "br show {bead_id} --json returned multiple matching beads"
                ))));
            }
            Ok(Some(detail))
        }
    }
}

struct MilestoneCommandControllerRuntime<'a> {
    base_dir: &'a std::path::Path,
    milestone_id: &'a MilestoneId,
}

impl MilestoneCommandControllerRuntime<'_> {
    fn query_br_json<T: serde::de::DeserializeOwned>(
        &self,
        args: &[&str],
        context: &str,
    ) -> AppResult<T> {
        let output = std::process::Command::new("br")
            .args(args)
            .current_dir(self.base_dir)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let details = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                format!("exit status {}", output.status)
            };
            return Err(AppError::ResumeFailed {
                reason: format!("{context}: br {} failed: {details}", args.join(" ")),
            });
        }

        serde_json::from_slice(&output.stdout).map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "{context}: failed to parse br {} JSON: {error}",
                args.join(" ")
            ),
        })
    }

    fn planned_bead_membership_refs(&self) -> AppResult<std::collections::HashSet<String>> {
        let plan_json = FsMilestonePlanStore.read_plan_json(self.base_dir, self.milestone_id)?;
        let bundle: MilestoneBundle =
            serde_json::from_str(&plan_json).map_err(|error| AppError::CorruptRecord {
                file: format!("milestones/{}/plan.json", self.milestone_id),
                details: error.to_string(),
            })?;

        crate::contexts::milestone_record::bundle::planned_bead_membership_refs(&bundle)
            .map(|refs| refs.into_iter().collect())
            .map_err(|errors| AppError::CorruptRecord {
                file: format!("milestones/{}/plan.json", self.milestone_id),
                details: errors.join("; "),
            })
    }
}

impl MilestoneControllerResumePort for MilestoneCommandControllerRuntime<'_> {
    fn bead_status(&self, bead_id: &str) -> AppResult<ControllerBeadStatus> {
        let Some(detail) = load_bead_detail_from_br(self.base_dir, self.milestone_id, bead_id)?
        else {
            return Ok(ControllerBeadStatus::Missing);
        };
        Ok(match detail.status {
            BeadStatus::Closed => ControllerBeadStatus::Closed,
            _ => ControllerBeadStatus::Open,
        })
    }

    fn task_status(&self, task_id: &str) -> AppResult<ControllerTaskStatus> {
        let project_id =
            crate::shared::domain::ProjectId::new(task_id.to_owned()).map_err(|error| {
                AppError::ResumeFailed {
                    reason: format!("controller task identifier '{task_id}' is invalid: {error}"),
                }
            })?;
        if !FsProjectStore.project_exists(self.base_dir, &project_id)? {
            return Ok(ControllerTaskStatus::Missing);
        }

        let snapshot = FsRunSnapshotStore.read_run_snapshot(self.base_dir, &project_id)?;
        Ok(match snapshot.status {
            RunStatus::Running => ControllerTaskStatus::Running,
            RunStatus::Completed => ControllerTaskStatus::Succeeded,
            RunStatus::NotStarted | RunStatus::Paused | RunStatus::Failed => {
                ControllerTaskStatus::Pending
            }
        })
    }

    fn has_ready_beads(&self) -> AppResult<bool> {
        let ready: Vec<ReadyBead> =
            self.query_br_json(&["ready", "--json"], "milestone controller resume")?;
        let planned_refs = self.planned_bead_membership_refs()?;
        Ok(ready.iter().any(|bead| {
            planned_refs.contains(&bead.id)
                || planned_refs.contains(&format!("{}.{}", self.milestone_id, bead.id))
                || bead
                    .id
                    .strip_prefix(&format!("{}.", self.milestone_id))
                    .is_some_and(|short_ref| planned_refs.contains(short_ref))
        }))
    }

    fn all_beads_closed(&self) -> AppResult<bool> {
        let snapshot = milestone_service::load_snapshot(
            &FsMilestoneSnapshotStore,
            self.base_dir,
            self.milestone_id,
        )?;
        let closed_beads = snapshot
            .progress
            .completed_beads
            .saturating_add(snapshot.progress.skipped_beads);
        Ok(snapshot.status == MilestoneStatus::Completed
            || (snapshot.progress.total_beads > 0
                && closed_beads >= snapshot.progress.total_beads
                && snapshot.progress.in_progress_beads == 0
                && snapshot.progress.failed_beads == 0))
    }
}

fn load_existing_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneRecord> {
    if !store.milestone_exists(base_dir, milestone_id)? {
        return Err(AppError::MilestoneNotFound {
            milestone_id: milestone_id.to_string(),
        });
    }
    milestone_service::load_milestone(store, base_dir, milestone_id)
}

fn load_milestone_summary(
    store: &impl MilestoneStorePort,
    snapshot_store: &impl MilestoneSnapshotPort,
    plan_store: &impl MilestonePlanPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneSummaryView> {
    let record = load_existing_milestone(store, base_dir, milestone_id)?;
    let inspection = load_inspection_state(
        snapshot_store,
        plan_store,
        requirements_store,
        base_dir,
        milestone_id,
    )?;
    let bead_count = inspection
        .plan
        .as_ref()
        .map(|plan| plan.bundle.bead_count() as u32)
        .unwrap_or(0);

    Ok(MilestoneSummaryView {
        id: record.id.to_string(),
        name: record.name,
        status: inspection.display_status,
        bead_count,
        progress: inspection.snapshot.progress,
        active_bead: inspection.snapshot.active_bead,
        pending_requirements: inspection.pending_requirements,
        created_at: record.created_at,
        updated_at: inspection.snapshot.updated_at,
    })
}

fn load_milestone_detail(
    store: &impl MilestoneStorePort,
    snapshot_store: &impl MilestoneSnapshotPort,
    plan_store: &impl MilestonePlanPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneDetailView> {
    let record = load_existing_milestone(store, base_dir, milestone_id)?;
    let inspection = load_inspection_state(
        snapshot_store,
        plan_store,
        requirements_store,
        base_dir,
        milestone_id,
    )?;
    let bead_count = inspection
        .plan
        .as_ref()
        .map(|bundle| bundle.bundle.bead_count() as u32)
        .unwrap_or(0);
    let has_plan = inspection.plan.is_some();

    Ok(MilestoneDetailView {
        id: record.id.to_string(),
        name: record.name,
        description: record.description,
        status: inspection.display_status,
        bead_count,
        progress: inspection.snapshot.progress,
        active_bead: inspection.snapshot.active_bead,
        pending_requirements: inspection.pending_requirements,
        plan_version: inspection.snapshot.plan_version,
        plan_hash: inspection.snapshot.plan_hash,
        created_at: record.created_at,
        updated_at: inspection.snapshot.updated_at,
        has_plan,
    })
}

#[derive(Debug)]
struct LoadedMilestoneBundle {
    bundle: MilestoneBundle,
}

#[derive(Debug)]
struct MilestoneInspectionState {
    snapshot: MilestoneSnapshot,
    plan: Option<LoadedMilestoneBundle>,
    display_status: String,
    pending_requirements: Option<PendingRequirementsView>,
}

fn load_inspection_state(
    snapshot_store: &impl MilestoneSnapshotPort,
    plan_store: &impl MilestonePlanPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneInspectionState> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let snapshot =
            load_snapshot_for_action(snapshot_store, base_dir, milestone_id, "inspection")?;
        let plan = load_live_plan_bundle(plan_store, base_dir, milestone_id, &snapshot)
            .map_err(|error| map_inspection_error(milestone_id, error))?;
        let (display_status, pending_requirements) =
            load_pending_requirements_view(requirements_store, base_dir, milestone_id, &snapshot)
                .map_err(|error| map_inspection_error(milestone_id, error))?;
        Ok(MilestoneInspectionState {
            snapshot,
            plan,
            display_status,
            pending_requirements,
        })
    })
}

fn load_pending_requirements_view(
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    snapshot: &MilestoneSnapshot,
) -> AppResult<(String, Option<PendingRequirementsView>)> {
    let Some(run_id) = snapshot.pending_requirements_run_id.as_deref() else {
        return Ok((snapshot.status.to_string(), None));
    };

    if is_pending_requirements_start_reservation(run_id) {
        return Ok((
            MilestoneStatus::Planning.to_string(),
            Some(PendingRequirementsView {
                run_id: run_id.to_owned(),
                status: "starting".to_owned(),
                status_summary: "requirements run is starting".to_owned(),
            }),
        ));
    }

    let run = requirements_store
        .read_run(base_dir, run_id)
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "inspection".to_owned(),
            details: format!(
                "pending requirements run '{}' could not be inspected: {}",
                run_id, error
            ),
        })?;
    validate_pending_requirements_run_compatibility(milestone_id, "inspection", run_id, &run)?;
    let display_status = match run.status {
        RequirementsStatus::Drafting | RequirementsStatus::Completed => {
            MilestoneStatus::Planning.to_string()
        }
        RequirementsStatus::AwaitingAnswers => RequirementsStatus::AwaitingAnswers.to_string(),
        RequirementsStatus::Failed => MilestoneStatus::Failed.to_string(),
    };

    Ok((
        display_status,
        Some(PendingRequirementsView {
            run_id: run_id.to_owned(),
            status: run.status.to_string(),
            status_summary: run.status_summary,
        }),
    ))
}

fn load_live_plan_bundle(
    plan_store: &impl MilestonePlanPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    snapshot: &MilestoneSnapshot,
) -> AppResult<Option<LoadedMilestoneBundle>> {
    match plan_store.read_plan_json(base_dir, milestone_id) {
        Ok(raw) => {
            let bundle: MilestoneBundle =
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

            bundle
                .validate()
                .map_err(|errors| AppError::CorruptRecord {
                    file: format!("milestones/{}/plan.json", milestone_id),
                    details: errors.join("; "),
                })?;

            let mut hasher = Sha256::new();
            hasher.update(raw.as_bytes());
            let plan_hash = format!("{:x}", hasher.finalize());
            validate_live_plan_snapshot(
                milestone_id,
                snapshot.plan_hash.as_deref(),
                snapshot.plan_version,
                &plan_hash,
            )?;

            Ok(Some(LoadedMilestoneBundle { bundle }))
        }
        Err(AppError::Io(error)) if error.kind() == ErrorKind::NotFound => {
            if snapshot.plan_hash.is_some() || snapshot.plan_version > 0 {
                return Err(AppError::CorruptRecord {
                    file: format!("milestones/{}/status.json", milestone_id),
                    details: format!(
                        "status.json references a live plan, but milestones/{}/plan.json is missing",
                        milestone_id
                    ),
                });
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn validate_live_plan_snapshot(
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

fn print_json<T: Serialize>(value: &T) -> AppResult<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn load_bead_execution_history(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<BeadExecutionHistoryView> {
    milestone_service::bead_execution_history(
        &FsMilestoneStore,
        &FsMilestonePlanStore,
        &FsTaskRunLineageStore,
        base_dir,
        milestone_id,
        bead_id,
    )
}

fn load_milestone_task_list(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneTaskListView> {
    milestone_service::list_tasks_for_milestone(
        &FsMilestoneStore,
        &FsMilestonePlanStore,
        &FsProjectStore,
        base_dir,
        milestone_id,
    )
}

fn validate_workspace(base_dir: &std::path::Path) -> AppResult<()> {
    let config = workspace_governance::load_workspace_config(base_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)
}

fn load_snapshot_for_action(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    action: &str,
) -> AppResult<MilestoneSnapshot> {
    let snapshot = milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: error.to_string(),
        })?;
    snapshot
        .validate_semantics()
        .map_err(|details| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: format!("status.json is inconsistent: {details}"),
        })?;
    Ok(snapshot)
}

fn map_inspection_error(milestone_id: &MilestoneId, error: AppError) -> AppError {
    match error {
        AppError::MilestoneNotFound { .. } | AppError::MilestoneOperationFailed { .. } => error,
        other => AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "inspection".to_owned(),
            details: other.to_string(),
        },
    }
}

fn planning_error(milestone_id: &MilestoneId, details: impl Into<String>) -> AppError {
    AppError::MilestoneOperationFailed {
        milestone_id: milestone_id.to_string(),
        action: "planning".to_owned(),
        details: details.into(),
    }
}

async fn load_or_start_milestone_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    record: &MilestoneRecord,
) -> AppResult<(
    String,
    crate::contexts::requirements_drafting::model::RequirementsRun,
)> {
    let mut recovered_stale_pending_run = false;

    loop {
        match reserve_pending_requirements_run(snapshot_store, base_dir, milestone_id)? {
            PendingRequirementsRunReservation::Existing(run_id) => {
                if let Some(run) = load_pending_requirements_run(
                    snapshot_store,
                    requirements_store,
                    base_dir,
                    milestone_id,
                    &run_id,
                )? {
                    return Ok((run_id, run));
                }

                if recovered_stale_pending_run {
                    return Err(planning_error(
                        milestone_id,
                        "stale pending requirements run could not be recovered automatically",
                    ));
                }
                recovered_stale_pending_run = true;
            }
            PendingRequirementsRunReservation::Reserved(reservation_id) => {
                return start_reserved_requirements_run(
                    snapshot_store,
                    requirements_store,
                    base_dir,
                    milestone_id,
                    record,
                    &reservation_id,
                )
                .await;
            }
        }
    }
}

fn load_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    run_id: &str,
) -> AppResult<Option<crate::contexts::requirements_drafting::model::RequirementsRun>> {
    let run = match requirements_store.read_run(base_dir, run_id) {
        Ok(run) => run,
        Err(AppError::InvalidRequirementsState {
            run_id: missing_run_id,
            details,
        }) if missing_run_id == run_id && details == "requirements run not found" => {
            clear_pending_requirements_run(snapshot_store, base_dir, milestone_id, Some(run_id))?;
            return Ok(None);
        }
        Err(error) => {
            return Err(planning_error(
                milestone_id,
                format!(
                    "pending requirements run '{}' could not be inspected: {}",
                    run_id, error
                ),
            ));
        }
    };
    validate_pending_requirements_run_compatibility(milestone_id, "planning", run_id, &run)?;

    if run.status == RequirementsStatus::Drafting {
        if !pending_requirements_drafting_run_is_stale(&run) {
            return Err(planning_error(
                milestone_id,
                format!(
                    "requirements run '{}' is still drafting in another process; rerun once it reaches awaiting_answers or completed",
                    run_id
                ),
            ));
        }
        clear_pending_requirements_run(snapshot_store, base_dir, milestone_id, Some(run_id))?;
        return Ok(None);
    }

    Ok(Some(run))
}

fn validate_pending_requirements_run_compatibility(
    milestone_id: &MilestoneId,
    action: &str,
    run_id: &str,
    run: &RequirementsRun,
) -> AppResult<()> {
    if run.mode != RequirementsMode::Milestone {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: format!(
                "pending requirements run '{}' is incompatible with milestone planning: expected mode 'milestone', found '{}'",
                run_id, run.mode
            ),
        });
    }

    if run.output_kind != RequirementsOutputKind::MilestoneBundle {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: format!(
                "pending requirements run '{}' is incompatible with milestone planning: expected output kind 'milestone_bundle', found '{}'",
                run_id, run.output_kind
            ),
        });
    }

    Ok(())
}

async fn start_reserved_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    record: &MilestoneRecord,
    reservation_id: &str,
) -> AppResult<(
    String,
    crate::contexts::requirements_drafting::model::RequirementsRun,
)> {
    let now = Utc::now();
    let run_id = requirements_service::generate_run_id(now);
    let effective_config = match EffectiveConfig::load(base_dir) {
        Ok(config) => config,
        Err(error) => {
            clear_pending_requirements_run(
                snapshot_store,
                base_dir,
                milestone_id,
                Some(reservation_id),
            )?;
            return Err(planning_error(milestone_id, error.to_string()));
        }
    };
    let requirements_cli_service =
        match agent_execution_builder::build_requirements_service(&effective_config) {
            Ok(service) => service,
            Err(error) => {
                clear_pending_requirements_run(
                    snapshot_store,
                    base_dir,
                    milestone_id,
                    Some(reservation_id),
                )?;
                return Err(planning_error(milestone_id, error.to_string()));
            }
        };
    replace_pending_requirements_run(
        snapshot_store,
        base_dir,
        milestone_id,
        reservation_id,
        &run_id,
    )?;
    if let Err(error) = requirements_cli_service
        .draft_milestone_with_run_id(base_dir, run_id.clone(), &record.description, now, None)
        .await
    {
        clear_pending_requirements_run(snapshot_store, base_dir, milestone_id, Some(&run_id))?;
        return Err(planning_error(milestone_id, error.to_string()));
    }
    let run = requirements_store
        .read_run(base_dir, &run_id)
        .map_err(|error| {
            planning_error(
                milestone_id,
                format!(
                    "requirements run '{}' could not be inspected: {}",
                    run_id, error
                ),
            )
        })?;
    Ok((run_id, run))
}

#[derive(Debug)]
enum PendingRequirementsRunReservation {
    Existing(String),
    Reserved(String),
}

fn reserve_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<PendingRequirementsRunReservation> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot =
            load_snapshot_for_action(snapshot_store, base_dir, milestone_id, "planning")?;
        if snapshot.status == MilestoneStatus::Running {
            return Err(planning_error(
                milestone_id,
                "cannot replan a milestone while status is 'running'; pause or complete execution before planning again",
            ));
        }
        if let Some(run_id) = snapshot.pending_requirements_run_id.clone() {
            if is_pending_requirements_start_reservation(&run_id) {
                if !pending_requirements_start_reservation_is_stale(&run_id) {
                    return Err(planning_error(
                        milestone_id,
                        "planning is already starting in another process; rerun once the pending requirements run is recorded",
                    ));
                }

                snapshot.pending_requirements_run_id = None;
                snapshot.updated_at = Utc::now();
                planning_snapshot_write(snapshot_store, base_dir, milestone_id, &snapshot)?;
            } else {
                return Ok(PendingRequirementsRunReservation::Existing(run_id));
            }
        }

        let reservation_id = pending_requirements_start_reservation();
        snapshot.pending_requirements_run_id = Some(reservation_id.clone());
        snapshot.updated_at = Utc::now();
        planning_snapshot_write(snapshot_store, base_dir, milestone_id, &snapshot)?;
        Ok(PendingRequirementsRunReservation::Reserved(reservation_id))
    })
}

fn replace_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    expected_run_id: &str,
    next_run_id: &str,
) -> AppResult<()> {
    let replaced = snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot =
            load_snapshot_for_action(snapshot_store, base_dir, milestone_id, "planning")?;
        if snapshot.pending_requirements_run_id.as_deref() != Some(expected_run_id) {
            return Ok(false);
        }

        snapshot.pending_requirements_run_id = Some(next_run_id.to_owned());
        snapshot.updated_at = Utc::now();
        planning_snapshot_write(snapshot_store, base_dir, milestone_id, &snapshot)?;
        Ok(true)
    })?;

    if replaced {
        Ok(())
    } else {
        Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "planning".to_owned(),
            details: format!(
                "pending requirements run changed before reservation '{}' could be finalized",
                expected_run_id
            ),
        })
    }
}

fn clear_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    expected_run_id: Option<&str>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot =
            load_snapshot_for_action(snapshot_store, base_dir, milestone_id, "planning")?;
        if expected_run_id.is_none()
            || snapshot.pending_requirements_run_id.as_deref() == expected_run_id
        {
            snapshot.pending_requirements_run_id = None;
            snapshot.updated_at = Utc::now();
            planning_snapshot_write(snapshot_store, base_dir, milestone_id, &snapshot)?;
        }
        Ok(())
    })
}

fn pending_requirements_start_reservation() -> String {
    let timestamp = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
    format!(
        "{}{}-{}",
        PENDING_REQUIREMENTS_START_PREFIX,
        std::process::id(),
        timestamp
    )
}

fn is_pending_requirements_start_reservation(run_id: &str) -> bool {
    run_id.starts_with(PENDING_REQUIREMENTS_START_PREFIX)
}

fn pending_requirements_start_reservation_is_stale(run_id: &str) -> bool {
    let Some((pid, timestamp_nanos)) = parse_pending_requirements_start_reservation(run_id) else {
        return true;
    };
    let now_nanos = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000);
    let elapsed_nanos = now_nanos.saturating_sub(timestamp_nanos);
    let stale_after_nanos = PENDING_REQUIREMENTS_START_STALE_AFTER_SECONDS * 1_000_000_000;
    let proc_root = std::path::Path::new("/proc");
    let process_missing = proc_root.is_dir() && !proc_root.join(pid.to_string()).exists();

    elapsed_nanos >= stale_after_nanos || process_missing
}

fn parse_pending_requirements_start_reservation(run_id: &str) -> Option<(u32, i64)> {
    let suffix = run_id.strip_prefix(PENDING_REQUIREMENTS_START_PREFIX)?;
    let (pid, timestamp_nanos) = suffix.split_once('-')?;
    Some((pid.parse().ok()?, timestamp_nanos.parse().ok()?))
}

fn pending_requirements_drafting_run_is_stale(
    run: &crate::contexts::requirements_drafting::model::RequirementsRun,
) -> bool {
    let elapsed = Utc::now().signed_duration_since(run.updated_at);
    elapsed.num_seconds() >= PENDING_REQUIREMENTS_DRAFTING_STALE_AFTER_SECONDS
}

fn planning_snapshot_write(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    snapshot: &MilestoneSnapshot,
) -> AppResult<()> {
    snapshot_store
        .write_snapshot(base_dir, milestone_id, snapshot)
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "planning".to_owned(),
            details: error.to_string(),
        })
}

fn print_milestone_detail(detail: &MilestoneDetailView) {
    println!("Milestone:    {}", detail.id);
    println!("Name:         {}", detail.name);
    println!("Description:  {}", detail.description);
    println!("Status:       {}", detail.status);
    println!("Beads:        {}", detail.bead_count);
    println!(
        "Progress:     {}",
        format_progress_line(detail.bead_count, &detail.progress)
    );
    println!("Plan Version: {}", detail.plan_version);
    if let Some(plan_hash) = &detail.plan_hash {
        println!("Plan Hash:    {plan_hash}");
    }
    if let Some(pending) = &detail.pending_requirements {
        println!("Pending Run:  {} ({})", pending.run_id, pending.status);
        println!("Pending Info: {}", pending.status_summary);
    }
    if let Some(active_bead) = &detail.active_bead {
        println!("Active Bead:  {active_bead}");
    }
    println!("Created At:   {}", detail.created_at);
    println!("Updated At:   {}", detail.updated_at);
}

fn format_progress_line(bead_count: u32, progress: &MilestoneProgress) -> String {
    format!(
        "{}/{} completed; {} in progress; {} failed; {} blocked; {} skipped; {} remaining",
        progress.completed_beads,
        bead_count.max(progress.total_beads),
        progress.in_progress_beads,
        progress.failed_beads,
        progress.blocked_beads,
        progress.skipped_beads,
        progress.remaining()
    )
}

fn default_planning_idea(name: &str) -> String {
    format!("Plan milestone '{name}'.")
}

fn derive_milestone_id(name: &str) -> AppResult<String> {
    let mut slug = String::new();
    let mut previous_was_separator = false;

    for character in name.trim().chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !previous_was_separator && !slug.is_empty() {
            slug.push('-');
            previous_was_separator = true;
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    if slug.is_empty() {
        return Err(AppError::InvalidIdentifier {
            value: name.trim().to_owned(),
        });
    }

    if !slug.starts_with("ms-") {
        slug.insert_str(0, "ms-");
    }

    MilestoneId::new(slug.clone())?;
    Ok(slug)
}

fn retarget_bundle(bundle: &mut MilestoneBundle, milestone_id: &MilestoneId, milestone_name: &str) {
    let previous_id = bundle.identity.id.clone();
    bundle.identity.id = milestone_id.to_string();
    bundle.identity.name = milestone_name.to_owned();

    if previous_id == milestone_id.as_str() {
        return;
    }

    let previous_prefix = format!("{previous_id}.");
    let next_prefix = format!("{}.", milestone_id);
    for criterion in &mut bundle.acceptance_map {
        for covered_by in &mut criterion.covered_by {
            if let Some(suffix) = covered_by.strip_prefix(&previous_prefix) {
                *covered_by = format!("{next_prefix}{suffix}");
            }
        }
    }

    for workstream in &mut bundle.workstreams {
        for bead in &mut workstream.beads {
            if let Some(bead_id) = &mut bead.bead_id {
                if let Some(suffix) = bead_id.strip_prefix(&previous_prefix) {
                    *bead_id = format!("{next_prefix}{suffix}");
                }
            }
            for depends_on in &mut bead.depends_on {
                if let Some(suffix) = depends_on.strip_prefix(&previous_prefix) {
                    *depends_on = format!("{next_prefix}{suffix}");
                }
            }
        }
    }
}

fn map_create_error(milestone_id: &str, error: AppError) -> AppError {
    match error {
        AppError::DuplicateProject { .. } | AppError::DuplicateMilestone { .. } => {
            AppError::DuplicateMilestone {
                milestone_id: milestone_id.to_owned(),
            }
        }
        other => AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_owned(),
            action: "creation".to_owned(),
            details: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        default_planning_idea, derive_milestone_id, load_bead_execution_history,
        load_milestone_task_list, reserve_pending_requirements_run, retarget_bundle,
        PendingRequirementsRunReservation, PENDING_REQUIREMENTS_START_PREFIX,
    };
    use chrono::{TimeZone, Utc};
    use clap::Parser;
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FsJournalStore, FsMilestoneSnapshotStore, FsMilestoneStore, FsProjectStore,
        FsTaskRunLineageStore,
    };
    use crate::cli::{Cli, Commands};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::model::{MilestoneId, TaskRunEntry, TaskRunOutcome};
    use crate::contexts::milestone_record::service::{
        self as milestone_service, CreateMilestoneInput, TaskRunLineagePort,
    };
    use crate::contexts::project_run_record::model::{
        ProjectStatusSummary, TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        self as project_service, CreateProjectInput,
    };
    use crate::shared::domain::FlowPreset;

    #[test]
    fn derive_milestone_id_prefixes_and_slugifies_name() {
        let milestone_id = derive_milestone_id("Alpha Launch!").expect("milestone id");
        assert_eq!(milestone_id, "ms-alpha-launch");
    }

    #[test]
    fn derive_milestone_id_preserves_existing_ms_prefix() {
        let milestone_id = derive_milestone_id("MS Alpha").expect("milestone id");
        assert_eq!(milestone_id, "ms-alpha");
    }

    #[test]
    fn default_planning_idea_mentions_name() {
        assert_eq!(default_planning_idea("Alpha"), "Plan milestone 'Alpha'.");
    }

    #[test]
    fn retarget_bundle_rewrites_qualified_bead_references() {
        let mut bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-stub".to_owned(),
                name: "Stub".to_owned(),
            },
            executive_summary: "Summary".to_owned(),
            goals: vec![],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Covered".to_owned(),
                covered_by: vec!["ms-stub.bead-1".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Planning".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("ms-stub.bead-1".to_owned()),
                    explicit_id: None,
                    title: "Bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec!["ms-stub.bead-0".to_owned()],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: Some(FlowPreset::Minimal),
                }],
            }],
            default_flow: FlowPreset::Minimal,
            agents_guidance: None,
        };

        retarget_bundle(
            &mut bundle,
            &MilestoneId::new("ms-alpha").expect("milestone id"),
            "Alpha",
        );

        assert_eq!(bundle.identity.id, "ms-alpha");
        assert_eq!(bundle.identity.name, "Alpha");
        assert_eq!(bundle.acceptance_map[0].covered_by, vec!["ms-alpha.bead-1"]);
        assert_eq!(
            bundle.workstreams[0].beads[0].bead_id.as_deref(),
            Some("ms-alpha.bead-1")
        );
        assert_eq!(
            bundle.workstreams[0].beads[0].depends_on,
            vec!["ms-alpha.bead-0"]
        );
    }

    #[test]
    fn reserve_pending_requirements_run_blocks_second_starter() {
        let temp_dir = tempdir().expect("tempdir");
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 0, 0)
            .single()
            .expect("valid timestamp");

        milestone_service::create_milestone(
            &store,
            temp_dir.path(),
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: "Alpha".to_owned(),
                description: "Plan alpha".to_owned(),
            },
            now,
        )
        .expect("create milestone");

        let first =
            reserve_pending_requirements_run(&snapshot_store, temp_dir.path(), &milestone_id)
                .expect("first reservation");
        match first {
            PendingRequirementsRunReservation::Reserved(run_id) => {
                assert!(
                    run_id.starts_with(PENDING_REQUIREMENTS_START_PREFIX),
                    "reservation should use the startup marker"
                );
            }
            PendingRequirementsRunReservation::Existing(run_id) => {
                panic!("expected reservation, found existing run {run_id}");
            }
        }

        let second =
            reserve_pending_requirements_run(&snapshot_store, temp_dir.path(), &milestone_id)
                .expect_err("second starter should be rejected");
        let message = second.to_string();
        assert!(message.contains("milestone 'ms-alpha' planning failed"));
        assert!(message.contains("already starting in another process"));
    }

    #[test]
    fn milestone_bead_history_parses_correctly() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "milestone",
            "bead-history",
            "ms-alpha",
            "bead-1",
            "--json",
        ]);
        let Commands::Milestone(command) = cli.command else {
            panic!("expected milestone command");
        };
        let super::MilestoneSubcommand::BeadHistory {
            milestone_id,
            bead_id,
            json,
        } = command.command
        else {
            panic!("expected bead-history subcommand");
        };
        assert_eq!(milestone_id, "ms-alpha");
        assert_eq!(bead_id, "bead-1");
        assert!(json);
    }

    #[test]
    fn milestone_tasks_parses_correctly() {
        let cli = Cli::parse_from(["ralph-burning", "milestone", "tasks", "ms-alpha", "--json"]);
        let Commands::Milestone(command) = cli.command else {
            panic!("expected milestone command");
        };
        let super::MilestoneSubcommand::Tasks { milestone_id, json } = command.command else {
            panic!("expected tasks subcommand");
        };
        assert_eq!(milestone_id, "ms-alpha");
        assert!(json);
    }

    #[test]
    fn load_bead_execution_history_returns_lineage_and_runs() {
        let temp_dir = tempdir().expect("tempdir");
        let milestone_store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let started_at = Utc
            .with_ymd_and_hms(2026, 4, 15, 15, 0, 0)
            .single()
            .expect("valid timestamp");
        let finished_at = Utc
            .with_ymd_and_hms(2026, 4, 15, 15, 5, 0)
            .single()
            .expect("valid timestamp");

        milestone_service::create_milestone(
            &milestone_store,
            temp_dir.path(),
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: "Alpha".to_owned(),
                description: "Plan alpha".to_owned(),
            },
            started_at,
        )
        .expect("create milestone");

        lineage_store
            .append_task_run(
                temp_dir.path(),
                &milestone_id,
                &TaskRunEntry {
                    milestone_id: milestone_id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "task-alpha".to_owned(),
                    run_id: Some("run-1".to_owned()),
                    plan_hash: None,
                    outcome: TaskRunOutcome::Succeeded,
                    outcome_detail: Some("completed".to_owned()),
                    started_at,
                    finished_at: Some(finished_at),
                    task_id: Some("task-1".to_owned()),
                },
            )
            .expect("append task run");

        let history =
            load_bead_execution_history(temp_dir.path(), &milestone_id, "bead-1").expect("history");

        assert_eq!(history.lineage.milestone_id, "ms-alpha");
        assert_eq!(history.lineage.milestone_name, "Alpha");
        assert_eq!(history.lineage.bead_id, "bead-1");
        assert_eq!(history.runs.len(), 1);
        assert_eq!(history.runs[0].project_id, "task-alpha");
        assert_eq!(history.runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(history.runs[0].duration_ms, Some(300_000));
    }

    #[test]
    fn load_milestone_task_list_returns_matching_tasks() {
        let temp_dir = tempdir().expect("tempdir");
        let milestone_store = FsMilestoneStore;
        let project_store = FsProjectStore;
        let journal_store = FsJournalStore;
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let created_at = Utc
            .with_ymd_and_hms(2026, 4, 15, 16, 0, 0)
            .single()
            .expect("valid timestamp");

        milestone_service::create_milestone(
            &milestone_store,
            temp_dir.path(),
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: "Alpha".to_owned(),
                description: "Plan alpha".to_owned(),
            },
            created_at,
        )
        .expect("create milestone");

        project_service::create_project(
            &project_store,
            &journal_store,
            temp_dir.path(),
            CreateProjectInput {
                id: crate::shared::domain::ProjectId::new("task-alpha".to_owned())
                    .expect("project id"),
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Implement alpha".to_owned(),
                prompt_hash: "hash-alpha".to_owned(),
                created_at,
                task_source: Some(TaskSource {
                    milestone_id: milestone_id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: None,
                    plan_version: None,
                }),
            },
        )
        .expect("create matching project");

        project_service::create_project(
            &project_store,
            &journal_store,
            temp_dir.path(),
            CreateProjectInput {
                id: crate::shared::domain::ProjectId::new("task-other".to_owned())
                    .expect("project id"),
                name: "Task Other".to_owned(),
                flow: FlowPreset::Minimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Implement other".to_owned(),
                prompt_hash: "hash-other".to_owned(),
                created_at,
                task_source: Some(TaskSource {
                    milestone_id: "ms-other".to_owned(),
                    bead_id: "bead-9".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: None,
                    plan_version: None,
                }),
            },
        )
        .expect("create non-matching project");

        let tasks =
            load_milestone_task_list(temp_dir.path(), &milestone_id).expect("task list view");

        assert_eq!(tasks.milestone_id, "ms-alpha");
        assert_eq!(tasks.milestone_name, "Alpha");
        assert_eq!(tasks.tasks.len(), 1);
        assert_eq!(tasks.tasks[0].project_id, "task-alpha");
        assert_eq!(tasks.tasks[0].bead_id, "bead-1");
        assert_eq!(tasks.tasks[0].flow, FlowPreset::QuickDev);
        assert_eq!(tasks.tasks[0].status_summary, ProjectStatusSummary::Created);
    }
}
