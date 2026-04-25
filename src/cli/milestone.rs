use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::adapters::br_models::{BeadDetail, BeadStatus, ReadyBead};
use crate::adapters::br_process::{BrAdapter, BrMutationAdapter};
use crate::adapters::bv_process::BvAdapter;
use crate::adapters::fs::{
    FileSystem, FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
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
use crate::contexts::project_run_record::service::{
    default_project_id_for_bead, ProjectStorePort, RunSnapshotPort,
};
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
const PENDING_BEAD_EXPORT_ATTEMPT_FILE: &str = "bead-export-attempt.json";
const PENDING_BEAD_EXPORT_LOCK_FILE: &str = "bead-export-attempt.lock";
const MAX_MILESTONE_RUN_STEPS: usize = 256;

#[derive(Debug, Args)]
#[command(about = "Manage milestones and their bead graphs.")]
pub struct MilestoneCommand {
    #[command(subcommand)]
    pub command: MilestoneSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum MilestoneSubcommand {
    #[command(
        about = "Create a milestone record from a name or idea.",
        long_about = "Create a milestone record and make it available for planning.\n\nExample: ralph-burning milestone create ms-dogfood --from-idea \"dogfood the next milestone flow\""
    )]
    Create(MilestoneCreateArgs),
    #[command(
        about = "Plan a milestone's bead graph via the requirements pipeline.",
        long_about = "Draft requirements and turn the milestone into a planned bead graph.\n\nExample: ralph-burning milestone plan ms-alpha-plan"
    )]
    Plan { milestone_id: String },
    #[command(
        about = "Export a milestone's bead graph to the beads store.",
        long_about = "Write planned milestone beads into the workspace beads graph.\n\nExample: ralph-burning milestone export-beads ms-alpha-plan"
    )]
    ExportBeads { milestone_id: String },
    #[command(
        about = "Report the next actionable bead for the active milestone.",
        long_about = "Find the next actionable bead for a milestone, or use the active milestone when omitted.\nRetryable failed beads remain actionable.\n\nExample: ralph-burning milestone next ms-dogfood"
    )]
    Next {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        about = "Start the next actionable bead as a task run.",
        long_about = "Start or resume the next actionable bead's task run, or use the active milestone when omitted.\nRetryable failed beads remain actionable.\n\nExample: ralph-burning milestone run ms-dogfood"
    )]
    Run {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        about = "Show milestone details and bead summary.",
        long_about = "Show milestone metadata, plan state, and bead progress.\n\nExample: ralph-burning milestone show ms-alpha-plan"
    )]
    Show {
        milestone_id: String,
        #[arg(long)]
        json: bool,
    },
    #[command(
        about = "Show history of bead-backed runs for a milestone.",
        long_about = "Show task run attempts and outcomes for a specific milestone bead.\n\nExample: ralph-burning milestone bead-history ms-dogfood ralph-burning-9ni.4.1"
    )]
    BeadHistory {
        milestone_id: String,
        bead_id: String,
        #[arg(long)]
        json: bool,
    },
    #[command(
        about = "Show milestone progress (completed / in-progress / blocked / remaining).",
        long_about = "Summarize one milestone when given an ID, or list all milestone progress when omitted.\n\nExample: ralph-burning milestone status ms-alpha-plan"
    )]
    Status {
        milestone_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(
        about = "List tasks for a milestone.",
        long_about = "List tasks linked to a milestone's beads.\n\nExample: ralph-burning milestone tasks ms-dogfood"
    )]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PendingBeadExportAttempt {
    plan_hash: String,
    owner_token: String,
    pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proc_start_ticks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proc_start_marker: Option<String>,
}

#[derive(Debug)]
struct BeadExportAttemptGuard {
    attempt: PendingBeadExportAttempt,
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
        MilestoneSubcommand::ExportBeads { milestone_id } => {
            handle_export_beads(milestone_id).await
        }
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

async fn handle_export_beads(milestone_id: String) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let journal_store = FsMilestoneJournalStore;
    let plan_store = FsMilestonePlanStore;
    let milestone_id = MilestoneId::new(milestone_id)?;
    load_existing_milestone(&store, &current_dir, &milestone_id)?;
    let (bundle, plan_hash) =
        milestone_service::load_plan_bundle(&plan_store, &current_dir, &milestone_id)?;
    let export_attempt = reserve_bead_export_attempt(&current_dir, &milestone_id, &plan_hash)?;
    let br_mutation = BrMutationAdapter::with_adapter_id(
        BrAdapter::new().with_working_dir(current_dir.clone()),
        bead_export_adapter_id(
            &milestone_id,
            &plan_hash,
            &export_attempt.attempt.owner_token,
        ),
    );

    let report = milestone_service::materialize_beads(&bundle, &current_dir, &br_mutation).await?;
    milestone_service::record_beads_exported_event(
        &journal_store,
        &current_dir,
        &milestone_id,
        &plan_hash,
        &report,
        Utc::now(),
    )?;
    clear_bead_export_attempt(
        &current_dir,
        &milestone_id,
        &export_attempt.attempt.owner_token,
    )?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    println!(
        "Exported beads for milestone '{}' (root: {}, created: {}, reused: {})",
        milestone_id, report.root_epic_id, report.created_beads, report.reused_beads
    );
    Ok(())
}

fn bead_export_adapter_id(
    milestone_id: &MilestoneId,
    plan_hash: &str,
    owner_token: &str,
) -> String {
    format!("milestone-export-beads-{milestone_id}-{plan_hash}-{owner_token}")
}

fn reserve_bead_export_attempt(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    plan_hash: &str,
) -> AppResult<BeadExportAttemptGuard> {
    with_bead_export_attempt_lock(base_dir, milestone_id, || {
        let current_pid = std::process::id();
        let current_ticks = FileSystem::proc_start_ticks_for_pid(current_pid);
        let current_marker = FileSystem::proc_start_marker_for_pid(current_pid);
        let current_attempt = PendingBeadExportAttempt {
            plan_hash: plan_hash.to_owned(),
            owner_token: Uuid::new_v4().to_string(),
            pid: current_pid,
            proc_start_ticks: current_ticks,
            proc_start_marker: current_marker,
        };
        let path = bead_export_attempt_path(base_dir, milestone_id);

        match read_bead_export_attempt(base_dir, milestone_id)? {
            Some(existing) if existing.plan_hash == plan_hash && existing.pid == current_pid => {
                write_bead_export_attempt(
                    base_dir,
                    milestone_id,
                    &PendingBeadExportAttempt {
                        proc_start_ticks: current_attempt.proc_start_ticks,
                        proc_start_marker: current_attempt.proc_start_marker.clone(),
                        ..existing.clone()
                    },
                )?;
                Ok(BeadExportAttemptGuard {
                    attempt: PendingBeadExportAttempt {
                        proc_start_ticks: current_attempt.proc_start_ticks,
                        proc_start_marker: current_attempt.proc_start_marker,
                        ..existing
                    },
                })
            }
            Some(existing) if bead_export_attempt_is_live(&existing) => {
                let details = if existing.plan_hash == plan_hash {
                    format!(
                        "bead export is already running in another process (pid {}) for plan hash '{}'; rerun after that export finishes",
                        existing.pid, existing.plan_hash
                    )
                } else {
                    format!(
                        "bead export is already running in another process (pid {}) for plan hash '{}'; refusing to start a new export for '{}'",
                        existing.pid, existing.plan_hash, plan_hash
                    )
                };
                Err(AppError::MilestoneOperationFailed {
                    milestone_id: milestone_id.to_string(),
                    action: "export beads".to_owned(),
                    details,
                })
            }
            Some(existing) if existing.plan_hash == plan_hash => {
                let recovered = PendingBeadExportAttempt {
                    plan_hash: plan_hash.to_owned(),
                    owner_token: existing.owner_token,
                    pid: current_pid,
                    proc_start_ticks: current_attempt.proc_start_ticks,
                    proc_start_marker: current_attempt.proc_start_marker,
                };
                write_bead_export_attempt(base_dir, milestone_id, &recovered)?;
                Ok(BeadExportAttemptGuard { attempt: recovered })
            }
            Some(_) | None => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                write_bead_export_attempt(base_dir, milestone_id, &current_attempt)?;
                Ok(BeadExportAttemptGuard {
                    attempt: current_attempt,
                })
            }
        }
    })
}

fn clear_bead_export_attempt(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    expected_owner_token: &str,
) -> AppResult<()> {
    with_bead_export_attempt_lock(base_dir, milestone_id, || {
        let path = bead_export_attempt_path(base_dir, milestone_id);
        let Some(existing) = read_bead_export_attempt(base_dir, milestone_id)? else {
            return Ok(());
        };
        if existing.owner_token == expected_owner_token {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(AppError::Io(error)),
            }
        }
        Ok(())
    })
}

fn bead_export_attempt_is_live(attempt: &PendingBeadExportAttempt) -> bool {
    FileSystem::process_identity_matches_live_process(
        attempt.pid,
        attempt.proc_start_ticks,
        attempt.proc_start_marker.as_deref(),
    )
}

fn bead_export_attempt_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
    base_dir
        .join(".ralph-burning/milestones")
        .join(milestone_id.as_str())
        .join(PENDING_BEAD_EXPORT_ATTEMPT_FILE)
}

fn bead_export_attempt_lock_path(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
    base_dir
        .join(".ralph-burning/milestones")
        .join(milestone_id.as_str())
        .join(PENDING_BEAD_EXPORT_LOCK_FILE)
}

fn read_bead_export_attempt(
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Option<PendingBeadExportAttempt>> {
    let path = bead_export_attempt_path(base_dir, milestone_id);
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|error| AppError::CorruptRecord {
                file: format!(
                    "milestones/{}/{}",
                    milestone_id, PENDING_BEAD_EXPORT_ATTEMPT_FILE
                ),
                details: error.to_string(),
            }),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(AppError::Io(error)),
    }
}

fn write_bead_export_attempt(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    attempt: &PendingBeadExportAttempt,
) -> AppResult<()> {
    let path = bead_export_attempt_path(base_dir, milestone_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_vec_pretty(attempt)
        .map_err(|error| AppError::Io(std::io::Error::other(error.to_string())))?;
    std::fs::write(path, payload)?;
    Ok(())
}

fn with_bead_export_attempt_lock<T, F>(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    operation: F,
) -> AppResult<T>
where
    F: FnOnce() -> AppResult<T>,
{
    let lock_path = bead_export_attempt_lock_path(base_dir, milestone_id);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    let _lock = Flock::lock(file, FlockArg::LockExclusive)
        .map_err(|(_, error)| AppError::Io(std::io::Error::from(error)))?;
    operation()
}

async fn handle_next(milestone_id: Option<String>, json: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    validate_workspace(&current_dir)?;

    let store = FsMilestoneStore;
    let snapshot_store = FsMilestoneSnapshotStore;
    let plan_store = FsMilestonePlanStore;
    let requirements_store = FsRequirementsStore;
    let milestone_id = resolve_requested_milestone(&store, &current_dir, milestone_id)?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    ensure_execution_plan_available(
        &snapshot_store,
        &plan_store,
        &requirements_store,
        &current_dir,
        &milestone_id,
        "next",
    )?;

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
    let snapshot_store = FsMilestoneSnapshotStore;
    let plan_store = FsMilestonePlanStore;
    let requirements_store = FsRequirementsStore;
    let milestone_id = resolve_requested_milestone(&store, &current_dir, milestone_id)?;
    workspace_governance::set_active_milestone(&current_dir, &milestone_id)?;
    ensure_execution_plan_available(
        &snapshot_store,
        &plan_store,
        &requirements_store,
        &current_dir,
        &milestone_id,
        "run",
    )?;

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
    let blocked_retry_context = controller_has_retry_context(&controller);

    match controller.state {
        MilestoneControllerState::Claimed
        | MilestoneControllerState::Running
        | MilestoneControllerState::Reconciling => next_view_for_active_bead(
            base_dir,
            milestone_id,
            &milestone_id_text,
            &controller,
            "next",
        ),
        MilestoneControllerState::Blocked if blocked_retry_context => next_view_for_active_bead(
            base_dir,
            milestone_id,
            &milestone_id_text,
            &controller,
            "next",
        ),
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

fn next_view_for_active_bead(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    milestone_id_text: &str,
    controller: &milestone_controller::MilestoneControllerRecord,
    action: &str,
) -> AppResult<MilestoneNextView> {
    let bead_id =
        controller
            .active_bead_id
            .as_deref()
            .ok_or_else(|| AppError::MilestoneOperationFailed {
                milestone_id: milestone_id_text.to_owned(),
                action: action.to_owned(),
                details: format!(
                    "controller state '{}' is missing an active bead identifier",
                    controller_state_label(controller.state)
                ),
            })?;
    let bead = load_bead_view(base_dir, milestone_id, bead_id, controller.state, action)?;
    Ok(MilestoneNextView {
        milestone_id: milestone_id_text.to_owned(),
        status: MilestoneCommandStatus::Success,
        message: if controller.state == MilestoneControllerState::Blocked {
            format!(
                "next bead is '{}' (retryable after failed attempt)",
                bead.id
            )
        } else {
            format!(
                "next bead is '{}' ({})",
                bead.id,
                controller_state_readiness(controller.state)
            )
        },
        bead: Some(bead),
    })
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

    let blocked_retry_context = controller_has_retry_context(&controller);
    if matches!(
        controller.state,
        MilestoneControllerState::Idle | MilestoneControllerState::Selecting
    ) || (controller.state == MilestoneControllerState::Blocked && !blocked_retry_context)
    {
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
        let blocked_retry_context = controller_has_retry_context(&controller);
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
            MilestoneControllerState::Blocked if !blocked_retry_context => {
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
            | MilestoneControllerState::Blocked
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
                        let controller =
                            resolve_controller_for_next(base_dir, milestone_id).await?;
                        let message = controller
                            .last_transition_reason
                            .clone()
                            .unwrap_or_else(|| snapshot.status_summary.clone());
                        let status = match controller.state {
                            MilestoneControllerState::Blocked => MilestoneCommandStatus::Blocked,
                            MilestoneControllerState::NeedsOperator => {
                                MilestoneCommandStatus::NeedsOperator
                            }
                            _ => MilestoneCommandStatus::NeedsOperator,
                        };
                        return Ok(MilestoneRunView {
                            milestone_id: milestone_id.to_string(),
                            status,
                            message,
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

    let create_project_id =
        next_available_controller_bead_project_id(base_dir, milestone_id, bead_id)?;
    recover_existing_bead_project_after_create_conflict(
        base_dir,
        milestone_id,
        bead_id,
        project::execute_create_from_bead_in_dir(
            base_dir,
            project::CreateFromBeadArgs {
                milestone_id: milestone_id.to_string(),
                bead_id: bead_id.to_owned(),
                project_id: Some(create_project_id.to_string()),
                prompt_file: None,
                flow: None,
            },
        )
        .await,
    )
}

fn next_available_controller_bead_project_id(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<crate::shared::domain::ProjectId> {
    let default_project_id = default_project_id_for_bead(&milestone_id.to_string(), bead_id)?;
    if !FsProjectStore.project_exists(base_dir, &default_project_id)? {
        return Ok(default_project_id);
    }

    let base_id = default_project_id.as_str();
    for suffix in 2.. {
        let candidate = crate::shared::domain::ProjectId::new(format!("{base_id}-{suffix}"))?;
        if !FsProjectStore.project_exists(base_dir, &candidate)? {
            return Ok(candidate);
        }
    }

    unreachable!("finite project id space unexpectedly exhausted")
}

fn recover_existing_bead_project_after_create_conflict(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    create_result: AppResult<crate::shared::domain::ProjectId>,
) -> AppResult<crate::shared::domain::ProjectId> {
    match create_result {
        Ok(project_id) => Ok(project_id),
        Err(error) if project::is_create_from_bead_duplicate_conflict(&error) => {
            if let Some(project_id) =
                project::find_existing_bead_project(base_dir, milestone_id, bead_id)?
            {
                milestone_controller::sync_controller_task_claimed(
                    &FsMilestoneControllerStore,
                    base_dir,
                    milestone_id,
                    bead_id,
                    project_id.as_str(),
                    "adopted existing bead-backed project after create-time duplicate detection",
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
        MilestoneControllerState::Blocked => "retryable",
        MilestoneControllerState::Idle
        | MilestoneControllerState::Selecting
        | MilestoneControllerState::NeedsOperator
        | MilestoneControllerState::Completed => "unknown",
    }
}

fn controller_has_retry_context(
    controller: &milestone_controller::MilestoneControllerRecord,
) -> bool {
    controller.state == MilestoneControllerState::Blocked
        && controller.active_bead_id.is_some()
        && controller.active_task_id.is_some()
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
    let primary = std::process::Command::new("br")
        .args(["show", bead_id, "--json"])
        .current_dir(base_dir)
        .output()?;
    if let Some(result) = parse_bead_detail_from_br_output(&primary, milestone_id, bead_id)? {
        return Ok(Some(result));
    }

    if bead_id.contains('.') {
        return Ok(None);
    }

    let no_db = std::process::Command::new("br")
        .args(["show", bead_id, "--json", "--no-db"])
        .current_dir(base_dir)
        .output()?;
    parse_bead_detail_from_br_output(&no_db, milestone_id, bead_id)
}

fn parse_bead_detail_from_br_output(
    output: &std::process::Output,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<Option<BeadDetail>> {
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
                return Ok((detail.id == bead_id).then_some(detail));
            }
            Ok(milestone_bead_refs_match(milestone_id, &detail.id, bead_id).then_some(detail))
        }
        BrShowResponse::Many(details) => {
            let mut matches = details.into_iter().filter(|detail| {
                if bead_id.contains('.') {
                    detail.id == bead_id
                } else {
                    milestone_bead_refs_match(milestone_id, &detail.id, bead_id)
                }
            });
            let detail = matches.next();
            if matches.next().is_some() {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "br show {bead_id} --json returned multiple matching beads"
                ))));
            }
            Ok(detail)
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
        milestone_service::load_runtime_bead_membership_refs(
            &FsMilestonePlanStore,
            &FsMilestoneJournalStore,
            self.base_dir,
            self.milestone_id,
        )
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
            RunStatus::Failed => ControllerTaskStatus::Failed,
            RunStatus::NotStarted | RunStatus::Paused => ControllerTaskStatus::Pending,
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

fn ensure_execution_plan_available(
    snapshot_store: &impl MilestoneSnapshotPort,
    plan_store: &impl MilestonePlanPort,
    requirements_store: &impl RequirementsStorePort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    action: &str,
) -> AppResult<()> {
    let inspection = load_inspection_state(
        snapshot_store,
        plan_store,
        requirements_store,
        base_dir,
        milestone_id,
    )
    .map_err(|error| map_action_error(milestone_id, action, error))?;

    if inspection.plan.is_some() {
        return Ok(());
    }

    let details = if inspection.display_status == MilestoneStatus::Planning.to_string() {
        format!(
            "milestone is still planning and has no plan.json yet; run `ralph-burning milestone plan {}` and retry",
            milestone_id
        )
    } else {
        format!(
            "milestone has no live plan.json; run `ralph-burning milestone plan {}` and retry",
            milestone_id
        )
    };

    Err(AppError::MilestoneOperationFailed {
        milestone_id: milestone_id.to_string(),
        action: action.to_owned(),
        details,
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
        &FsProjectStore,
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

fn map_action_error(milestone_id: &MilestoneId, action: &str, error: AppError) -> AppError {
    match error {
        AppError::MilestoneNotFound { .. } | AppError::MilestoneOperationFailed { .. } => error,
        other => AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
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
        default_planning_idea, derive_milestone_id, inspect_next_milestone_action,
        load_bead_execution_history, load_milestone_task_list, read_bead_export_attempt,
        reserve_bead_export_attempt, reserve_pending_requirements_run, retarget_bundle,
        write_bead_export_attempt, MilestoneCommandControllerRuntime, PendingBeadExportAttempt,
        PendingRequirementsRunReservation, PENDING_REQUIREMENTS_START_PREFIX,
    };
    use chrono::{TimeZone, Utc};
    use clap::Parser;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsJournalStore, FsMilestoneControllerStore, FsMilestoneSnapshotStore,
        FsMilestoneStore, FsProjectStore, FsRunSnapshotWriteStore, FsTaskRunLineageStore,
    };
    use crate::cli::{Cli, Commands};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller::{
        self as milestone_controller, ControllerTransitionRequest, MilestoneControllerResumePort,
        MilestoneControllerState,
    };
    use crate::contexts::milestone_record::model::{MilestoneId, TaskRunEntry, TaskRunOutcome};
    use crate::contexts::milestone_record::service::{
        self as milestone_service, CreateMilestoneInput, TaskRunLineagePort,
    };
    use crate::contexts::project_run_record::model::{
        ActiveRun, ProjectStatusSummary, RunSnapshot, RunStatus, TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        self as project_service, CreateProjectInput, RunSnapshotWritePort,
    };
    use crate::contexts::workspace_governance;
    use crate::shared::domain::FlowPreset;
    use crate::shared::error::AppError;
    #[cfg(unix)]
    use crate::test_support::env::{lock_path_mutex, PathGuard};

    fn create_controller_test_milestone(
        base_dir: &std::path::Path,
        milestone_id: &MilestoneId,
        now: chrono::DateTime<Utc>,
    ) {
        workspace_governance::initialize_workspace(base_dir, now)
            .expect("initialize controller test workspace");
        milestone_service::create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: "Alpha".to_owned(),
                description: "Controller milestone".to_owned(),
            },
            now,
        )
        .expect("create controller milestone");
    }

    fn create_controller_test_bead_project(
        base_dir: &std::path::Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        now: chrono::DateTime<Utc>,
    ) -> crate::shared::domain::ProjectId {
        let project_id =
            crate::shared::domain::ProjectId::new(project_id.to_owned()).expect("project id");
        project_service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: format!("Project {project_id}"),
                flow: FlowPreset::Minimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Bead-backed project".to_owned(),
                prompt_hash: "prompt-hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: milestone_id.to_string(),
                    bead_id: bead_id.to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: None,
                    plan_version: None,
                    plan_workstream_index: None,
                    plan_bead_index: None,
                }),
            },
        )
        .expect("create bead-backed project");
        project_id
    }

    fn write_controller_test_run_status(
        base_dir: &std::path::Path,
        project_id: &crate::shared::domain::ProjectId,
        status: RunStatus,
    ) {
        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base_dir,
                project_id,
                &RunSnapshot {
                    active_run: None,
                    interrupted_run: None,
                    status,
                    cycle_history: Vec::new(),
                    completion_rounds: 0,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: status.to_string(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write test run snapshot");
    }

    #[cfg(unix)]
    fn install_fake_br_show_script(base_dir: &std::path::Path, show_json: &str) {
        let fake_bin = base_dir.join("fake-bin");
        std::fs::create_dir_all(&fake_bin).expect("create fake bin");

        let escaped_show_json = show_json.replace('\'', "'\"'\"'");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"update\" ]; then\n  printf 'Updated\\n'\n  exit 0\nfi\nif [ \"$1\" = \"sync\" ]; then\n  printf 'Synced\\n'\n  exit 0\nfi\nif [ \"$1\" = \"show\" ] && [ \"$3\" = \"--json\" ]; then\n  printf '%s\\n' '{escaped_show_json}'\n  exit 0\nfi\nprintf 'unexpected br invocation: %s\\n' \"$*\" >&2\nexit 1\n"
        );
        let br_path = fake_bin.join("br");
        std::fs::write(&br_path, script).expect("write fake br");
        std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake br");
    }

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
    fn milestone_controller_runtime_reports_failed_task_status() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 17, 12, 0, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let project_id =
            crate::shared::domain::ProjectId::new("bead-failed".to_owned()).expect("project id");

        project_service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Bead Failed".to_owned(),
                flow: FlowPreset::Minimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Investigate failure".to_owned(),
                prompt_hash: "prompt-hash".to_owned(),
                created_at: now,
                task_source: None,
            },
        )
        .expect("create project");

        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base_dir,
                &project_id,
                &RunSnapshot {
                    active_run: None,
                    interrupted_run: None,
                    status: RunStatus::Failed,
                    cycle_history: Vec::new(),
                    completion_rounds: 0,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: "failed before milestone sync".to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write failed snapshot");

        let runtime = MilestoneCommandControllerRuntime {
            base_dir,
            milestone_id: &milestone_id,
        };

        assert_eq!(
            runtime
                .task_status(project_id.as_str())
                .expect("query task status"),
            milestone_controller::ControllerTaskStatus::Failed
        );
    }

    #[test]
    fn recover_existing_bead_project_after_duplicate_create_adopts_not_started_project() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 9, 0, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead_id = "ms-alpha.bead-2";
        create_controller_test_milestone(base_dir, &milestone_id, now);
        let project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "bead-existing",
            now,
        );

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selected bead for execution",
            )
            .with_bead(bead_id),
            now,
        )
        .expect("seed claimed controller");

        let adopted = super::recover_existing_bead_project_after_create_conflict(
            base_dir,
            &milestone_id,
            bead_id,
            Err(AppError::DuplicateBeadProject {
                bead_id: bead_id.to_owned(),
                existing_project_id: project_id.to_string(),
            }),
        )
        .expect("recover existing not-started project");

        assert_eq!(adopted, project_id);
        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(controller.active_bead_id.as_deref(), Some(bead_id));
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );
        assert_eq!(controller.state, MilestoneControllerState::Claimed);
        assert_eq!(
            controller.last_transition_reason.as_deref(),
            Some("adopted existing bead-backed project after create-time duplicate detection")
        );
    }

    #[test]
    fn recover_existing_bead_project_after_duplicate_create_adopts_active_project() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 10, 0, 0)
            .single()
            .expect("valid timestamp");
        let started_at = Utc
            .with_ymd_and_hms(2026, 4, 18, 10, 5, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead_id = "ms-alpha.bead-2";
        create_controller_test_milestone(base_dir, &milestone_id, now);
        let project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "bead-running",
            now,
        );

        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base_dir,
                &project_id,
                &RunSnapshot {
                    active_run: Some(ActiveRun {
                        run_id: "run-existing".to_owned(),
                        stage_cursor: crate::shared::domain::StageCursor::new(
                            crate::shared::domain::StageId::Planning,
                            1,
                            1,
                            1,
                        )
                        .expect("stage cursor"),
                        started_at,
                        prompt_hash_at_cycle_start: String::new(),
                        prompt_hash_at_stage_start: String::new(),
                        qa_iterations_current_cycle: 0,
                        review_iterations_current_cycle: 0,
                        final_review_restart_count: 0,
                        iterative_implementer_state: None,
                        stage_resolution_snapshot: None,
                    }),
                    interrupted_run: None,
                    status: RunStatus::Running,
                    cycle_history: Vec::new(),
                    completion_rounds: 0,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: "running".to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write running snapshot");
        FsTaskRunLineageStore
            .append_task_run(
                base_dir,
                &milestone_id,
                &TaskRunEntry {
                    milestone_id: milestone_id.to_string(),
                    bead_id: bead_id.to_owned(),
                    project_id: project_id.to_string(),
                    run_id: Some("run-existing".to_owned()),
                    plan_hash: None,
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at,
                    finished_at: None,
                    task_id: Some(project_id.to_string()),
                },
            )
            .expect("append active task run");

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selected bead for execution",
            )
            .with_bead(bead_id),
            now,
        )
        .expect("seed claimed controller");

        let adopted = super::recover_existing_bead_project_after_create_conflict(
            base_dir,
            &milestone_id,
            bead_id,
            Err(AppError::DuplicateActiveBead {
                bead_id: bead_id.to_owned(),
                existing_project_id: project_id.to_string(),
                existing_run_id: "run-existing".to_owned(),
            }),
        )
        .expect("recover existing active project");

        assert_eq!(adopted, project_id);
        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(controller.active_bead_id.as_deref(), Some(bead_id));
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );
        assert_eq!(controller.state, MilestoneControllerState::Claimed);
        assert_eq!(
            controller.last_transition_reason.as_deref(),
            Some("adopted existing bead-backed project after create-time duplicate detection")
        );
    }

    #[tokio::test]
    async fn ensure_project_for_controller_recovers_all_terminal_same_bead_history() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 11, 0, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead_id = "ms-alpha.bead-2";
        create_controller_test_milestone(base_dir, &milestone_id, now);
        let default_project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "task-ms-alpha-bead-2",
            now,
        );
        let retry_project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "retry-completed",
            now,
        );
        write_controller_test_run_status(base_dir, &default_project_id, RunStatus::Completed);
        write_controller_test_run_status(base_dir, &retry_project_id, RunStatus::Completed);

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selected bead for execution",
            )
            .with_bead(bead_id),
            now,
        )
        .expect("seed claimed controller");

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("load controller")
        .expect("controller exists");

        let adopted = super::ensure_project_for_controller(base_dir, &milestone_id, &controller)
            .await
            .expect("recover deterministic completed project");

        assert_eq!(adopted, default_project_id);
        let updated_controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("reload controller")
        .expect("controller exists");
        assert_eq!(updated_controller.active_bead_id.as_deref(), Some(bead_id));
        assert_eq!(
            updated_controller.active_task_id.as_deref(),
            Some(default_project_id.as_str())
        );
        assert_eq!(updated_controller.state, MilestoneControllerState::Claimed);
        assert_eq!(
            updated_controller.last_transition_reason.as_deref(),
            Some("adopted existing bead-backed project")
        );
    }

    #[tokio::test]
    async fn ensure_project_for_controller_adopts_failed_active_same_bead_project() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 11, 15, 0)
            .single()
            .expect("valid timestamp");
        let started_at = Utc
            .with_ymd_and_hms(2026, 4, 18, 11, 20, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead_id = "ms-alpha.bead-2";
        create_controller_test_milestone(base_dir, &milestone_id, now);
        let project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "bead-failed",
            now,
        );

        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base_dir,
                &project_id,
                &RunSnapshot {
                    active_run: None,
                    interrupted_run: Some(ActiveRun {
                        run_id: "run-existing".to_owned(),
                        stage_cursor: crate::shared::domain::StageCursor::new(
                            crate::shared::domain::StageId::Planning,
                            1,
                            1,
                            1,
                        )
                        .expect("stage cursor"),
                        started_at,
                        prompt_hash_at_cycle_start: String::new(),
                        prompt_hash_at_stage_start: String::new(),
                        qa_iterations_current_cycle: 0,
                        review_iterations_current_cycle: 0,
                        final_review_restart_count: 0,
                        iterative_implementer_state: None,
                        stage_resolution_snapshot: None,
                    }),
                    status: RunStatus::Failed,
                    cycle_history: Vec::new(),
                    completion_rounds: 0,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: "failed".to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write failed snapshot");
        FsTaskRunLineageStore
            .append_task_run(
                base_dir,
                &milestone_id,
                &TaskRunEntry {
                    milestone_id: milestone_id.to_string(),
                    bead_id: bead_id.to_owned(),
                    project_id: project_id.to_string(),
                    run_id: Some("run-existing".to_owned()),
                    plan_hash: None,
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at,
                    finished_at: None,
                    task_id: Some(project_id.to_string()),
                },
            )
            .expect("append active task run");

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selected bead for execution",
            )
            .with_bead(bead_id),
            now,
        )
        .expect("seed claimed controller");

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("load controller")
        .expect("controller exists");

        let adopted = super::ensure_project_for_controller(base_dir, &milestone_id, &controller)
            .await
            .expect("recover failed active same-bead project");

        assert_eq!(adopted, project_id);
        let updated_controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("reload controller")
        .expect("controller exists");
        assert_eq!(updated_controller.active_bead_id.as_deref(), Some(bead_id));
        assert_eq!(
            updated_controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );
        assert_eq!(updated_controller.state, MilestoneControllerState::Claimed);
        assert_eq!(
            updated_controller.last_transition_reason.as_deref(),
            Some("adopted existing bead-backed project")
        );
    }

    #[tokio::test]
    async fn ensure_project_for_controller_rejects_unresumable_failed_same_bead_project() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 11, 30, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let bead_id = "ms-alpha.bead-2";
        create_controller_test_milestone(base_dir, &milestone_id, now);
        let project_id = create_controller_test_bead_project(
            base_dir,
            &milestone_id,
            bead_id,
            "bead-failed",
            now,
        );
        write_controller_test_run_status(base_dir, &project_id, RunStatus::Failed);

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selected bead for execution",
            )
            .with_bead(bead_id),
            now,
        )
        .expect("seed claimed controller");

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("load controller")
        .expect("controller exists");

        let error = super::ensure_project_for_controller(base_dir, &milestone_id, &controller)
            .await
            .expect_err("unresumable failed same-bead project should fail closed");

        let rendered = error.to_string();
        assert!(matches!(
            error,
            AppError::MilestoneOperationFailed { ref action, .. } if action == "run"
        ));
        assert!(rendered.contains("has no resumable run metadata"));
        assert!(rendered.contains("repair projects/bead-failed/run.json"));

        let updated_controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .expect("reload controller")
        .expect("controller exists");
        assert_eq!(updated_controller.active_task_id, None);
    }

    #[test]
    fn next_available_controller_bead_project_id_skips_unrelated_manual_default_project() {
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 18, 11, 30, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");

        create_controller_test_milestone(base_dir, &milestone_id, now);
        project_service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: crate::shared::domain::ProjectId::new("task-ms-alpha-bead-2".to_owned())
                    .expect("project id"),
                name: "Unrelated manual project".to_owned(),
                flow: FlowPreset::Minimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Manual project".to_owned(),
                prompt_hash: "prompt-hash".to_owned(),
                created_at: now,
                task_source: None,
            },
        )
        .expect("create manual project");

        let selected = super::next_available_controller_bead_project_id(
            base_dir,
            &milestone_id,
            "ms-alpha.bead-2",
        )
        .expect("select available controller project id");

        assert_eq!(selected.as_str(), "task-ms-alpha-bead-2-2");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inspect_next_milestone_action_surfaces_blocked_retry_bead() {
        let _path_lock = lock_path_mutex();
        let temp_dir = tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc
            .with_ymd_and_hms(2026, 4, 17, 13, 0, 0)
            .single()
            .expect("valid timestamp");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let project_id =
            crate::shared::domain::ProjectId::new("bead-run".to_owned()).expect("project id");
        create_controller_test_milestone(base_dir, &milestone_id, now);
        install_fake_br_show_script(
            base_dir,
            r#"{"id":"ms-alpha.bead-2","title":"Retry bead","status":"open","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}"#,
        );
        let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));
        project_service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Retry bead".to_owned(),
                flow: FlowPreset::Minimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "Retry bead".to_owned(),
                prompt_hash: "prompt-hash".to_owned(),
                created_at: now,
                task_source: None,
            },
        )
        .expect("create retry project");
        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base_dir,
                &project_id,
                &RunSnapshot {
                    active_run: None,
                    interrupted_run: None,
                    status: RunStatus::Failed,
                    cycle_history: Vec::new(),
                    completion_rounds: 0,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: "failed attempt 1/3".to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write failed retry snapshot");

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Blocked,
                "retry remains available after failed attempt 1/3",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task(project_id.as_str()),
            now,
        )
        .expect("seed blocked retry controller");

        let outcome = inspect_next_milestone_action(base_dir, &milestone_id)
            .await
            .expect("inspect next should succeed");

        assert!(matches!(
            outcome.status,
            super::MilestoneCommandStatus::Success
        ));
        assert_eq!(
            outcome.bead.as_ref().map(|bead| bead.id.as_str()),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(
            outcome.bead.as_ref().map(|bead| bead.readiness.as_str()),
            Some("retryable")
        );
        assert!(outcome.message.contains("retryable after failed attempt"));
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
    fn milestone_export_beads_parses_correctly() {
        let cli = Cli::parse_from(["ralph-burning", "milestone", "export-beads", "ms-alpha"]);
        let Commands::Milestone(command) = cli.command else {
            panic!("expected milestone command");
        };
        let super::MilestoneSubcommand::ExportBeads { milestone_id } = command.command else {
            panic!("expected export-beads subcommand");
        };
        assert_eq!(milestone_id, "ms-alpha");
    }

    #[test]
    fn reserve_bead_export_attempt_reuses_stale_same_plan_owner() {
        let temp_dir = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let milestone_dir = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        std::fs::create_dir_all(&milestone_dir).expect("milestone dir");
        let stale_attempt = PendingBeadExportAttempt {
            plan_hash: "plan-hash".to_owned(),
            owner_token: "stale-owner".to_owned(),
            pid: u32::MAX,
            proc_start_ticks: None,
            proc_start_marker: None,
        };
        write_bead_export_attempt(temp_dir.path(), &milestone_id, &stale_attempt)
            .expect("write stale attempt");

        let guard = reserve_bead_export_attempt(temp_dir.path(), &milestone_id, "plan-hash")
            .expect("reserve stale attempt");
        let persisted = read_bead_export_attempt(temp_dir.path(), &milestone_id)
            .expect("read persisted attempt")
            .expect("persisted attempt");

        assert_eq!(guard.attempt.owner_token, "stale-owner");
        assert_eq!(persisted.owner_token, "stale-owner");
        assert_eq!(persisted.pid, std::process::id());
        assert_eq!(persisted.plan_hash, "plan-hash");
    }

    #[test]
    fn reserve_bead_export_attempt_rejects_live_other_process() {
        let temp_dir = tempdir().expect("tempdir");
        let milestone_id = MilestoneId::new("ms-alpha").expect("milestone id");
        let milestone_dir = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        std::fs::create_dir_all(&milestone_dir).expect("milestone dir");
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleeper");
        let child_pid = child.id();
        let live_attempt = PendingBeadExportAttempt {
            plan_hash: "plan-hash".to_owned(),
            owner_token: "live-owner".to_owned(),
            pid: child_pid,
            proc_start_ticks: FileSystem::proc_start_ticks_for_pid(child_pid),
            proc_start_marker: FileSystem::proc_start_marker_for_pid(child_pid),
        };
        write_bead_export_attempt(temp_dir.path(), &milestone_id, &live_attempt)
            .expect("write live attempt");

        let error = reserve_bead_export_attempt(temp_dir.path(), &milestone_id, "plan-hash")
            .expect_err("live foreign attempt should block reuse");

        let _ = child.kill();
        let _ = child.wait();

        match error {
            AppError::MilestoneOperationFailed {
                milestone_id: error_milestone_id,
                action,
                details,
            } => {
                assert_eq!(error_milestone_id, "ms-alpha");
                assert_eq!(action, "export beads");
                assert!(details.contains("already running in another process"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
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
                    plan_workstream_index: None,
                    plan_bead_index: None,
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
                    plan_workstream_index: None,
                    plan_bead_index: None,
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
