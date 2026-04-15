use std::io::ErrorKind;

use chrono::{DateTime, Utc};
use clap::{Args, Subcommand};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::adapters::fs::{
    FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
    FsRequirementsStore,
};
use crate::composition::agent_execution_builder;
use crate::contexts::milestone_record::bundle::MilestoneBundle;
use crate::contexts::milestone_record::model::{
    MilestoneId, MilestoneProgress, MilestoneRecord, MilestoneSnapshot, MilestoneStatus,
};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CreateMilestoneInput, MilestonePlanPort, MilestoneSnapshotPort,
    MilestoneStorePort,
};
use crate::contexts::requirements_drafting::model::RequirementsStatus;
use crate::contexts::requirements_drafting::service as requirements_service;
use crate::contexts::requirements_drafting::service::RequirementsStorePort;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::error::{AppError, AppResult};

const PENDING_REQUIREMENTS_START_PREFIX: &str = "__starting__:";
const PENDING_REQUIREMENTS_START_STALE_AFTER_SECONDS: i64 = 30;
const PENDING_REQUIREMENTS_DRAFTING_STALE_AFTER_SECONDS: i64 = 300;

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
    Show {
        milestone_id: String,
        #[arg(long)]
        json: bool,
    },
    Status {
        milestone_id: Option<String>,
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

pub async fn handle(command: MilestoneCommand) -> AppResult<()> {
    match command.command {
        MilestoneSubcommand::Create(args) => handle_create(args).await,
        MilestoneSubcommand::Plan { milestone_id } => handle_plan(milestone_id).await,
        MilestoneSubcommand::Show { milestone_id, json } => handle_show(milestone_id, json).await,
        MilestoneSubcommand::Status { milestone_id, json } => {
            handle_status(milestone_id, json).await
        }
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
    milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id).map_err(|error| {
        AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: error.to_string(),
        }
    })
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
        default_planning_idea, derive_milestone_id, reserve_pending_requirements_run,
        retarget_bundle, PendingRequirementsRunReservation, PENDING_REQUIREMENTS_START_PREFIX,
    };
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    use crate::adapters::fs::{FsMilestoneSnapshotStore, FsMilestoneStore};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::model::MilestoneId;
    use crate::contexts::milestone_record::service::{
        self as milestone_service, CreateMilestoneInput,
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
}
