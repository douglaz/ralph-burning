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
    MilestoneId, MilestoneProgress, MilestoneRecord, MilestoneSnapshot,
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
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
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
    let handoff = requirements_service::extract_milestone_bundle_handoff(
        &requirements_store,
        &current_dir,
        &run_id,
    )
    .map_err(|error| AppError::MilestoneOperationFailed {
        milestone_id: milestone_id.to_string(),
        action: "planning".to_owned(),
        details: format!(
            "requirements run '{}' did not produce a usable milestone bundle: {}",
            run_id, error
        ),
    })?;

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
    let milestone_id = MilestoneId::new(milestone_id)?;
    let detail = load_milestone_detail(
        &store,
        &snapshot_store,
        &plan_store,
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

    if let Some(milestone_id) = milestone_id {
        let milestone_id = MilestoneId::new(milestone_id)?;
        let detail = load_milestone_detail(
            &store,
            &snapshot_store,
            &plan_store,
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
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneSummaryView> {
    let record = load_existing_milestone(store, base_dir, milestone_id)?;
    let snapshot = milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)?;
    let plan = load_live_plan_bundle(plan_store, base_dir, milestone_id, &snapshot)
        .map_err(|error| map_inspection_error(milestone_id, error))?;
    let bead_count = plan
        .as_ref()
        .map(|plan| plan.bundle.bead_count() as u32)
        .unwrap_or(0);

    Ok(MilestoneSummaryView {
        id: record.id.to_string(),
        name: record.name,
        status: snapshot.status.to_string(),
        bead_count,
        progress: snapshot.progress,
        active_bead: snapshot.active_bead,
        created_at: record.created_at,
        updated_at: snapshot.updated_at,
    })
}

fn load_milestone_detail(
    store: &impl MilestoneStorePort,
    snapshot_store: &impl MilestoneSnapshotPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneDetailView> {
    let record = load_existing_milestone(store, base_dir, milestone_id)?;
    let snapshot = milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)?;
    let plan_bundle = load_live_plan_bundle(plan_store, base_dir, milestone_id, &snapshot)
        .map_err(|error| map_inspection_error(milestone_id, error))?;
    let bead_count = plan_bundle
        .as_ref()
        .map(|bundle| bundle.bundle.bead_count() as u32)
        .unwrap_or(0);
    let has_plan = plan_bundle.is_some();

    Ok(MilestoneDetailView {
        id: record.id.to_string(),
        name: record.name,
        description: record.description,
        status: snapshot.status.to_string(),
        bead_count,
        progress: snapshot.progress,
        active_bead: snapshot.active_bead,
        plan_version: snapshot.plan_version,
        plan_hash: snapshot.plan_hash,
        created_at: record.created_at,
        updated_at: snapshot.updated_at,
        has_plan,
    })
}

#[derive(Debug)]
struct LoadedMilestoneBundle {
    bundle: MilestoneBundle,
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

fn map_inspection_error(milestone_id: &MilestoneId, error: AppError) -> AppError {
    match error {
        AppError::MilestoneNotFound { .. } => error,
        other => AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "inspection".to_owned(),
            details: other.to_string(),
        },
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
    let snapshot = milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)?;
    if let Some(run_id) = snapshot.pending_requirements_run_id {
        let run = requirements_store
            .read_run(base_dir, &run_id)
            .map_err(|error| AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "planning".to_owned(),
                details: format!(
                    "pending requirements run '{}' could not be inspected: {}",
                    run_id, error
                ),
            })?;
        return Ok((run_id, run));
    }

    let effective_config = EffectiveConfig::load(base_dir)?;
    let requirements_cli_service =
        agent_execution_builder::build_requirements_service(&effective_config)?;
    let run_id = requirements_cli_service
        .draft_milestone(base_dir, &record.description, Utc::now(), None)
        .await
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "planning".to_owned(),
            details: error.to_string(),
        })?;
    set_pending_requirements_run(snapshot_store, base_dir, milestone_id, Some(&run_id))?;
    let run = requirements_store
        .read_run(base_dir, &run_id)
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "planning".to_owned(),
            details: format!(
                "requirements run '{}' could not be inspected: {}",
                run_id, error
            ),
        })?;

    Ok((run_id, run))
}

fn clear_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    expected_run_id: Option<&str>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot =
            milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)?;
        if expected_run_id.is_none()
            || snapshot.pending_requirements_run_id.as_deref() == expected_run_id
        {
            snapshot.pending_requirements_run_id = None;
            snapshot.updated_at = Utc::now();
            snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;
        }
        Ok(())
    })
}

fn set_pending_requirements_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    run_id: Option<&str>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot =
            milestone_service::load_snapshot(snapshot_store, base_dir, milestone_id)?;
        snapshot.pending_requirements_run_id = run_id.map(str::to_owned);
        snapshot.updated_at = Utc::now();
        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;
        Ok(())
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
    use super::{default_planning_idea, derive_milestone_id, retarget_bundle};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::model::MilestoneId;
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
}
