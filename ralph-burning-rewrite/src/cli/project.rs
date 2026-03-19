use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use clap::{ArgGroup, Args, Subcommand};

use crate::adapters::fs::{
    FileSystem, FsActiveProjectStore, FsAmendmentQueueStore, FsDaemonStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRequirementsStore, FsRunSnapshotStore,
    FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
};
use crate::composition::agent_execution_builder;
use crate::contexts::automation_runtime::cli_writer_lease::{
    CliWriterLeaseGuard, CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};
use crate::contexts::project_run_record::model::{ProjectDetail, ProjectStatusSummary, RunStatus};
use crate::contexts::project_run_record::service::{
    self, CreateProjectInput, ProjectStorePort, RunSnapshotPort,
};
use crate::contexts::requirements_drafting::model::RequirementsStatus;
use crate::contexts::requirements_drafting::service::{
    self as requirements_service, RequirementsStorePort,
};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct ProjectCommand {
    #[command(subcommand)]
    pub command: ProjectSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ProjectSubcommand {
    Create(ProjectCreateArgs),
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
    #[arg(long, required_unless_present = "from_requirements")]
    pub flow: Option<String>,
    #[arg(
        long = "from-requirements",
        conflicts_with_all = ["id", "name", "prompt", "flow"]
    )]
    pub from_requirements: Option<String>,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("bootstrap_input")
        .required(true)
        .multiple(false)
        .args(["idea", "from_file"])
))]
pub struct BootstrapArgs {
    #[arg(long, group = "bootstrap_input")]
    pub idea: Option<String>,
    #[arg(long = "from-file", group = "bootstrap_input")]
    pub from_file: Option<PathBuf>,
    #[arg(long)]
    pub flow: Option<String>,
    #[arg(long)]
    pub start: bool,
}

pub async fn handle(command: ProjectCommand) -> AppResult<()> {
    match command.command {
        ProjectSubcommand::Select { id } => {
            let current_dir = std::env::current_dir()?;
            let project_id = ProjectId::new(id)?;
            workspace_governance::set_active_project(&current_dir, &project_id)?;
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

    // Validate flow preset
    let flow: FlowPreset = args.flow.expect("clap should require --flow").parse()?;

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

    let handoff = load_seed_handoff(&current_dir, &run_id)?;
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
    Ok(())
}

async fn handle_bootstrap(args: BootstrapArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let flow_override = parse_flow_override(args.flow.as_deref())?;
    let idea = read_bootstrap_idea(&current_dir, &args)?;
    let effective_config = EffectiveConfig::load(&current_dir)?;
    let requirements_cli_service =
        agent_execution_builder::build_requirements_service(&effective_config)?;
    let run_id = requirements_cli_service
        .quick(&current_dir, &idea, Utc::now())
        .await?;
    let handoff =
        requirements_service::extract_seed_handoff(&FsRequirementsStore, &current_dir, &run_id)?;

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
    .map_err(|error| map_requirements_project_error(error, &run_id))?;

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

fn load_project_detail(base_dir: &Path, project_id: &ProjectId) -> AppResult<ProjectDetail> {
    let store = FsProjectStore;
    let run_store = FsRunSnapshotStore;
    let journal_store = FsJournalStore;
    let active_store = FsActiveProjectStore;

    service::show_project(
        &store,
        &run_store,
        &journal_store,
        &active_store,
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

fn load_seed_handoff(
    base_dir: &Path,
    run_id: &str,
) -> AppResult<requirements_service::SeedHandoff> {
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

    requirements_service::extract_seed_handoff(&store, base_dir, run_id)
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
    let agent_service = agent_execution_builder::build_agent_execution_service()?;
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
        let lock_path = current_dir.join(format!(
            ".ralph-burning/daemon/leases/writer-{}.lock",
            project_id.as_str()
        ));
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
        let lock_path = current_dir.join(format!(
            ".ralph-burning/daemon/leases/writer-{}.lock",
            project_id.as_str()
        ));
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
        let lock_path = current_dir.join(format!(
            ".ralph-burning/daemon/leases/writer-{}.lock",
            project_id.as_str()
        ));
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
