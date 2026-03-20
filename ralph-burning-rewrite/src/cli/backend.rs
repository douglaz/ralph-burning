//! `ralph-burning backend` — read-only backend diagnostics CLI surface.

use clap::{Args, Subcommand};

use crate::adapters::fs::FsProjectStore;
use crate::contexts::agent_execution::diagnostics::{
    BackendCheckResult, BackendDiagnosticsService, BackendListEntry, BackendProbeResult,
    EffectiveBackendView,
};
use crate::contexts::project_run_record::service::ProjectStorePort;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{BackendSelection, ExecutionMode, FlowPreset};
use crate::shared::error::{AppError, AppResult};

// ── CLI arg structs ─────────────────────────────────────────────────────────

#[derive(Debug, Args)]
pub struct BackendCommand {
    #[command(subcommand)]
    pub command: BackendSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum BackendSubcommand {
    /// List supported backend families and their enablement state.
    List {
        /// Emit a stable JSON array for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Evaluate readiness of all effectively required backends.
    Check {
        /// Emit a stable JSON object for scripts.
        #[arg(long)]
        json: bool,
        /// Override the base backend for this check.
        #[arg(long)]
        backend: Option<String>,
        /// Override the planner backend for this check.
        #[arg(long = "planner-backend")]
        planner_backend: Option<String>,
        /// Override the implementer backend for this check.
        #[arg(long = "implementer-backend")]
        implementer_backend: Option<String>,
        /// Override the reviewer backend for this check.
        #[arg(long = "reviewer-backend")]
        reviewer_backend: Option<String>,
        /// Override the QA backend for this check.
        #[arg(long = "qa-backend")]
        qa_backend: Option<String>,
        /// Override execution mode for this check.
        #[arg(long = "execution-mode")]
        execution_mode: Option<String>,
        /// Override whether backend output streams into runtime logs.
        #[arg(long = "stream-output")]
        stream_output: Option<bool>,
    },
    /// Show resolved backend selection with source precedence.
    ShowEffective {
        /// Emit a stable JSON object for scripts.
        #[arg(long)]
        json: bool,
        /// Override the base backend for this view.
        #[arg(long)]
        backend: Option<String>,
        /// Override the planner backend for this view.
        #[arg(long = "planner-backend")]
        planner_backend: Option<String>,
        /// Override the implementer backend for this view.
        #[arg(long = "implementer-backend")]
        implementer_backend: Option<String>,
        /// Override the reviewer backend for this view.
        #[arg(long = "reviewer-backend")]
        reviewer_backend: Option<String>,
        /// Override the QA backend for this view.
        #[arg(long = "qa-backend")]
        qa_backend: Option<String>,
        /// Override execution mode for this view.
        #[arg(long = "execution-mode")]
        execution_mode: Option<String>,
        /// Override whether backend output streams into runtime logs.
        #[arg(long = "stream-output")]
        stream_output: Option<bool>,
    },
    /// Probe backend resolution for a given role and flow.
    Probe {
        /// The role or panel target to probe (e.g. "planner", "completion_panel").
        #[arg(long)]
        role: String,
        /// The flow preset to probe (e.g. "standard", "quick_dev").
        #[arg(long)]
        flow: String,
        /// The cycle number (defaults to 1).
        #[arg(long)]
        cycle: Option<u32>,
        /// Emit a stable JSON object for scripts.
        #[arg(long)]
        json: bool,
        /// Override the base backend for this probe.
        #[arg(long)]
        backend: Option<String>,
        /// Override the planner backend for this probe.
        #[arg(long = "planner-backend")]
        planner_backend: Option<String>,
        /// Override the implementer backend for this probe.
        #[arg(long = "implementer-backend")]
        implementer_backend: Option<String>,
        /// Override the reviewer backend for this probe.
        #[arg(long = "reviewer-backend")]
        reviewer_backend: Option<String>,
        /// Override the QA backend for this probe.
        #[arg(long = "qa-backend")]
        qa_backend: Option<String>,
        /// Override execution mode for this probe.
        #[arg(long = "execution-mode")]
        execution_mode: Option<String>,
        /// Override whether backend output streams into runtime logs.
        #[arg(long = "stream-output")]
        stream_output: Option<bool>,
    },
}

// ── Dispatch ────────────────────────────────────────────────────────────────

pub async fn handle(command: BackendCommand) -> AppResult<()> {
    match command.command {
        BackendSubcommand::List { json } => handle_list(json).await,
        BackendSubcommand::Check {
            json,
            backend,
            planner_backend,
            implementer_backend,
            reviewer_backend,
            qa_backend,
            execution_mode,
            stream_output,
        } => {
            let overrides = build_overrides(
                backend.as_deref(),
                planner_backend.as_deref(),
                implementer_backend.as_deref(),
                reviewer_backend.as_deref(),
                qa_backend.as_deref(),
                execution_mode.as_deref(),
                stream_output,
            )?;
            handle_check(json, overrides).await
        }
        BackendSubcommand::ShowEffective {
            json,
            backend,
            planner_backend,
            implementer_backend,
            reviewer_backend,
            qa_backend,
            execution_mode,
            stream_output,
        } => {
            let overrides = build_overrides(
                backend.as_deref(),
                planner_backend.as_deref(),
                implementer_backend.as_deref(),
                reviewer_backend.as_deref(),
                qa_backend.as_deref(),
                execution_mode.as_deref(),
                stream_output,
            )?;
            handle_show_effective(json, overrides).await
        }
        BackendSubcommand::Probe {
            role,
            flow,
            cycle,
            json,
            backend,
            planner_backend,
            implementer_backend,
            reviewer_backend,
            qa_backend,
            execution_mode,
            stream_output,
        } => {
            let overrides = build_overrides(
                backend.as_deref(),
                planner_backend.as_deref(),
                implementer_backend.as_deref(),
                reviewer_backend.as_deref(),
                qa_backend.as_deref(),
                execution_mode.as_deref(),
                stream_output,
            )?;
            handle_probe(role, flow, cycle, json, overrides).await
        }
    }
}

// ── Subcommand handlers ─────────────────────────────────────────────────────

async fn handle_list(json: bool) -> AppResult<()> {
    let config = load_effective_config(CliBackendOverrides::default())?;
    let service = BackendDiagnosticsService::new(&config);
    let entries = service.list_backends();

    if json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else {
        render_list_text(&entries);
    }

    Ok(())
}

async fn handle_check(json: bool, overrides: CliBackendOverrides) -> AppResult<()> {
    let config = load_effective_config(overrides)?;
    let service = BackendDiagnosticsService::new(&config);
    let flow = resolve_active_project_flow(&config);

    // Use adapter availability checks when possible. Build a process adapter
    // directly for diagnostics so RALPH_BURNING_BACKEND env var doesn't
    // redirect checks to the wrong transport.
    let result =
        match crate::composition::agent_execution_builder::build_process_backend_adapter(Some(
            &config,
        )) {
            Ok(adapter) => {
                service
                    .check_backends_with_availability(flow, &adapter)
                    .await
            }
            Err(adapter_err) => service.check_backends_with_adapter_failure(flow, &adapter_err),
        };

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        render_check_text(&result);
    }

    if result.passed {
        Ok(())
    } else {
        Err(AppError::BackendCheckFailed {
            failure_count: result.failures.len(),
        })
    }
}

async fn handle_show_effective(json: bool, overrides: CliBackendOverrides) -> AppResult<()> {
    let config = load_effective_config(overrides)?;
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    if json {
        println!("{}", serde_json::to_string_pretty(&view)?);
    } else {
        render_show_effective_text(&view);
    }

    Ok(())
}

async fn handle_probe(
    role: String,
    flow_str: String,
    cycle: Option<u32>,
    json: bool,
    overrides: CliBackendOverrides,
) -> AppResult<()> {
    let flow: FlowPreset = flow_str.parse()?;
    let config = load_effective_config(overrides)?;
    let service = BackendDiagnosticsService::new(&config);

    // Use adapter availability when possible so optional unavailable members
    // are correctly reported as omitted and required unavailable members fail.
    // If the adapter cannot be constructed, surface that as an error instead of
    // silently falling back to config-only resolution.
    let result =
        match crate::composition::agent_execution_builder::build_backend_adapter_with_config(Some(
            &config,
        )) {
            Ok(adapter) => {
                service
                    .probe_with_availability(&role, flow, cycle.unwrap_or(1), &adapter)
                    .await?
            }
            Err(adapter_err) => {
                return Err(AppError::BackendUnavailable {
                    backend: "adapter".to_owned(),
                    details: format!(
                        "cannot construct backend adapter for probe: {}",
                        adapter_err
                    ),
                });
            }
        };

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        render_probe_text(&result);
    }

    Ok(())
}

// ── Config loading ──────────────────────────────────────────────────────────

fn load_effective_config(overrides: CliBackendOverrides) -> AppResult<EffectiveConfig> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace exists
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Try to resolve active project for project-level overrides;
    // fall back to workspace-only config if no project is active.
    match workspace_governance::resolve_active_project(&current_dir) {
        Ok(project_id) => {
            EffectiveConfig::load_for_project(&current_dir, Some(&project_id), overrides)
        }
        Err(AppError::NoActiveProject) => {
            EffectiveConfig::load_for_project(&current_dir, None, overrides)
        }
        Err(error) => Err(error),
    }
}

fn build_overrides(
    backend: Option<&str>,
    planner_backend: Option<&str>,
    implementer_backend: Option<&str>,
    reviewer_backend: Option<&str>,
    qa_backend: Option<&str>,
    execution_mode: Option<&str>,
    stream_output: Option<bool>,
) -> AppResult<CliBackendOverrides> {
    Ok(CliBackendOverrides {
        backend: backend
            .map(BackendSelection::from_backend_name)
            .transpose()?,
        planner_backend: planner_backend
            .map(BackendSelection::from_backend_name)
            .transpose()?,
        implementer_backend: implementer_backend
            .map(BackendSelection::from_backend_name)
            .transpose()?,
        reviewer_backend: reviewer_backend
            .map(BackendSelection::from_backend_name)
            .transpose()?,
        qa_backend: qa_backend
            .map(BackendSelection::from_backend_name)
            .transpose()?,
        execution_mode: execution_mode
            .map(str::parse::<ExecutionMode>)
            .transpose()?,
        stream_output,
    })
}

/// Resolve the canonical flow for the active project. Falls back to the
/// workspace/config default flow when no project is selected.
fn resolve_active_project_flow(config: &EffectiveConfig) -> FlowPreset {
    let current_dir = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(_) => return config.default_flow(),
    };

    match workspace_governance::resolve_active_project(&current_dir) {
        Ok(project_id) => {
            let store = FsProjectStore;
            match store.read_project_record(&current_dir, &project_id) {
                Ok(record) => record.flow,
                Err(_) => config.default_flow(),
            }
        }
        Err(_) => config.default_flow(),
    }
}

// ── Text renderers ──────────────────────────────────────────────────────────

fn render_list_text(entries: &[BackendListEntry]) {
    println!("Backend Families:");
    println!("{:-<60}", "");
    for entry in entries {
        let status = if entry.enabled { "enabled" } else { "disabled" };
        let compile_note = match entry.compile_only {
            Some(true) => " (compile-time only)",
            _ => "",
        };
        println!(
            "  {:<14} {:<10} {}{}",
            entry.family, status, entry.transport, compile_note
        );
    }
}

fn render_check_text(result: &BackendCheckResult) {
    if result.passed {
        println!("All backend checks passed.");
        return;
    }

    println!(
        "Backend check FAILED ({} failure(s)):",
        result.failures.len()
    );
    println!();
    for failure in &result.failures {
        println!(
            "  [{failure_kind}] {role} / {backend}",
            failure_kind = failure.failure_kind,
            role = failure.role,
            backend = failure.backend_family,
        );
        println!("    {}", failure.details);
        println!("    source: {}", failure.config_source);
        println!();
    }
}

fn render_show_effective_text(view: &EffectiveBackendView) {
    println!("Effective Backend Configuration:");
    println!("{:-<60}", "");
    println!(
        "  Base backend:     {} (source: {})",
        view.base_backend.value, view.base_backend.source
    );
    println!(
        "  Default model:    {} (source: {})",
        view.default_model.value, view.default_model.source
    );
    println!("  Default session:  {}", view.default_session_policy);
    println!("  Default timeout:  {}s", view.default_timeout_seconds);
    println!();
    println!("  Per-role resolution:");
    for role in &view.roles {
        if let Some(err) = &role.resolution_error {
            println!(
                "    {:<20} {}/{} [UNRESOLVED: {}]",
                role.role, role.backend_family, role.model_id, err,
            );
            println!("    {:<20} sources: backend={}", "", role.override_source,);
        } else {
            println!(
                "    {:<20} {}/{} timeout={}s session={}",
                role.role,
                role.backend_family,
                role.model_id,
                role.timeout_seconds,
                role.session_policy,
            );
            println!(
                "    {:<20} sources: backend={}, model={}, timeout={}",
                "", role.override_source, role.model_source, role.timeout_source,
            );
        }
    }
}

fn render_probe_text(result: &BackendProbeResult) {
    println!(
        "Probe: role={} flow={} cycle={}",
        result.role, result.flow, result.cycle
    );
    println!("{:-<60}", "");
    println!(
        "  Target: {}/{}  timeout={}s",
        result.target.backend_family, result.target.model_id, result.target.timeout_seconds,
    );

    if let Some(panel) = &result.panel {
        println!();
        println!(
            "  Panel: {} (minimum={}, resolved={})",
            panel.panel_type, panel.minimum, panel.resolved_count,
        );
        for member in &panel.members {
            let req = if member.required {
                "required"
            } else {
                "optional"
            };
            println!("    [{req}] {}/{}", member.backend_family, member.model_id,);
        }
        if let Some(arbiter) = &panel.arbiter {
            println!(
                "    [arbiter] {}/{}",
                arbiter.backend_family, arbiter.model_id,
            );
        }
        for omitted in &panel.omitted {
            println!(
                "    [omitted] {} — {} (was_optional={})",
                omitted.backend_family, omitted.reason, omitted.was_optional,
            );
        }
    }
}
