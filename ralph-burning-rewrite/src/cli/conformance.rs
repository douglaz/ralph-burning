use clap::{Args, Subcommand};

use crate::contexts::conformance_spec::catalog;
use crate::contexts::conformance_spec::cutover_guard;
use crate::contexts::conformance_spec::runner;
use crate::contexts::conformance_spec::scenarios;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct ConformanceCommand {
    #[command(subcommand)]
    pub command: ConformanceSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum ConformanceSubcommand {
    List,
    Run {
        #[arg(long)]
        filter: Option<String>,
    },
}

pub async fn handle(command: ConformanceCommand) -> AppResult<()> {
    match command.command {
        ConformanceSubcommand::List => handle_list(),
        ConformanceSubcommand::Run { filter } => handle_run(filter),
    }
}

fn handle_list() -> AppResult<()> {
    let scenarios = catalog::discover_scenarios()?;
    catalog::validate_ids(&scenarios)?;

    println!(
        "{:<30} {:<40} {:<40} {}",
        "SCENARIO ID", "FEATURE", "SCENARIO", "SOURCE"
    );
    println!("{}", "-".repeat(140));

    for s in &scenarios {
        println!(
            "{:<30} {:<40} {:<40} {}:{}",
            s.id,
            truncate(&s.feature_title, 38),
            truncate(&s.scenario_title, 38),
            s.source_file,
            s.source_line,
        );
    }

    println!();
    println!("Total: {} scenarios", scenarios.len());

    Ok(())
}

fn handle_run(filter: Option<String>) -> AppResult<()> {
    // Phase 1: Discovery and validation
    let scenarios = catalog::discover_scenarios()?;
    catalog::validate_ids(&scenarios)?;

    // Phase 2: Build and validate registry
    let registry = scenarios::build_registry();
    runner::validate_registry(&scenarios, &registry)?;

    // Phase 3: Cutover guard
    let compile_time_src = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let src_dir = if compile_time_src.is_dir() {
        compile_time_src.to_path_buf()
    } else {
        // Fallback for Nix-built binaries where CARGO_MANIFEST_DIR
        // points to the build sandbox.  Resolve relative to CWD.
        std::path::PathBuf::from("src")
    };
    cutover_guard::check_cutover_guard(&src_dir)?;

    // Phase 4: Filter resolution
    let selected: Vec<&_> = if let Some(ref filter_id) = filter {
        runner::resolve_filter(&scenarios, filter_id)?
    } else {
        scenarios.iter().collect()
    };

    if selected.is_empty() {
        return Err(AppError::ConformanceDiscoveryFailed {
            details: "no scenarios selected for execution".to_owned(),
        });
    }

    eprintln!("Running {} conformance scenarios...", selected.len());
    eprintln!();

    // Phase 5: Execution
    let report = runner::run_scenarios(&selected, &registry);

    // Phase 6: Summary
    eprint!("{report}");

    // Phase 7: Exit code
    // The runner already printed the failing scenario ID and reason during execution;
    // returning ConformanceRunFailed gives a non-zero exit without double-printing.
    if report.failed > 0 {
        Err(AppError::ConformanceRunFailed)
    } else {
        Ok(())
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
