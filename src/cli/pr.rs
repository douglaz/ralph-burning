use clap::{Args, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

use crate::adapters::br_process::BrAdapter;
use crate::adapters::fs::{FsJournalStore, FsProjectStore, FsRunSnapshotStore};
use crate::contexts::bead_workflow::pr_open::{
    open_pr_for_completed_run, PrOpenError, PrOpenRequest, PrOpenStores, ProcessPrToolPort,
};
use crate::contexts::bead_workflow::pr_watch::{
    watch_pr, PrWatchError, PrWatchRequest, SystemPrWatchClock,
};
use crate::contexts::workspace_governance;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
#[command(about = "Pull request automation for completed runs.")]
pub struct PrCommand {
    #[arg(long = "br-path", value_name = "PATH", global = true)]
    pub br_path: Option<PathBuf>,
    #[command(subcommand)]
    pub command: PrSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum PrSubcommand {
    #[command(about = "Squash checkpoint commits, push, and open a PR for a completed run.")]
    Open(PrOpenArgs),
    #[command(about = "Poll CI and codex bot approval for a PR, then squash-merge on success.")]
    Watch(PrWatchArgs),
}

#[derive(Debug, Args)]
pub struct PrOpenArgs {
    #[arg(long = "bead-id")]
    pub bead_id: Option<String>,
    #[arg(long)]
    pub skip_gates: bool,
}

#[derive(Debug, Args)]
pub struct PrWatchArgs {
    pub pr_number: u64,
    #[arg(long = "max-wait", value_parser = parse_duration, default_value = "60m")]
    pub max_wait: Duration,
    #[arg(
        long = "poll-interval",
        value_parser = parse_positive_seconds,
        default_value_t = 30
    )]
    pub poll_interval: u64,
}

pub async fn handle(command: PrCommand) -> AppResult<()> {
    match command.command {
        PrSubcommand::Open(args) => {
            let br_path = crate::cli::resolve_br_path_for_command(command.br_path.as_deref())?;
            handle_open(args, br_path).await
        }
        PrSubcommand::Watch(args) => handle_watch(args).await,
    }
}

async fn handle_open(args: PrOpenArgs, br_path: PathBuf) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;
    let br = BrAdapter::with_binary_path(br_path).with_working_dir(current_dir.clone());
    let output = open_pr_for_completed_run(
        PrOpenRequest {
            base_dir: &current_dir,
            project_id: &project_id,
            bead_id_override: args.bead_id.as_deref(),
            skip_gates: args.skip_gates,
        },
        PrOpenStores {
            project_store: &FsProjectStore,
            run_store: &FsRunSnapshotStore,
            journal_store: &FsJournalStore,
        },
        &br,
        &ProcessPrToolPort,
    )
    .await
    .map_err(map_pr_open_error)?;

    for warning in output.warnings {
        eprintln!("warning: {warning}");
    }
    println!("{}", output.pr_url);
    Ok(())
}

async fn handle_watch(args: PrWatchArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let output = watch_pr(
        PrWatchRequest {
            base_dir: &current_dir,
            pr_number: args.pr_number,
            max_wait: args.max_wait,
            poll_interval: Duration::from_secs(args.poll_interval),
        },
        &ProcessPrToolPort,
        &SystemPrWatchClock::started_now(),
    )
    .await
    .map_err(map_pr_watch_error)?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn map_pr_open_error(error: PrOpenError) -> AppError {
    AppError::PrOpenFailed {
        reason: error.to_string(),
    }
}

fn map_pr_watch_error(error: PrWatchError) -> AppError {
    AppError::PrWatchFailed {
        reason: error.to_string(),
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("duration must not be empty".to_owned());
    }
    let (number, unit) = value.split_at(
        value
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(value.len()),
    );
    let amount = number
        .parse::<u64>()
        .map_err(|_| "duration must start with an unsigned integer".to_owned())?;
    let seconds = match unit {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => amount,
        "m" | "min" | "mins" | "minute" | "minutes" => amount
            .checked_mul(60)
            .ok_or_else(|| "duration is too large".to_owned())?,
        "h" | "hr" | "hrs" | "hour" | "hours" => amount
            .checked_mul(60 * 60)
            .ok_or_else(|| "duration is too large".to_owned())?,
        _ => return Err("duration unit must be seconds, minutes, or hours".to_owned()),
    };
    Ok(Duration::from_secs(seconds))
}

fn parse_positive_seconds(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "seconds must be an unsigned integer".to_owned())?;
    if seconds == 0 {
        return Err("seconds must be greater than zero".to_owned());
    }
    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{Cli, Commands};
    use crate::shared::error::AppError;

    use super::*;

    #[test]
    fn pr_open_parses_options() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "pr",
            "open",
            "--bead-id",
            "2qlo",
            "--skip-gates",
        ]);
        let Commands::Pr(command) = cli.command else {
            panic!("expected pr command");
        };
        let PrSubcommand::Open(args) = command.command else {
            panic!("expected open command");
        };
        assert!(command.br_path.is_none());
        assert_eq!(args.bead_id.as_deref(), Some("2qlo"));
        assert!(args.skip_gates);
    }

    #[test]
    fn pr_global_br_path_parses_for_open() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "pr",
            "--br-path",
            "/opt/beads/bin/br",
            "open",
            "--bead-id",
            "2qlo",
        ]);
        let Commands::Pr(command) = cli.command else {
            panic!("expected pr command");
        };
        assert_eq!(
            command.br_path.as_deref(),
            Some(std::path::Path::new("/opt/beads/bin/br"))
        );
    }

    #[test]
    fn pr_watch_parses_defaults_and_duration_options() {
        let cli = Cli::parse_from([
            "ralph-burning",
            "pr",
            "watch",
            "42",
            "--max-wait",
            "2h",
            "--poll-interval",
            "15",
        ]);
        let Commands::Pr(command) = cli.command else {
            panic!("expected pr command");
        };
        let PrSubcommand::Watch(args) = command.command else {
            panic!("expected watch command");
        };
        assert_eq!(args.pr_number, 42);
        assert_eq!(args.max_wait, Duration::from_secs(2 * 60 * 60));
        assert_eq!(args.poll_interval, 15);
    }

    #[test]
    fn pr_watch_defaults_to_documented_polling_window() {
        use crate::contexts::bead_workflow::pr_watch::{DEFAULT_MAX_WAIT, DEFAULT_POLL_INTERVAL};

        let cli = Cli::parse_from(["ralph-burning", "pr", "watch", "42"]);
        let Commands::Pr(command) = cli.command else {
            panic!("expected pr command");
        };
        let PrSubcommand::Watch(args) = command.command else {
            panic!("expected watch command");
        };
        assert_eq!(args.max_wait, DEFAULT_MAX_WAIT);
        assert_eq!(
            Duration::from_secs(args.poll_interval),
            DEFAULT_POLL_INTERVAL
        );
    }

    #[test]
    fn pr_watch_rejects_zero_poll_interval() {
        let result =
            Cli::try_parse_from(["ralph-burning", "pr", "watch", "42", "--poll-interval", "0"]);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn pr_watch_does_not_preflight_br_path() {
        let missing_br = std::env::temp_dir().join("ralph-burning-missing-br-for-pr-watch");
        let result = handle(PrCommand {
            br_path: Some(missing_br),
            command: PrSubcommand::Watch(PrWatchArgs {
                pr_number: 42,
                max_wait: Duration::from_secs(0),
                poll_interval: 1,
            }),
        })
        .await;

        assert!(
            !matches!(result, Err(AppError::BrUnavailable { .. })),
            "pr watch must not preflight br because it does not invoke br: {result:?}"
        );
    }
}
