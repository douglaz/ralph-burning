use std::path::Path;

use crate::shared::error::{AppError, AppResult};

use super::model::{DispatchMode, WatchedIssueMeta};
use super::routing::RoutingEngine;
use crate::shared::domain::FlowPreset;

/// Port trait for watching external issue sources.
///
/// Implementations poll an external source (e.g. GitHub issues, files) and
/// return candidate issues that should be ingested as daemon tasks.
pub trait IssueWatcherPort {
    /// Poll the external source and return watched issue candidates.
    fn poll(&self, base_dir: &Path) -> AppResult<Vec<WatchedIssueMeta>>;
}

/// Parse a requirements command from issue body or routing command.
///
/// Accepts:
/// - `/rb requirements draft`
/// - `/rb requirements quick`
///
/// Returns `None` if the text does not contain a requirements command,
/// allowing the caller to default to `DispatchMode::Workflow`.
///
/// Returns `Err` for malformed requirements commands (e.g. `/rb requirements`
/// without a subcommand, unknown subcommands, or extra tokens).
pub fn parse_requirements_command(text: &str) -> AppResult<Option<DispatchMode>> {
    for line in text.lines() {
        let trimmed = line.trim();
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.len() >= 2
            && matches!(tokens[0], "/rb" | "rb")
            && tokens[1] == "requirements"
        {
            if tokens.len() == 3 {
                return match tokens[2] {
                    "draft" => Ok(Some(DispatchMode::RequirementsDraft)),
                    "quick" => Ok(Some(DispatchMode::RequirementsQuick)),
                    other => Err(AppError::WatcherIngestionFailed {
                        issue_ref: trimmed.to_owned(),
                        details: format!(
                            "unknown requirements subcommand '{}'; expected 'draft' or 'quick'",
                            other
                        ),
                    }),
                };
            }
            // Malformed: missing subcommand or extra tokens
            return Err(AppError::WatcherIngestionFailed {
                issue_ref: trimmed.to_owned(),
                details: "malformed requirements command; expected '/rb requirements draft' or '/rb requirements quick'".to_owned(),
            });
        }
    }
    Ok(None)
}

/// Check if a routing command string is a requirements command (not a flow command).
/// Used to prevent requirements commands from being passed to flow routing.
pub fn is_requirements_command(text: &str) -> bool {
    let tokens: Vec<&str> = text.trim().split_whitespace().collect();
    tokens.len() >= 2
        && matches!(tokens[0], "/rb" | "rb")
        && tokens[1] == "requirements"
}

/// Resolve the dispatch mode for a watched issue.
///
/// Checks the routing command first, then the issue body for `/rb requirements ...`.
/// Falls back to `DispatchMode::Workflow` if no requirements command is found.
pub fn resolve_dispatch_mode(issue: &WatchedIssueMeta) -> AppResult<DispatchMode> {
    // Check routing command first
    if let Some(ref cmd) = issue.routing_command {
        if let Some(mode) = parse_requirements_command(cmd)? {
            return Ok(mode);
        }
    }
    // Check issue body
    if let Some(mode) = parse_requirements_command(&issue.body)? {
        return Ok(mode);
    }
    Ok(DispatchMode::Workflow)
}

/// Resolve flow from watched issue metadata using the routing engine.
pub fn resolve_watched_issue_flow(
    issue: &WatchedIssueMeta,
    routing_engine: &RoutingEngine,
    default_flow: FlowPreset,
) -> AppResult<super::model::RoutingResolution> {
    routing_engine.resolve_flow(
        issue.routing_command.as_deref(),
        &issue.labels,
        default_flow,
    )
}
