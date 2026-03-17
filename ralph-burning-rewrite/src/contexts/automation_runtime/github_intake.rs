//! GitHub intake: polls candidate issues from registered repos, extracts
//! commands, manages label transitions, and maps candidates to daemon tasks.

use std::path::Path;

use crate::adapters::github::{GithubIssue, GithubPort};
use crate::shared::domain::FlowPreset;
use crate::shared::error::AppResult;

use super::model::{DaemonTask, GithubTaskMeta, WatchedIssueMeta};
use super::repo_registry::{label_for_status, RepoRegistration, LABEL_VOCABULARY};
use super::routing::RoutingEngine;
use super::task_service::DaemonTaskService;
use super::watcher;
use super::DaemonStorePort;

/// Extract an explicit command from issue comments or body.
///
/// Recognizes:
/// - `/rb flow <preset>`
/// - `/rb requirements`
/// - `/rb run`
/// - `/rb retry`
/// - `/rb abort`
pub fn extract_command(body: &str, comments: &[String]) -> Option<String> {
    // Check comments last-to-first (most recent command wins)
    for comment in comments.iter().rev() {
        if let Some(cmd) = find_rb_command(comment) {
            return Some(cmd);
        }
    }
    // Fall back to issue body
    find_rb_command(body)
}

fn find_rb_command(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim();
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.len() >= 2 && tokens[0] == "/rb" {
            match tokens[1] {
                "flow" if tokens.len() >= 3 => {
                    return Some(trimmed.to_owned());
                }
                // Accept both `/rb requirements` (bare) and `/rb requirements draft|quick`
                "requirements" => {
                    return Some(trimmed.to_owned());
                }
                "run" | "retry" | "abort" => {
                    return Some(trimmed.to_owned());
                }
                _ => {}
            }
        }
    }
    None
}

/// Convert a GitHub issue into a `WatchedIssueMeta` for ingestion.
pub fn issue_to_watched_meta(
    issue: &GithubIssue,
    repo_slug: &str,
    routing_command: Option<String>,
) -> WatchedIssueMeta {
    let issue_ref = format!("{repo_slug}#{}", issue.number);
    WatchedIssueMeta {
        issue_ref,
        source_revision: issue.updated_at.clone(),
        title: issue.title.clone(),
        body: issue.body.clone().unwrap_or_default(),
        labels: issue.labels.iter().map(|l| l.name.clone()).collect(),
        routing_command,
    }
}

/// Build GitHub-specific task metadata from an issue.
pub fn build_github_meta(
    repo_slug: &str,
    issue: &GithubIssue,
) -> GithubTaskMeta {
    GithubTaskMeta {
        repo_slug: repo_slug.to_owned(),
        issue_number: issue.number,
        issue_ref: format!("{repo_slug}#{}", issue.number),
        pr_url: None,
        last_seen_comment_id: None,
        last_seen_review_id: None,
    }
}

/// Poll a single registered repo for candidate issues and ingest them as
/// daemon tasks. Returns the number of newly created tasks.
///
/// A GitHub API failure (polling, comments, labels) for this repo propagates
/// as an `Err` so the caller can skip further mutations for this repo in the
/// current cycle (multi-repo failure isolation).
pub async fn poll_and_ingest_repo<G: GithubPort>(
    github: &G,
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    registration: &RepoRegistration,
    routing_engine: &RoutingEngine,
    default_flow: FlowPreset,
) -> AppResult<u32> {
    let (owner, repo) = super::repo_registry::parse_repo_slug(&registration.repo_slug)?;

    // Poll for issues labeled `rb:ready`
    let issues = github.poll_candidate_issues(owner, repo, "rb:ready").await
        .map_err(|e| {
            eprintln!(
                "github-intake: failed to poll {}: {e}",
                registration.repo_slug
            );
            e
        })?;

    let mut created = 0u32;
    for issue in &issues {
        // 1. Fetch comments for this issue — failure stops this repo for the cycle
        let comments = github.fetch_issue_comments(owner, repo, issue.number).await
            .map_err(|e| {
                eprintln!(
                    "github-intake: failed to fetch comments for {}#{}: {e}",
                    registration.repo_slug, issue.number
                );
                e
            })?
            .into_iter()
            .map(|c| c.body)
            .collect::<Vec<_>>();

        // 2. Extract command from body + comments
        let routing_command = extract_command(
            issue.body.as_deref().unwrap_or(""),
            &comments,
        );

        // 3. Handle daemon commands inline
        if let Some(ref cmd) = routing_command {
            if cmd.trim() == "/rb retry" || cmd.trim() == "/rb abort" {
                // Handle as operation on existing task
                let issue_number = issue.number;
                let repo_slug = &registration.repo_slug;

                if cmd.trim() == "/rb retry" {
                    if let Ok(Some(task)) = DaemonTaskService::find_task_by_issue(
                        store, base_dir, repo_slug, issue_number,
                    ) {
                        if task.status == super::model::TaskStatus::Failed {
                            let _ = DaemonTaskService::retry_task(
                                store, base_dir, &task.task_id,
                            );
                        }
                    }
                } else {
                    // /rb abort
                    if let Ok(Some(task)) = DaemonTaskService::find_task_by_issue(
                        store, base_dir, repo_slug, issue_number,
                    ) {
                        if !task.status.is_terminal() {
                            let _ = DaemonTaskService::mark_aborted(
                                store, base_dir, &task.task_id,
                            );
                        }
                    }
                }
                // Remove rb:ready label so it doesn't keep being polled
                let _ = github.remove_label(owner, repo, issue_number, "rb:ready").await;
                continue; // Don't create a new task
            }
        }

        // For /rb run (and any other daemon command), clear the routing_command
        // so flow resolution uses labels/default routing instead
        let routing_command_for_task =
            routing_command.filter(|cmd| !RoutingEngine::is_daemon_command(cmd));

        let meta = issue_to_watched_meta(issue, &registration.repo_slug, routing_command_for_task);
        let dispatch_mode = match watcher::resolve_dispatch_mode(&meta) {
            Ok(mode) => mode,
            Err(e) => {
                eprintln!(
                    "github-intake: skipping {}#{}: {e}",
                    registration.repo_slug, issue.number
                );
                continue;
            }
        };

        match DaemonTaskService::create_task_from_watched_issue(
            store,
            base_dir,
            routing_engine,
            default_flow,
            &meta,
            dispatch_mode,
        ) {
            Ok(Some(mut task)) => {
                // Attach GitHub metadata to the task
                let github_meta = build_github_meta(&registration.repo_slug, issue);
                task.repo_slug = Some(github_meta.repo_slug);
                task.issue_number = Some(github_meta.issue_number);
                task.pr_url = github_meta.pr_url;
                task.last_seen_comment_id = github_meta.last_seen_comment_id;
                task.last_seen_review_id = github_meta.last_seen_review_id;
                store.write_task(base_dir, &task)?;

                // Transition label from rb:ready to rb:in-progress
                // Partial label failure is tolerated — task record exists, dedup prevents re-creation
                if let Err(e) = github.remove_label(owner, repo, issue.number, "rb:ready").await {
                    eprintln!(
                        "github-intake: warning: failed to remove rb:ready from {}#{}: {e}",
                        registration.repo_slug, issue.number
                    );
                }
                // We don't add rb:in-progress yet — the task is Pending, not Claimed.
                // The daemon loop will add it when the task is claimed.

                created += 1;
            }
            Ok(None) => {
                // Idempotent no-op
            }
            Err(e) => {
                eprintln!(
                    "github-intake: failed to ingest {}#{}: {e}",
                    registration.repo_slug, issue.number
                );
            }
        }
    }

    Ok(created)
}

/// Update the GitHub label on an issue to match the task's durable status.
pub async fn sync_label_for_task<G: GithubPort>(
    github: &G,
    task: &DaemonTask,
) -> AppResult<()> {
    let Some(ref repo_slug) = task.repo_slug else {
        return Ok(());
    };
    let Some(issue_number) = task.issue_number else {
        return Ok(());
    };
    let (owner, repo) = super::repo_registry::parse_repo_slug(repo_slug)?;

    let target_label = label_for_status(&task.status);

    // Remove all rb: status labels, then add the correct one
    let status_labels = ["rb:ready", "rb:in-progress", "rb:failed", "rb:completed", "rb:waiting-feedback"];
    for label in &status_labels {
        let _ = github.remove_label(owner, repo, issue_number, label).await;
    }
    if let Some(label) = target_label {
        let _ = github.add_label(owner, repo, issue_number, label).await;
    }

    Ok(())
}

/// Ensure the label vocabulary exists on all registered repos.
pub async fn ensure_labels_on_repos<G: GithubPort>(
    github: &G,
    registrations: &[RepoRegistration],
) -> AppResult<()> {
    for reg in registrations {
        let (owner, repo) = super::repo_registry::parse_repo_slug(&reg.repo_slug)?;
        github.ensure_labels(owner, repo, LABEL_VOCABULARY).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_flow_command() {
        assert_eq!(
            extract_command("/rb flow standard", &[]),
            Some("/rb flow standard".to_owned())
        );
    }

    #[test]
    fn extract_run_command() {
        assert_eq!(
            extract_command("some text\n/rb run\nmore text", &[]),
            Some("/rb run".to_owned())
        );
    }

    #[test]
    fn extract_command_from_comments() {
        let comments = vec!["looks good".to_owned(), "/rb retry".to_owned()];
        assert_eq!(
            extract_command("issue body", &comments),
            Some("/rb retry".to_owned())
        );
    }

    #[test]
    fn extract_bare_requirements_command() {
        assert_eq!(
            extract_command("/rb requirements", &[]),
            Some("/rb requirements".to_owned())
        );
    }

    #[test]
    fn extract_requirements_with_subcommand() {
        assert_eq!(
            extract_command("/rb requirements draft", &[]),
            Some("/rb requirements draft".to_owned())
        );
    }

    #[test]
    fn extract_no_command() {
        assert_eq!(extract_command("no commands here", &[]), None);
    }

    #[test]
    fn issue_to_meta_format() {
        let issue = GithubIssue {
            number: 42,
            title: "Fix bug".to_owned(),
            body: Some("Bug description".to_owned()),
            labels: vec![crate::adapters::github::GithubLabel {
                name: "rb:ready".to_owned(),
            }],
            user: crate::adapters::github::GithubUser {
                login: "user".to_owned(),
                id: 1,
            },
            html_url: "https://github.com/acme/widgets/issues/42".to_owned(),
            pull_request: None,
            updated_at: "2026-03-17T00:00:00Z".to_owned(),
        };

        let meta = issue_to_watched_meta(&issue, "acme/widgets", None);
        assert_eq!(meta.issue_ref, "acme/widgets#42");
        assert_eq!(meta.title, "Fix bug");
        assert_eq!(meta.body, "Bug description");
    }
}
