//! GitHub intake: polls candidate issues from registered repos, extracts
//! commands, manages label transitions, and maps candidates to daemon tasks.

use std::path::Path;

use crate::adapters::github::{GithubIssue, GithubPort};
use crate::shared::domain::FlowPreset;
use crate::shared::error::AppResult;

use super::lease_service::{LeaseService, ReleaseMode};
use super::model::{DaemonTask, DispatchMode, GithubTaskMeta, TaskStatus, WatchedIssueMeta};
use super::repo_registry::{label_for_status, RepoRegistration, LABEL_VOCABULARY};
use super::routing::RoutingEngine;
use super::task_service::DaemonTaskService;
use super::watcher;
use super::{DaemonStorePort, WorktreePort};

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

/// Build GitHub-specific task metadata from an issue, capturing the dedup
/// cursor from the fetched comments so later cycles and slice-9 review
/// ingestion can deduplicate from persisted task state.
pub fn build_github_meta(
    repo_slug: &str,
    issue: &GithubIssue,
    comments: &[crate::adapters::github::GithubComment],
) -> GithubTaskMeta {
    // Compute the maximum comment ID as the dedup cursor. Subsequent
    // polling cycles only need to inspect comments with ID > this value.
    let max_comment_id = comments.iter().map(|c| c.id).max();

    GithubTaskMeta {
        repo_slug: repo_slug.to_owned(),
        issue_number: issue.number,
        issue_ref: format!("{repo_slug}#{}", issue.number),
        pr_url: None,
        last_seen_comment_id: max_comment_id,
        // Review IDs are populated by slice-9 PR review ingestion.
        last_seen_review_id: None,
    }
}

/// Poll a single registered repo for candidate issues and ingest them as
/// daemon tasks. Returns the number of newly created tasks.
///
/// Polls `rb:ready` for new issue intake AND `rb:in-progress`, `rb:failed`,
/// and `rb:waiting-feedback` for explicit commands (`/rb retry`, `/rb abort`).
/// This ensures that commands on non-ready issues are observed regardless of
/// the issue's current status label.
///
/// A GitHub API failure (polling, comments, labels) for this repo propagates
/// as an `Err` so the caller can skip further mutations for this repo in the
/// current cycle (multi-repo failure isolation).
pub async fn poll_and_ingest_repo<G: GithubPort>(
    github: &G,
    store: &dyn DaemonStorePort,
    worktree: &dyn WorktreePort,
    base_dir: &Path,
    registration: &RepoRegistration,
    routing_engine: &RoutingEngine,
    default_flow: FlowPreset,
) -> AppResult<u32> {
    let (owner, repo) = super::repo_registry::parse_repo_slug(&registration.repo_slug)?;

    // Phase A: Poll for explicit commands on non-ready issues (rb:in-progress, rb:failed,
    // rb:waiting-feedback). These issues already have tasks; we only need to check for
    // /rb retry or /rb abort.
    for command_label in &["rb:in-progress", "rb:failed", "rb:waiting-feedback"] {
        let command_issues = github
            .poll_candidate_issues(owner, repo, command_label)
            .await
            .map_err(|e| {
                eprintln!(
                    "github-intake: failed to poll {} for {}: {e}",
                    command_label, registration.repo_slug
                );
                e
            })?;

        for issue in &command_issues {
            let raw_comments = github
                .fetch_issue_comments(owner, repo, issue.number)
                .await
                .map_err(|e| {
                    eprintln!(
                        "github-intake: failed to fetch comments for {}#{}: {e}",
                        registration.repo_slug, issue.number
                    );
                    e
                })?;
            // Only consider comments newer than the stored cursor to avoid
            // replaying already-processed /rb retry and /rb abort commands.
            let last_seen = DaemonTaskService::find_task_by_issue(
                store,
                base_dir,
                &registration.repo_slug,
                issue.number,
            )
            .ok()
            .flatten()
            .and_then(|t| t.last_seen_comment_id)
            .unwrap_or(0);
            let new_comment_bodies: Vec<String> = raw_comments
                .iter()
                .filter(|c| c.id > last_seen)
                .map(|c| c.body.clone())
                .collect();

            let routing_command =
                extract_command(issue.body.as_deref().unwrap_or(""), &new_comment_bodies);

            let max_comment_id = raw_comments.iter().map(|c| c.id).max();

            if let Some(ref cmd) = routing_command {
                let cmd_trimmed = cmd.trim();
                if cmd_trimmed == "/rb retry" || cmd_trimmed == "/rb abort" {
                    handle_explicit_command(
                        github,
                        store,
                        worktree,
                        base_dir,
                        registration,
                        issue,
                        cmd_trimmed,
                    )
                    .await?;
                }
            }

            // Advance cursor only after command handling succeeds — if the
            // command fails, we want to retry it on the next poll cycle.
            update_task_cursor(
                store,
                base_dir,
                &registration.repo_slug,
                issue.number,
                max_comment_id,
            );
        }
    }

    // Phase B: Poll for new issues labeled `rb:ready` — standard intake path.
    let issues = github
        .poll_candidate_issues(owner, repo, "rb:ready")
        .await
        .map_err(|e| {
            eprintln!(
                "github-intake: failed to poll {}: {e}",
                registration.repo_slug
            );
            e
        })?;

    let mut created = 0u32;
    for issue in &issues {
        // 1. Fetch comments for this issue — failure stops this repo for the cycle.
        //    Keep the full GithubComment objects so we can extract dedup cursors.
        let raw_comments = github
            .fetch_issue_comments(owner, repo, issue.number)
            .await
            .map_err(|e| {
                eprintln!(
                    "github-intake: failed to fetch comments for {}#{}: {e}",
                    registration.repo_slug, issue.number
                );
                e
            })?;
        let comment_bodies: Vec<String> = raw_comments.iter().map(|c| c.body.clone()).collect();

        // 2. Extract command from body + comments
        let routing_command = extract_command(issue.body.as_deref().unwrap_or(""), &comment_bodies);

        // 3. Handle daemon commands (/rb retry, /rb abort) on rb:ready issues too
        if let Some(ref cmd) = routing_command {
            let cmd_trimmed = cmd.trim();
            if cmd_trimmed == "/rb retry" || cmd_trimmed == "/rb abort" {
                handle_explicit_command(
                    github,
                    store,
                    worktree,
                    base_dir,
                    registration,
                    issue,
                    cmd_trimmed,
                )
                .await?;
                continue; // Don't create a new task
            }
        }

        // Check whether the extracted explicit command is `/rb run` BEFORE
        // clearing it for flow routing.  `/rb run` means "run the workflow
        // now" and must force `DispatchMode::Workflow`, overriding any stale
        // `/rb requirements` text that may still be present in the issue body.
        let explicit_run = routing_command
            .as_deref()
            .map(|cmd| cmd.trim() == "/rb run")
            .unwrap_or(false);

        // For /rb run (and any other daemon command), clear the routing_command
        // so flow resolution uses labels/default routing instead
        let routing_command_for_task =
            routing_command.filter(|cmd| !RoutingEngine::is_daemon_command(cmd));

        let meta = issue_to_watched_meta(issue, &registration.repo_slug, routing_command_for_task);

        // If `/rb run` was the extracted explicit command, force workflow
        // dispatch.  Otherwise, resolve from the routing command / body as
        // usual.  This ensures a newer `/rb run` comment always overrides
        // stale `/rb requirements` text in the issue body.
        let dispatch_mode = if explicit_run {
            DispatchMode::Workflow
        } else {
            match watcher::resolve_dispatch_mode(&meta) {
                Ok(mode) => mode,
                Err(e) => {
                    eprintln!(
                        "github-intake: skipping {}#{}: {e}",
                        registration.repo_slug, issue.number
                    );
                    continue;
                }
            }
        };

        // Build GitHub metadata BEFORE task creation so it is persisted
        // atomically with the initial task record. This prevents a window
        // where a live task exists without repo_slug/issue_number if the
        // second write were to fail.
        let github_meta = build_github_meta(&registration.repo_slug, issue, &raw_comments);

        match DaemonTaskService::create_task_from_watched_issue(
            store,
            base_dir,
            routing_engine,
            default_flow,
            &meta,
            dispatch_mode,
            Some(&github_meta),
        ) {
            Ok(Some(_task)) => {
                // Task is Pending → label stays rb:ready (matches label_for_status).
                // The daemon loop calls sync_label_for_task when the task transitions
                // to Claimed (rb:in-progress). We do NOT remove rb:ready here because
                // Pending maps to rb:ready and the label must reflect durable state.

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

/// Handle an explicit `/rb retry` or `/rb abort` command on an issue,
/// mutate the durable task state, clean up retained leases/worktrees,
/// and reconcile the GitHub label.
async fn handle_explicit_command<G: GithubPort>(
    github: &G,
    store: &dyn DaemonStorePort,
    worktree: &dyn WorktreePort,
    base_dir: &Path,
    registration: &RepoRegistration,
    issue: &GithubIssue,
    cmd: &str,
) -> AppResult<()> {
    let issue_number = issue.number;
    let repo_slug = &registration.repo_slug;

    let task =
        match DaemonTaskService::find_task_by_issue(store, base_dir, repo_slug, issue_number)? {
            Some(t) => t,
            None => return Ok(()), // No matching task — ignore
        };

    if cmd == "/rb retry" {
        if task.status == TaskStatus::Failed || task.status == TaskStatus::Aborted {
            // If the task retains a lease from partial cleanup, attempt
            // cleanup before retry so retry_task() doesn't reject it.
            if let Some(ref lid) = task.lease_id {
                if let Ok(lease) = store.read_lease(base_dir, lid) {
                    let result = LeaseService::release(
                        store,
                        worktree,
                        base_dir,
                        &registration.repo_root,
                        &lease,
                        ReleaseMode::Idempotent,
                    );
                    if let Ok(ref r) = result {
                        if r.resources_released {
                            let _ = DaemonTaskService::clear_lease_reference(
                                store,
                                base_dir,
                                &task.task_id,
                            );
                        }
                    }
                    // If cleanup failed, retry_task() will reject with
                    // LeaseCleanupPartialFailure — caller propagates the error.
                }
            }
            let retried = DaemonTaskService::retry_task(store, base_dir, &task.task_id)?;
            // Reconcile label: retried task is Pending → rb:ready
            // Mark label_dirty on failure so reconcile can repair.
            if let Err(e) = sync_label_for_task(github, &retried).await {
                let _ = DaemonTaskService::mark_label_dirty(store, base_dir, &retried.task_id);
                return Err(e);
            }
        }
    } else if cmd == "/rb abort" {
        if !task.status.is_terminal() {
            let original_status = task.status;
            DaemonTaskService::mark_aborted(store, base_dir, &task.task_id)?;

            // Clean up lease/worktree for Claimed/Active tasks (same as
            // the CLI abort path) to prevent stranding live resources.
            if matches!(original_status, TaskStatus::Claimed | TaskStatus::Active) {
                if let Some(ref lid) = task.lease_id {
                    if let Ok(lease) = store.read_lease(base_dir, lid) {
                        let result = LeaseService::release(
                            store,
                            worktree,
                            base_dir,
                            &registration.repo_root,
                            &lease,
                            ReleaseMode::Idempotent,
                        );
                        match result {
                            Ok(ref r) if r.resources_released => {
                                let _ = DaemonTaskService::clear_lease_reference(
                                    store,
                                    base_dir,
                                    &task.task_id,
                                );
                            }
                            _ => {
                                // Partial cleanup: lease reference preserved for
                                // reconcile. This is not fatal — the task is already
                                // aborted and the resources remain discoverable.
                            }
                        }
                    }
                }
            }

            let aborted = store.read_task(base_dir, &task.task_id)?;
            // Reconcile label: Aborted → rb:failed
            // Mark label_dirty on failure so reconcile can repair.
            if let Err(e) = sync_label_for_task(github, &aborted).await {
                let _ = DaemonTaskService::mark_label_dirty(store, base_dir, &aborted.task_id);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Best-effort update of the dedup comment cursor on an existing task.
/// Called during each polling cycle so later cycles and slice-9 review
/// ingestion can skip already-processed comments.
fn update_task_cursor(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    repo_slug: &str,
    issue_number: u64,
    max_comment_id: Option<u64>,
) {
    if max_comment_id.is_none() {
        return; // No comments — nothing to update
    }
    if let Ok(Some(mut task)) =
        DaemonTaskService::find_task_by_issue(store, base_dir, repo_slug, issue_number)
    {
        let current = task.last_seen_comment_id.unwrap_or(0);
        if let Some(new_max) = max_comment_id {
            if new_max > current {
                task.last_seen_comment_id = Some(new_max);
                let _ = store.write_task(base_dir, &task);
            }
        }
    }
}

/// Update the GitHub label on an issue to match the task's durable status.
pub async fn sync_label_for_task<G: GithubPort>(github: &G, task: &DaemonTask) -> AppResult<()> {
    let Some(ref repo_slug) = task.repo_slug else {
        return Ok(());
    };
    let Some(issue_number) = task.issue_number else {
        return Ok(());
    };
    let (owner, repo) = super::repo_registry::parse_repo_slug(repo_slug)?;

    let target_label = label_for_status(&task.status);

    // Add the target label FIRST, then remove stale labels.
    // This ordering prevents the "no status label" stranded-task state:
    // if the add succeeds but a remove fails, the issue has the correct
    // label plus possibly an extra stale one (benign).  If the add fails,
    // the old label(s) are still present and the issue remains visible to
    // polling.
    let status_labels = [
        "rb:ready",
        "rb:in-progress",
        "rb:failed",
        "rb:completed",
        "rb:waiting-feedback",
    ];
    if let Some(label) = target_label {
        github.add_label(owner, repo, issue_number, label).await?;
    }
    for &label in &status_labels {
        // Skip removal of the label we just added
        if Some(label) == target_label {
            continue;
        }
        if let Err(e) = github.remove_label(owner, repo, issue_number, label).await {
            // 404 (label not present) is not a real failure — only propagate others
            let msg = e.to_string();
            if !msg.contains("404") && !msg.contains("not found") {
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Ensure the label vocabulary exists on all registered repos.
/// Returns the set of repos where label ensure succeeded. Repos where label
/// ensure fails are quarantined (logged and excluded) rather than blocking
/// the entire daemon startup.
pub async fn ensure_labels_on_repos<G: GithubPort>(
    github: &G,
    registrations: &[RepoRegistration],
) -> Vec<RepoRegistration> {
    let mut valid = Vec::new();
    for reg in registrations {
        match super::repo_registry::parse_repo_slug(&reg.repo_slug) {
            Ok((owner, repo)) => match github.ensure_labels(owner, repo, LABEL_VOCABULARY).await {
                Ok(()) => valid.push(reg.clone()),
                Err(e) => {
                    eprintln!(
                        "daemon: quarantining repo '{}': failed to ensure labels: {e}",
                        reg.repo_slug
                    );
                }
            },
            Err(e) => {
                eprintln!(
                    "daemon: quarantining repo '{}': invalid slug: {e}",
                    reg.repo_slug
                );
            }
        }
    }
    valid
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
