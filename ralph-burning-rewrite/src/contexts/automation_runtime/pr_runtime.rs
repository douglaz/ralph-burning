use std::path::Path;

use serde_json::json;

use crate::adapters::github::GithubPort;
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::automation_runtime::repo_registry::parse_repo_slug;
use crate::contexts::automation_runtime::{
    DaemonStorePort, DaemonTask, DaemonTaskService, WorktreeLease, WorktreePort,
};
use crate::shared::domain::{EffectiveDaemonPrPolicy, PrPolicy};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionPrAction {
    Skipped,
    DraftCreated { url: String },
    ExistingPrRetained { url: String },
    Closed { url: String },
    MarkedReady { url: String },
}

pub struct PrRuntimeService<'a, G> {
    store: &'a dyn DaemonStorePort,
    worktree: &'a dyn WorktreePort,
    github: &'a G,
}

impl<'a, G> PrRuntimeService<'a, G> {
    pub fn new(
        store: &'a dyn DaemonStorePort,
        worktree: &'a dyn WorktreePort,
        github: &'a G,
    ) -> Self {
        Self {
            store,
            worktree,
            github,
        }
    }
}

impl<G> PrRuntimeService<'_, G>
where
    G: GithubPort,
{
    pub async fn ensure_draft_pr(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        lease: &WorktreeLease,
        cancel: &CancellationToken,
    ) -> AppResult<Option<String>> {
        self.ensure_not_cancelled(cancel)?;
        let task = self.store.read_task(base_dir, task_id)?;
        if let Some(url) = task.pr_url.clone() {
            return Ok(Some(url));
        }

        let Some(repo_slug) = task.repo_slug.as_deref() else {
            return Ok(None);
        };
        let (owner, repo) = parse_repo_slug(repo_slug)?;
        let base_branch = self.base_branch_name(repo_root)?;
        let ahead = self
            .github
            .is_branch_ahead(owner, repo, &base_branch, &lease.branch_name)
            .await?;
        if !ahead {
            return Ok(None);
        }

        self.push_and_create_draft(base_dir, repo_root, &task, lease, &base_branch, cancel)
            .await
            .map(Some)
    }

    pub async fn handle_completion_pr(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        lease: &WorktreeLease,
        policy: &EffectiveDaemonPrPolicy,
        cancel: &CancellationToken,
    ) -> AppResult<CompletionPrAction> {
        self.ensure_not_cancelled(cancel)?;
        let task = self.store.read_task(base_dir, task_id)?;
        let Some(repo_slug) = task.repo_slug.as_deref() else {
            return Ok(CompletionPrAction::Skipped);
        };
        let (owner, repo) = parse_repo_slug(repo_slug)?;
        let base_branch = self.base_branch_name(repo_root)?;
        let ahead = self
            .github
            .is_branch_ahead(owner, repo, &base_branch, &lease.branch_name)
            .await?;
        if !ahead {
            return self
                .close_or_skip_pr(base_dir, &task, owner, repo, policy, cancel)
                .await;
        }

        let pr_url = match task.pr_url.clone() {
            Some(url) => url,
            None => {
                return self
                    .push_and_create_draft(
                        base_dir,
                        repo_root,
                        &task,
                        lease,
                        &base_branch,
                        cancel,
                    )
                    .await
                    .map(|url| CompletionPrAction::DraftCreated { url });
            }
        };

        self.ensure_not_cancelled(cancel)?;
        if let Some(pr_number) = parse_pr_number(&pr_url) {
            if let Ok(pr_state) = self.github.fetch_pr_state(owner, repo, pr_number).await {
                if pr_state.state.eq_ignore_ascii_case("open")
                    && pr_state.draft.unwrap_or(false)
                    && self.github.mark_pr_ready(owner, repo, pr_number).await.is_ok()
                {
                    DaemonTaskService::append_journal_event(
                        self.store,
                        base_dir,
                        super::model::DaemonJournalEventType::PrMarkedReady,
                        json!({
                            "task_id": task.task_id,
                            "pr_url": pr_url,
                            "pr_number": pr_number,
                        }),
                    )?;
                    return Ok(CompletionPrAction::MarkedReady { url: pr_url });
                }
            }
        }

        Ok(CompletionPrAction::ExistingPrRetained { url: pr_url })
    }

    pub async fn push_and_create_draft(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
        lease: &WorktreeLease,
        base_branch: &str,
        cancel: &CancellationToken,
    ) -> AppResult<String> {
        let Some(repo_slug) = task.repo_slug.as_deref() else {
            return Err(AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task is missing repo_slug for PR creation".to_owned(),
            });
        };
        let (owner, repo) = parse_repo_slug(repo_slug)?;

        self.ensure_not_cancelled(cancel)?;
        self.worktree
            .push_branch(repo_root, &lease.worktree_path, &lease.branch_name)?;
        self.ensure_not_cancelled(cancel)?;

        let pr = self
            .github
            .create_draft_pr(
                owner,
                repo,
                &build_pr_title(task),
                &build_pr_body(task),
                &lease.branch_name,
                base_branch,
            )
            .await?;

        let mut updated = self.store.read_task(base_dir, &task.task_id)?;
        updated.pr_url = Some(pr.html_url.clone());
        updated.updated_at = chrono::Utc::now();
        self.store.write_task(base_dir, &updated)?;
        DaemonTaskService::append_journal_event(
            self.store,
            base_dir,
            super::model::DaemonJournalEventType::DraftPrCreated,
            json!({
                "task_id": task.task_id,
                "pr_url": pr.html_url,
                "pr_number": pr.number,
                "branch_name": lease.branch_name,
                "base_branch": base_branch,
            }),
        )?;

        Ok(pr.html_url)
    }

    pub async fn close_or_skip_pr(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
        owner: &str,
        repo: &str,
        policy: &EffectiveDaemonPrPolicy,
        cancel: &CancellationToken,
    ) -> AppResult<CompletionPrAction> {
        match (policy.no_diff_action, task.pr_url.as_deref()) {
            (PrPolicy::SkipOnNoDiff, _) | (_, None) => Ok(CompletionPrAction::Skipped),
            (PrPolicy::CloseOnNoDiff, Some(pr_url)) => {
                self.ensure_not_cancelled(cancel)?;
                let Some(pr_number) = parse_pr_number(pr_url) else {
                    return Ok(CompletionPrAction::Skipped);
                };
                let state = self.github.fetch_pr_state(owner, repo, pr_number).await?;
                if state.state.eq_ignore_ascii_case("closed") {
                    return Ok(CompletionPrAction::Skipped);
                }
                self.github.close_pr(owner, repo, pr_number).await?;
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::PrClosed,
                    json!({
                        "task_id": task.task_id,
                        "pr_url": pr_url,
                        "pr_number": pr_number,
                        "reason": "no_diff_from_base",
                    }),
                )?;
                Ok(CompletionPrAction::Closed {
                    url: pr_url.to_owned(),
                })
            }
        }
    }

    fn ensure_not_cancelled(&self, cancel: &CancellationToken) -> AppResult<()> {
        if cancel.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: "github".to_owned(),
                contract_id: "daemon.pr_runtime".to_owned(),
            });
        }
        Ok(())
    }

    fn base_branch_name(&self, repo_root: &Path) -> AppResult<String> {
        let raw = self.worktree.default_branch_name(repo_root)?;
        Ok(raw
            .trim()
            .trim_start_matches("origin/")
            .trim_start_matches("refs/heads/")
            .to_owned())
    }
}

fn build_pr_title(task: &DaemonTask) -> String {
    match (task.issue_number, task.project_name.as_deref()) {
        (Some(issue_number), Some(name)) => format!("#{issue_number} {name}"),
        (Some(issue_number), None) => format!("#{issue_number} {}", task.project_id),
        (None, Some(name)) => name.to_owned(),
        (None, None) => format!("Automated task {}", task.task_id),
    }
}

fn build_pr_body(task: &DaemonTask) -> String {
    let mut body = format!("Automated change for `{}`.", task.project_id);
    if let Some(issue_number) = task.issue_number {
        body.push_str(&format!("\n\nCloses #{}.", issue_number));
    } else if !task.issue_ref.trim().is_empty() {
        body.push_str(&format!("\n\nSource issue: `{}`.", task.issue_ref));
    }
    body
}

fn parse_pr_number(pr_url: &str) -> Option<u64> {
    let candidate = pr_url
        .trim()
        .trim_end_matches('/')
        .rsplit('/')
        .next()?;
    candidate.parse::<u64>().ok()
}
