use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::adapters::github::GithubPort;
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::automation_runtime::repo_registry::parse_repo_slug;
use crate::contexts::automation_runtime::{
    DaemonStorePort, DaemonTask, DaemonTaskService, TaskStatus, WorktreeLease, WorktreePort,
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
        self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        let mut task = self.store.read_task(base_dir, task_id)?;
        let Some(repo_slug) = task.repo_slug.clone() else {
            return Ok(None);
        };
        let (owner, repo) = parse_repo_slug(&repo_slug)?;
        self.refresh_closed_pr_reference(base_dir, &mut task, owner, repo, cancel)
            .await?;
        if let Some(url) = task.pr_url.clone() {
            return Ok(Some(url));
        }
        let base_branch = self.base_branch_name(repo_root)?;
        self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        self.worktree
            .force_push_branch(repo_root, &lease.worktree_path, &lease.branch_name)?;
        self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        let ahead = self
            .github
            .is_branch_ahead(owner, repo, &base_branch, &lease.branch_name)
            .await?;
        if !ahead {
            return Ok(None);
        }

        self.create_draft_after_push(base_dir, &task, lease, &base_branch, cancel)
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
        self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        let mut task = self.store.read_task(base_dir, task_id)?;
        let Some(repo_slug) = task.repo_slug.clone() else {
            return Ok(CompletionPrAction::Skipped);
        };
        let (owner, repo) = parse_repo_slug(&repo_slug)?;
        self.refresh_closed_pr_reference(base_dir, &mut task, owner, repo, cancel)
            .await?;
        let base_branch = self.base_branch_name(repo_root)?;
        if task.pr_url.is_none() {
            self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
            self.worktree
                .force_push_branch(repo_root, &lease.worktree_path, &lease.branch_name)?;
            self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        }
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
                    .create_draft_after_push(base_dir, &task, lease, &base_branch, cancel)
                    .await
                    .map(|url| CompletionPrAction::DraftCreated { url });
            }
        };

        self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
        if let Some(pr_number) = parse_pr_number(&pr_url) {
            if let Ok(pr_state) = self.github.fetch_pr_state(owner, repo, pr_number).await {
                self.ensure_task_not_cancelled(base_dir, task_id, cancel)?;
                if pr_state.state.eq_ignore_ascii_case("open") && pr_state.draft.unwrap_or(false) {
                    self.github
                        .mark_pr_ready(owner, repo, pr_number)
                        .await
                        .map_err(|e| AppError::BackendUnavailable {
                            backend: "github".to_owned(),
                            details: format!("failed to promote draft PR #{pr_number}: {e}"),
                        })?;
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
        self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
        self.worktree
            .force_push_branch(repo_root, &lease.worktree_path, &lease.branch_name)?;
        self.create_draft_after_push(base_dir, task, lease, base_branch, cancel)
            .await
    }

    async fn create_draft_after_push(
        &self,
        base_dir: &Path,
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

        self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
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
                self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
                let Some(pr_number) = parse_pr_number(pr_url) else {
                    return Ok(CompletionPrAction::Skipped);
                };
                let state = self.github.fetch_pr_state(owner, repo, pr_number).await?;
                self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
                if state.state.eq_ignore_ascii_case("closed") {
                    self.clear_task_pr_url(base_dir, &task.task_id)?;
                    return Ok(CompletionPrAction::Skipped);
                }
                self.github.close_pr(owner, repo, pr_number).await?;
                self.clear_task_pr_url(base_dir, &task.task_id)?;
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

    fn ensure_task_not_cancelled(
        &self,
        base_dir: &Path,
        task_id: &str,
        cancel: &CancellationToken,
    ) -> AppResult<()> {
        self.ensure_not_cancelled(cancel)?;
        if self.store.read_task(base_dir, task_id)?.status == TaskStatus::Aborted {
            return Err(AppError::InvocationCancelled {
                backend: "github".to_owned(),
                contract_id: "daemon.pr_runtime".to_owned(),
            });
        }
        Ok(())
    }

    async fn refresh_closed_pr_reference(
        &self,
        base_dir: &Path,
        task: &mut DaemonTask,
        owner: &str,
        repo: &str,
        cancel: &CancellationToken,
    ) -> AppResult<()> {
        let Some(pr_url) = task.pr_url.clone() else {
            return Ok(());
        };
        let Some(pr_number) = parse_pr_number(&pr_url) else {
            return Ok(());
        };
        self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
        if let Ok(state) = self.github.fetch_pr_state(owner, repo, pr_number).await {
            self.ensure_task_not_cancelled(base_dir, &task.task_id, cancel)?;
            if state.state.eq_ignore_ascii_case("closed") {
                self.clear_task_pr_url(base_dir, &task.task_id)?;
                task.pr_url = None;
            }
        }
        Ok(())
    }

    fn clear_task_pr_url(&self, base_dir: &Path, task_id: &str) -> AppResult<()> {
        let mut updated = self.store.read_task(base_dir, task_id)?;
        if updated.pr_url.is_none() {
            return Ok(());
        }
        updated.pr_url = None;
        updated.updated_at = Utc::now();
        self.store.write_task(base_dir, &updated)
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
    let candidate = pr_url.trim().trim_end_matches('/').rsplit('/').next()?;
    candidate.parse::<u64>().ok()
}
