use std::collections::HashSet;
use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::adapters::github::{GithubComment, GithubPort, GithubReview};
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::automation_runtime::repo_registry::parse_repo_slug;
use crate::contexts::automation_runtime::{
    DaemonStorePort, DaemonTask, DaemonTaskService, ReviewWhitelist, TaskStatus,
};
use crate::contexts::project_run_record::model::QueuedAmendment;
use crate::contexts::project_run_record::service::{
    self as record_service, AmendmentQueuePort, JournalStorePort, ProjectStorePort,
    RunSnapshotPort, RunSnapshotWritePort,
};
use crate::shared::domain::{ProjectId, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestedReviewBatch {
    pub staged_count: usize,
    pub reopened_project: bool,
    pub last_seen_comment_id: Option<u64>,
    pub last_seen_review_id: Option<u64>,
}

enum ReviewItem {
    IssueComment(GithubComment),
    PullComment(GithubComment),
    Review(GithubReview),
}

impl ReviewItem {
    fn key(&self) -> String {
        match self {
            Self::IssueComment(comment) => format!("issue_comment:{}", comment.id),
            Self::PullComment(comment) => format!("pull_comment:{}", comment.id),
            Self::Review(review) => format!("review:{}", review.id),
        }
    }

    fn author(&self) -> &str {
        match self {
            Self::IssueComment(comment) | Self::PullComment(comment) => &comment.user.login,
            Self::Review(review) => &review.user.login,
        }
    }

    fn body(&self) -> &str {
        match self {
            Self::IssueComment(comment) | Self::PullComment(comment) => &comment.body,
            Self::Review(review) => review.body.as_deref().unwrap_or(""),
        }
    }
}

pub struct PrReviewIngestionService<'a, G> {
    store: &'a dyn DaemonStorePort,
    project_store: &'a dyn ProjectStorePort,
    run_snapshot_read: &'a dyn RunSnapshotPort,
    run_snapshot_write: &'a dyn RunSnapshotWritePort,
    amendment_queue: &'a dyn AmendmentQueuePort,
    journal_store: &'a dyn JournalStorePort,
    github: &'a G,
}

impl<'a, G> PrReviewIngestionService<'a, G> {
    pub fn new(
        store: &'a dyn DaemonStorePort,
        project_store: &'a dyn ProjectStorePort,
        run_snapshot_read: &'a dyn RunSnapshotPort,
        run_snapshot_write: &'a dyn RunSnapshotWritePort,
        amendment_queue: &'a dyn AmendmentQueuePort,
        journal_store: &'a dyn JournalStorePort,
        github: &'a G,
    ) -> Self {
        Self {
            store,
            project_store,
            run_snapshot_read,
            run_snapshot_write,
            amendment_queue,
            journal_store,
            github,
        }
    }
}

impl<G> PrReviewIngestionService<'_, G>
where
    G: GithubPort,
{
    pub async fn ingest_reviews(
        &self,
        daemon_dir: &Path,
        workspace_dir: &Path,
        task_id: &str,
        whitelist: &ReviewWhitelist,
        cancel: &CancellationToken,
    ) -> AppResult<IngestedReviewBatch> {
        self.ensure_not_cancelled(cancel)?;
        let mut task = self.store.read_task(daemon_dir, task_id)?;
        let Some(repo_slug) = task.repo_slug.as_deref() else {
            return Ok(IngestedReviewBatch::default());
        };
        let Some(pr_url) = task.pr_url.clone() else {
            return Ok(IngestedReviewBatch::default());
        };
        let Some(pr_number) = parse_pr_number(&pr_url) else {
            return Ok(IngestedReviewBatch::default());
        };
        let (owner, repo) = parse_repo_slug(repo_slug)?;

        self.ensure_not_cancelled(cancel)?;
        let issue_comments = self
            .github
            .fetch_issue_comments(owner, repo, pr_number)
            .await?;
        self.ensure_not_cancelled(cancel)?;
        let pull_comments = self
            .github
            .fetch_pr_review_comments(owner, repo, pr_number)
            .await?;
        self.ensure_not_cancelled(cancel)?;
        let reviews = self.github.fetch_pr_reviews(owner, repo, pr_number).await?;

        let next_comment_cursor = combine_max_id(
            task.last_seen_comment_id,
            issue_comments
                .iter()
                .map(|comment| comment.id)
                .chain(pull_comments.iter().map(|comment| comment.id))
                .max(),
        );
        let next_review_cursor = combine_max_id(
            task.last_seen_review_id,
            reviews.iter().map(|review| review.id).max(),
        );

        let items = self
            .deduplicate_comments(
                task.last_seen_comment_id.unwrap_or(0),
                task.last_seen_review_id.unwrap_or(0),
                issue_comments,
                pull_comments,
                reviews,
            )
            .into_iter()
            .collect::<Vec<_>>();
        let accepted = self.filter_by_whitelist(items, whitelist);
        let amendments = self.convert_to_amendments(workspace_dir, &task, &accepted)?;

        // For active tasks, only write amendment files to disk without touching
        // canonical run state (run.json/journal). The workflow engine owns
        // snapshot mutations during execution and will pick up staged files.
        // For completed/paused tasks, use the full shared staging service which
        // handles dedup, journal persistence, snapshot sync, and reopen.
        let staged_ids: Vec<String> = if !amendments.is_empty() {
            let project_id = ProjectId::new(task.project_id.clone())?;
            if task.status == TaskStatus::Active {
                // Disk-only staging: write amendment files without run.json mutation.
                // Deduplicate by dedup_key against existing on-disk amendments.
                let existing = self.amendment_queue
                    .list_pending_amendments(workspace_dir, &project_id)?;
                let mut seen_keys: std::collections::HashSet<String> = existing
                    .iter()
                    .map(|a| a.dedup_key.clone())
                    .collect();
                let mut ids: Vec<String> = Vec::new();
                for amendment in &amendments {
                    if !seen_keys.insert(amendment.dedup_key.clone()) {
                        continue;
                    }
                    if let Err(e) = self.amendment_queue
                        .write_amendment(workspace_dir, &project_id, amendment)
                    {
                        // Roll back files written earlier in this batch so a
                        // partial failure doesn't leak a subset of amendments.
                        for written_id in &ids {
                            let _ = self.amendment_queue
                                .remove_amendment(workspace_dir, &project_id, written_id);
                        }
                        return Err(e.into());
                    }
                    ids.push(amendment.amendment_id.clone());
                }
                ids
            } else {
                // Full staging with canonical state updates
                let staged_ids = record_service::stage_amendment_batch(
                    self.amendment_queue,
                    self.run_snapshot_read,
                    self.run_snapshot_write,
                    self.journal_store,
                    self.project_store,
                    workspace_dir,
                    &project_id,
                    &amendments,
                )?;
                staged_ids
            }
        } else {
            Vec::new()
        };
        let staged_count = staged_ids.len();

        // Check if the project was reopened (task was completed + amendments staged).
        let mut reopened_project = false;
        if task.status == TaskStatus::Completed && staged_count > 0 {
            // The shared service already reopened the project; just update task state.
            task.status = TaskStatus::Pending;
            task.failure_class = None;
            task.failure_message = None;
            task.updated_at = Utc::now();
            reopened_project = true;
        }

        task.last_seen_comment_id = next_comment_cursor;
        task.last_seen_review_id = next_review_cursor;
        task.updated_at = Utc::now();
        self.store.write_task(daemon_dir, &task)?;

        DaemonTaskService::append_journal_event(
            self.store,
            daemon_dir,
            super::model::DaemonJournalEventType::ReviewsIngested,
            json!({
                "task_id": task.task_id,
                "pr_url": pr_url,
                "accepted_count": accepted.len(),
                "staged_count": staged_count,
                "last_seen_comment_id": next_comment_cursor,
                "last_seen_review_id": next_review_cursor,
            }),
        )?;
        if staged_count > 0 {
            DaemonTaskService::append_journal_event(
                self.store,
                daemon_dir,
                super::model::DaemonJournalEventType::AmendmentsStaged,
                json!({
                    "task_id": task.task_id,
                    "count": staged_count,
                    "amendment_ids": &staged_ids,
                }),
            )?;
        }
        if reopened_project {
            DaemonTaskService::append_journal_event(
                self.store,
                daemon_dir,
                super::model::DaemonJournalEventType::ProjectReopened,
                json!({
                    "task_id": task.task_id,
                    "project_id": task.project_id,
                    "reason": "pr_review_amendments",
                }),
            )?;
        }

        Ok(IngestedReviewBatch {
            staged_count,
            reopened_project,
            last_seen_comment_id: next_comment_cursor,
            last_seen_review_id: next_review_cursor,
        })
    }

    fn filter_by_whitelist(
        &self,
        items: Vec<ReviewItem>,
        whitelist: &ReviewWhitelist,
    ) -> Vec<ReviewItem> {
        items
            .into_iter()
            .filter(|item| whitelist.allows(item.author()))
            .filter(|item| !item.body().trim().is_empty())
            .collect()
    }

    fn deduplicate_comments(
        &self,
        last_seen_comment_id: u64,
        last_seen_review_id: u64,
        issue_comments: Vec<GithubComment>,
        pull_comments: Vec<GithubComment>,
        reviews: Vec<GithubReview>,
    ) -> Vec<ReviewItem> {
        let mut seen = HashSet::new();
        let mut items = Vec::new();

        for comment in issue_comments {
            if comment.id <= last_seen_comment_id {
                continue;
            }
            let item = ReviewItem::IssueComment(comment);
            if seen.insert(item.key()) {
                items.push(item);
            }
        }
        for comment in pull_comments {
            if comment.id <= last_seen_comment_id {
                continue;
            }
            let item = ReviewItem::PullComment(comment);
            if seen.insert(item.key()) {
                items.push(item);
            }
        }
        for review in reviews {
            if review.id <= last_seen_review_id {
                continue;
            }
            // Skip approval-only reviews — only COMMENTED and CHANGES_REQUESTED
            // states should create amendments. An APPROVED review with text like
            // "LGTM" should not reopen a completed task.
            if review.state.eq_ignore_ascii_case("APPROVED") {
                continue;
            }
            let item = ReviewItem::Review(review);
            if seen.insert(item.key()) {
                items.push(item);
            }
        }

        items
    }

    fn convert_to_amendments(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
        items: &[ReviewItem],
    ) -> AppResult<Vec<QueuedAmendment>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let project_id = ProjectId::new(task.project_id.clone())?;
        let snapshot = self
            .run_snapshot_read
            .read_run_snapshot(base_dir, &project_id)?;
        let source_cycle = snapshot
            .cycle_history
            .last()
            .map(|entry| entry.cycle)
            .unwrap_or(1);
        let source_completion_round = snapshot.completion_rounds.max(1);
        let created_at = Utc::now();

        Ok(items
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let body = item.body().trim().to_owned();
                let source = crate::contexts::project_run_record::model::AmendmentSource::PrReview;
                let dedup_key = QueuedAmendment::compute_dedup_key(&source, &body);
                QueuedAmendment {
                    amendment_id: format!("pr-review-{}", item.key().replace(':', "-")),
                    source_stage: StageId::Review,
                    source_cycle,
                    source_completion_round,
                    body,
                    created_at,
                    batch_sequence: (idx + 1) as u32,
                    source,
                    dedup_key,
                }
            })
            .collect())
    }

    fn ensure_not_cancelled(&self, cancel: &CancellationToken) -> AppResult<()> {
        if cancel.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: "github".to_owned(),
                contract_id: "daemon.pr_review".to_owned(),
            });
        }
        Ok(())
    }
}

fn combine_max_id(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn parse_pr_number(pr_url: &str) -> Option<u64> {
    let candidate = pr_url.trim().trim_end_matches('/').rsplit('/').next()?;
    candidate.parse::<u64>().ok()
}
