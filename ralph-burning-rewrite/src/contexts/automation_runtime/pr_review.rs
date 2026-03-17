use std::collections::HashSet;
use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::adapters::fs::FileSystem;
use crate::adapters::github::{GithubComment, GithubPort, GithubReview};
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::automation_runtime::repo_registry::parse_repo_slug;
use crate::contexts::automation_runtime::{
    DaemonStorePort, DaemonTask, DaemonTaskService, ReviewWhitelist, TaskStatus,
};
use crate::contexts::project_run_record::model::{ActiveRun, QueuedAmendment, RunStatus};
use crate::contexts::project_run_record::service::{
    AmendmentQueuePort, ProjectStorePort, RunSnapshotPort, RunSnapshotWritePort,
};
use crate::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};
use crate::shared::error::{AppError, AppResult};
use crate::contexts::workspace_governance::WORKSPACE_DIR;

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
    github: &'a G,
}

impl<'a, G> PrReviewIngestionService<'a, G> {
    pub fn new(
        store: &'a dyn DaemonStorePort,
        project_store: &'a dyn ProjectStorePort,
        run_snapshot_read: &'a dyn RunSnapshotPort,
        run_snapshot_write: &'a dyn RunSnapshotWritePort,
        amendment_queue: &'a dyn AmendmentQueuePort,
        github: &'a G,
    ) -> Self {
        Self {
            store,
            project_store,
            run_snapshot_read,
            run_snapshot_write,
            amendment_queue,
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
        base_dir: &Path,
        task_id: &str,
        whitelist: &ReviewWhitelist,
        cancel: &CancellationToken,
    ) -> AppResult<IngestedReviewBatch> {
        self.ensure_not_cancelled(cancel)?;
        let mut task = self.store.read_task(base_dir, task_id)?;
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
        let issue_comments = self.github.fetch_issue_comments(owner, repo, pr_number).await?;
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
        let next_review_cursor =
            combine_max_id(task.last_seen_review_id, reviews.iter().map(|review| review.id).max());

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
        let amendments = self.convert_to_amendments(base_dir, &task, &accepted)?;
        if !amendments.is_empty() {
            self.stage_amendments(base_dir, &task, &amendments)?;
        }

        let mut reopened_project = false;
        if task.status == TaskStatus::Completed && !amendments.is_empty() {
            self.reopen_completed_project(base_dir, &mut task)?;
            reopened_project = true;
        }

        task.last_seen_comment_id = next_comment_cursor;
        task.last_seen_review_id = next_review_cursor;
        task.updated_at = Utc::now();
        self.store.write_task(base_dir, &task)?;

        DaemonTaskService::append_journal_event(
            self.store,
            base_dir,
            super::model::DaemonJournalEventType::ReviewsIngested,
            json!({
                "task_id": task.task_id,
                "pr_url": pr_url,
                "accepted_count": accepted.len(),
                "staged_count": amendments.len(),
                "last_seen_comment_id": next_comment_cursor,
                "last_seen_review_id": next_review_cursor,
            }),
        )?;
        if !amendments.is_empty() {
            DaemonTaskService::append_journal_event(
                self.store,
                base_dir,
                super::model::DaemonJournalEventType::AmendmentsStaged,
                json!({
                    "task_id": task.task_id,
                    "count": amendments.len(),
                    "amendment_ids": amendments.iter().map(|a| a.amendment_id.as_str()).collect::<Vec<_>>(),
                }),
            )?;
        }
        if reopened_project {
            DaemonTaskService::append_journal_event(
                self.store,
                base_dir,
                super::model::DaemonJournalEventType::ProjectReopened,
                json!({
                    "task_id": task.task_id,
                    "project_id": task.project_id,
                    "reason": "pr_review_amendments",
                }),
            )?;
        }

        Ok(IngestedReviewBatch {
            staged_count: amendments.len(),
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
        items.into_iter()
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
        let snapshot = self.run_snapshot_read.read_run_snapshot(base_dir, &project_id)?;
        let source_cycle = snapshot.cycle_history.last().map(|entry| entry.cycle).unwrap_or(1);
        let source_completion_round = snapshot.completion_rounds.max(1);
        let created_at = Utc::now();

        Ok(items
            .iter()
            .enumerate()
            .map(|(idx, item)| QueuedAmendment {
                amendment_id: format!("pr-review-{}", item.key().replace(':', "-")),
                source_stage: StageId::Review,
                source_cycle,
                source_completion_round,
                body: item.body().trim().to_owned(),
                created_at,
                batch_sequence: (idx + 1) as u32,
            })
            .collect())
    }

    fn stage_amendments(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
        amendments: &[QueuedAmendment],
    ) -> AppResult<()> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        for amendment in amendments {
            self.amendment_queue
                .write_amendment(base_dir, &project_id, amendment)?;
        }
        Ok(())
    }

    fn reopen_completed_project(
        &self,
        base_dir: &Path,
        task: &mut DaemonTask,
    ) -> AppResult<()> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let project_record = self.project_store.read_project_record(base_dir, &project_id)?;
        let mut snapshot = self.run_snapshot_read.read_run_snapshot(base_dir, &project_id)?;
        if snapshot.status == RunStatus::Completed {
            let prompt_path = base_dir
                .join(WORKSPACE_DIR)
                .join("projects")
                .join(project_id.as_str())
                .join(&project_record.prompt_reference);
            let prompt_contents = std::fs::read_to_string(&prompt_path).map_err(|error| {
                AppError::CorruptRecord {
                    file: prompt_path.display().to_string(),
                    details: format!("failed to read prompt for project reopen: {error}"),
                }
            })?;
            let prompt_hash = FileSystem::prompt_hash(&prompt_contents);
            let planning_stage = planning_stage_for_flow(task.resolved_flow.unwrap_or(project_record.flow));
            let current_cycle = snapshot.cycle_history.last().map(|entry| entry.cycle).unwrap_or(1);
            let completion_round = snapshot.completion_rounds.max(1);

            snapshot.interrupted_run = Some(ActiveRun {
                run_id: format!("reopen-{}", task.project_id),
                stage_cursor: StageCursor::new(planning_stage, current_cycle, 1, completion_round)?,
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: snapshot.last_stage_resolution_snapshot.clone(),
            });
            snapshot.active_run = None;
            snapshot.status = RunStatus::Paused;
            snapshot.status_summary = "paused: PR review amendments staged".to_owned();
            self.run_snapshot_write
                .write_run_snapshot(base_dir, &project_id, &snapshot)?;
        }

        task.status = TaskStatus::Pending;
        task.failure_class = None;
        task.failure_message = None;
        task.updated_at = Utc::now();
        Ok(())
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

fn planning_stage_for_flow(flow: FlowPreset) -> StageId {
    match flow {
        FlowPreset::Standard => StageId::Planning,
        FlowPreset::QuickDev => StageId::PlanAndImplement,
        FlowPreset::DocsChange => StageId::DocsPlan,
        FlowPreset::CiImprovement => StageId::CiPlan,
    }
}

fn parse_pr_number(pr_url: &str) -> Option<u64> {
    let candidate = pr_url
        .trim()
        .trim_end_matches('/')
        .rsplit('/')
        .next()?;
    candidate.parse::<u64>().ok()
}
