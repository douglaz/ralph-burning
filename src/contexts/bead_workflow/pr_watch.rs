#![forbid(unsafe_code)]

//! Success-path watcher for PR review, CI, and merge completion.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use super::pr_open::{CommandOutput, PrToolError, ProcessPrToolPort};

pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(60 * 60);

pub const CODEX_BOT_LOGIN: &str = "chatgpt-codex-connector[bot]";
pub const KNOWN_CI_FLAKES: &[&str] =
    &["adapters::br_process::tests::check_available_times_out_when_version_probe_hangs"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WatchOutcome {
    Merged {
        sha: String,
        pr_url: String,
    },
    BlockedByCi {
        failing: Vec<String>,
        after_rerun: bool,
    },
    BlockedByBot {
        reasons: Vec<String>,
    },
    Timeout {
        elapsed: Duration,
    },
    Aborted {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrWatchRequest<'a> {
    pub base_dir: &'a Path,
    pub pr_number: u64,
    pub max_wait: Duration,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrStatusSnapshot {
    pub ci: CiState,
    pub mergeable: MergeableState,
    pub head_sha: Option<String>,
    pub head_review_watermark_at: Option<DateTime<Utc>>,
    pub latest_push_at: Option<DateTime<Utc>>,
    pub pr_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiState {
    Pending,
    Success,
    Failed {
        failing: Vec<String>,
        failed_run_ids: Vec<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeableState {
    Mergeable,
    Conflicting,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotBodyReaction {
    None,
    Eyes,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotBodyReview {
    pub reaction: BotBodyReaction,
    pub created_at: Option<DateTime<Utc>>,
    pub approved_head_sha: Option<String>,
}

impl BotBodyReview {
    fn new(reaction: BotBodyReaction, created_at: Option<DateTime<Utc>>) -> Self {
        Self {
            reaction,
            created_at,
            approved_head_sha: None,
        }
    }

    fn reaction(reaction: BotBodyReaction) -> Self {
        Self::new(reaction, None)
    }

    fn bind_to_head(
        mut self,
        head_sha: Option<&str>,
        head_review_watermark_at: Option<DateTime<Utc>>,
    ) -> Self {
        if self.reaction == BotBodyReaction::Approved {
            if let (Some(approved_at), Some(head_sha)) = (self.created_at.as_ref(), head_sha) {
                // If we know a review watermark (latest push time), require the
                // approval to be at or after it — otherwise an old approval
                // would silently apply to a freshly-pushed head. If the
                // watermark lookup returned None (best-effort lookup, can
                // legitimately fail on repos where push-timestamp data isn't
                // available), accept the approval against the current head:
                // we have no later signal to compare against, and refusing to
                // bind in that case would guarantee a timeout on every
                // mergeable PR in those repos. Bot's PR #209 finding.
                let approves_head = match head_review_watermark_at.as_ref() {
                    Some(watermark) => approved_at >= watermark,
                    None => true,
                };
                if approves_head {
                    self.approved_head_sha = Some(head_sha.to_owned());
                }
            }
        }
        self
    }

    fn is_approved_for_head(&self, status: &PrStatusSnapshot) -> bool {
        self.reaction == BotBodyReaction::Approved
            && status
                .head_sha
                .as_deref()
                .is_some_and(|head_sha| self.approved_head_sha.as_deref() == Some(head_sha))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotLineComment {
    pub body: String,
    pub path: Option<String>,
    pub line: Option<u64>,
    pub url: Option<String>,
}

impl BotLineComment {
    fn reason(&self) -> String {
        let location = match (&self.path, self.line) {
            (Some(path), Some(line)) => format!("{path}:{line}"),
            (Some(path), None) => path.clone(),
            _ => "line comment".to_owned(),
        };
        match &self.url {
            Some(url) if !url.is_empty() => format!("{location}: {} ({url})", self.body),
            _ => format!("{location}: {}", self.body),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrMergeOutput {
    pub sha: String,
    pub pr_url: String,
}

#[derive(Debug, Error)]
pub enum PrWatchError {
    #[error("{action} failed: {source}")]
    Tool {
        action: &'static str,
        source: PrToolError,
    },
}

#[allow(async_fn_in_trait)]
pub trait PrWatchToolPort {
    async fn repo_slug(&self, repo_root: &Path) -> Result<String, PrToolError>;
    async fn pr_status(
        &self,
        repo_root: &Path,
        repo_slug: &str,
        pr_number: u64,
    ) -> Result<PrStatusSnapshot, PrToolError>;
    async fn codex_bot_reaction(
        &self,
        repo_root: &Path,
        repo_slug: &str,
        pr_number: u64,
        head_sha: Option<&str>,
        head_review_watermark_at: Option<DateTime<Utc>>,
    ) -> Result<BotBodyReview, PrToolError>;
    async fn codex_bot_line_comments_since_latest_push(
        &self,
        repo_root: &Path,
        repo_slug: &str,
        pr_number: u64,
        head_sha: Option<&str>,
        latest_push_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<BotLineComment>, PrToolError>;
    async fn failed_run_logs(
        &self,
        repo_root: &Path,
        run_ids: &[u64],
    ) -> Result<Vec<String>, PrToolError>;
    async fn rerun_failed_runs(
        &self,
        repo_root: &Path,
        pr_number: u64,
        run_ids: &[u64],
    ) -> Result<(), PrToolError>;
    async fn merge_pr(
        &self,
        repo_root: &Path,
        pr_number: u64,
        head_sha: &str,
    ) -> Result<PrMergeOutput, PrToolError>;
}

#[allow(async_fn_in_trait)]
pub trait PrWatchClock {
    fn elapsed(&self) -> Duration;
    async fn sleep(&self, duration: Duration);
}

pub struct SystemPrWatchClock {
    started_at: Instant,
}

impl SystemPrWatchClock {
    pub fn started_now() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl PrWatchClock for SystemPrWatchClock {
    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

pub async fn watch_pr<T, C>(
    request: PrWatchRequest<'_>,
    tools: &T,
    clock: &C,
) -> Result<WatchOutcome, PrWatchError>
where
    T: PrWatchToolPort,
    C: PrWatchClock,
{
    let repo_slug =
        tools
            .repo_slug(request.base_dir)
            .await
            .map_err(|source| PrWatchError::Tool {
                action: "read repository slug",
                source,
            })?;
    let mut rerun_attempted = false;

    loop {
        if clock.elapsed() >= request.max_wait {
            return Ok(WatchOutcome::Timeout {
                elapsed: clock.elapsed(),
            });
        }

        let status = tools
            .pr_status(request.base_dir, &repo_slug, request.pr_number)
            .await
            .map_err(|source| PrWatchError::Tool {
                action: "read PR status",
                source,
            })?;
        let reaction = tools
            .codex_bot_reaction(
                request.base_dir,
                &repo_slug,
                request.pr_number,
                status.head_sha.as_deref(),
                status.head_review_watermark_at,
            )
            .await
            .map_err(|source| PrWatchError::Tool {
                action: "read codex bot reaction",
                source,
            })?;
        let comments = tools
            .codex_bot_line_comments_since_latest_push(
                request.base_dir,
                &repo_slug,
                request.pr_number,
                status.head_sha.as_deref(),
                status.latest_push_at,
            )
            .await
            .map_err(|source| PrWatchError::Tool {
                action: "read codex bot line comments",
                source,
            })?;

        if !comments.is_empty() {
            return Ok(WatchOutcome::BlockedByBot {
                reasons: comments.iter().map(BotLineComment::reason).collect(),
            });
        }
        if reaction.reaction == BotBodyReaction::Rejected {
            return Ok(WatchOutcome::BlockedByBot {
                reasons: vec!["rejected".to_owned()],
            });
        }

        let approved_for_head = reaction.is_approved_for_head(&status);

        match status.ci {
            CiState::Pending => {}
            CiState::Success if approved_for_head => {
                if status.mergeable == MergeableState::Conflicting {
                    return Ok(WatchOutcome::Aborted {
                        reason: "PR is not mergeable".to_owned(),
                    });
                }
                // GitHub reports MergeableState::Unknown transiently right
                // after a push or status update. Treat it as non-terminal:
                // fall through to the post-match sleep and re-poll instead
                // of attempting the merge with `gh pr merge`, which would
                // turn a retriable state into a hard tool error. Bot's
                // PR #209 finding.
                if status.mergeable != MergeableState::Unknown {
                    let Some(head_sha) = status.head_sha.as_deref() else {
                        return Ok(WatchOutcome::Aborted {
                            reason: "cannot merge without observed PR head SHA".to_owned(),
                        });
                    };
                    let merged = tools
                        .merge_pr(request.base_dir, request.pr_number, head_sha)
                        .await
                        .map_err(|source| PrWatchError::Tool {
                            action: "merge PR",
                            source,
                        })?;
                    return Ok(WatchOutcome::Merged {
                        sha: merged.sha,
                        pr_url: merged.pr_url,
                    });
                }
            }
            CiState::Success
                if reaction.reaction == BotBodyReaction::Approved && status.head_sha.is_none() =>
            {
                return Ok(WatchOutcome::Aborted {
                    reason: "cannot merge without observed PR head SHA".to_owned(),
                });
            }
            CiState::Success => {}
            CiState::Failed {
                failing,
                failed_run_ids,
            } => {
                let failed_run_logs = tools
                    .failed_run_logs(request.base_dir, failed_run_ids.as_slice())
                    .await
                    .map_err(|source| PrWatchError::Tool {
                        action: "read failed CI logs",
                        source,
                    })?;
                let failing = failures_with_failed_run_log_matches(failing, &failed_run_logs);
                if !rerun_attempted && is_known_flake_failure(&failing) {
                    tools
                        .rerun_failed_runs(
                            request.base_dir,
                            request.pr_number,
                            failed_run_ids.as_slice(),
                        )
                        .await
                        .map_err(|source| PrWatchError::Tool {
                            action: "rerun failed CI",
                            source,
                        })?;
                    rerun_attempted = true;
                } else {
                    return Ok(WatchOutcome::BlockedByCi {
                        failing,
                        after_rerun: rerun_attempted,
                    });
                }
            }
        }

        clock.sleep(request.poll_interval).await;
    }
}

fn is_known_flake_failure(failing: &[String]) -> bool {
    let has_known_flake = failing
        .iter()
        .any(|failure| KNOWN_CI_FLAKES.iter().any(|flake| failure.contains(flake)));
    has_known_flake
        && failing.iter().all(|failure| {
            !looks_like_rust_test_name(failure)
                || KNOWN_CI_FLAKES.iter().any(|flake| failure.contains(flake))
        })
}

fn looks_like_rust_test_name(failure: &str) -> bool {
    failure.contains("::")
}

fn failures_with_failed_run_log_matches(
    mut failing: Vec<String>,
    failed_run_logs: &[String],
) -> Vec<String> {
    for failed_test in failed_test_names_from_logs(failed_run_logs) {
        if !failing.iter().any(|failure| failure == &failed_test) {
            failing.push(failed_test);
        }
    }
    failing
}

fn failed_test_names_from_logs(failed_run_logs: &[String]) -> Vec<String> {
    let mut failed_tests = Vec::new();
    for log in failed_run_logs {
        let mut in_failures_list = false;
        for line in log.lines() {
            let trimmed = line.trim();
            if trimmed == "failures:" {
                in_failures_list = true;
                continue;
            }
            if in_failures_list {
                if trimmed.is_empty() {
                    continue;
                }
                if looks_like_rust_test_name(trimmed) {
                    push_unique(&mut failed_tests, trimmed);
                    continue;
                }
                if trimmed.starts_with("test result:") {
                    in_failures_list = false;
                }
            }
            if let Some(failed_test) = rust_test_name_from_test_line(trimmed) {
                push_unique(&mut failed_tests, failed_test);
                continue;
            }
            if let Some(failed_test) = rust_test_name_from_stdout_header(trimmed) {
                push_unique(&mut failed_tests, failed_test);
            }
        }
    }
    failed_tests
}

fn rust_test_name_from_test_line(line: &str) -> Option<&str> {
    let (_, after_test) = line.split_once("test ")?;
    let (candidate, result) = after_test.split_once(" ... ")?;
    (result.contains("FAILED") && looks_like_rust_test_name(candidate)).then_some(candidate)
}

fn rust_test_name_from_stdout_header(line: &str) -> Option<&str> {
    let candidate = line.strip_prefix("---- ")?.strip_suffix(" stdout ----")?;
    looks_like_rust_test_name(candidate).then_some(candidate)
}

fn push_unique(items: &mut Vec<String>, item: &str) {
    if !items.iter().any(|existing| existing == item) {
        items.push(item.to_owned());
    }
}

impl PrWatchToolPort for ProcessPrToolPort {
    async fn repo_slug(&self, repo_root: &Path) -> Result<String, PrToolError> {
        let output = gh(repo_root, &["repo", "view", "--json", "nameWithOwner"])?;
        let value = parse_json(&output, "gh repo view --json nameWithOwner")?;
        value
            .get("nameWithOwner")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| PrToolError::Io("gh repo view did not return nameWithOwner".to_owned()))
    }

    async fn pr_status(
        &self,
        repo_root: &Path,
        _repo_slug: &str,
        pr_number: u64,
    ) -> Result<PrStatusSnapshot, PrToolError> {
        let pr_number_arg = pr_number.to_string();
        let output = gh(
            repo_root,
            &[
                "pr",
                "view",
                &pr_number_arg,
                "--json",
                "statusCheckRollup,mergeable,headRefOid,url",
            ],
        )?;
        let value = parse_json(&output, "gh pr view --json statusCheckRollup,mergeable")?;
        let head_sha = value
            .get("headRefOid")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let latest_push_at =
            latest_head_push_at(repo_root, _repo_slug, pr_number, head_sha.as_deref())?;
        Ok(PrStatusSnapshot {
            ci: parse_ci_state(value.get("statusCheckRollup")),
            mergeable: parse_mergeable(value.get("mergeable")),
            head_sha,
            // The reaction API is not commit-scoped. The port binds +1 to this
            // exact head only when the reaction is newer than the current
            // head's latest push, so non-push PR activity after approval does
            // not invalidate the bot's approval for the reviewed head.
            head_review_watermark_at: latest_push_at,
            // GitHub's pull-review comments can be anchored to older commits
            // even when the bot created them after the current head was pushed.
            // Use the current head's pushedDate, with timeline push events as
            // a fallback, so those findings still block automatic merge.
            latest_push_at,
            pr_url: value
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        })
    }

    async fn codex_bot_reaction(
        &self,
        repo_root: &Path,
        repo_slug: &str,
        pr_number: u64,
        head_sha: Option<&str>,
        head_review_watermark_at: Option<DateTime<Utc>>,
    ) -> Result<BotBodyReview, PrToolError> {
        // In this repository the codex connector's issue-body reactions are
        // the review state: eyes means reading, +1 approved, and -1 rejected.
        let endpoint = format!("repos/{repo_slug}/issues/{pr_number}/reactions");
        let output = gh_api_paginated(repo_root, &endpoint)?;
        let value = parse_paginated_json(&output, "gh api issue reactions")?;
        Ok(parse_codex_reaction(&value).bind_to_head(head_sha, head_review_watermark_at))
    }

    async fn codex_bot_line_comments_since_latest_push(
        &self,
        repo_root: &Path,
        repo_slug: &str,
        pr_number: u64,
        head_sha: Option<&str>,
        latest_push_at: Option<DateTime<Utc>>,
    ) -> Result<Vec<BotLineComment>, PrToolError> {
        let endpoint = format!("repos/{repo_slug}/pulls/{pr_number}/comments");
        let output = gh_api_paginated(repo_root, &endpoint)?;
        let value = parse_paginated_json(&output, "gh api pull comments")?;
        Ok(parse_bot_line_comments(&value, head_sha, latest_push_at))
    }

    async fn failed_run_logs(
        &self,
        repo_root: &Path,
        run_ids: &[u64],
    ) -> Result<Vec<String>, PrToolError> {
        let mut logs = Vec::new();
        for run_id in run_ids {
            let run_id = run_id.to_string();
            let output = gh(repo_root, &["run", "view", &run_id, "--log-failed"])?;
            logs.push(output.stdout);
        }
        Ok(logs)
    }

    async fn rerun_failed_runs(
        &self,
        repo_root: &Path,
        pr_number: u64,
        run_ids: &[u64],
    ) -> Result<(), PrToolError> {
        if run_ids.is_empty() {
            return Err(PrToolError::Io(format!(
                "cannot rerun failed CI for PR #{pr_number}: no GitHub Actions run id found"
            )));
        }
        for run_id in run_ids {
            let run_id = run_id.to_string();
            gh(repo_root, &["run", "rerun", &run_id, "--failed"])?;
        }
        Ok(())
    }

    async fn merge_pr(
        &self,
        repo_root: &Path,
        pr_number: u64,
        head_sha: &str,
    ) -> Result<PrMergeOutput, PrToolError> {
        let pr_number = pr_number.to_string();
        gh(
            repo_root,
            &[
                "pr",
                "merge",
                &pr_number,
                "--squash",
                "--delete-branch",
                "--match-head-commit",
                head_sha,
            ],
        )?;
        let output = gh(
            repo_root,
            &["pr", "view", &pr_number, "--json", "mergeCommit,url"],
        )?;
        let value = parse_json(&output, "gh pr view --json mergeCommit,url")?;
        let sha = value
            .get("mergeCommit")
            .and_then(|merge_commit| merge_commit.get("oid"))
            .and_then(Value::as_str)
            .ok_or_else(|| PrToolError::Io("gh pr view did not return mergeCommit.oid".to_owned()))?
            .to_owned();
        let pr_url = value
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        Ok(PrMergeOutput { sha, pr_url })
    }
}

fn gh(repo_root: &Path, args: &[&str]) -> Result<CommandOutput, PrToolError> {
    ProcessPrToolPort::run_command(repo_root, "gh", args)
}

fn gh_api_paginated(repo_root: &Path, endpoint: &str) -> Result<CommandOutput, PrToolError> {
    ProcessPrToolPort::run_command(
        repo_root,
        "gh",
        &[
            "api",
            endpoint,
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github+json",
        ],
    )
}

fn gh_api_graphql(
    repo_root: &Path,
    query: &str,
    fields: &[(&str, &str)],
) -> Result<CommandOutput, PrToolError> {
    let mut args = vec![
        "api".to_owned(),
        "graphql".to_owned(),
        "-f".to_owned(),
        format!("query={query}"),
    ];
    for (key, value) in fields {
        args.push("-F".to_owned());
        args.push(format!("{key}={value}"));
    }
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    gh(repo_root, &args)
}

fn parse_json(output: &CommandOutput, command: &str) -> Result<Value, PrToolError> {
    serde_json::from_str(&output.stdout)
        .map_err(|error| PrToolError::Io(format!("{command} returned invalid JSON: {error}")))
}

fn parse_paginated_json(output: &CommandOutput, command: &str) -> Result<Value, PrToolError> {
    parse_json(output, command).map(flatten_slurped_pages)
}

fn flatten_slurped_pages(value: Value) -> Value {
    match value {
        Value::Array(pages) if pages.iter().all(Value::is_array) => Value::Array(
            pages
                .into_iter()
                .flat_map(|page| match page {
                    Value::Array(items) => items,
                    _ => Vec::new(),
                })
                .collect(),
        ),
        other => other,
    }
}

fn parse_ci_state(raw: Option<&Value>) -> CiState {
    let mut entries = Vec::new();
    collect_status_check_entries(raw.unwrap_or(&Value::Null), &mut entries);
    if entries.is_empty() {
        return CiState::Pending;
    }

    let mut pending = false;
    let mut failing = Vec::new();
    let mut failed_run_ids = BTreeSet::new();
    for entry in entries {
        if entry.failed {
            failing.push(entry.name);
            if let Some(run_id) = entry.run_id {
                failed_run_ids.insert(run_id);
            }
        } else if entry.pending {
            pending = true;
        }
    }

    if !failing.is_empty() {
        CiState::Failed {
            failing,
            failed_run_ids: failed_run_ids.into_iter().collect(),
        }
    } else if pending {
        CiState::Pending
    } else {
        CiState::Success
    }
}

#[derive(Debug)]
struct StatusCheckEntry {
    name: String,
    pending: bool,
    failed: bool,
    run_id: Option<u64>,
}

fn collect_status_check_entries(value: &Value, entries: &mut Vec<StatusCheckEntry>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_status_check_entries(item, entries);
            }
        }
        Value::Object(map) => {
            if let Some(nodes) = map.get("nodes") {
                collect_status_check_entries(nodes, entries);
                return;
            }
            if let Some(edges) = map.get("edges") {
                collect_status_check_entries(edges, entries);
                return;
            }
            if let Some(node) = map.get("node") {
                collect_status_check_entries(node, entries);
                return;
            }

            let conclusion = string_field(value, &["conclusion", "state"]);
            let status = string_field(value, &["status", "state"]);
            if conclusion.is_none() && status.is_none() {
                return;
            }

            let conclusion = conclusion.unwrap_or_default().to_ascii_uppercase();
            let status = status.unwrap_or_default().to_ascii_uppercase();
            let name = string_field(value, &["name", "context", "workflowName", "displayName"])
                .unwrap_or_else(|| "unknown check".to_owned());
            let failed = is_failed_check_conclusion(&conclusion);
            let success = is_success_check_conclusion(&conclusion)
                || (conclusion.is_empty() && matches!(status.as_str(), "SUCCESS" | "COMPLETED"));
            entries.push(StatusCheckEntry {
                name,
                pending: !failed && !success,
                failed,
                run_id: actions_run_id(value),
            });
        }
        _ => {}
    }
}

fn is_failed_check_conclusion(conclusion: &str) -> bool {
    matches!(
        conclusion,
        "FAILURE"
            | "FAILED"
            | "ERROR"
            | "CANCELLED"
            | "TIMED_OUT"
            | "ACTION_REQUIRED"
            | "STARTUP_FAILURE"
            | "STALE"
    )
}

fn is_success_check_conclusion(conclusion: &str) -> bool {
    matches!(conclusion, "SUCCESS" | "NEUTRAL" | "SKIPPED" | "COMPLETED")
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn date_time_field(value: &Value, key: &str) -> Option<DateTime<Utc>> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

const PR_HEAD_PUSH_QUERY: &str = r#"
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      commits(last: 100) {
        nodes {
          commit {
            oid
            pushedDate
          }
        }
      }
    }
  }
}
"#;

fn latest_head_push_at(
    repo_root: &Path,
    repo_slug: &str,
    pr_number: u64,
    head_sha: Option<&str>,
) -> Result<Option<DateTime<Utc>>, PrToolError> {
    let Some(head_sha) = head_sha else {
        return Ok(None);
    };
    let (owner, name) = repo_slug.split_once('/').ok_or_else(|| {
        PrToolError::Io(format!(
            "repository slug must be owner/name for PR watch, got {repo_slug:?}"
        ))
    })?;
    let pr_number = pr_number.to_string();
    let output = gh_api_graphql(
        repo_root,
        PR_HEAD_PUSH_QUERY,
        &[("owner", owner), ("name", name), ("number", &pr_number)],
    )?;
    let value = parse_json(&output, "gh api graphql pull request commits")?;
    let timeline = gh_api_paginated(
        repo_root,
        &format!("repos/{repo_slug}/issues/{pr_number}/timeline"),
    )?;
    let timeline = parse_paginated_json(&timeline, "gh api issue timeline")?;
    Ok(latest_head_push_at_from_sources(
        &value, &timeline, head_sha,
    ))
}

fn latest_head_push_at_from_sources(
    graphql: &Value,
    timeline: &Value,
    head_sha: &str,
) -> Option<DateTime<Utc>> {
    [
        latest_head_push_at_from_graphql(graphql, head_sha),
        latest_head_timeline_update_at(timeline, Some(head_sha)),
    ]
    .into_iter()
    .flatten()
    .max()
}

fn latest_head_push_at_from_graphql(value: &Value, head_sha: &str) -> Option<DateTime<Utc>> {
    value
        .pointer("/data/repository/pullRequest/commits/nodes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|node| node.get("commit"))
        .filter(|commit| commit.get("oid").and_then(Value::as_str) == Some(head_sha))
        .filter_map(|commit| date_time_field(commit, "pushedDate"))
        .max()
}

fn latest_head_timeline_update_at(value: &Value, head_sha: Option<&str>) -> Option<DateTime<Utc>> {
    let head_sha = head_sha?;
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|event| timeline_event_matches_head(event, head_sha))
        .filter_map(timeline_event_timestamp)
        .max()
}

fn timeline_event_matches_head(event: &Value, head_sha: &str) -> bool {
    [
        &["sha"][..],
        &["commit_id"][..],
        &["after"][..],
        &["after_commit", "sha"][..],
        &["afterCommit", "oid"][..],
    ]
    .iter()
    .any(|path| string_at_path(event, path).as_deref() == Some(head_sha))
}

fn string_at_path(value: &Value, path: &[&str]) -> Option<String> {
    path.iter()
        .try_fold(value, |value, key| value.get(*key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn timeline_event_timestamp(event: &Value) -> Option<DateTime<Utc>> {
    date_time_field(event, "created_at")
}

fn actions_run_id(value: &Value) -> Option<u64> {
    let url = string_field(value, &["detailsUrl", "targetUrl", "url", "link"])?;
    let (_, tail) = url.split_once("/actions/runs/")?;
    tail.split(|ch: char| !ch.is_ascii_digit())
        .next()
        .and_then(|run_id| run_id.parse().ok())
}

fn parse_mergeable(raw: Option<&Value>) -> MergeableState {
    match raw
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_uppercase()
        .as_str()
    {
        "MERGEABLE" => MergeableState::Mergeable,
        "CONFLICTING" => MergeableState::Conflicting,
        _ => MergeableState::Unknown,
    }
}

fn parse_codex_reaction(value: &Value) -> BotBodyReview {
    let mut latest: Option<(DateTime<Utc>, BotBodyReaction)> = None;
    for reaction in value.as_array().into_iter().flatten() {
        if reaction
            .get("user")
            .and_then(|user| user.get("login"))
            .and_then(Value::as_str)
            != Some(CODEX_BOT_LOGIN)
        {
            continue;
        }
        let content = reaction
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("");
        let parsed = match content {
            "+1" => BotBodyReaction::Approved,
            "-1" => BotBodyReaction::Rejected,
            "eyes" => BotBodyReaction::Eyes,
            _ => continue,
        };
        let Some(created_at) = reaction
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
            .map(|timestamp| timestamp.with_timezone(&Utc))
        else {
            continue;
        };
        if latest
            .as_ref()
            .is_none_or(|(latest_at, _)| created_at >= *latest_at)
        {
            latest = Some((created_at, parsed));
        }
    }
    latest
        .map(|(created_at, reaction)| BotBodyReview::new(reaction, Some(created_at)))
        .unwrap_or_else(|| BotBodyReview::reaction(BotBodyReaction::None))
}

fn parse_bot_line_comments(
    value: &Value,
    head_sha: Option<&str>,
    latest_push_at: Option<DateTime<Utc>>,
) -> Vec<BotLineComment> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter(|comment| {
            comment
                .get("user")
                .and_then(|user| user.get("login"))
                .and_then(Value::as_str)
                == Some(CODEX_BOT_LOGIN)
        })
        .filter(|comment| comment_is_since_latest_push(comment, head_sha, latest_push_at))
        .map(|comment| BotLineComment {
            body: comment
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_owned(),
            path: comment
                .get("path")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            line: comment
                .get("line")
                .or_else(|| comment.get("original_line"))
                .and_then(Value::as_u64),
            url: comment
                .get("html_url")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        })
        .collect()
}

fn comment_is_since_latest_push(
    comment: &Value,
    head_sha: Option<&str>,
    latest_push_at: Option<DateTime<Utc>>,
) -> bool {
    if let Some(latest_push_at) = latest_push_at {
        return comment
            .get("created_at")
            .and_then(Value::as_str)
            .and_then(|timestamp| DateTime::parse_from_rfc3339(timestamp).ok())
            .map(|created_at| created_at.with_timezone(&Utc) >= latest_push_at)
            .unwrap_or(false);
    }
    if let Some(head_sha) = head_sha {
        return comment.get("commit_id").and_then(Value::as_str) == Some(head_sha)
            || comment.get("original_commit_id").and_then(Value::as_str) == Some(head_sha);
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Debug, Clone)]
    struct Poll {
        status: PrStatusSnapshot,
        reaction: BotBodyReview,
        comments: Vec<BotLineComment>,
    }

    impl Poll {
        fn new(ci: CiState, reaction: BotBodyReaction) -> Self {
            let head_review_watermark_at = timestamp("2026-04-30T08:00:00Z");
            let reaction = BotBodyReview::new(reaction, Some(timestamp("2026-04-30T08:01:00Z")));
            Self {
                status: PrStatusSnapshot {
                    ci,
                    mergeable: MergeableState::Mergeable,
                    head_sha: Some("head-sha".to_owned()),
                    head_review_watermark_at: Some(head_review_watermark_at),
                    latest_push_at: None,
                    pr_url: "https://github.com/acme/ralph-burning/pull/42".to_owned(),
                },
                reaction,
                comments: Vec::new(),
            }
        }

        fn with_comments(mut self, comments: Vec<BotLineComment>) -> Self {
            self.comments = comments;
            self
        }
    }

    #[derive(Debug, Default)]
    struct MockWatchTools {
        polls: Mutex<VecDeque<Poll>>,
        calls: Mutex<Vec<String>>,
        failed_run_logs: Mutex<Vec<String>>,
        reruns: Mutex<u32>,
        merges: Mutex<u32>,
        merge_head_shas: Mutex<Vec<String>>,
    }

    impl MockWatchTools {
        fn with_polls(polls: Vec<Poll>) -> Self {
            Self {
                polls: Mutex::new(VecDeque::from(polls)),
                ..Self::default()
            }
        }

        fn record(&self, call: &str) {
            self.calls.lock().unwrap().push(call.to_owned());
        }

        fn current_poll(&self) -> Poll {
            self.polls
                .lock()
                .unwrap()
                .front()
                .expect("missing poll fixture")
                .clone()
        }

        fn set_failed_run_logs(&self, logs: Vec<String>) {
            *self.failed_run_logs.lock().unwrap() = logs;
        }
    }

    impl PrWatchToolPort for MockWatchTools {
        async fn repo_slug(&self, _repo_root: &Path) -> Result<String, PrToolError> {
            self.record("repo_slug");
            Ok("acme/ralph-burning".to_owned())
        }

        async fn pr_status(
            &self,
            _repo_root: &Path,
            _repo_slug: &str,
            _pr_number: u64,
        ) -> Result<PrStatusSnapshot, PrToolError> {
            self.record("status");
            Ok(self.current_poll().status)
        }

        async fn codex_bot_reaction(
            &self,
            _repo_root: &Path,
            _repo_slug: &str,
            _pr_number: u64,
            head_sha: Option<&str>,
            head_review_watermark_at: Option<DateTime<Utc>>,
        ) -> Result<BotBodyReview, PrToolError> {
            self.record("reaction");
            let reaction = self.current_poll().reaction;
            if reaction.approved_head_sha.is_some() {
                Ok(reaction)
            } else {
                Ok(reaction.bind_to_head(head_sha, head_review_watermark_at))
            }
        }

        async fn codex_bot_line_comments_since_latest_push(
            &self,
            _repo_root: &Path,
            _repo_slug: &str,
            _pr_number: u64,
            _head_sha: Option<&str>,
            _latest_push_at: Option<DateTime<Utc>>,
        ) -> Result<Vec<BotLineComment>, PrToolError> {
            self.record("comments");
            Ok(self.current_poll().comments)
        }

        async fn failed_run_logs(
            &self,
            _repo_root: &Path,
            _run_ids: &[u64],
        ) -> Result<Vec<String>, PrToolError> {
            self.record("failed_run_logs");
            Ok(self.failed_run_logs.lock().unwrap().clone())
        }

        async fn rerun_failed_runs(
            &self,
            _repo_root: &Path,
            _pr_number: u64,
            _run_ids: &[u64],
        ) -> Result<(), PrToolError> {
            self.record("rerun");
            *self.reruns.lock().unwrap() += 1;
            Ok(())
        }

        async fn merge_pr(
            &self,
            _repo_root: &Path,
            _pr_number: u64,
            head_sha: &str,
        ) -> Result<PrMergeOutput, PrToolError> {
            self.record("merge");
            *self.merges.lock().unwrap() += 1;
            self.merge_head_shas
                .lock()
                .unwrap()
                .push(head_sha.to_owned());
            Ok(PrMergeOutput {
                sha: "merge-sha".to_owned(),
                pr_url: "https://github.com/acme/ralph-burning/pull/42".to_owned(),
            })
        }
    }

    #[derive(Default)]
    struct MockClock {
        elapsed: Mutex<Duration>,
        sleeps: Mutex<Vec<Duration>>,
        after_sleep: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    impl MockClock {
        fn with_after_sleep(after_sleep: Arc<dyn Fn() + Send + Sync>) -> Self {
            Self {
                after_sleep: Some(after_sleep),
                ..Self::default()
            }
        }
    }

    impl PrWatchClock for MockClock {
        fn elapsed(&self) -> Duration {
            *self.elapsed.lock().unwrap()
        }

        async fn sleep(&self, duration: Duration) {
            self.sleeps.lock().unwrap().push(duration);
            *self.elapsed.lock().unwrap() += duration;
            if let Some(after_sleep) = &self.after_sleep {
                after_sleep();
            }
        }
    }

    fn request(max_wait: Duration) -> PrWatchRequest<'static> {
        PrWatchRequest {
            base_dir: Path::new("/repo"),
            pr_number: 42,
            max_wait,
            poll_interval: Duration::from_secs(30),
        }
    }

    fn failed_ci(failing: Vec<String>) -> CiState {
        CiState::Failed {
            failing,
            failed_run_ids: vec![123456],
        }
    }

    fn timestamp(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn terminal_startup_failure_and_stale_checks_block_ci() {
        let raw = serde_json::json!({
            "nodes": [
                {
                    "name": "build",
                    "conclusion": "STARTUP_FAILURE",
                    "detailsUrl": "https://github.com/acme/ralph-burning/actions/runs/101/jobs/1"
                },
                {
                    "name": "lint",
                    "conclusion": "STALE"
                }
            ]
        });

        assert_eq!(
            parse_ci_state(Some(&raw)),
            CiState::Failed {
                failing: vec!["build".to_owned(), "lint".to_owned()],
                failed_run_ids: vec![101]
            }
        );
    }

    #[tokio::test]
    async fn happy_path_merges_when_ci_green_and_bot_approved() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            CiState::Success,
            BotBodyReaction::Approved,
        )]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::Merged {
                sha: "merge-sha".to_owned(),
                pr_url: "https://github.com/acme/ralph-burning/pull/42".to_owned()
            }
        );
        assert_eq!(*tools.merges.lock().unwrap(), 1);
        assert_eq!(
            tools.merge_head_shas.lock().unwrap().as_slice(),
            ["head-sha"]
        );
    }

    #[tokio::test]
    async fn approval_after_latest_push_merges_even_if_pr_was_updated_later() {
        let latest_push_at = timestamp("2026-04-30T08:00:00Z");
        let approved_at = timestamp("2026-04-30T08:05:00Z");
        let later_pr_updated_at = timestamp("2026-04-30T08:10:00Z");
        assert!(latest_push_at < approved_at);
        assert!(approved_at < later_pr_updated_at);

        let mut poll = Poll::new(CiState::Success, BotBodyReaction::Approved);
        poll.status.latest_push_at = Some(latest_push_at);
        poll.status.head_review_watermark_at = poll.status.latest_push_at;
        poll.reaction = BotBodyReview::new(BotBodyReaction::Approved, Some(approved_at));
        let tools = MockWatchTools::with_polls(vec![poll]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert!(matches!(outcome, WatchOutcome::Merged { .. }));
        assert_eq!(*tools.merges.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn stale_bot_approval_for_previous_head_does_not_merge() {
        let mut poll = Poll::new(CiState::Success, BotBodyReaction::Approved);
        poll.status.head_sha = Some("new-head-sha".to_owned());
        poll.status.head_review_watermark_at = Some(timestamp("2026-04-30T08:10:00Z"));
        poll.reaction = BotBodyReview::new(
            BotBodyReaction::Approved,
            Some(timestamp("2026-04-30T08:01:00Z")),
        );
        let tools = MockWatchTools::with_polls(vec![poll]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::Timeout {
                elapsed: Duration::from_secs(60)
            }
        );
        assert_eq!(*tools.merges.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn approval_for_different_head_sha_does_not_merge() {
        let mut poll = Poll::new(CiState::Success, BotBodyReaction::Approved);
        poll.status.head_sha = Some("new-head-sha".to_owned());
        poll.reaction.approved_head_sha = Some("old-head-sha".to_owned());
        let tools = MockWatchTools::with_polls(vec![poll]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::Timeout {
                elapsed: Duration::from_secs(60)
            }
        );
        assert_eq!(*tools.merges.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn happy_path_aborts_when_head_sha_is_missing() {
        let mut poll = Poll::new(CiState::Success, BotBodyReaction::Approved);
        poll.status.head_sha = None;
        let tools = MockWatchTools::with_polls(vec![poll]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::Aborted {
                reason: "cannot merge without observed PR head SHA".to_owned()
            }
        );
        assert_eq!(*tools.merges.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn ci_in_progress_polls_again_before_merge() {
        let tools = Arc::new(MockWatchTools::with_polls(vec![
            Poll::new(CiState::Pending, BotBodyReaction::Eyes),
            Poll::new(CiState::Success, BotBodyReaction::Approved),
        ]));
        let after_sleep = {
            let tools = Arc::clone(&tools);
            Arc::new(move || {
                tools.polls.lock().unwrap().pop_front();
            })
        };
        let clock = MockClock::with_after_sleep(after_sleep);

        let outcome = watch_pr(request(Duration::from_secs(60)), tools.as_ref(), &clock)
            .await
            .unwrap();

        assert!(matches!(outcome, WatchOutcome::Merged { .. }));
        assert_eq!(
            clock.sleeps.lock().unwrap().as_slice(),
            [Duration::from_secs(30)]
        );
    }

    #[tokio::test]
    async fn known_flake_reruns_once_then_merges_if_failed_run_log_contains_flake() {
        let tools = Arc::new(MockWatchTools::with_polls(vec![
            Poll::new(
                failed_ci(vec!["Build, Test, and Conformance Gate".to_owned()]),
                BotBodyReaction::Approved,
            ),
            Poll::new(CiState::Success, BotBodyReaction::Approved),
        ]));
        tools.set_failed_run_logs(vec![format!("test {} ... FAILED", KNOWN_CI_FLAKES[0])]);
        let after_sleep = {
            let tools = Arc::clone(&tools);
            Arc::new(move || {
                tools.polls.lock().unwrap().pop_front();
            })
        };
        let clock = MockClock::with_after_sleep(after_sleep);

        let outcome = watch_pr(request(Duration::from_secs(60)), tools.as_ref(), &clock)
            .await
            .unwrap();

        assert!(matches!(outcome, WatchOutcome::Merged { .. }));
        assert_eq!(*tools.reruns.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn known_flake_failing_twice_blocks_after_rerun() {
        let tools = Arc::new(MockWatchTools::with_polls(vec![
            Poll::new(
                failed_ci(vec!["Build, Test, and Conformance Gate".to_owned()]),
                BotBodyReaction::Approved,
            ),
            Poll::new(
                failed_ci(vec!["Build, Test, and Conformance Gate".to_owned()]),
                BotBodyReaction::Approved,
            ),
        ]));
        tools.set_failed_run_logs(vec![format!("test {} ... FAILED", KNOWN_CI_FLAKES[0])]);
        let after_sleep = {
            let tools = Arc::clone(&tools);
            Arc::new(move || {
                tools.polls.lock().unwrap().pop_front();
            })
        };
        let clock = MockClock::with_after_sleep(after_sleep);

        let outcome = watch_pr(request(Duration::from_secs(60)), tools.as_ref(), &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByCi {
                failing: vec![
                    "Build, Test, and Conformance Gate".to_owned(),
                    KNOWN_CI_FLAKES[0].to_owned()
                ],
                after_rerun: true
            }
        );
    }

    #[tokio::test]
    async fn non_flake_ci_failure_blocks_without_rerun() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            failed_ci(vec!["unit_tests::regression_failed".to_owned()]),
            BotBodyReaction::Approved,
        )]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByCi {
                failing: vec!["unit_tests::regression_failed".to_owned()],
                after_rerun: false
            }
        );
        assert_eq!(*tools.reruns.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn non_flake_failed_test_from_run_log_blocks_without_rerun() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            failed_ci(vec!["Build, Test, and Conformance Gate".to_owned()]),
            BotBodyReaction::Approved,
        )]);
        tools.set_failed_run_logs(vec![
            "test unit_tests::regression_failed ... FAILED".to_owned()
        ]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByCi {
                failing: vec![
                    "Build, Test, and Conformance Gate".to_owned(),
                    "unit_tests::regression_failed".to_owned(),
                ],
                after_rerun: false
            }
        );
        assert_eq!(*tools.reruns.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn detailed_non_flake_test_failure_is_not_rerun_even_with_known_flake_log_match() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            failed_ci(vec!["unit_tests::regression_failed".to_owned()]),
            BotBodyReaction::Approved,
        )]);
        tools.set_failed_run_logs(vec![format!("test {} ... FAILED", KNOWN_CI_FLAKES[0])]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByCi {
                failing: vec![
                    "unit_tests::regression_failed".to_owned(),
                    KNOWN_CI_FLAKES[0].to_owned()
                ],
                after_rerun: false
            }
        );
        assert_eq!(*tools.reruns.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn bot_line_comment_blocks_with_comment_text() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            CiState::Success,
            BotBodyReaction::Approved,
        )
        .with_comments(vec![BotLineComment {
            body: "please handle this case".to_owned(),
            path: Some("src/cli/pr.rs".to_owned()),
            line: Some(44),
            url: None,
        }])]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByBot {
                reasons: vec!["src/cli/pr.rs:44: please handle this case".to_owned()]
            }
        );
    }

    #[test]
    fn paginated_bot_line_comments_find_comment_on_second_page() {
        let output = CommandOutput {
            stdout: format!(
                r#"[[],[{{"user":{{"login":"{CODEX_BOT_LOGIN}"}},"body":"second page finding","path":"src/lib.rs","line":9,"html_url":"https://github.com/acme/ralph-burning/pull/42#discussion_r1","created_at":"2026-04-30T08:00:00Z","commit_id":"older-sha"}}]]"#
            ),
            stderr: String::new(),
            exit_code: 0,
        };
        let value = parse_paginated_json(&output, "gh api pull comments").unwrap();

        assert_eq!(
            parse_bot_line_comments(
                &value,
                Some("head-sha"),
                Some(
                    DateTime::parse_from_rfc3339("2026-04-30T07:59:00Z")
                        .unwrap()
                        .with_timezone(&Utc)
                )
            ),
            vec![BotLineComment {
                body: "second page finding".to_owned(),
                path: Some("src/lib.rs".to_owned()),
                line: Some(9),
                url: Some("https://github.com/acme/ralph-burning/pull/42#discussion_r1".to_owned()),
            }]
        );
    }

    #[test]
    fn graphql_head_push_timestamp_blocks_old_commit_comment_after_latest_push() {
        let graphql = serde_json::json!({
            "data": {
                "repository": {
                    "pullRequest": {
                        "commits": {
                            "nodes": [
                                {
                                    "commit": {
                                        "oid": "previous-sha",
                                        "pushedDate": "2026-04-30T07:30:00Z"
                                    }
                                },
                                {
                                    "commit": {
                                        "oid": "head-sha",
                                        "pushedDate": "2026-04-30T08:00:00Z"
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        });
        let comments = serde_json::json!([
            {
                "user": {"login": CODEX_BOT_LOGIN},
                "body": "finding after current head update",
                "path": "src/lib.rs",
                "line": 12,
                "created_at": "2026-04-30T08:01:00Z",
                "commit_id": "previous-sha"
            },
            {
                "user": {"login": CODEX_BOT_LOGIN},
                "body": "stale finding",
                "path": "src/lib.rs",
                "line": 9,
                "created_at": "2026-04-30T07:59:00Z",
                "commit_id": "head-sha"
            }
        ]);
        let latest_push_at = latest_head_push_at_from_graphql(&graphql, "head-sha");

        assert_eq!(latest_push_at, Some(timestamp("2026-04-30T08:00:00Z")));
        assert_eq!(
            parse_bot_line_comments(&comments, Some("head-sha"), latest_push_at),
            vec![BotLineComment {
                body: "finding after current head update".to_owned(),
                path: Some("src/lib.rs".to_owned()),
                line: Some(12),
                url: None,
            }]
        );
    }

    #[test]
    fn timeline_head_update_timestamp_handles_force_push_events() {
        let timeline = serde_json::json!([
            {
                "event": "head_ref_force_pushed",
                "commit_id": "old-head-sha",
                "created_at": "2026-04-30T07:45:00Z"
            },
            {
                "event": "head_ref_force_pushed",
                "commit_id": "head-sha",
                "created_at": "2026-04-30T08:15:00Z"
            }
        ]);

        assert_eq!(
            latest_head_timeline_update_at(&timeline, Some("head-sha")),
            Some(timestamp("2026-04-30T08:15:00Z"))
        );
    }

    #[test]
    fn timeline_head_update_wins_when_newer_than_graphql_pushed_date() {
        let graphql = serde_json::json!({
            "data": {
                "repository": {
                    "pullRequest": {
                        "commits": {
                            "nodes": [
                                {
                                    "commit": {
                                        "oid": "head-sha",
                                        "pushedDate": "2026-04-30T07:00:00Z"
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        });
        let timeline = serde_json::json!([
            {
                "event": "head_ref_force_pushed",
                "commit_id": "head-sha",
                "created_at": "2026-04-30T08:15:00Z"
            }
        ]);

        assert_eq!(
            latest_head_push_at_from_sources(&graphql, &timeline, "head-sha"),
            Some(timestamp("2026-04-30T08:15:00Z"))
        );
    }

    #[test]
    fn timeline_committed_event_timestamp_is_not_treated_as_push_time() {
        let timeline = serde_json::json!([
            {
                "event": "committed",
                "sha": "head-sha",
                "committer": {"date": "2026-04-30T08:00:00Z"}
            }
        ]);

        assert_eq!(
            latest_head_timeline_update_at(&timeline, Some("head-sha")),
            None
        );
    }

    #[test]
    fn codex_reaction_parser_preserves_approval_timestamp() {
        let value = serde_json::json!([
            {
                "user": {"login": CODEX_BOT_LOGIN},
                "content": "eyes",
                "created_at": "2026-04-30T08:00:00Z"
            },
            {
                "user": {"login": CODEX_BOT_LOGIN},
                "content": "+1",
                "created_at": "2026-04-30T08:05:00Z"
            }
        ]);

        assert_eq!(
            parse_codex_reaction(&value),
            BotBodyReview::new(
                BotBodyReaction::Approved,
                Some(timestamp("2026-04-30T08:05:00Z"))
            )
        );
    }

    #[test]
    fn codex_reaction_binds_approval_to_current_head_only_after_watermark() {
        let value = serde_json::json!([
            {
                "user": {"login": CODEX_BOT_LOGIN},
                "content": "+1",
                "created_at": "2026-04-30T08:05:00Z"
            }
        ]);

        let bound = parse_codex_reaction(&value)
            .bind_to_head(Some("head-sha"), Some(timestamp("2026-04-30T08:00:00Z")));
        assert_eq!(bound.approved_head_sha.as_deref(), Some("head-sha"));

        let stale = parse_codex_reaction(&value)
            .bind_to_head(Some("head-sha"), Some(timestamp("2026-04-30T08:10:00Z")));
        assert_eq!(stale.approved_head_sha, None);
    }

    #[test]
    fn latest_push_time_takes_precedence_over_head_sha_for_comment_filtering() {
        let before_push = serde_json::json!({
            "created_at": "2026-04-30T07:59:00Z",
            "commit_id": "head-sha",
        });
        let after_push_on_older_commit = serde_json::json!({
            "created_at": "2026-04-30T08:01:00Z",
            "commit_id": "older-sha",
        });
        let latest_push_at = DateTime::parse_from_rfc3339("2026-04-30T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert!(!comment_is_since_latest_push(
            &before_push,
            Some("head-sha"),
            Some(latest_push_at)
        ));
        assert!(comment_is_since_latest_push(
            &after_push_on_older_commit,
            Some("head-sha"),
            Some(latest_push_at)
        ));
    }

    #[test]
    fn comment_filter_falls_back_to_head_sha_without_push_time() {
        let head_comment = serde_json::json!({
            "created_at": "2026-04-30T08:01:00Z",
            "commit_id": "head-sha",
        });
        let older_comment = serde_json::json!({
            "created_at": "2026-04-30T08:02:00Z",
            "commit_id": "older-sha",
        });

        assert!(comment_is_since_latest_push(
            &head_comment,
            Some("head-sha"),
            None
        ));
        assert!(!comment_is_since_latest_push(
            &older_comment,
            Some("head-sha"),
            None
        ));
    }

    #[tokio::test]
    async fn bot_rejection_blocks() {
        let tools = MockWatchTools::with_polls(vec![Poll::new(
            CiState::Pending,
            BotBodyReaction::Rejected,
        )]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::BlockedByBot {
                reasons: vec!["rejected".to_owned()]
            }
        );
    }

    #[tokio::test]
    async fn timeout_returns_elapsed_duration() {
        let tools =
            MockWatchTools::with_polls(vec![Poll::new(CiState::Pending, BotBodyReaction::None)]);
        let clock = MockClock::default();

        let outcome = watch_pr(request(Duration::from_secs(60)), &tools, &clock)
            .await
            .unwrap();

        assert_eq!(
            outcome,
            WatchOutcome::Timeout {
                elapsed: Duration::from_secs(60)
            }
        );
        assert_eq!(
            clock.sleeps.lock().unwrap().as_slice(),
            [Duration::from_secs(30), Duration::from_secs(30)]
        );
    }
}
