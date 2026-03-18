//! GitHub adapter providing the P0 API surface required by slices 8 and 9.
//!
//! All operations go through `GithubClient`, which wraps `reqwest` and the
//! GitHub REST API. The adapter is designed for daemon use — every method is
//! self-contained and does not hold persistent connections between calls.

use serde::{Deserialize, Serialize};

use crate::shared::error::{AppError, AppResult};

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Configuration for the GitHub adapter.
#[derive(Debug, Clone)]
pub struct GithubClientConfig {
    /// Personal access token or app installation token.
    pub token: String,
    /// Base URL for the GitHub API (default: `https://api.github.com`).
    pub api_base_url: String,
}

impl GithubClientConfig {
    pub fn from_env() -> AppResult<Self> {
        let token = std::env::var("GITHUB_TOKEN").map_err(|_| AppError::InvalidConfigValue {
            key: "GITHUB_TOKEN".to_owned(),
            value: String::new(),
            reason: "GITHUB_TOKEN environment variable is not set".to_owned(),
        })?;
        let api_base_url =
            std::env::var("GITHUB_API_URL").unwrap_or_else(|_| "https://api.github.com".to_owned());
        Ok(Self {
            token,
            api_base_url,
        })
    }
}

/// GitHub REST API client for daemon operations.
pub struct GithubClient {
    config: GithubClientConfig,
    http: reqwest::Client,
}

impl GithubClient {
    pub fn new(config: GithubClientConfig) -> Self {
        let http = reqwest::Client::builder()
            .user_agent("ralph-burning-daemon/1.0")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("build reqwest client");
        Self { config, http }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}{}", self.config.api_base_url.trim_end_matches('/'), path)
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.config.token)
    }

    // ── Labels ────────────────────────────────────────────────────────

    /// Ensure a label exists on the repo, creating it if absent.
    pub async fn ensure_label(&self, owner: &str, repo: &str, label_name: &str) -> AppResult<()> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/labels/{label_name}"));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if resp.status().is_success() {
            return Ok(());
        }

        if resp.status().as_u16() == 404 {
            let create_url = self.api_url(&format!("/repos/{owner}/{repo}/labels"));
            let body = serde_json::json!({
                "name": label_name,
                "color": "ededed",
            });
            let create_resp = self
                .http
                .post(&create_url)
                .header("Authorization", self.auth_header())
                .header("Accept", "application/vnd.github+json")
                .json(&body)
                .send()
                .await
                .map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: e.to_string(),
                })?;

            if !create_resp.status().is_success() && create_resp.status().as_u16() != 422 {
                let status = create_resp.status();
                let text = create_resp.text().await.unwrap_or_default();
                return Err(AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to create label '{label_name}': {status} {text}"),
                });
            }
        }

        Ok(())
    }

    /// Ensure all labels in the vocabulary exist on the repo.
    pub async fn ensure_labels(&self, owner: &str, repo: &str, labels: &[&str]) -> AppResult<()> {
        for label in labels {
            self.ensure_label(owner, repo, label).await?;
        }
        Ok(())
    }

    // ── Issues ────────────────────────────────────────────────────────

    /// Poll candidate issues with a specific label.
    pub async fn poll_candidate_issues(
        &self,
        owner: &str,
        repo: &str,
        label: &str,
    ) -> AppResult<Vec<GithubIssue>> {
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues?labels={label}&state=open&sort=created&direction=asc"
        ));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to poll issues: {status} {text}"),
            });
        }

        let issues: Vec<GithubIssue> =
            resp.json()
                .await
                .map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to parse issues response: {e}"),
                })?;

        // Filter out pull requests (GitHub API returns PRs as issues too)
        Ok(issues
            .into_iter()
            .filter(|i| i.pull_request.is_none())
            .collect())
    }

    // ── Issue Labels ──────────────────────────────────────────────────

    /// Read labels on an issue.
    pub async fn read_issue_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<String>> {
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues/{issue_number}/labels"
        ));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to read labels: {status} {text}"),
            });
        }

        let labels: Vec<GithubLabel> =
            resp.json()
                .await
                .map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to parse labels: {e}"),
                })?;

        Ok(labels.into_iter().map(|l| l.name).collect())
    }

    /// Add a label to an issue.
    pub async fn add_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues/{issue_number}/labels"
        ));
        let body = serde_json::json!({ "labels": [label] });
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to add label '{label}': {status} {text}"),
            });
        }

        Ok(())
    }

    /// Remove a label from an issue.
    pub async fn remove_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues/{issue_number}/labels/{label}"
        ));
        let resp = self
            .http
            .delete(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        // 404 is acceptable — label may already be absent
        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to remove label '{label}': {status} {text}"),
            });
        }

        Ok(())
    }

    /// Replace all labels on an issue with the given set.
    pub async fn replace_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        labels: &[&str],
    ) -> AppResult<()> {
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues/{issue_number}/labels"
        ));
        let body = serde_json::json!({ "labels": labels });
        let resp = self
            .http
            .put(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to replace labels: {status} {text}"),
            });
        }

        Ok(())
    }

    // ── Comments ──────────────────────────────────────────────────────

    /// Fetch all comments on an issue (paginated).
    pub async fn fetch_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        let mut all_comments = Vec::new();
        let mut page = 1u32;
        loop {
            let url = self.api_url(&format!(
                "/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100&page={page}"
            ));
            let resp = self
                .http
                .get(&url)
                .header("Authorization", self.auth_header())
                .header("Accept", "application/vnd.github+json")
                .send()
                .await
                .map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: e.to_string(),
                })?;

            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                return Err(AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to fetch issue comments: {status} {text}"),
                });
            }

            let comments: Vec<GithubComment> =
                resp.json().await.map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to parse comments: {e}"),
                })?;
            let is_last_page = comments.len() < 100;
            all_comments.extend(comments);
            if is_last_page {
                break;
            }
            page += 1;
        }
        Ok(all_comments)
    }

    /// Post an idempotent comment (checks for existing comment with same marker).
    pub async fn post_idempotent_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        marker: &str,
        body: &str,
    ) -> AppResult<()> {
        // Check existing comments for the exact hidden HTML comment marker.
        // We match the full `<!-- marker -->` form instead of bare `contains(marker)`
        // to avoid false positives on ordinary user text that happens to include
        // the marker string.
        let hidden_marker = format!("<!-- {marker} -->");
        let comments = self.fetch_issue_comments(owner, repo, issue_number).await?;
        if comments.iter().any(|c| c.body.contains(&hidden_marker)) {
            return Ok(());
        }

        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/issues/{issue_number}/comments"
        ));
        let comment_body = format!("{body}\n\n<!-- {marker} -->");
        let payload = serde_json::json!({ "body": comment_body });
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to post comment: {status} {text}"),
            });
        }

        Ok(())
    }

    // ── Pull Requests ─────────────────────────────────────────────────

    /// Fetch PR review comments (inline code comments).
    pub async fn fetch_pr_review_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls/{pr_number}/comments"));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to fetch PR review comments: {status} {text}"),
            });
        }

        resp.json().await.map_err(|e| AppError::BackendUnavailable {
            backend: "github".to_owned(),
            details: format!("failed to parse PR review comments: {e}"),
        })
    }

    /// Fetch PR review summaries.
    pub async fn fetch_pr_reviews(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubReview>> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls/{pr_number}/reviews"));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to fetch PR reviews: {status} {text}"),
            });
        }

        resp.json().await.map_err(|e| AppError::BackendUnavailable {
            backend: "github".to_owned(),
            details: format!("failed to parse PR reviews: {e}"),
        })
    }

    /// Create a draft pull request.
    pub async fn create_draft_pr(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
    ) -> AppResult<GithubPullRequest> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls"));
        let payload = serde_json::json!({
            "title": title,
            "body": body,
            "head": head,
            "base": base,
            "draft": true,
        });
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to create draft PR: {status} {text}"),
            });
        }

        resp.json().await.map_err(|e| AppError::BackendUnavailable {
            backend: "github".to_owned(),
            details: format!("failed to parse PR response: {e}"),
        })
    }

    /// Mark a draft PR as ready for review.
    pub async fn mark_pr_ready(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()> {
        // GitHub REST API doesn't support this — requires GraphQL.
        let url = self.api_url("/graphql");
        let node_id = self.fetch_pr_node_id(owner, repo, pr_number).await?;
        let query = format!(
            r#"mutation {{ markPullRequestReadyForReview(input: {{ pullRequestId: "{node_id}" }}) {{ pullRequest {{ id }} }} }}"#,
        );
        let payload = serde_json::json!({ "query": query });
        let resp = self
            .http
            .post(&url)
            .header("Authorization", self.auth_header())
            .json(&payload)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to mark PR ready: {status} {body_text}"),
            });
        }

        // GitHub GraphQL can return HTTP 200 with application-level errors.
        // Parse the response and verify the mutation actually succeeded.
        let parsed: serde_json::Value =
            serde_json::from_str(&body_text).map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to parse GraphQL response: {e}"),
            })?;

        if let Some(errors) = parsed.get("errors") {
            if let Some(arr) = errors.as_array() {
                if !arr.is_empty() {
                    let msgs: Vec<String> = arr
                        .iter()
                        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                        .map(String::from)
                        .collect();
                    return Err(AppError::BackendUnavailable {
                        backend: "github".to_owned(),
                        details: format!("GraphQL errors marking PR ready: {}", msgs.join("; ")),
                    });
                }
            }
        }

        // Verify the expected data path is present.
        if parsed
            .pointer("/data/markPullRequestReadyForReview/pullRequest/id")
            .is_none()
        {
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("GraphQL response missing expected pullRequest.id: {body_text}"),
            });
        }

        Ok(())
    }

    /// Close a pull request.
    pub async fn close_pr(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls/{pr_number}"));
        let payload = serde_json::json!({ "state": "closed" });
        let resp = self
            .http
            .patch(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to close PR: {status} {text}"),
            });
        }

        Ok(())
    }

    /// Fetch PR URL and state.
    pub async fn fetch_pr_state(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<GithubPullRequest> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls/{pr_number}"));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to fetch PR state: {status} {text}"),
            });
        }

        resp.json().await.map_err(|e| AppError::BackendUnavailable {
            backend: "github".to_owned(),
            details: format!("failed to parse PR state: {e}"),
        })
    }

    /// Update PR body.
    pub async fn update_pr_body(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> AppResult<()> {
        let url = self.api_url(&format!("/repos/{owner}/{repo}/pulls/{pr_number}"));
        let payload = serde_json::json!({ "body": body });
        let resp = self
            .http
            .patch(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to update PR body: {status} {text}"),
            });
        }

        Ok(())
    }

    // ── Branch comparison ─────────────────────────────────────────────

    /// Detect whether a branch is ahead of base.
    pub async fn is_branch_ahead(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> AppResult<bool> {
        // URL-encode ref names: branch names like `rb/42-project` contain
        // slashes that must be percent-encoded in the compare URL path.
        let encode_ref = |r: &str| r.replace('%', "%25").replace('/', "%2F");
        let url = self.api_url(&format!(
            "/repos/{owner}/{repo}/compare/{}...{}",
            encode_ref(base),
            encode_ref(head),
        ));
        let resp = self
            .http
            .get(&url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await
            .map_err(|e| AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("failed to compare branches: {status} {text}"),
            });
        }

        let comparison: GithubComparison =
            resp.json()
                .await
                .map_err(|e| AppError::BackendUnavailable {
                    backend: "github".to_owned(),
                    details: format!("failed to parse comparison: {e}"),
                })?;

        Ok(comparison.ahead_by > 0)
    }

    // ── Internal helpers ──────────────────────────────────────────────

    async fn fetch_pr_node_id(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<String> {
        let pr = self.fetch_pr_state(owner, repo, pr_number).await?;
        Ok(pr.node_id)
    }
}

// ---------------------------------------------------------------------------
// GitHub API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubIssue {
    pub number: u64,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    pub labels: Vec<GithubLabel>,
    pub user: GithubUser,
    pub html_url: String,
    #[serde(default)]
    pub pull_request: Option<serde_json::Value>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubLabel {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubUser {
    pub login: String,
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubComment {
    pub id: u64,
    pub body: String,
    pub user: GithubUser,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubReview {
    pub id: u64,
    pub user: GithubUser,
    pub body: Option<String>,
    pub state: String,
    pub submitted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubPullRequest {
    pub number: u64,
    pub html_url: String,
    pub state: String,
    pub draft: Option<bool>,
    pub node_id: String,
    #[serde(default)]
    pub head: Option<GithubPrRef>,
    #[serde(default)]
    pub base: Option<GithubPrRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubPrRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub sha: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubComparison {
    pub ahead_by: u64,
    pub behind_by: u64,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Port trait for testability
// ---------------------------------------------------------------------------

/// Port trait abstracting GitHub operations for the daemon.
///
/// Production code uses `GithubClient`; tests use an in-memory stub.
#[allow(async_fn_in_trait)]
pub trait GithubPort: Send + Sync {
    async fn ensure_labels(&self, owner: &str, repo: &str, labels: &[&str]) -> AppResult<()>;
    async fn poll_candidate_issues(
        &self,
        owner: &str,
        repo: &str,
        label: &str,
    ) -> AppResult<Vec<GithubIssue>>;
    async fn read_issue_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<String>>;
    async fn add_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()>;
    async fn remove_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()>;
    async fn replace_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        labels: &[&str],
    ) -> AppResult<()>;
    async fn fetch_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<GithubComment>>;
    async fn post_idempotent_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        marker: &str,
        body: &str,
    ) -> AppResult<()>;

    // ── PR / branch operations (slice-9 handoff) ─────────────────────

    async fn fetch_pr_review_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubComment>>;

    async fn fetch_pr_reviews(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubReview>>;

    async fn create_draft_pr(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
    ) -> AppResult<GithubPullRequest>;

    async fn mark_pr_ready(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()>;

    async fn close_pr(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()>;

    async fn fetch_pr_state(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<GithubPullRequest>;

    async fn update_pr_body(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> AppResult<()>;

    async fn is_branch_ahead(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> AppResult<bool>;
}

impl GithubPort for GithubClient {
    async fn ensure_labels(&self, owner: &str, repo: &str, labels: &[&str]) -> AppResult<()> {
        self.ensure_labels(owner, repo, labels).await
    }

    async fn poll_candidate_issues(
        &self,
        owner: &str,
        repo: &str,
        label: &str,
    ) -> AppResult<Vec<GithubIssue>> {
        self.poll_candidate_issues(owner, repo, label).await
    }

    async fn read_issue_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<String>> {
        self.read_issue_labels(owner, repo, issue_number).await
    }

    async fn add_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        self.add_label(owner, repo, issue_number, label).await
    }

    async fn remove_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        self.remove_label(owner, repo, issue_number, label).await
    }

    async fn replace_labels(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        labels: &[&str],
    ) -> AppResult<()> {
        self.replace_labels(owner, repo, issue_number, labels).await
    }

    async fn fetch_issue_comments(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        self.fetch_issue_comments(owner, repo, issue_number).await
    }

    async fn post_idempotent_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: u64,
        marker: &str,
        body: &str,
    ) -> AppResult<()> {
        self.post_idempotent_comment(owner, repo, issue_number, marker, body)
            .await
    }

    async fn fetch_pr_review_comments(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        self.fetch_pr_review_comments(owner, repo, pr_number).await
    }

    async fn fetch_pr_reviews(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubReview>> {
        self.fetch_pr_reviews(owner, repo, pr_number).await
    }

    async fn create_draft_pr(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        base: &str,
    ) -> AppResult<GithubPullRequest> {
        self.create_draft_pr(owner, repo, title, body, head, base)
            .await
    }

    async fn mark_pr_ready(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()> {
        self.mark_pr_ready(owner, repo, pr_number).await
    }

    async fn close_pr(&self, owner: &str, repo: &str, pr_number: u64) -> AppResult<()> {
        self.close_pr(owner, repo, pr_number).await
    }

    async fn fetch_pr_state(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
    ) -> AppResult<GithubPullRequest> {
        self.fetch_pr_state(owner, repo, pr_number).await
    }

    async fn update_pr_body(
        &self,
        owner: &str,
        repo: &str,
        pr_number: u64,
        body: &str,
    ) -> AppResult<()> {
        self.update_pr_body(owner, repo, pr_number, body).await
    }

    async fn is_branch_ahead(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> AppResult<bool> {
        self.is_branch_ahead(owner, repo, base, head).await
    }
}

/// In-memory stub for testing. Stores state but does not call any real API.
#[derive(Debug, Default)]
pub struct InMemoryGithubClient {
    pub issues: std::sync::Mutex<Vec<GithubIssue>>,
    pub labels_ensured: std::sync::Mutex<Vec<String>>,
    pub pull_requests: std::sync::Mutex<Vec<GithubPullRequest>>,
    pub pr_review_comments: std::sync::Mutex<Vec<(u64, GithubComment)>>,
    pub pr_reviews: std::sync::Mutex<Vec<(u64, GithubReview)>>,
    /// Tracks which branches are ahead of base (key: `owner/repo:base...head`).
    pub branches_ahead: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Posted issue comments: `(issue_number, marker, body)`.
    pub posted_comments: std::sync::Mutex<Vec<(u64, String, String)>>,
    /// Updated PR bodies: `(pr_number, body)`.
    pub updated_pr_bodies: std::sync::Mutex<Vec<(u64, String)>>,
    next_pr_number: std::sync::Mutex<u64>,
    /// Repos for which `ensure_labels` should fail (set of `owner/repo` keys).
    ensure_labels_failure_repos: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Issue numbers for which `add_label` should fail (for label-sync failure testing).
    pub add_label_failure_issues: std::sync::Mutex<std::collections::HashSet<u64>>,
}

impl InMemoryGithubClient {
    pub fn new() -> Self {
        Self {
            next_pr_number: std::sync::Mutex::new(100),
            ..Self::default()
        }
    }

    pub fn with_issues(issues: Vec<GithubIssue>) -> Self {
        Self {
            issues: std::sync::Mutex::new(issues),
            next_pr_number: std::sync::Mutex::new(100),
            ..Self::default()
        }
    }

    /// Configure `ensure_labels` to fail for a specific `owner/repo` pair.
    pub fn set_ensure_labels_failure(&self, owner: &str, repo: &str) {
        self.ensure_labels_failure_repos
            .lock()
            .unwrap()
            .insert(format!("{owner}/{repo}"));
    }

    /// Configure `add_label` to fail for a specific issue number.
    pub fn set_add_label_failure(&self, issue_number: u64) {
        self.add_label_failure_issues
            .lock()
            .unwrap()
            .insert(issue_number);
    }

    /// Clear the `add_label` failure for a specific issue number.
    pub fn clear_add_label_failure(&self, issue_number: u64) {
        self.add_label_failure_issues
            .lock()
            .unwrap()
            .remove(&issue_number);
    }
}

impl GithubPort for InMemoryGithubClient {
    async fn ensure_labels(&self, owner: &str, repo: &str, labels: &[&str]) -> AppResult<()> {
        let key = format!("{owner}/{repo}");
        if self
            .ensure_labels_failure_repos
            .lock()
            .unwrap()
            .contains(&key)
        {
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("simulated ensure_labels failure for {key}"),
            });
        }
        let mut ensured = self.labels_ensured.lock().unwrap();
        for label in labels {
            if !ensured.contains(&label.to_string()) {
                ensured.push(label.to_string());
            }
        }
        Ok(())
    }

    async fn poll_candidate_issues(
        &self,
        _owner: &str,
        _repo: &str,
        label: &str,
    ) -> AppResult<Vec<GithubIssue>> {
        let issues = self.issues.lock().unwrap();
        Ok(issues
            .iter()
            .filter(|i| i.labels.iter().any(|l| l.name == label))
            .cloned()
            .collect())
    }

    async fn read_issue_labels(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<String>> {
        let issues = self.issues.lock().unwrap();
        Ok(issues
            .iter()
            .find(|i| i.number == issue_number)
            .map(|i| i.labels.iter().map(|l| l.name.clone()).collect())
            .unwrap_or_default())
    }

    async fn add_label(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        if self
            .add_label_failure_issues
            .lock()
            .unwrap()
            .contains(&issue_number)
        {
            return Err(AppError::BackendUnavailable {
                backend: "github".to_owned(),
                details: format!("simulated add_label failure for issue {issue_number}"),
            });
        }
        let mut issues = self.issues.lock().unwrap();
        if let Some(issue) = issues.iter_mut().find(|i| i.number == issue_number) {
            if !issue.labels.iter().any(|l| l.name == label) {
                issue.labels.push(GithubLabel {
                    name: label.to_owned(),
                });
            }
        }
        Ok(())
    }

    async fn remove_label(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
        label: &str,
    ) -> AppResult<()> {
        let mut issues = self.issues.lock().unwrap();
        if let Some(issue) = issues.iter_mut().find(|i| i.number == issue_number) {
            issue.labels.retain(|l| l.name != label);
        }
        Ok(())
    }

    async fn replace_labels(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
        labels: &[&str],
    ) -> AppResult<()> {
        let mut issues = self.issues.lock().unwrap();
        if let Some(issue) = issues.iter_mut().find(|i| i.number == issue_number) {
            issue.labels = labels
                .iter()
                .map(|l| GithubLabel {
                    name: l.to_string(),
                })
                .collect();
        }
        Ok(())
    }

    async fn fetch_issue_comments(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        let posted = self.posted_comments.lock().unwrap();
        Ok(posted
            .iter()
            .filter(|(num, _, _)| *num == issue_number)
            .map(|(_, marker, body)| GithubComment {
                id: 0,
                body: format!("<!-- {marker} -->\n{body}"),
                user: GithubUser {
                    login: "ralph-burning-bot".to_owned(),
                    id: 0,
                },
                created_at: String::new(),
                updated_at: String::new(),
            })
            .collect())
    }

    async fn post_idempotent_comment(
        &self,
        _owner: &str,
        _repo: &str,
        issue_number: u64,
        marker: &str,
        body: &str,
    ) -> AppResult<()> {
        let mut posted = self.posted_comments.lock().unwrap();
        // Idempotent: replace existing comment with same marker, or add new.
        if let Some(existing) = posted
            .iter_mut()
            .find(|(num, m, _)| *num == issue_number && m == marker)
        {
            existing.2 = body.to_owned();
        } else {
            posted.push((issue_number, marker.to_owned(), body.to_owned()));
        }
        Ok(())
    }

    async fn fetch_pr_review_comments(
        &self,
        _owner: &str,
        _repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubComment>> {
        let comments = self.pr_review_comments.lock().unwrap();
        Ok(comments
            .iter()
            .filter(|(pr, _)| *pr == pr_number)
            .map(|(_, c)| c.clone())
            .collect())
    }

    async fn fetch_pr_reviews(
        &self,
        _owner: &str,
        _repo: &str,
        pr_number: u64,
    ) -> AppResult<Vec<GithubReview>> {
        let reviews = self.pr_reviews.lock().unwrap();
        Ok(reviews
            .iter()
            .filter(|(pr, _)| *pr == pr_number)
            .map(|(_, r)| r.clone())
            .collect())
    }

    async fn create_draft_pr(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: &str,
        head: &str,
        _base: &str,
    ) -> AppResult<GithubPullRequest> {
        let mut prs = self.pull_requests.lock().unwrap();
        let mut next = self.next_pr_number.lock().unwrap();
        let number = *next;
        *next += 1;
        let pr = GithubPullRequest {
            number,
            html_url: format!("https://github.com/{owner}/{repo}/pull/{number}"),
            state: "open".to_owned(),
            draft: Some(true),
            node_id: format!("PR_{number}"),
            head: Some(GithubPrRef {
                ref_name: head.to_owned(),
                sha: "0000000".to_owned(),
            }),
            base: None,
        };
        prs.push(pr.clone());
        // Suppress unused variable warnings by using title/body in debug context
        let _ = (title, body);
        Ok(pr)
    }

    async fn mark_pr_ready(&self, _owner: &str, _repo: &str, pr_number: u64) -> AppResult<()> {
        let mut prs = self.pull_requests.lock().unwrap();
        if let Some(pr) = prs.iter_mut().find(|p| p.number == pr_number) {
            pr.draft = Some(false);
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: "github-inmemory".to_owned(),
                details: format!("PR #{pr_number} not found"),
            })
        }
    }

    async fn close_pr(&self, _owner: &str, _repo: &str, pr_number: u64) -> AppResult<()> {
        let mut prs = self.pull_requests.lock().unwrap();
        if let Some(pr) = prs.iter_mut().find(|p| p.number == pr_number) {
            pr.state = "closed".to_owned();
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: "github-inmemory".to_owned(),
                details: format!("PR #{pr_number} not found"),
            })
        }
    }

    async fn fetch_pr_state(
        &self,
        _owner: &str,
        _repo: &str,
        pr_number: u64,
    ) -> AppResult<GithubPullRequest> {
        let prs = self.pull_requests.lock().unwrap();
        prs.iter()
            .find(|p| p.number == pr_number)
            .cloned()
            .ok_or_else(|| AppError::BackendUnavailable {
                backend: "github-inmemory".to_owned(),
                details: format!("PR #{pr_number} not found"),
            })
    }

    async fn update_pr_body(
        &self,
        _owner: &str,
        _repo: &str,
        pr_number: u64,
        body: &str,
    ) -> AppResult<()> {
        let prs = self.pull_requests.lock().unwrap();
        if prs.iter().any(|p| p.number == pr_number) {
            let mut bodies = self.updated_pr_bodies.lock().unwrap();
            bodies.push((pr_number, body.to_owned()));
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: "github-inmemory".to_owned(),
                details: format!("PR #{pr_number} not found"),
            })
        }
    }

    async fn is_branch_ahead(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> AppResult<bool> {
        let ahead = self.branches_ahead.lock().unwrap();
        Ok(ahead.contains(&format!("{owner}/{repo}:{base}...{head}")))
    }
}
