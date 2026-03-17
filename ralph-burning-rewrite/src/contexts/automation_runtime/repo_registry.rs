use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::shared::error::{AppError, AppResult};

/// Durable registration for a repo managed by the multi-repo daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoRegistration {
    /// Canonical slug in `owner/repo` form.
    pub repo_slug: String,
    /// Absolute path to the repo checkout (or bare clone).
    pub repo_root: PathBuf,
    /// Absolute path to the `.ralph-burning` workspace inside the checkout.
    pub workspace_root: PathBuf,
}

/// Required label vocabulary that must exist on every registered repo.
pub const LABEL_VOCABULARY: &[&str] = &[
    "rb:ready",
    "rb:in-progress",
    "rb:failed",
    "rb:completed",
    "rb:flow:standard",
    "rb:flow:quick_dev",
    "rb:flow:docs_change",
    "rb:flow:ci_improvement",
    "rb:requirements",
    "rb:waiting-feedback",
];

/// Status-label mapping for daemon task state.
pub fn label_for_status(status: &super::model::TaskStatus) -> Option<&'static str> {
    use super::model::TaskStatus;
    match status {
        TaskStatus::Pending => Some("rb:ready"),
        TaskStatus::Claimed | TaskStatus::Active => Some("rb:in-progress"),
        TaskStatus::WaitingForRequirements => Some("rb:waiting-feedback"),
        TaskStatus::Completed => Some("rb:completed"),
        TaskStatus::Failed | TaskStatus::Aborted => Some("rb:failed"),
    }
}

/// Parse and validate a repo slug in `owner/repo` form.
pub fn parse_repo_slug(slug: &str) -> AppResult<(&str, &str)> {
    let trimmed = slug.trim();
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts.len() != 2
        || parts[0].is_empty()
        || parts[1].is_empty()
        || parts.iter().any(|p| p.contains(std::path::MAIN_SEPARATOR))
    {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: slug.to_owned(),
            reason: "expected owner/repo format".to_owned(),
        });
    }
    Ok((parts[0], parts[1]))
}

/// Data-dir layout helpers.
///
/// ```text
/// <data-dir>/
///   repos/<owner>/<repo>/
///     repo/                 # checkout root
///     worktrees/            # per-task worktrees
///     daemon/
///       tasks/
///       leases/
///       journal.ndjson
/// ```
pub struct DataDirLayout;

impl DataDirLayout {
    /// Root for a specific repo within the data dir.
    pub fn repo_root(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        data_dir.join("repos").join(owner).join(repo)
    }

    /// Path to the repo checkout directory.
    pub fn checkout_path(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::repo_root(data_dir, owner, repo).join("repo")
    }

    /// Path to the worktrees directory for a repo.
    pub fn worktrees_dir(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::repo_root(data_dir, owner, repo).join("worktrees")
    }

    /// Path to a specific task worktree.
    pub fn task_worktree_path(
        data_dir: &Path,
        owner: &str,
        repo: &str,
        task_id: &str,
    ) -> PathBuf {
        Self::worktrees_dir(data_dir, owner, repo).join(task_id)
    }

    /// Path to the daemon state directory for a repo.
    pub fn daemon_dir(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::repo_root(data_dir, owner, repo).join("daemon")
    }

    /// Path to the tasks directory for a repo.
    pub fn tasks_dir(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::daemon_dir(data_dir, owner, repo).join("tasks")
    }

    /// Path to the leases directory for a repo.
    pub fn leases_dir(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::daemon_dir(data_dir, owner, repo).join("leases")
    }

    /// Path to the daemon journal for a repo.
    pub fn journal_path(data_dir: &Path, owner: &str, repo: &str) -> PathBuf {
        Self::daemon_dir(data_dir, owner, repo).join("journal.ndjson")
    }

    /// Branch name for a task: `rb/<issue-number>-<project-id>`.
    pub fn branch_name(issue_number: u64, project_id: &str) -> String {
        format!("rb/{issue_number}-{project_id}")
    }

    /// Ensure the required directory structure exists for a repo.
    pub fn ensure_repo_dirs(data_dir: &Path, owner: &str, repo: &str) -> AppResult<()> {
        let dirs = [
            Self::checkout_path(data_dir, owner, repo),
            Self::worktrees_dir(data_dir, owner, repo),
            Self::tasks_dir(data_dir, owner, repo),
            Self::leases_dir(data_dir, owner, repo),
        ];
        for dir in &dirs {
            std::fs::create_dir_all(dir)?;
        }
        Ok(())
    }
}

/// Port trait for persisting and reading repo registrations.
pub trait RepoRegistryPort {
    fn list_registrations(&self, data_dir: &Path) -> AppResult<Vec<RepoRegistration>>;
    fn read_registration(&self, data_dir: &Path, repo_slug: &str) -> AppResult<RepoRegistration>;
    fn write_registration(
        &self,
        data_dir: &Path,
        registration: &RepoRegistration,
    ) -> AppResult<()>;
}

/// Validate that the data-dir exists and is usable.
pub fn validate_data_dir(data_dir: &Path) -> AppResult<()> {
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }
    if !data_dir.is_dir() {
        return Err(AppError::InvalidConfigValue {
            key: "data-dir".to_owned(),
            value: data_dir.display().to_string(),
            reason: "path exists but is not a directory".to_owned(),
        });
    }
    Ok(())
}

/// Validate that a repo checkout exists, is a usable Git checkout, and has
/// a `.ralph-burning` workspace. This catches unusable directories early at
/// `daemon start` time instead of failing later on worktree/rebase operations.
pub fn validate_repo_checkout(checkout_path: &Path) -> AppResult<()> {
    if !checkout_path.is_dir() {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: checkout_path.display().to_string(),
            reason: "checkout directory does not exist".to_owned(),
        });
    }

    // Verify this is a usable Git checkout: .git must exist as a directory
    // (normal repo) or a file (worktree/submodule).
    let git_marker = checkout_path.join(".git");
    if !git_marker.exists() {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: checkout_path.display().to_string(),
            reason: "checkout is not a Git repository (missing .git)".to_owned(),
        });
    }

    // Quick sanity check: run `git rev-parse --git-dir` to verify Git can
    // actually use this checkout. This catches corrupt or incomplete clones.
    let git_check = std::process::Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .current_dir(checkout_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    match git_check {
        Ok(status) if !status.success() => {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: checkout_path.display().to_string(),
                reason: "checkout has .git but is not a usable Git repository".to_owned(),
            });
        }
        Err(e) => {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: checkout_path.display().to_string(),
                reason: format!("failed to verify Git checkout: {e}"),
            });
        }
        _ => {}
    }

    let workspace_dir = checkout_path.join(".ralph-burning");
    if !workspace_dir.is_dir() {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: checkout_path.display().to_string(),
            reason: "checkout is missing .ralph-burning workspace directory".to_owned(),
        });
    }
    Ok(())
}

/// Build a `RepoRegistration` from a validated checkout, creating the data-dir
/// structure if needed.
pub fn register_repo(
    data_dir: &Path,
    repo_slug: &str,
) -> AppResult<RepoRegistration> {
    let (owner, repo) = parse_repo_slug(repo_slug)?;
    let checkout_path = DataDirLayout::checkout_path(data_dir, owner, repo);
    DataDirLayout::ensure_repo_dirs(data_dir, owner, repo)?;

    let workspace_root = checkout_path.join(".ralph-burning");

    Ok(RepoRegistration {
        repo_slug: repo_slug.to_owned(),
        repo_root: checkout_path,
        workspace_root,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_slug() {
        let (owner, repo) = parse_repo_slug("acme/widgets").unwrap();
        assert_eq!(owner, "acme");
        assert_eq!(repo, "widgets");
    }

    #[test]
    fn parse_invalid_slugs() {
        assert!(parse_repo_slug("").is_err());
        assert!(parse_repo_slug("noslash").is_err());
        assert!(parse_repo_slug("too/many/parts").is_err());
        assert!(parse_repo_slug("/repo").is_err());
        assert!(parse_repo_slug("owner/").is_err());
    }

    #[test]
    fn data_dir_layout_paths() {
        let data_dir = Path::new("/tmp/daemon-data");
        assert_eq!(
            DataDirLayout::checkout_path(data_dir, "acme", "widgets"),
            PathBuf::from("/tmp/daemon-data/repos/acme/widgets/repo")
        );
        assert_eq!(
            DataDirLayout::task_worktree_path(data_dir, "acme", "widgets", "task-1"),
            PathBuf::from("/tmp/daemon-data/repos/acme/widgets/worktrees/task-1")
        );
        assert_eq!(
            DataDirLayout::journal_path(data_dir, "acme", "widgets"),
            PathBuf::from("/tmp/daemon-data/repos/acme/widgets/daemon/journal.ndjson")
        );
    }

    #[test]
    fn branch_name_format() {
        assert_eq!(
            DataDirLayout::branch_name(42, "my-project"),
            "rb/42-my-project"
        );
    }

    #[test]
    fn label_for_status_mapping() {
        use super::super::model::TaskStatus;
        assert_eq!(label_for_status(&TaskStatus::Pending), Some("rb:ready"));
        assert_eq!(label_for_status(&TaskStatus::Claimed), Some("rb:in-progress"));
        assert_eq!(label_for_status(&TaskStatus::Active), Some("rb:in-progress"));
        assert_eq!(
            label_for_status(&TaskStatus::WaitingForRequirements),
            Some("rb:waiting-feedback")
        );
        assert_eq!(label_for_status(&TaskStatus::Completed), Some("rb:completed"));
        assert_eq!(label_for_status(&TaskStatus::Failed), Some("rb:failed"));
        assert_eq!(label_for_status(&TaskStatus::Aborted), Some("rb:failed"));
    }
}
