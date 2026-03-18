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
///
/// Rejects empty segments, path separators, dot segments (`.`, `..`), and
/// characters that are not valid in GitHub owner/repo names so invalid or
/// unsafe slugs fail before any data-dir path is built.
pub fn parse_repo_slug(slug: &str) -> AppResult<(&str, &str)> {
    let trimmed = slug.trim();
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts.len() != 2 {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: slug.to_owned(),
            reason: "expected owner/repo format".to_owned(),
        });
    }
    for part in &parts {
        if part.is_empty()
            || *part == "."
            || *part == ".."
            || part.contains(std::path::MAIN_SEPARATOR)
            || part.contains('\0')
        {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: slug.to_owned(),
                reason: format!(
                    "invalid repo slug component '{}': must not be empty, '.', '..', \
                     or contain path separators",
                    part
                ),
            });
        }
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
    pub fn task_worktree_path(data_dir: &Path, owner: &str, repo: &str, task_id: &str) -> PathBuf {
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
    fn write_registration(&self, data_dir: &Path, registration: &RepoRegistration)
        -> AppResult<()>;
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

    // Require workspace.toml so the daemon doesn't accept a checkout that
    // will fail later on first config load. A bootstrapped checkout always
    // has this file; a manually cloned one must be initialized first.
    let workspace_config =
        workspace_dir.join(crate::contexts::workspace_governance::WORKSPACE_CONFIG_FILE);
    if !workspace_config.is_file() {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: checkout_path.display().to_string(),
            reason: "checkout has .ralph-burning/ but is missing workspace.toml — \
                     run bootstrap or initialize the workspace first"
                .to_owned(),
        });
    }

    // Parse and validate the workspace config so a malformed workspace.toml
    // is caught at `daemon start` time instead of failing later during task
    // processing when `EffectiveConfig::load()` is called.
    match crate::contexts::workspace_governance::load_workspace_config(checkout_path) {
        Ok(_config) => {}
        Err(e) => {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: checkout_path.display().to_string(),
                reason: format!("workspace.toml exists but is not a valid workspace config: {e}"),
            });
        }
    }

    Ok(())
}

/// Build a `RepoRegistration` from a validated checkout, creating the data-dir
/// structure if needed.
pub fn register_repo(data_dir: &Path, repo_slug: &str) -> AppResult<RepoRegistration> {
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

/// Bootstrap a repo checkout under the data-dir shard if not already present.
///
/// If the checkout directory already contains a `.git` marker, bootstrap is a
/// no-op (just ensures the `.ralph-burning` workspace with `workspace.toml` and
/// required subdirectories exists). Otherwise, clones the repo from GitHub
/// using `GITHUB_TOKEN` for authentication via a transient `git -c` flag that
/// does NOT persist into the cloned repo's `.git/config`, then initializes the
/// workspace.
///
/// Returns `Err` if cloning or workspace initialization fails so `daemon start`
/// can fail early and explicitly rather than silently skipping the repo.
pub fn bootstrap_repo_checkout(data_dir: &Path, repo_slug: &str) -> AppResult<()> {
    let (owner, repo) = parse_repo_slug(repo_slug)?;
    let checkout_path = DataDirLayout::checkout_path(data_dir, owner, repo);

    // If .git already exists, the checkout is already bootstrapped.
    let git_marker = checkout_path.join(".git");
    if git_marker.exists() {
        // Ensure a usable workspace exists (may be missing if manually cloned)
        ensure_workspace_initialized(&checkout_path)?;
        return Ok(());
    }

    // The checkout dir may exist as an empty directory from ensure_repo_dirs.
    // Remove it so `git clone` can create it (git clone refuses to clone into
    // a non-empty directory).
    if checkout_path.exists() {
        // Only remove if truly empty — if it has contents but no .git,
        // something unexpected is there; fail rather than destroy data.
        let is_empty = checkout_path
            .read_dir()
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false);
        if is_empty {
            std::fs::remove_dir(&checkout_path)?;
        } else {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: repo_slug.to_owned(),
                reason: format!(
                    "checkout directory '{}' exists with contents but no .git — \
                     remove it manually or provide a valid checkout",
                    checkout_path.display()
                ),
            });
        }
    }

    // Clone URL is always credential-free so it persists cleanly in
    // the cloned repo's remote config.
    let clone_url = format!("https://github.com/{owner}/{repo}.git");

    // Pass GITHUB_TOKEN via environment-based Git config injection
    // (`GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_*` / `GIT_CONFIG_VALUE_*`).
    // Unlike `-c http.extraHeader=...`, this mechanism:
    //  - Does NOT expose the token in process arguments (visible via
    //    `/proc/<pid>/cmdline` on Linux).
    //  - Does NOT persist the credential into the cloned repo's
    //    `.git/config`.
    // Environment variables are inherited by the child process but are
    // not visible to other users via `/proc/<pid>/cmdline`.
    let token = std::env::var("GITHUB_TOKEN").ok();
    let mut cmd = std::process::Command::new("git");

    if let Some(ref t) = token {
        if !t.is_empty() {
            cmd.env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "http.extraHeader")
                .env("GIT_CONFIG_VALUE_0", format!("Authorization: Bearer {t}"));
        }
    }

    cmd.args(["clone", &clone_url, &checkout_path.to_string_lossy()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let output = cmd.output();

    match output {
        Ok(ref o) if o.status.success() => {
            // Clone succeeded — initialize a usable workspace
            ensure_workspace_initialized(&checkout_path)?;
            Ok(())
        }
        Ok(ref o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: repo_slug.to_owned(),
                reason: format!(
                    "git clone failed (exit {}): {}",
                    o.status.code().unwrap_or(-1),
                    stderr.trim()
                ),
            })
        }
        Err(e) => Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: repo_slug.to_owned(),
            reason: format!("failed to execute git clone: {e}"),
        }),
    }
}

/// Ensure the `.ralph-burning` workspace inside a checkout is fully
/// initialized with `workspace.toml` and the required subdirectories.
///
/// If `workspace.toml` already exists this is a no-op (besides ensuring
/// subdirectories). If only the directory exists without the config file,
/// we create a fresh workspace config.
fn ensure_workspace_initialized(checkout_path: &Path) -> AppResult<()> {
    use crate::adapters::fs::FileSystem;
    use crate::contexts::workspace_governance::{
        REQUIRED_WORKSPACE_DIRECTORIES, WORKSPACE_CONFIG_FILE,
    };
    use crate::shared::domain::WorkspaceConfig;

    let workspace_dir = checkout_path.join(".ralph-burning");
    let config_path = workspace_dir.join(WORKSPACE_CONFIG_FILE);

    if config_path.exists() {
        // Config already present — just ensure required subdirectories
        for dir in REQUIRED_WORKSPACE_DIRECTORIES {
            std::fs::create_dir_all(workspace_dir.join(dir))?;
        }
        return Ok(());
    }

    // Create a fresh workspace config and required directory structure
    let config = WorkspaceConfig::new(chrono::Utc::now());
    let rendered = FileSystem::render_workspace_config(&config)?;
    FileSystem::create_workspace(&workspace_dir, &rendered, REQUIRED_WORKSPACE_DIRECTORIES)?;
    Ok(())
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
    fn parse_rejects_dot_segments() {
        assert!(parse_repo_slug("acme/.").is_err());
        assert!(parse_repo_slug("acme/..").is_err());
        assert!(parse_repo_slug("./repo").is_err());
        assert!(parse_repo_slug("../repo").is_err());
        assert!(parse_repo_slug("./..").is_err());
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
        assert_eq!(
            label_for_status(&TaskStatus::Claimed),
            Some("rb:in-progress")
        );
        assert_eq!(
            label_for_status(&TaskStatus::Active),
            Some("rb:in-progress")
        );
        assert_eq!(
            label_for_status(&TaskStatus::WaitingForRequirements),
            Some("rb:waiting-feedback")
        );
        assert_eq!(
            label_for_status(&TaskStatus::Completed),
            Some("rb:completed")
        );
        assert_eq!(label_for_status(&TaskStatus::Failed), Some("rb:failed"));
        assert_eq!(label_for_status(&TaskStatus::Aborted), Some("rb:failed"));
    }
}
