use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::contexts::automation_runtime::WorktreePort;
use crate::contexts::project_run_record::service::RepositoryResetPort;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Default, Clone, Copy)]
pub struct WorktreeAdapter;

impl WorktreeAdapter {
    fn git(repo_root: &Path, args: &[&str]) -> AppResult<std::process::Output> {
        Command::new("git")
            .args(args)
            .current_dir(repo_root)
            .output()
            .map_err(AppError::from)
    }

    fn git_in(dir: &Path, args: &[&str]) -> AppResult<std::process::Output> {
        Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .map_err(AppError::from)
    }

    fn default_branch_ref(&self, repo_root: &Path) -> AppResult<String> {
        let origin_head = Self::git(
            repo_root,
            &[
                "symbolic-ref",
                "--quiet",
                "--short",
                "refs/remotes/origin/HEAD",
            ],
        )?;
        if origin_head.status.success() {
            let value = String::from_utf8_lossy(&origin_head.stdout)
                .trim()
                .to_owned();
            if !value.is_empty() {
                return Ok(value);
            }
        }

        let current = Self::git(repo_root, &["branch", "--show-current"])?;
        if current.status.success() {
            let branch = String::from_utf8_lossy(&current.stdout).trim().to_owned();
            if !branch.is_empty() {
                return Ok(branch);
            }
        }

        Ok("HEAD".to_owned())
    }

    pub fn current_head_sha(&self, repo_root: &Path) -> AppResult<Option<String>> {
        let output = Self::git(repo_root, &["rev-parse", "HEAD"])?;
        if !output.status.success() {
            return Ok(None);
        }

        let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if sha.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sha))
        }
    }

    pub fn hard_reset_to_sha(&self, repo_root: &Path, sha: &str) -> AppResult<()> {
        let output = Self::git(repo_root, &["reset", "--hard", sha])?;
        if output.status.success() {
            return Ok(());
        }

        Err(AppError::Io(std::io::Error::other(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )))
    }
}

impl RepositoryResetPort for WorktreeAdapter {
    fn reset_to_sha(&self, repo_root: &Path, sha: &str) -> AppResult<()> {
        self.hard_reset_to_sha(repo_root, sha)
    }
}

impl WorktreePort for WorktreeAdapter {
    fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/task/{task_id}")
    }

    fn create_worktree(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
        task_id: &str,
    ) -> AppResult<()> {
        if let Some(parent) = worktree_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if worktree_path.exists() {
            return Err(AppError::WorktreeCreationFailed {
                task_id: task_id.to_owned(),
                details: format!("worktree path '{}' already exists", worktree_path.display()),
            });
        }

        let base_ref = self.default_branch_ref(repo_root)?;
        let output = Self::git(
            repo_root,
            &[
                "worktree",
                "add",
                "-B",
                branch_name,
                &worktree_path.to_string_lossy(),
                &base_ref,
            ],
        )?;

        if output.status.success() {
            return Ok(());
        }

        Err(AppError::WorktreeCreationFailed {
            task_id: task_id.to_owned(),
            details: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }

    fn remove_worktree(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        task_id: &str,
    ) -> AppResult<()> {
        if !worktree_path.exists() {
            return Ok(());
        }

        let output = Self::git(
            repo_root,
            &[
                "worktree",
                "remove",
                "--force",
                &worktree_path.to_string_lossy(),
            ],
        )?;
        if !output.status.success() {
            return Err(AppError::WorktreeRemovalFailed {
                task_id: task_id.to_owned(),
                details: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }

        if worktree_path.exists() {
            fs::remove_dir_all(worktree_path).map_err(|error| AppError::WorktreeRemovalFailed {
                task_id: task_id.to_owned(),
                details: error.to_string(),
            })?;
        }

        Ok(())
    }

    fn rebase_onto_default_branch(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> AppResult<()> {
        let upstream = self.default_branch_ref(repo_root)?;
        let output = Self::git_in(worktree_path, &["rebase", &upstream])?;
        if output.status.success() {
            return Ok(());
        }

        let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
        Err(AppError::RebaseConflict {
            branch_name: branch_name.to_owned(),
            details: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        })
    }
}
