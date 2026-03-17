use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use uuid::Uuid;

use crate::contexts::automation_runtime::{WorktreeCleanupOutcome, WorktreePort};
use crate::contexts::project_run_record::service::RepositoryResetPort;
use crate::contexts::workflow_composition::checkpoints::{
    checkpoint_body, checkpoint_subject, parse_checkpoint_commit_message, VcsCheckpointPort,
};
use crate::contexts::workspace_governance::WORKSPACE_DIR;
use crate::shared::domain::{ProjectId, RunId, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Default, Clone, Copy)]
pub struct WorktreeAdapter;

struct TemporaryGitIndex {
    path: PathBuf,
}

impl TemporaryGitIndex {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("rb-checkpoint-index-{}", Uuid::new_v4())),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryGitIndex {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl WorktreeAdapter {
    fn git_error(output: &Output) -> AppError {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let message = if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            "git command failed".to_owned()
        };

        AppError::Io(std::io::Error::other(message))
    }

    fn git_command_with_index(dir: &Path, index_path: &Path) -> Command {
        let mut command = Self::git_command(dir);
        command.env("GIT_INDEX_FILE", index_path);
        command
    }

    fn git_in_index(repo_root: &Path, index_path: &Path, args: &[&str]) -> AppResult<Output> {
        Self::git_command_with_index(repo_root, index_path)
            .args(args)
            .output()
            .map_err(AppError::from)
    }

    fn build_checkpoint_tree(repo_root: &Path, parent_sha: Option<&str>) -> AppResult<String> {
        let checkpoint_index = TemporaryGitIndex::new();
        let index_path = checkpoint_index.path();

        let read_tree_output = match parent_sha {
            Some(parent_sha) => {
                Self::git_in_index(repo_root, index_path, &["read-tree", parent_sha])?
            }
            None => Self::git_in_index(repo_root, index_path, &["read-tree", "--empty"])?,
        };
        if !read_tree_output.status.success() {
            return Err(Self::git_error(&read_tree_output));
        }

        let remove_workspace_output = Self::git_in_index(
            repo_root,
            index_path,
            &[
                "rm",
                "-r",
                "--cached",
                "--ignore-unmatch",
                "--quiet",
                "--",
                WORKSPACE_DIR,
            ],
        )?;
        if !remove_workspace_output.status.success() {
            return Err(Self::git_error(&remove_workspace_output));
        }

        let workspace_exclude = format!(":(exclude){WORKSPACE_DIR}");
        let add_output = Self::git_in_index(
            repo_root,
            index_path,
            &["add", "-A", "--", ".", &workspace_exclude],
        )?;
        if !add_output.status.success() {
            return Err(Self::git_error(&add_output));
        }

        let write_tree_output = Self::git_in_index(repo_root, index_path, &["write-tree"])?;
        if !write_tree_output.status.success() {
            return Err(Self::git_error(&write_tree_output));
        }

        let tree_sha = String::from_utf8_lossy(&write_tree_output.stdout)
            .trim()
            .to_owned();
        if tree_sha.is_empty() {
            return Err(AppError::Io(std::io::Error::other(
                "git write-tree returned no tree SHA",
            )));
        }

        Ok(tree_sha)
    }

    fn git_command(dir: &Path) -> Command {
        let mut command = Command::new("git");
        command
            .current_dir(dir)
            .env(
                "GIT_AUTHOR_NAME",
                std::env::var("GIT_AUTHOR_NAME").unwrap_or_else(|_| "ralph-burning".to_owned()),
            )
            .env(
                "GIT_AUTHOR_EMAIL",
                std::env::var("GIT_AUTHOR_EMAIL")
                    .unwrap_or_else(|_| "ralph-burning@example.invalid".to_owned()),
            )
            .env(
                "GIT_COMMITTER_NAME",
                std::env::var("GIT_COMMITTER_NAME").unwrap_or_else(|_| "ralph-burning".to_owned()),
            )
            .env(
                "GIT_COMMITTER_EMAIL",
                std::env::var("GIT_COMMITTER_EMAIL")
                    .unwrap_or_else(|_| "ralph-burning@example.invalid".to_owned()),
            );
        command
    }

    fn git(repo_root: &Path, args: &[&str]) -> AppResult<Output> {
        Self::git_command(repo_root)
            .args(args)
            .output()
            .map_err(AppError::from)
    }

    fn git_with_input(repo_root: &Path, args: &[&str], input: &str) -> AppResult<Output> {
        let mut child = Self::git_command(repo_root)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(AppError::from)?;

        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(input.as_bytes()).map_err(AppError::from)?;
        }

        child.wait_with_output().map_err(AppError::from)
    }

    fn git_in(dir: &Path, args: &[&str]) -> AppResult<std::process::Output> {
        Self::git_command(dir)
            .args(args)
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

impl VcsCheckpointPort for WorktreeAdapter {
    fn create_checkpoint(
        &self,
        repo_root: &Path,
        project_id: &ProjectId,
        run_id: &RunId,
        stage_id: StageId,
        cycle: u32,
        completion_round: u32,
    ) -> AppResult<String> {
        let parent_sha = self.current_head_sha(repo_root)?;
        let tree_sha = Self::build_checkpoint_tree(repo_root, parent_sha.as_deref())?;

        let subject = checkpoint_subject(project_id, stage_id, cycle, completion_round);
        let body = checkpoint_body(project_id, run_id, stage_id, cycle, completion_round);
        let message = format!("{subject}\n\n{body}\n");
        let mut commit_args = vec!["commit-tree", tree_sha.as_str()];
        if let Some(parent_sha) = parent_sha.as_deref() {
            commit_args.extend(["-p", parent_sha]);
        }
        commit_args.extend(["-F", "-"]);
        let commit_output = Self::git_with_input(repo_root, &commit_args, &message)?;
        if !commit_output.status.success() {
            return Err(Self::git_error(&commit_output));
        }

        let checkpoint_sha = String::from_utf8_lossy(&commit_output.stdout)
            .trim()
            .to_owned();
        if checkpoint_sha.is_empty() {
            return Err(AppError::Io(std::io::Error::other(
                "git commit-tree returned no commit SHA",
            )));
        }

        let reset_output = Self::git(
            repo_root,
            &["reset", "--mixed", "-q", checkpoint_sha.as_str()],
        )?;
        if !reset_output.status.success() {
            return Err(Self::git_error(&reset_output));
        }

        Ok(checkpoint_sha)
    }

    fn find_checkpoint(
        &self,
        repo_root: &Path,
        project_id: &ProjectId,
        stage_id: StageId,
        cycle: u32,
        completion_round: u32,
    ) -> AppResult<Option<String>> {
        let output = Self::git(
            repo_root,
            &["log", "--format=%H%x1f%B%x1e", "--grep", "^rb: checkpoint "],
        )?;
        if !output.status.success() {
            return Err(AppError::Io(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            )));
        }

        let log = String::from_utf8_lossy(&output.stdout);
        for entry in log.split('\u{1e}') {
            let trimmed = entry.trim_matches(|ch| ch == '\n' || ch == '\r');
            if trimmed.is_empty() {
                continue;
            }

            let Some((sha, message)) = trimmed.split_once('\u{1f}') else {
                continue;
            };
            let Some(metadata) = parse_checkpoint_commit_message(message) else {
                continue;
            };

            if metadata.project_id == project_id.as_str()
                && metadata.stage_id == stage_id.as_str()
                && metadata.cycle == cycle
                && metadata.completion_round == completion_round
            {
                return Ok(Some(sha.trim().to_owned()));
            }
        }

        Ok(None)
    }

    fn reset_to_checkpoint(&self, repo_root: &Path, sha: &str) -> AppResult<()> {
        self.hard_reset_to_sha(repo_root, sha)
    }
}

impl RepositoryResetPort for WorktreeAdapter {
    fn reset_to_sha(&self, repo_root: &Path, sha: &str) -> AppResult<()> {
        self.reset_to_checkpoint(repo_root, sha)
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
    ) -> AppResult<WorktreeCleanupOutcome> {
        if !worktree_path.exists() {
            return Ok(WorktreeCleanupOutcome::AlreadyAbsent);
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

        Ok(WorktreeCleanupOutcome::Removed)
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
