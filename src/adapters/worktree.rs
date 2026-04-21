use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;

use uuid::Uuid;

use crate::contexts::automation_runtime::{
    RebaseConflictFile, RebaseConflictRequest, RebaseConflictResolver, RebaseFailureClassification,
    RebaseOutcome, WorktreeCleanupOutcome, WorktreePort,
};
use crate::contexts::project_run_record::service::RepositoryResetPort;
use crate::contexts::workflow_composition::checkpoints::{
    checkpoint_body, checkpoint_subject, parse_checkpoint_commit_message, VcsCheckpointPort,
};
use crate::shared::domain::EffectiveRebasePolicy;
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
    fn is_executable(path: &Path) -> bool {
        let Ok(metadata) = fs::metadata(path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    fn git_binary() -> &'static Path {
        static GIT_BINARY: OnceLock<PathBuf> = OnceLock::new();
        GIT_BINARY
            .get_or_init(|| {
                let candidates = [
                    std::env::var_os("RALPH_BURNING_TEST_GIT").map(PathBuf::from),
                    std::env::var_os("PATH").and_then(|paths| {
                        std::env::split_paths(&paths)
                            .map(|dir| dir.join("git"))
                            .find(|path| Self::is_executable(path))
                    }),
                    Some(PathBuf::from("/run/current-system/sw/bin/git")),
                    Some(PathBuf::from("/usr/bin/git")),
                    Some(PathBuf::from("/usr/local/bin/git")),
                    Some(PathBuf::from("/opt/homebrew/bin/git")),
                ];
                candidates
                    .into_iter()
                    .flatten()
                    .find(|path| Self::is_executable(path))
                    .map(|p| {
                        // Absolutize so the cached path works regardless of
                        // the child process's current_dir().
                        std::fs::canonicalize(&p).unwrap_or(p)
                    })
                    .unwrap_or_else(|| PathBuf::from("git"))
            })
            .as_path()
    }

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

    fn tracked_gitignored_paths(repo_root: &Path, index_path: &Path) -> AppResult<Vec<String>> {
        let ignored_output = Self::git_in_index(
            repo_root,
            index_path,
            &["ls-files", "-z", "-i", "-c", "--exclude-standard"],
        )?;
        if !ignored_output.status.success() {
            return Err(Self::git_error(&ignored_output));
        }

        Ok(ignored_output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| String::from_utf8_lossy(path).into_owned())
            .collect())
    }

    fn assume_unchanged_paths(repo_root: &Path) -> AppResult<Vec<String>> {
        let tracked_output = Self::git(repo_root, &["ls-files", "-z", "-v"])?;
        if !tracked_output.status.success() {
            return Err(Self::git_error(&tracked_output));
        }

        Ok(tracked_output
            .stdout
            .split(|entry| *entry == 0)
            .filter_map(|entry| match entry {
                [status, b' ', path @ ..] if status.is_ascii_lowercase() && !path.is_empty() => {
                    Some(String::from_utf8_lossy(path).into_owned())
                }
                _ => None,
            })
            .collect())
    }

    fn remove_ignored_paths_from_index(repo_root: &Path, index_path: &Path) -> AppResult<()> {
        let mut ignored_paths: BTreeSet<String> =
            Self::tracked_gitignored_paths(repo_root, index_path)?
                .into_iter()
                .collect();
        ignored_paths.extend(Self::assume_unchanged_paths(repo_root)?);
        if ignored_paths.is_empty() {
            return Ok(());
        }

        let mut remove_args: Vec<&str> = vec!["update-index", "--force-remove", "--"];
        for path in &ignored_paths {
            remove_args.push(path.as_str());
        }
        let remove_output = Self::git_in_index(repo_root, index_path, &remove_args)?;
        if !remove_output.status.success() {
            return Err(Self::git_error(&remove_output));
        }

        Ok(())
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

        let add_output = Self::git_in_index(repo_root, index_path, &["add", "-A", "--", "."])?;
        if !add_output.status.success() {
            return Err(Self::git_error(&add_output));
        }

        Self::remove_ignored_paths_from_index(repo_root, index_path)?;

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
        let mut command = Command::new(Self::git_binary());
        command
            .current_dir(dir)
            .env("GIT_EDITOR", "true")
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

    /// Returns the merge-base SHA between HEAD (in `dir`) and `base_ref`,
    /// suitable for use as a range boundary (e.g. `<merge-base>..HEAD`) to
    /// scope git log to branch-local commits only. Returns `None` if the
    /// merge-base cannot be determined.
    fn branch_fork_point(dir: &Path, base_ref: &str) -> Option<String> {
        let mb = Self::git_in(dir, &["merge-base", "HEAD", base_ref]).ok()?;
        if mb.status.success() {
            let sha = String::from_utf8_lossy(&mb.stdout).trim().to_owned();
            if !sha.is_empty() {
                return Some(sha);
            }
        }
        None
    }

    fn conflicted_file_paths(worktree_path: &Path) -> AppResult<Vec<String>> {
        let conflicts = Self::git_in(worktree_path, &["diff", "--name-only", "--diff-filter=U"])?;
        if !conflicts.status.success() {
            return Err(Self::git_error(&conflicts));
        }

        Ok(String::from_utf8_lossy(&conflicts.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }

    fn worktree_relative_path(worktree_path: &Path, relative: &str) -> AppResult<PathBuf> {
        let relative_path = Path::new(relative);
        if relative_path.is_absolute()
            || relative_path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(AppError::Io(std::io::Error::other(format!(
                "invalid conflicted file path '{relative}'"
            ))));
        }

        Ok(worktree_path.join(relative_path))
    }

    fn read_rebase_conflict_request(
        worktree_path: &Path,
        branch_name: &str,
        upstream: &str,
        failure_details: &str,
    ) -> AppResult<RebaseConflictRequest> {
        let conflicted_files = Self::conflicted_file_paths(worktree_path)?
            .into_iter()
            .map(|path| {
                let file_path = Self::worktree_relative_path(worktree_path, &path)?;
                let contents = fs::read_to_string(&file_path).map_err(|error| {
                    AppError::Io(std::io::Error::other(format!(
                        "failed to read conflicted file '{}': {error}",
                        file_path.display()
                    )))
                })?;
                Ok(RebaseConflictFile { path, contents })
            })
            .collect::<AppResult<Vec<_>>>()?;

        Ok(RebaseConflictRequest {
            branch_name: branch_name.to_owned(),
            upstream: upstream.to_owned(),
            failure_details: failure_details.to_owned(),
            conflicted_files,
        })
    }

    fn apply_rebase_resolution(
        worktree_path: &Path,
        request: &RebaseConflictRequest,
        resolution: &crate::contexts::automation_runtime::RebaseConflictResolution,
    ) -> AppResult<Vec<String>> {
        let mut resolved_by_path = BTreeMap::new();
        for file in &resolution.resolved_files {
            if resolved_by_path
                .insert(file.path.clone(), file.content.clone())
                .is_some()
            {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "agent returned duplicate resolution for '{}'",
                    file.path
                ))));
            }
        }

        let expected = request
            .conflicted_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let missing = expected
            .iter()
            .filter(|path| !resolved_by_path.contains_key(*path))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(AppError::Io(std::io::Error::other(format!(
                "agent omitted conflicted files: {}",
                missing.join(", ")
            ))));
        }

        let extras = resolved_by_path
            .keys()
            .filter(|path| !expected.iter().any(|expected_path| expected_path == *path))
            .cloned()
            .collect::<Vec<_>>();
        if !extras.is_empty() {
            return Err(AppError::Io(std::io::Error::other(format!(
                "agent returned non-conflicted files: {}",
                extras.join(", ")
            ))));
        }

        for path in &expected {
            let file_path = Self::worktree_relative_path(worktree_path, path)?;
            let contents = resolved_by_path
                .get(path)
                .expect("validated resolved file must exist");
            fs::write(&file_path, contents).map_err(|error| {
                AppError::Io(std::io::Error::other(format!(
                    "failed to write resolved file '{}': {error}",
                    file_path.display()
                )))
            })?;

            let add = Self::git_in(worktree_path, &["add", "--", path])?;
            if !add.status.success() {
                return Err(Self::git_error(&add));
            }
        }

        Ok(expected)
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

        // Detached HEAD: resolve to concrete SHA so the ref works correctly
        // when passed to merge-base in a different worktree context (where
        // "HEAD" would resolve to the worktree's own HEAD, not repo_root's).
        let head_sha = Self::git(repo_root, &["rev-parse", "HEAD"])?;
        if head_sha.status.success() {
            let sha = String::from_utf8_lossy(&head_sha.stdout).trim().to_owned();
            if !sha.is_empty() {
                return Ok(sha);
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

/// Stages that precede actual implementation work. Checkpoints from these
/// stages are not worth preserving on failure or resuming from on retry.
const PRE_IMPL_STAGES: &[&str] = &["prompt_review", "planning", "docs_plan", "ci_plan"];

impl WorktreePort for WorktreeAdapter {
    fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
        base_dir.join("worktrees").join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
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

    fn default_branch_name(&self, repo_root: &Path) -> AppResult<String> {
        self.default_branch_ref(repo_root)
    }

    fn push_branch(
        &self,
        _repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> AppResult<()> {
        let output = Self::git_in(
            worktree_path,
            &["push", "--set-upstream", "origin", branch_name],
        )?;
        if output.status.success() {
            return Ok(());
        }

        Err(AppError::Io(std::io::Error::other(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )))
    }

    fn force_push_branch(
        &self,
        _repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> AppResult<()> {
        let output = Self::git_in(
            worktree_path,
            &[
                "push",
                "--force-with-lease",
                "--set-upstream",
                "origin",
                branch_name,
            ],
        )?;
        if output.status.success() {
            return Ok(());
        }

        Err(AppError::Io(std::io::Error::other(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        )))
    }

    fn has_checkpoint_commits(&self, repo_root: &Path, worktree_path: &Path) -> bool {
        // Scope to branch-local commits only, so inherited checkpoint commits
        // from the default branch history are not counted. Use the actual
        // default branch ref (via repo_root) to compute the fork point.
        let Ok(base_ref) = self.default_branch_ref(repo_root) else {
            return false;
        };
        let Some(fork_sha) = Self::branch_fork_point(worktree_path, &base_ref) else {
            return false;
        };
        let range = format!("{fork_sha}..HEAD");
        let Ok(output) = Self::git_in(
            worktree_path,
            &["log", &range, "--format=%s", "--grep", "^rb: checkpoint "],
        ) else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout.lines().any(|line| {
            // Subject format: rb: checkpoint project=X stage=Y cycle=N round=M
            line.split_whitespace()
                .find(|part| part.starts_with("stage="))
                .map(|part| &part["stage=".len()..])
                .is_some_and(|stage| !PRE_IMPL_STAGES.contains(&stage))
        })
    }

    fn try_resume_from_remote(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> AppResult<bool> {
        // Fetch the remote branch into an explicit remote-tracking ref.
        // Using a refspec ensures refs/remotes/origin/<branch> is created
        // even in narrow / --single-branch clones where a bare
        // `git fetch origin <branch>` only updates FETCH_HEAD.
        let refspec = format!("+refs/heads/{branch_name}:refs/remotes/origin/{branch_name}");
        let fetch_output = Self::git(repo_root, &["fetch", "origin", &refspec]);
        match fetch_output {
            Ok(ref o) if o.status.success() => {}
            Ok(ref o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                // "couldn't find remote ref" is git's message for a missing branch —
                // this is the only expected failure (first attempt or cleaned up).
                if stderr.contains("couldn't find remote ref") {
                    return Ok(false);
                }
                // Any other fetch failure is unexpected and should be surfaced
                // so operators can diagnose network/auth/config issues.
                return Err(std::io::Error::other(format!(
                    "git fetch origin '{}' failed: {}",
                    refspec,
                    stderr.trim()
                ))
                .into());
            }
            Err(e) => {
                eprintln!("daemon: retry resume fetch of '{branch_name}' failed: {e}");
                return Err(e);
            }
        }
        // Capture current HEAD so we can restore it if resume fails. The
        // worktree was just created by create_worktree on the default branch,
        // and we must leave it there if no qualifying checkpoint is found.
        let original_head = Self::git_in(worktree_path, &["rev-parse", "HEAD"])?;
        let original_sha = String::from_utf8_lossy(&original_head.stdout)
            .trim()
            .to_owned();

        // Helper: restore original HEAD on failure paths after the mutation.
        let restore_original = |wt: &Path, sha: &str| {
            if let Ok(out) = Self::git_in(wt, &["reset", "--hard", sha]) {
                if !out.status.success() {
                    eprintln!(
                        "daemon: retry resume restore to original HEAD {sha} failed: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    );
                }
            } else {
                eprintln!("daemon: retry resume restore to original HEAD {sha} failed: git error");
            }
        };

        // Reset the worktree to the fetched remote branch tip first, so the
        // full commit history is available for checkpoint discovery.
        // The fetch above explicitly created refs/remotes/origin/<branch>,
        // so this reset should always succeed after a successful fetch.
        let remote_ref = format!("origin/{branch_name}");
        let reset_output = Self::git_in(worktree_path, &["reset", "--hard", &remote_ref])?;
        if !reset_output.status.success() {
            let stderr = String::from_utf8_lossy(&reset_output.stderr);
            eprintln!(
                "daemon: retry resume reset to '{}' failed: {}",
                remote_ref,
                stderr.trim()
            );
            restore_original(worktree_path, &original_sha);
            return Err(std::io::Error::other(format!(
                "git reset --hard '{}' failed: {}",
                remote_ref,
                stderr.trim()
            ))
            .into());
        }

        // Find the latest branch-local implementation-stage checkpoint and
        // reset to it. Use the actual default branch ref (via repo_root) to
        // compute the fork point, so inherited checkpoints from the default
        // branch are not matched. Returns true only if we actually reset to a
        // qualifying checkpoint; false otherwise (with worktree restored to
        // original HEAD).
        let base_ref = self.default_branch_ref(repo_root).unwrap_or_default();
        let Some(fork_sha) = Self::branch_fork_point(worktree_path, &base_ref) else {
            eprintln!(
                "daemon: retry resume for '{branch_name}': could not resolve fork point, \
                 starting fresh"
            );
            restore_original(worktree_path, &original_sha);
            return Ok(false);
        };
        let range = format!("{fork_sha}..HEAD");
        let log_output = Self::git_in(
            worktree_path,
            &[
                "log",
                &range,
                "--format=%H %s",
                "--grep",
                "^rb: checkpoint ",
            ],
        )?;
        if !log_output.status.success() {
            restore_original(worktree_path, &original_sha);
            return Err(std::io::Error::other(
                "git log for checkpoint discovery failed after successful fetch",
            )
            .into());
        }
        let stdout = String::from_utf8_lossy(&log_output.stdout);
        for line in stdout.lines() {
            // Format: "<sha> rb: checkpoint project=X stage=Y cycle=N round=M"
            let Some((sha, subject)) = line.split_once(' ') else {
                continue;
            };
            let is_impl_stage = subject
                .split_whitespace()
                .find(|part| part.starts_with("stage="))
                .map(|part| &part["stage=".len()..])
                .is_some_and(|stage| !PRE_IMPL_STAGES.contains(&stage));
            if is_impl_stage {
                match Self::git_in(worktree_path, &["reset", "--hard", sha]) {
                    Ok(ref o) if o.status.success() => return Ok(true),
                    Ok(ref o) => {
                        let stderr = String::from_utf8_lossy(&o.stderr);
                        eprintln!(
                            "daemon: retry resume reset to checkpoint {} failed: {}",
                            sha,
                            stderr.trim()
                        );
                        restore_original(worktree_path, &original_sha);
                        return Err(std::io::Error::other(format!(
                            "git reset --hard '{}' failed: {}",
                            sha,
                            stderr.trim()
                        ))
                        .into());
                    }
                    Err(e) => {
                        eprintln!("daemon: retry resume reset to checkpoint {sha} failed: {e}");
                        restore_original(worktree_path, &original_sha);
                        return Err(e);
                    }
                }
            }
        }
        // No qualifying implementation-stage checkpoint found.
        restore_original(worktree_path, &original_sha);
        Ok(false)
    }

    fn rebase_with_agent_resolution(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
        policy: &EffectiveRebasePolicy,
        resolver: Option<&dyn RebaseConflictResolver>,
    ) -> AppResult<RebaseOutcome> {
        let upstream = self.default_branch_ref(repo_root)?;
        let output = Self::git_in(worktree_path, &["rebase", &upstream])?;
        if output.status.success() {
            return Ok(RebaseOutcome::Success);
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if !policy.agent_resolution_enabled {
            let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
            return Ok(RebaseOutcome::Failed {
                classification: RebaseFailureClassification::Conflict,
                details: stderr,
            });
        }

        if policy.agent_timeout == 0 {
            let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
            return Ok(RebaseOutcome::Failed {
                classification: RebaseFailureClassification::Timeout,
                details: "agent-assisted rebase resolution timed out".to_owned(),
            });
        }

        let Some(resolver) = resolver else {
            let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
            return Ok(RebaseOutcome::Failed {
                classification: RebaseFailureClassification::Unknown,
                details: "agent resolution is enabled but no resolver is configured".to_owned(),
            });
        };

        let request =
            Self::read_rebase_conflict_request(worktree_path, branch_name, &upstream, &stderr)?;
        if request.conflicted_files.is_empty() {
            let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
            return Ok(RebaseOutcome::Failed {
                classification: RebaseFailureClassification::Conflict,
                details: stderr,
            });
        }

        let resolution = match resolver.resolve_conflicts(&request) {
            Ok(resolution) => resolution,
            Err(AppError::InvocationTimeout { .. }) => {
                let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
                return Ok(RebaseOutcome::Failed {
                    classification: RebaseFailureClassification::Timeout,
                    details: "agent-assisted rebase resolution timed out".to_owned(),
                });
            }
            Err(error) => {
                let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
                return Ok(RebaseOutcome::Failed {
                    classification: RebaseFailureClassification::Unknown,
                    details: format!("agent-assisted rebase resolution failed: {error}"),
                });
            }
        };

        let resolved_files =
            match Self::apply_rebase_resolution(worktree_path, &request, &resolution) {
                Ok(files) => files,
                Err(error) => {
                    let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
                    return Ok(RebaseOutcome::Failed {
                        classification: RebaseFailureClassification::Unknown,
                        details: error.to_string(),
                    });
                }
            };

        let continue_output = Self::git_in(worktree_path, &["rebase", "--continue"])?;
        if continue_output.status.success() {
            return Ok(RebaseOutcome::AgentResolved {
                resolved_files,
                summary: resolution.summary,
            });
        }

        let details = String::from_utf8_lossy(&continue_output.stderr)
            .trim()
            .to_owned();
        let _ = Self::git_in(worktree_path, &["rebase", "--abort"]);
        Ok(RebaseOutcome::Failed {
            classification: RebaseFailureClassification::Conflict,
            details,
        })
    }
}
