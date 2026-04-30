#![forbid(unsafe_code)]

//! Success-path PR creation for completed bead-backed runs.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::Path;
use std::process::Command;

use thiserror::Error;

use crate::adapters::br_models::BeadDetail;
use crate::contexts::project_run_record::model::{JournalEvent, JournalEventType, RunStatus};
use crate::contexts::project_run_record::service::{
    JournalStorePort, ProjectStorePort, RunSnapshotPort,
};
use crate::contexts::workflow_composition::checkpoints::parse_checkpoint_commit_message;
use crate::shared::domain::ProjectId;

use super::project_prompt::{
    BeadPromptBrPort, BeadPromptReadError, ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE,
};

// Bot's PR #208 finding: hardcoding a fixed `Co-authored-by` trailer for
// every generated commit attributes the work to two specific identities
// regardless of who actually made the change. Git already records authorship
// from the operator's `user.name`/`user.email` config; if a particular
// installation wants Co-authored-by trailers, they can be added via a
// commit template (`git config commit.template`) or by the operator's
// AI-coding harness on a per-run basis. Don't bake an identity into the
// command itself.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    CargoFmt,
    CargoClippy,
    CargoTest,
    NixBuild,
}

impl Gate {
    pub fn all() -> [Self; 4] {
        [
            Self::CargoFmt,
            Self::CargoClippy,
            Self::CargoTest,
            Self::NixBuild,
        ]
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::CargoFmt => "cargo fmt --check",
            Self::CargoClippy => "cargo clippy --locked -- -D warnings",
            Self::CargoTest => "cargo test",
            Self::NixBuild => "nix build",
        }
    }

    fn program_and_args(self) -> (&'static str, &'static [&'static str]) {
        match self {
            Self::CargoFmt => ("cargo", &["fmt", "--check"]),
            Self::CargoClippy => ("cargo", &["clippy", "--locked", "--", "-D", "warnings"]),
            Self::CargoTest => ("cargo", &["test"]),
            Self::NixBuild => ("nix", &["build"]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffStat {
    pub path: String,
    pub additions: Option<u32>,
    pub deletions: Option<u32>,
}

impl DiffStat {
    fn display(&self) -> String {
        match (self.additions, self.deletions) {
            (Some(additions), Some(deletions)) => {
                format!("{} (+{additions}/-{deletions})", self.path)
            }
            _ => format!("{} (binary)", self.path),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedPrOpenText {
    pub title: String,
    pub body: String,
    pub commit_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrOpenOutput {
    pub pr_url: String,
    pub branch_name: String,
    pub warnings: Vec<String>,
    pub rendered: RenderedPrOpenText,
}

pub struct PrOpenRequest<'a> {
    pub base_dir: &'a Path,
    pub project_id: &'a ProjectId,
    pub bead_id_override: Option<&'a str>,
    pub skip_gates: bool,
}

pub struct PrOpenStores<'a, P, R, J> {
    pub project_store: &'a P,
    pub run_store: &'a R,
    pub journal_store: &'a J,
}

#[derive(Debug, Error)]
pub enum PrToolError {
    #[error("{command} failed with exit {exit_code}: {stderr}")]
    CommandFailed {
        command: String,
        exit_code: i32,
        stdout: String,
        stderr: String,
    },

    #[error("{0}")]
    Io(String),
}

impl From<std::io::Error> for PrToolError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

#[derive(Debug, Error)]
pub enum PrOpenError {
    #[error(
        "active run is '{status}', not completed; inspect with `ralph-burning run status` and continue with `ralph-burning run resume`"
    )]
    RunNotCompleted { status: String },

    #[error("gate failed: {gate}; stderr excerpt: {stderr_excerpt}")]
    GateFailed {
        gate: String,
        stderr_excerpt: String,
    },

    #[error("current branch '{branch_name}' is not a feature branch; create or switch to a feat/<bead-id>-... branch before opening a PR")]
    NotFeatureBranch { branch_name: String },

    #[error("origin/master is not an ancestor of HEAD; rebase or merge origin/master before running `ralph-burning pr open`")]
    OriginMasterNotAncestor,

    #[error("refusing to squash non-checkpoint commits: {subjects}")]
    NonCheckpointCommits { subjects: String },

    #[error("refusing to squash with unstaged real-code changes present: {paths}")]
    DirtyRealCodeWorktree { paths: String },

    #[error("refusing to squash with pre-existing staged real-code changes present: {paths}")]
    StagedRealCodeWorktree { paths: String },

    #[error("no staged real-code changes remain after reset; nothing to commit")]
    NoRealCodeChanges,

    #[error("project '{project_id}' has no bead binding; pass `--bead-id <id>`")]
    MissingBeadId { project_id: String },

    #[error("failed to read bead '{bead_id}' from br: {source}")]
    BeadRead {
        bead_id: String,
        source: BeadPromptReadError,
    },

    #[error("{action} failed: {source}")]
    Tool {
        action: &'static str,
        source: PrToolError,
    },

    #[error(transparent)]
    App(#[from] crate::shared::error::AppError),
}

#[allow(async_fn_in_trait)]
pub trait PrToolPort {
    async fn run_gate(&self, repo_root: &Path, gate: Gate) -> Result<(), PrToolError>;
    async fn current_branch(&self, repo_root: &Path) -> Result<String, PrToolError>;
    async fn fetch_origin_master(&self, repo_root: &Path) -> Result<(), PrToolError>;
    async fn origin_master_is_ancestor_of_head(
        &self,
        repo_root: &Path,
    ) -> Result<bool, PrToolError>;
    async fn commit_messages_since_origin_master(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<String>, PrToolError>;
    async fn diff_stats_since_origin_master(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError>;
    async fn unstaged_real_code_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError>;
    async fn staged_real_code_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError>;
    async fn soft_reset_origin_master(&self, repo_root: &Path) -> Result<(), PrToolError>;
    async fn staged_real_code_diff_stats(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError>;
    async fn staged_commit_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError>;
    async fn commit(
        &self,
        repo_root: &Path,
        message: &str,
        paths: &[String],
    ) -> Result<(), PrToolError>;
    async fn push_branch(&self, repo_root: &Path, branch_name: &str) -> Result<(), PrToolError>;
    async fn create_pr(
        &self,
        repo_root: &Path,
        branch_name: &str,
        title: &str,
        body: &str,
    ) -> Result<String, PrToolError>;
}

pub struct ProcessPrToolPort;

impl ProcessPrToolPort {
    pub(super) fn run_command(
        repo_root: &Path,
        program: &str,
        args: &[&str],
    ) -> Result<CommandOutput, PrToolError> {
        let output = Command::new(program)
            .args(args)
            .current_dir(repo_root)
            .output()?;
        let command = format!("{program} {}", args.join(" "));
        let result = CommandOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        };
        if !output.status.success() {
            return Err(PrToolError::CommandFailed {
                command,
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            });
        }
        Ok(result)
    }

    fn git(repo_root: &Path, args: &[&str]) -> Result<CommandOutput, PrToolError> {
        Self::run_command(repo_root, "git", args)
    }
}

impl PrToolPort for ProcessPrToolPort {
    async fn run_gate(&self, repo_root: &Path, gate: Gate) -> Result<(), PrToolError> {
        let (program, args) = gate.program_and_args();
        Self::run_command(repo_root, program, args).map(|_| ())
    }

    async fn current_branch(&self, repo_root: &Path) -> Result<String, PrToolError> {
        Ok(
            Self::git(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])?
                .stdout
                .trim()
                .to_owned(),
        )
    }

    async fn fetch_origin_master(&self, repo_root: &Path) -> Result<(), PrToolError> {
        Self::git(repo_root, &["fetch", "origin", "master"]).map(|_| ())
    }

    async fn origin_master_is_ancestor_of_head(
        &self,
        repo_root: &Path,
    ) -> Result<bool, PrToolError> {
        match Self::git(
            repo_root,
            &["merge-base", "--is-ancestor", "origin/master", "HEAD"],
        ) {
            Ok(_) => Ok(true),
            Err(PrToolError::CommandFailed { exit_code: 1, .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn commit_messages_since_origin_master(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<String>, PrToolError> {
        let output = Self::git(
            repo_root,
            &["log", "--format=%x1e%B", "origin/master..HEAD"],
        )?;
        Ok(output
            .stdout
            .split('\x1e')
            .map(|message| message.trim_matches('\n'))
            .filter(|message| !message.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }

    async fn diff_stats_since_origin_master(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError> {
        let output = Self::git(
            repo_root,
            &[
                "diff",
                "--numstat",
                "origin/master..HEAD",
                "--",
                ".",
                ":(exclude).ralph-burning/**",
            ],
        )?;
        Ok(review_scope_diff_stats(parse_numstat(&output.stdout)))
    }

    async fn unstaged_real_code_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError> {
        let unstaged = Self::git(
            repo_root,
            &[
                "diff",
                "--name-only",
                "--",
                ".",
                ":(exclude).ralph-burning/**",
            ],
        )?;
        let untracked = Self::git(
            repo_root,
            &[
                "ls-files",
                "--others",
                "--exclude-standard",
                "--",
                ".",
                ":(exclude).ralph-burning/**",
            ],
        )?;
        let mut paths = parse_path_lines(&unstaged.stdout);
        paths.extend(parse_path_lines(&untracked.stdout));
        Ok(review_scope_paths(paths))
    }

    async fn staged_real_code_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError> {
        let staged = Self::git(
            repo_root,
            &[
                "diff",
                "--cached",
                "--name-only",
                "--",
                ".",
                ":(exclude).ralph-burning/**",
            ],
        )?;
        Ok(review_scope_paths(parse_path_lines(&staged.stdout)))
    }

    async fn soft_reset_origin_master(&self, repo_root: &Path) -> Result<(), PrToolError> {
        Self::git(repo_root, &["reset", "--soft", "origin/master"]).map(|_| ())
    }

    async fn staged_real_code_diff_stats(
        &self,
        repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError> {
        let output = Self::git(
            repo_root,
            &[
                "diff",
                "--cached",
                "--numstat",
                "--",
                ".",
                ":(exclude).ralph-burning/**",
            ],
        )?;
        Ok(review_scope_diff_stats(parse_numstat(&output.stdout)))
    }

    async fn staged_commit_paths(&self, repo_root: &Path) -> Result<Vec<String>, PrToolError> {
        let output = Self::git(
            repo_root,
            &[
                "diff",
                "--cached",
                "--name-only",
                "-z",
                "--no-renames",
                "--",
                ".",
            ],
        )?;
        Ok(parse_nul_paths(&output.stdout))
    }

    async fn commit(
        &self,
        repo_root: &Path,
        message: &str,
        _paths: &[String],
    ) -> Result<(), PrToolError> {
        // Commit the already-staged index snapshot, NOT the path-limited
        // working-tree form (`git commit -- <paths>`). The pathspec form
        // re-reads working-tree contents for the listed paths, which can
        // silently sweep in unstaged local edits to .beads/ or
        // .ralph-burning/ files (the bot's PR #208 finding). The caller
        // has already arranged for the desired diff to live in the index
        // via the preceding soft-reset, so committing the index directly
        // is the correct snapshot.
        let mut command = Command::new("git");
        command
            .args(["commit", "-m", message])
            .current_dir(repo_root);
        let output = command.output()?;
        let result = CommandOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().unwrap_or(-1),
        };
        if !output.status.success() {
            return Err(PrToolError::CommandFailed {
                command: "git commit -m <message>".to_owned(),
                exit_code: result.exit_code,
                stdout: result.stdout,
                stderr: result.stderr,
            });
        }
        Ok(())
    }

    async fn push_branch(&self, repo_root: &Path, branch_name: &str) -> Result<(), PrToolError> {
        Self::git(repo_root, &["push", "-u", "origin", branch_name]).map(|_| ())
    }

    async fn create_pr(
        &self,
        repo_root: &Path,
        branch_name: &str,
        title: &str,
        body: &str,
    ) -> Result<String, PrToolError> {
        let output = Self::run_command(
            repo_root,
            "gh",
            &[
                "pr",
                "create",
                "--base",
                "master",
                "--head",
                branch_name,
                "--title",
                title,
                "--body",
                body,
            ],
        )?;
        Ok(output.stdout.trim().to_owned())
    }
}

pub async fn open_pr_for_completed_run<P, R, J, B, T>(
    request: PrOpenRequest<'_>,
    stores: PrOpenStores<'_, P, R, J>,
    br: &B,
    tools: &T,
) -> Result<PrOpenOutput, PrOpenError>
where
    P: ProjectStorePort,
    R: RunSnapshotPort,
    J: JournalStorePort,
    B: BeadPromptBrPort,
    T: PrToolPort,
{
    let project = stores
        .project_store
        .read_project_record(request.base_dir, request.project_id)?;
    let run_snapshot = stores
        .run_store
        .read_run_snapshot(request.base_dir, request.project_id)?;
    if run_snapshot.status != RunStatus::Completed {
        return Err(PrOpenError::RunNotCompleted {
            status: run_snapshot.status.display_str().to_owned(),
        });
    }

    let bead_id = request
        .bead_id_override
        .map(ToOwned::to_owned)
        .or_else(|| project.bead_id().map(ToOwned::to_owned))
        .ok_or_else(|| PrOpenError::MissingBeadId {
            project_id: request.project_id.to_string(),
        })?;

    let bead = br
        .bead_show(&bead_id)
        .await
        .map_err(|source| PrOpenError::BeadRead {
            bead_id: bead_id.clone(),
            source,
        })?;

    let journal = stores
        .journal_store
        .read_journal(request.base_dir, request.project_id)?;
    let current_run_id = latest_run_id(&journal).map(ToOwned::to_owned);
    let convergence_pattern = convergence_pattern(
        &journal,
        run_snapshot.completion_rounds,
        current_run_id.as_deref(),
    );

    let branch_name = tools
        .current_branch(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "read current branch",
            source,
        })?;
    if !branch_name.starts_with("feat/") {
        return Err(PrOpenError::NotFeatureBranch { branch_name });
    }

    if !request.skip_gates {
        for gate in Gate::all() {
            tools
                .run_gate(request.base_dir, gate)
                .await
                .map_err(|source| map_gate_error(gate, source))?;
        }
    }

    tools
        .fetch_origin_master(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "fetch origin/master",
            source,
        })?;
    if !tools
        .origin_master_is_ancestor_of_head(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "verify origin/master ancestry",
            source,
        })?
    {
        return Err(PrOpenError::OriginMasterNotAncestor);
    }

    let messages = tools
        .commit_messages_since_origin_master(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "inspect commits since origin/master",
            source,
        })?;

    if let Some(rendered) = existing_squash_commit_rendering(
        request.base_dir,
        tools,
        &messages,
        &bead,
        &convergence_pattern,
    )
    .await?
    {
        reject_pre_existing_real_code_changes(request.base_dir, tools).await?;
        tools
            .push_branch(request.base_dir, &branch_name)
            .await
            .map_err(|source| PrOpenError::Tool {
                action: "push branch",
                source,
            })?;
        let pr_url = tools
            .create_pr(
                request.base_dir,
                &branch_name,
                &rendered.title,
                &rendered.body,
            )
            .await
            .map_err(|source| PrOpenError::Tool {
                action: "create PR",
                source,
            })?;

        return Ok(PrOpenOutput {
            pr_url,
            branch_name,
            warnings: vec![
                "Squash commit already exists; retried push and PR creation without resetting."
                    .to_owned(),
            ],
            rendered,
        });
    }

    let invalid_checkpoint_subjects = invalid_checkpoint_subjects(
        &messages,
        request.project_id.as_str(),
        latest_run_id(&journal),
    );
    if !invalid_checkpoint_subjects.is_empty() {
        return Err(PrOpenError::NonCheckpointCommits {
            subjects: invalid_checkpoint_subjects.join("; "),
        });
    }

    reject_pre_existing_real_code_changes(request.base_dir, tools).await?;

    tools
        .soft_reset_origin_master(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "soft reset to origin/master",
            source,
        })?;
    let diff_stats = review_scope_diff_stats(
        tools
            .staged_real_code_diff_stats(request.base_dir)
            .await
            .map_err(|source| PrOpenError::Tool {
                action: "read staged real-code diff stats",
                source,
            })?,
    );
    if diff_stats.is_empty() {
        return Err(PrOpenError::NoRealCodeChanges);
    }

    let mut warnings = Vec::new();
    if messages.is_empty() {
        warnings.push(
            "No checkpoint commits were found to squash; continuing because reset produced review-scope changes."
                .to_owned(),
        );
    }

    let rendered = render_pr_open_text(&bead, &convergence_pattern, &diff_stats);
    let commit_paths = tools
        .staged_commit_paths(request.base_dir)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "read staged commit paths",
            source,
        })?;
    if commit_paths.is_empty() {
        return Err(PrOpenError::NoRealCodeChanges);
    }
    tools
        .commit(request.base_dir, &rendered.commit_message, &commit_paths)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "commit squashed work",
            source,
        })?;
    tools
        .push_branch(request.base_dir, &branch_name)
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "push branch",
            source,
        })?;
    let pr_url = tools
        .create_pr(
            request.base_dir,
            &branch_name,
            &rendered.title,
            &rendered.body,
        )
        .await
        .map_err(|source| PrOpenError::Tool {
            action: "create PR",
            source,
        })?;

    Ok(PrOpenOutput {
        pr_url,
        branch_name,
        warnings,
        rendered,
    })
}

async fn existing_squash_commit_rendering<T>(
    repo_root: &Path,
    tools: &T,
    messages: &[String],
    bead: &BeadDetail,
    convergence_pattern: &str,
) -> Result<Option<RenderedPrOpenText>, PrOpenError>
where
    T: PrToolPort,
{
    let [message] = messages else {
        return Ok(None);
    };
    if parse_checkpoint_commit_message(message).is_some() {
        return Ok(None);
    }

    let diff_stats = review_scope_diff_stats(
        tools
            .diff_stats_since_origin_master(repo_root)
            .await
            .map_err(|source| PrOpenError::Tool {
                action: "read committed real-code diff stats",
                source,
            })?,
    );
    if diff_stats.is_empty() {
        return Ok(None);
    }

    let rendered = render_pr_open_text(bead, convergence_pattern, &diff_stats);
    if message.trim_end() == rendered.commit_message {
        Ok(Some(rendered))
    } else {
        Ok(None)
    }
}

async fn reject_pre_existing_real_code_changes<T>(
    repo_root: &Path,
    tools: &T,
) -> Result<(), PrOpenError>
where
    T: PrToolPort,
{
    let dirty_paths = review_scope_paths(tools.unstaged_real_code_paths(repo_root).await.map_err(
        |source| PrOpenError::Tool {
            action: "inspect unstaged real-code changes",
            source,
        },
    )?);
    if !dirty_paths.is_empty() {
        return Err(PrOpenError::DirtyRealCodeWorktree {
            paths: dirty_paths.join("; "),
        });
    }

    let staged_paths = review_scope_paths(tools.staged_real_code_paths(repo_root).await.map_err(
        |source| PrOpenError::Tool {
            action: "inspect staged real-code changes",
            source,
        },
    )?);
    if !staged_paths.is_empty() {
        return Err(PrOpenError::StagedRealCodeWorktree {
            paths: staged_paths.join("; "),
        });
    }

    Ok(())
}

pub fn render_pr_open_text(
    bead: &BeadDetail,
    convergence_pattern: &str,
    diff_stats: &[DiffStat],
) -> RenderedPrOpenText {
    let diff_stats = review_scope_diff_stats(diff_stats.to_vec());
    let title = format!("{}: {}", bead.id, bead.title);
    let body = render_pr_body(bead, convergence_pattern, &diff_stats);
    let commit_message = render_commit_message(bead, convergence_pattern, &diff_stats);
    RenderedPrOpenText {
        title,
        body,
        commit_message,
    }
}

fn render_commit_message(
    bead: &BeadDetail,
    convergence_pattern: &str,
    diff_stats: &[DiffStat],
) -> String {
    let mut out = String::new();
    writeln!(out, "{}: {}", bead.id, bead.title).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Convergence pattern: {convergence_pattern}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Real-code diff stats:").unwrap();
    for stat in diff_stats {
        writeln!(out, "- {}", stat.display()).unwrap();
    }
    out.trim_end().to_owned()
}

fn render_pr_body(bead: &BeadDetail, convergence_pattern: &str, diff_stats: &[DiffStat]) -> String {
    let mut out = String::new();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", first_description_paragraph(&bead.description)).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Closes `{}`.", bead.id).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Run history").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- Convergence pattern: {convergence_pattern}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Diff stats").unwrap();
    writeln!(out).unwrap();
    for stat in diff_stats {
        writeln!(out, "- {}", stat.display()).unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "## Reviewer attention").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "Review these files:").unwrap();
    for stat in diff_stats {
        writeln!(out, "- `{}`", stat.path).unwrap();
    }
    writeln!(out).unwrap();
    writeln!(out, "## Test plan").unwrap();
    writeln!(out).unwrap();
    for criterion in test_plan_items(bead) {
        writeln!(out, "- [ ] {criterion}").unwrap();
    }
    out.trim_end().to_owned()
}

fn first_description_paragraph(description: &Option<String>) -> String {
    description
        .as_deref()
        .unwrap_or("")
        .split("\n\n")
        .map(str::trim)
        .find(|paragraph| !paragraph.is_empty())
        .unwrap_or("No bead description provided.")
        .to_owned()
}

fn test_plan_items(bead: &BeadDetail) -> Vec<String> {
    let mut items = bead
        .acceptance_criteria
        .iter()
        .filter(|item| item.trim() != "nix build")
        .cloned()
        .collect::<Vec<_>>();
    items.push("nix build".to_owned());
    items
}

fn convergence_pattern(
    events: &[JournalEvent],
    completion_rounds: u32,
    current_run_id: Option<&str>,
) -> String {
    let mut amendments_by_round = BTreeMap::<u32, u32>::new();
    for event in events {
        if event.event_type != JournalEventType::CompletionRoundAdvanced {
            continue;
        }
        if let Some(current_run_id) = current_run_id {
            if event.details.get("run_id").and_then(|value| value.as_str()) != Some(current_run_id)
            {
                continue;
            }
        }
        let Some(round) = event
            .details
            .get("from_round")
            .and_then(|value| value.as_u64())
        else {
            continue;
        };
        let amendment_count = event
            .details
            .get("amendment_count")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        amendments_by_round.insert(round as u32, amendment_count as u32);
    }

    let final_round = completion_rounds
        .max(*amendments_by_round.keys().max().unwrap_or(&1))
        .max(1);
    (1..=final_round)
        .map(|round| {
            amendments_by_round
                .get(&round)
                .copied()
                .unwrap_or(0)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_numstat(raw: &str) -> Vec<DiffStat> {
    raw.lines()
        .filter_map(|line| {
            let mut parts = line.splitn(3, '\t');
            let additions = parts.next()?;
            let deletions = parts.next()?;
            let path = parts.next()?.to_owned();
            Some(DiffStat {
                path,
                additions: additions.parse::<u32>().ok(),
                deletions: deletions.parse::<u32>().ok(),
            })
        })
        .collect()
}

fn parse_path_lines(raw: &str) -> Vec<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_nul_paths(raw: &str) -> Vec<String> {
    raw.split('\0')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn review_scope_paths(paths: Vec<String>) -> Vec<String> {
    paths
        .into_iter()
        .filter(|path| is_review_scope_path(path))
        .collect()
}

fn review_scope_diff_stats(stats: Vec<DiffStat>) -> Vec<DiffStat> {
    stats
        .into_iter()
        .filter(|stat| is_review_scope_path(&stat.path))
        .collect()
}

fn is_review_scope_path(path: &str) -> bool {
    let path = path.strip_prefix("./").unwrap_or(path);
    if path.is_empty() {
        return false;
    }

    let parts = path.split('/').collect::<Vec<_>>();
    if parts.iter().any(|part| {
        matches!(
            *part,
            "" | "." | ".." | ".ralph-burning" | ".beads" | ".git" | "target" | "node_modules"
        )
    }) {
        return false;
    }

    matches!(
        parts.first().copied().unwrap_or_default(),
        "src"
            | "tests"
            | "docs"
            | "benches"
            | "examples"
            | "fixtures"
            | "scripts"
            | "crates"
            | "templates"
            | ".github"
    ) || is_root_review_file(path)
        || parts
            .iter()
            .rev()
            .find(|part| !part.is_empty())
            .is_some_and(has_review_path_extension)
}

fn is_root_review_file(path: &str) -> bool {
    !path.contains('/')
        && matches!(
            path,
            "AGENTS.md"
                | "CLAUDE.md"
                | "Cargo.lock"
                | "Cargo.toml"
                | "Dockerfile"
                | "LICENSE"
                | "LICENSE.md"
                | "Makefile"
                | "README.md"
                | "clippy.toml"
                | "deny.toml"
                | "flake.lock"
                | "flake.nix"
                | "justfile"
                | "Justfile"
                | "rust-toolchain"
                | "rust-toolchain.toml"
                | "rustfmt.toml"
                | "workspace.toml"
        )
}

fn has_review_path_extension(segment: &&str) -> bool {
    [
        ".rs", ".md", ".toml", ".nix", ".json", ".jsonl", ".yaml", ".yml", ".sh", ".ts", ".tsx",
        ".js", ".jsx", ".py", ".go", ".rb", ".sql",
    ]
    .iter()
    .any(|suffix| segment.ends_with(suffix))
}

fn invalid_checkpoint_subjects(
    messages: &[String],
    project_id: &str,
    expected_run_id: Option<&str>,
) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| {
            let subject = first_line(message).to_owned();
            let metadata = parse_checkpoint_commit_message(message)?;
            if metadata.project_id != project_id {
                return Some(subject);
            }
            if let Some(expected_run_id) = expected_run_id {
                if metadata.run_id != expected_run_id {
                    return Some(subject);
                }
            }
            None
        })
        .chain(
            messages
                .iter()
                .filter(|message| parse_checkpoint_commit_message(message).is_none())
                .map(|message| first_line(message).to_owned()),
        )
        .collect()
}

fn first_line(message: &str) -> &str {
    message.lines().next().unwrap_or("<empty commit message>")
}

fn latest_run_id(events: &[JournalEvent]) -> Option<&str> {
    events.iter().rev().find_map(|event| {
        match event.event_type {
            JournalEventType::RunCompleted
            | JournalEventType::RunStarted
            | JournalEventType::RunResumed => {}
            _ => return None,
        }
        event.details.get("run_id").and_then(|value| value.as_str())
    })
}

fn map_gate_error(gate: Gate, source: PrToolError) -> PrOpenError {
    let stderr_excerpt = match &source {
        PrToolError::CommandFailed { stderr, stdout, .. } => {
            let raw = if stderr.trim().is_empty() {
                stdout.as_str()
            } else {
                stderr.as_str()
            };
            stderr_excerpt(raw)
        }
        PrToolError::Io(details) => details.clone(),
    };
    PrOpenError::GateFailed {
        gate: gate.name().to_owned(),
        stderr_excerpt,
    }
}

fn stderr_excerpt(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() <= 600 {
        return trimmed.to_owned();
    }
    let mut excerpt = trimmed.chars().take(600).collect::<String>();
    excerpt.push_str("...");
    excerpt
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::json;

    use crate::adapters::br_models::{BeadPriority, BeadStatus, BeadType, DependencyRef};
    use crate::contexts::project_run_record::model::{
        AmendmentQueueState, ProjectRecord, ProjectStatusSummary, RollbackPointMeta, RunSnapshot,
        TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        JournalStorePort, ProjectStorePort, RunSnapshotPort,
    };
    use crate::contexts::workflow_composition::checkpoints::checkpoint_commit_message;
    use crate::shared::domain::{FlowPreset, RunId, StageId};
    use crate::shared::error::AppResult;

    use super::*;

    #[derive(Clone)]
    struct MockProjectStore {
        record: ProjectRecord,
    }

    impl ProjectStorePort for MockProjectStore {
        fn project_exists(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
            Ok(true)
        }

        fn read_project_record(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<ProjectRecord> {
            Ok(self.record.clone())
        }

        fn list_project_ids(&self, _base_dir: &Path) -> AppResult<Vec<ProjectId>> {
            Ok(vec![self.record.id.clone()])
        }

        fn stage_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn commit_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn rollback_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn create_project_atomic(
            &self,
            _base_dir: &Path,
            _record: &ProjectRecord,
            _prompt_contents: &str,
            _run_snapshot: &RunSnapshot,
            _initial_journal_line: &str,
            _sessions: &crate::contexts::project_run_record::model::SessionStore,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockRunStore {
        snapshot: RunSnapshot,
    }

    impl RunSnapshotPort for MockRunStore {
        fn read_run_snapshot(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<RunSnapshot> {
            Ok(self.snapshot.clone())
        }
    }

    #[derive(Clone)]
    struct MockJournalStore {
        events: Vec<JournalEvent>,
    }

    impl JournalStorePort for MockJournalStore {
        fn read_journal(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<Vec<JournalEvent>> {
            Ok(self.events.clone())
        }

        fn append_event(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _line: &str,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockBr {
        bead: BeadDetail,
    }

    impl BeadPromptBrPort for MockBr {
        async fn bead_show(&self, _bead_id: &str) -> Result<BeadDetail, BeadPromptReadError> {
            Ok(self.bead.clone())
        }

        async fn bead_dep_tree(
            &self,
            _bead_id: &str,
        ) -> Result<Vec<crate::adapters::br_models::DepTreeNode>, BeadPromptReadError> {
            Ok(vec![])
        }
    }

    #[derive(Default)]
    struct MockTools {
        branch: String,
        messages: Vec<String>,
        diff_stats: Vec<DiffStat>,
        committed_diff_stats: Vec<DiffStat>,
        commit_paths: Vec<String>,
        origin_master_ancestor: bool,
        unstaged_paths: Vec<String>,
        staged_paths: Vec<String>,
        fail_gate: Option<Gate>,
        push_error: Option<String>,
        pr_error: Option<String>,
        calls: Arc<Mutex<Vec<String>>>,
        committed_message: Arc<Mutex<Option<String>>>,
        committed_paths: Arc<Mutex<Vec<String>>>,
        pr_body: Arc<Mutex<Option<String>>>,
    }

    impl MockTools {
        fn happy(project_id: &ProjectId) -> Self {
            let run_id = RunId::new("run-2qlo").unwrap();
            Self {
                branch: "feat/2qlo-pr-open".to_owned(),
                messages: vec![
                    checkpoint_commit_message(project_id, &run_id, StageId::Implementation, 1, 1),
                    checkpoint_commit_message(project_id, &run_id, StageId::Qa, 1, 1),
                ],
                diff_stats: vec![
                    DiffStat {
                        path: "src/cli/pr.rs".to_owned(),
                        additions: Some(24),
                        deletions: Some(0),
                    },
                    DiffStat {
                        path: "tests/unit/pr_open_test.rs".to_owned(),
                        additions: Some(80),
                        deletions: Some(2),
                    },
                ],
                committed_diff_stats: vec![
                    DiffStat {
                        path: "src/cli/pr.rs".to_owned(),
                        additions: Some(24),
                        deletions: Some(0),
                    },
                    DiffStat {
                        path: "tests/unit/pr_open_test.rs".to_owned(),
                        additions: Some(80),
                        deletions: Some(2),
                    },
                ],
                commit_paths: vec![
                    "src/cli/pr.rs".to_owned(),
                    "tests/unit/pr_open_test.rs".to_owned(),
                ],
                origin_master_ancestor: true,
                ..Self::default()
            }
        }

        fn record(&self, value: &str) {
            self.calls.lock().unwrap().push(value.to_owned());
        }
    }

    impl PrToolPort for MockTools {
        async fn run_gate(&self, _repo_root: &Path, gate: Gate) -> Result<(), PrToolError> {
            self.record(gate.name());
            if self.fail_gate == Some(gate) {
                return Err(PrToolError::CommandFailed {
                    command: gate.name().to_owned(),
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: format!("{} failed loudly", gate.name()),
                });
            }
            Ok(())
        }

        async fn current_branch(&self, _repo_root: &Path) -> Result<String, PrToolError> {
            self.record("current_branch");
            Ok(self.branch.clone())
        }

        async fn fetch_origin_master(&self, _repo_root: &Path) -> Result<(), PrToolError> {
            self.record("fetch");
            Ok(())
        }

        async fn origin_master_is_ancestor_of_head(
            &self,
            _repo_root: &Path,
        ) -> Result<bool, PrToolError> {
            self.record("ancestry");
            Ok(self.origin_master_ancestor)
        }

        async fn commit_messages_since_origin_master(
            &self,
            _repo_root: &Path,
        ) -> Result<Vec<String>, PrToolError> {
            self.record("messages");
            Ok(self.messages.clone())
        }

        async fn diff_stats_since_origin_master(
            &self,
            _repo_root: &Path,
        ) -> Result<Vec<DiffStat>, PrToolError> {
            self.record("committed_diff");
            Ok(self.committed_diff_stats.clone())
        }

        async fn unstaged_real_code_paths(
            &self,
            _repo_root: &Path,
        ) -> Result<Vec<String>, PrToolError> {
            self.record("unstaged");
            Ok(self.unstaged_paths.clone())
        }

        async fn staged_real_code_paths(
            &self,
            _repo_root: &Path,
        ) -> Result<Vec<String>, PrToolError> {
            self.record("staged");
            Ok(self.staged_paths.clone())
        }

        async fn soft_reset_origin_master(&self, _repo_root: &Path) -> Result<(), PrToolError> {
            self.record("reset");
            Ok(())
        }

        async fn staged_real_code_diff_stats(
            &self,
            _repo_root: &Path,
        ) -> Result<Vec<DiffStat>, PrToolError> {
            self.record("diff");
            Ok(self.diff_stats.clone())
        }

        async fn staged_commit_paths(&self, _repo_root: &Path) -> Result<Vec<String>, PrToolError> {
            self.record("commit_paths");
            Ok(self.commit_paths.clone())
        }

        async fn commit(
            &self,
            _repo_root: &Path,
            message: &str,
            paths: &[String],
        ) -> Result<(), PrToolError> {
            self.record("commit");
            *self.committed_message.lock().unwrap() = Some(message.to_owned());
            *self.committed_paths.lock().unwrap() = paths.to_vec();
            Ok(())
        }

        async fn push_branch(
            &self,
            _repo_root: &Path,
            _branch_name: &str,
        ) -> Result<(), PrToolError> {
            self.record("push");
            if let Some(error) = &self.push_error {
                return Err(PrToolError::CommandFailed {
                    command: "git push".to_owned(),
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: error.clone(),
                });
            }
            Ok(())
        }

        async fn create_pr(
            &self,
            _repo_root: &Path,
            _branch_name: &str,
            _title: &str,
            body: &str,
        ) -> Result<String, PrToolError> {
            self.record("pr");
            *self.pr_body.lock().unwrap() = Some(body.to_owned());
            if let Some(error) = &self.pr_error {
                return Err(PrToolError::CommandFailed {
                    command: "gh pr create".to_owned(),
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: error.clone(),
                });
            }
            Ok("https://github.com/acme/ralph-burning/pull/123".to_owned())
        }
    }

    fn request<'a>(
        project_id: &'a ProjectId,
        bead_id_override: Option<&'a str>,
        skip_gates: bool,
    ) -> PrOpenRequest<'a> {
        PrOpenRequest {
            base_dir: Path::new("/repo"),
            project_id,
            bead_id_override,
            skip_gates,
        }
    }

    fn store_refs<'a>(
        stores: &'a (MockProjectStore, MockRunStore, MockJournalStore),
    ) -> PrOpenStores<'a, MockProjectStore, MockRunStore, MockJournalStore> {
        PrOpenStores {
            project_store: &stores.0,
            run_store: &stores.1,
            journal_store: &stores.2,
        }
    }

    #[tokio::test]
    async fn happy_path_commits_pushes_and_opens_pr() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let br = MockBr {
            bead: bead_detail(),
        };
        let tools = MockTools::happy(&project_id);

        let output = open_pr_for_completed_run(
            request(&project_id, None, false),
            store_refs(&stores),
            &br,
            &tools,
        )
        .await
        .unwrap();

        assert_eq!(
            output.pr_url,
            "https://github.com/acme/ralph-burning/pull/123"
        );
        let commit_message = tools.committed_message.lock().unwrap().clone().unwrap();
        assert!(commit_message.starts_with("2qlo: Open PR automation"));
        assert!(commit_message.contains("Convergence pattern: 1, 2, 0"));
        assert!(
            !commit_message.contains("Co-authored-by"),
            "generated commit message must not bake in any Co-authored-by \
             identity; authorship comes from the operator's git config"
        );
        let body = tools.pr_body.lock().unwrap().clone().unwrap();
        assert!(body.contains("## Reviewer attention"));
        assert!(body.contains(ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE));
        assert!(body.contains("- [ ] nix build"));
        assert!(body.contains("src/cli/pr.rs"));
        assert_eq!(
            tools.calls.lock().unwrap().as_slice(),
            [
                "current_branch",
                "cargo fmt --check",
                "cargo clippy --locked -- -D warnings",
                "cargo test",
                "nix build",
                "fetch",
                "ancestry",
                "messages",
                "unstaged",
                "staged",
                "reset",
                "diff",
                "commit_paths",
                "commit",
                "push",
                "pr"
            ]
        );
    }

    #[tokio::test]
    async fn convergence_pattern_ignores_events_from_older_runs() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let mut stores = stores(project_id.clone(), RunStatus::Completed);
        stores.2.events = vec![
            journal_event(
                1,
                JournalEventType::RunStarted,
                json!({"run_id":"run-old","first_stage":"plan_and_implement"}),
            ),
            journal_event(
                2,
                JournalEventType::CompletionRoundAdvanced,
                json!({"run_id":"run-old","from_round":1,"to_round":2,"amendment_count":9}),
            ),
            journal_event(
                3,
                JournalEventType::RunCompleted,
                json!({"run_id":"run-old","completion_rounds":2}),
            ),
            journal_event(
                4,
                JournalEventType::RunStarted,
                json!({"run_id":"run-2qlo","first_stage":"plan_and_implement"}),
            ),
            journal_event(
                5,
                JournalEventType::CompletionRoundAdvanced,
                json!({"run_id":"run-2qlo","from_round":1,"to_round":2,"amendment_count":1}),
            ),
            journal_event(
                6,
                JournalEventType::CompletionRoundAdvanced,
                json!({"run_id":"run-2qlo","from_round":2,"to_round":3,"amendment_count":2}),
            ),
            journal_event(
                7,
                JournalEventType::RunCompleted,
                json!({"run_id":"run-2qlo","completion_rounds":3}),
            ),
        ];
        let br = MockBr {
            bead: bead_detail(),
        };
        let tools = MockTools::happy(&project_id);

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &br,
            &tools,
        )
        .await
        .unwrap();

        assert!(output
            .rendered
            .commit_message
            .contains("Convergence pattern: 1, 2, 0"));
        assert!(output
            .rendered
            .body
            .contains("- Convergence pattern: 1, 2, 0"));
        assert!(!output.rendered.body.contains("9"));
    }

    #[tokio::test]
    async fn includes_reviewable_paths_outside_original_source_whitelist() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.diff_stats = vec![
            DiffStat {
                path: "README.md".to_owned(),
                additions: Some(3),
                deletions: Some(1),
            },
            DiffStat {
                path: ".github/workflows/ci.yml".to_owned(),
                additions: Some(12),
                deletions: Some(0),
            },
            DiffStat {
                path: "scripts/release.sh".to_owned(),
                additions: Some(5),
                deletions: Some(2),
            },
            DiffStat {
                path: "templates/pr-body.md".to_owned(),
                additions: Some(7),
                deletions: Some(0),
            },
            DiffStat {
                path: ".beads/issues.jsonl".to_owned(),
                additions: Some(1),
                deletions: Some(1),
            },
        ];
        tools.commit_paths = vec![
            "README.md".to_owned(),
            ".github/workflows/ci.yml".to_owned(),
            "scripts/release.sh".to_owned(),
            "templates/pr-body.md".to_owned(),
            ".beads/issues.jsonl".to_owned(),
        ];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap();

        for path in [
            "README.md",
            ".github/workflows/ci.yml",
            "scripts/release.sh",
            "templates/pr-body.md",
        ] {
            assert!(output.rendered.body.contains(path));
        }
        assert!(!output.rendered.body.contains(".beads/issues.jsonl"));
        assert_eq!(
            tools.committed_paths.lock().unwrap().as_slice(),
            [
                "README.md",
                ".github/workflows/ci.yml",
                "scripts/release.sh",
                "templates/pr-body.md",
                ".beads/issues.jsonl"
            ]
        );
    }

    #[tokio::test]
    async fn commits_generated_state_while_excluding_it_from_pr_review_text() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.diff_stats = vec![
            DiffStat {
                path: "src/cli/pr.rs".to_owned(),
                additions: Some(24),
                deletions: Some(0),
            },
            DiffStat {
                path: ".ralph-burning/projects/project-2qlo/run.json".to_owned(),
                additions: Some(3),
                deletions: Some(1),
            },
            DiffStat {
                path: ".beads/issues.jsonl".to_owned(),
                additions: Some(1),
                deletions: Some(1),
            },
        ];
        tools.commit_paths = vec![
            "src/cli/pr.rs".to_owned(),
            ".ralph-burning/projects/project-2qlo/run.json".to_owned(),
            ".beads/issues.jsonl".to_owned(),
        ];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap();

        assert!(output.rendered.body.contains("src/cli/pr.rs"));
        assert!(!output
            .rendered
            .body
            .contains(".ralph-burning/projects/project-2qlo/run.json"));
        assert!(!output.rendered.body.contains(".beads/issues.jsonl"));
        assert_eq!(
            tools.committed_paths.lock().unwrap().as_slice(),
            [
                "src/cli/pr.rs",
                ".ralph-burning/projects/project-2qlo/run.json",
                ".beads/issues.jsonl"
            ]
        );
    }

    #[tokio::test]
    async fn commits_pathspec_safe_paths_for_staged_rename() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.diff_stats = vec![DiffStat {
            path: "src/{old_name.rs => new_name.rs}".to_owned(),
            additions: Some(4),
            deletions: Some(2),
        }];
        tools.commit_paths = vec!["src/old_name.rs".to_owned(), "src/new_name.rs".to_owned()];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap();

        assert!(output
            .rendered
            .body
            .contains("src/{old_name.rs => new_name.rs}"));
        assert_eq!(
            tools.committed_paths.lock().unwrap().as_slice(),
            ["src/old_name.rs", "src/new_name.rs"]
        );
    }

    #[tokio::test]
    async fn run_not_completed_errors_before_tools() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Running);
        let br = MockBr {
            bead: bead_detail(),
        };
        let tools = MockTools::happy(&project_id);

        let error = open_pr_for_completed_run(
            request(&project_id, None, false),
            store_refs(&stores),
            &br,
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::RunNotCompleted { .. }));
        assert!(tools.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn every_gate_failure_stops_before_push() {
        for gate in Gate::all() {
            let project_id = ProjectId::new(format!("project-{gate:?}")).unwrap();
            let stores = stores(project_id.clone(), RunStatus::Completed);
            let mut tools = MockTools::happy(&project_id);
            tools.fail_gate = Some(gate);

            let error = open_pr_for_completed_run(
                request(&project_id, Some("2qlo"), false),
                store_refs(&stores),
                &MockBr {
                    bead: bead_detail(),
                },
                &tools,
            )
            .await
            .unwrap_err();

            match error {
                PrOpenError::GateFailed {
                    gate: failed_gate,
                    stderr_excerpt,
                } => {
                    assert_eq!(failed_gate, gate.name());
                    assert!(stderr_excerpt.contains("failed loudly"));
                }
                other => panic!("expected gate failure, got {other:?}"),
            }
            assert!(!tools.calls.lock().unwrap().contains(&"push".to_owned()));
        }
    }

    #[tokio::test]
    async fn rejects_non_feature_branch() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.branch = "master".to_owned();

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::NotFeatureBranch { .. }));
    }

    #[tokio::test]
    async fn rejects_non_checkpoint_commits_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.messages = vec!["manual commit".to_owned()];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::NonCheckpointCommits { .. }));
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn rejects_checkpoint_prefixed_commit_without_metadata_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.messages = vec!["rb: checkpoint manual\n\nhand-written body".to_owned()];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::NonCheckpointCommits { .. }));
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn rejects_checkpoint_from_other_run_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let other_run_id = RunId::new("run-other").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.messages = vec![checkpoint_commit_message(
            &project_id,
            &other_run_id,
            StageId::Implementation,
            1,
            1,
        )];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::NonCheckpointCommits { .. }));
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn rejects_advanced_origin_master_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.origin_master_ancestor = false;

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::OriginMasterNotAncestor));
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn rejects_unstaged_real_code_changes_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.unstaged_paths = vec![
            "src/lib.rs".to_owned(),
            "tests/unit/pr_open_test.rs".to_owned(),
        ];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        match error {
            PrOpenError::DirtyRealCodeWorktree { paths } => {
                assert!(paths.contains("src/lib.rs"));
                assert!(paths.contains("tests/unit/pr_open_test.rs"));
            }
            other => panic!("expected dirty worktree error, got {other:?}"),
        }
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn rejects_pre_existing_staged_real_code_changes_before_reset() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.staged_paths = vec![
            "src/lib.rs".to_owned(),
            ".beads/issues.jsonl".to_owned(),
            ".ralph-burning/projects/2qlo-1/run.json".to_owned(),
        ];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        match error {
            PrOpenError::StagedRealCodeWorktree { paths } => {
                assert!(paths.contains("src/lib.rs"));
                assert!(!paths.contains(".beads/issues.jsonl"));
                assert!(!paths.contains(".ralph-burning"));
            }
            other => panic!("expected staged worktree error, got {other:?}"),
        }
        assert!(!tools.calls.lock().unwrap().contains(&"reset".to_owned()));
    }

    #[tokio::test]
    async fn no_checkpoint_commits_warns_when_diff_exists() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.messages = vec![];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap();

        assert_eq!(output.warnings.len(), 1);
        assert!(output.warnings[0].contains("No checkpoint commits"));
    }

    #[tokio::test]
    async fn git_push_failure_surfaces_and_skips_pr() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.push_error = Some("non-fast-forward".to_owned());

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("non-fast-forward"));
        assert!(!tools.calls.lock().unwrap().contains(&"pr".to_owned()));
    }

    #[tokio::test]
    async fn gh_pr_create_failure_surfaces_verbatim() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.pr_error = Some("a pull request already exists".to_owned());

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("a pull request already exists"));
    }

    #[tokio::test]
    async fn rerun_after_push_failure_reuses_existing_squash_commit() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut first_attempt = MockTools::happy(&project_id);
        first_attempt.push_error = Some("transient push failure".to_owned());

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &first_attempt,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("transient push failure"));
        let squash_message = first_attempt
            .committed_message
            .lock()
            .unwrap()
            .clone()
            .expect("first attempt should create the squash commit before push fails");

        let mut retry = MockTools::happy(&project_id);
        retry.messages = vec![squash_message];
        retry.commit_paths = vec![];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &retry,
        )
        .await
        .unwrap();

        assert_eq!(
            output.pr_url,
            "https://github.com/acme/ralph-burning/pull/123"
        );
        assert!(output.warnings[0].contains("Squash commit already exists"));
        let calls = retry.calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            [
                "current_branch",
                "fetch",
                "ancestry",
                "messages",
                "committed_diff",
                "unstaged",
                "staged",
                "push",
                "pr"
            ]
        );
        assert!(retry.committed_message.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn rerun_after_pr_create_failure_reuses_existing_squash_commit() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut first_attempt = MockTools::happy(&project_id);
        first_attempt.pr_error = Some("gh api temporarily unavailable".to_owned());

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &first_attempt,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("gh api temporarily unavailable"));
        assert!(first_attempt
            .calls
            .lock()
            .unwrap()
            .contains(&"push".to_owned()));
        let squash_message = first_attempt
            .committed_message
            .lock()
            .unwrap()
            .clone()
            .expect("first attempt should leave the squash commit for retry");

        let mut retry = MockTools::happy(&project_id);
        retry.messages = vec![squash_message];
        retry.commit_paths = vec![];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &retry,
        )
        .await
        .unwrap();

        assert_eq!(
            output.pr_url,
            "https://github.com/acme/ralph-burning/pull/123"
        );
        let calls = retry.calls.lock().unwrap().clone();
        assert!(calls.contains(&"push".to_owned()));
        assert!(calls.contains(&"pr".to_owned()));
        assert!(!calls.contains(&"reset".to_owned()));
        assert!(!calls.contains(&"commit".to_owned()));
    }

    #[test]
    fn pr_body_renderer_includes_required_sections_and_boilerplate() {
        let bead = bead_detail();
        let rendered = render_pr_open_text(
            &bead,
            "1, 2, 0",
            &[DiffStat {
                path: "src/contexts/bead_workflow/pr_open.rs".to_owned(),
                additions: Some(120),
                deletions: Some(3),
            }],
        );

        assert!(rendered.body.contains("## Summary"));
        assert!(rendered.body.contains("Automate the drain-loop PR exit."));
        assert!(rendered.body.contains("Closes `2qlo`."));
        assert!(rendered.body.contains("## Run history"));
        assert!(rendered.body.contains("Convergence pattern: 1, 2, 0"));
        assert!(rendered.body.contains("## Diff stats"));
        assert!(rendered
            .body
            .contains(ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE));
        assert!(rendered.body.contains("## Test plan"));
        assert!(rendered.body.trim_end().ends_with("- [ ] nix build"));
    }

    #[tokio::test]
    async fn ignores_non_review_scope_metadata_for_diff_stats() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.diff_stats = vec![
            DiffStat {
                path: ".beads/issues.jsonl".to_owned(),
                additions: Some(1),
                deletions: Some(1),
            },
            DiffStat {
                path: ".ralph-burning/projects/2qlo-1/run.json".to_owned(),
                additions: Some(2),
                deletions: Some(0),
            },
            DiffStat {
                path: "src/cli/pr.rs".to_owned(),
                additions: Some(24),
                deletions: Some(0),
            },
            DiffStat {
                path: "target/debug/build-artifact".to_owned(),
                additions: Some(3),
                deletions: Some(0),
            },
        ];

        let output = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap();

        assert!(output.rendered.body.contains("src/cli/pr.rs"));
        assert!(!output.rendered.body.contains(".beads/issues.jsonl"));
        assert!(!output
            .rendered
            .body
            .contains(".ralph-burning/projects/2qlo-1/run.json"));
        assert!(!output.rendered.body.contains("target/debug/build-artifact"));
    }

    #[tokio::test]
    async fn metadata_only_diff_is_not_committed_as_real_code() {
        let project_id = ProjectId::new("project-2qlo").unwrap();
        let stores = stores(project_id.clone(), RunStatus::Completed);
        let mut tools = MockTools::happy(&project_id);
        tools.diff_stats = vec![DiffStat {
            path: ".beads/issues.jsonl".to_owned(),
            additions: Some(1),
            deletions: Some(1),
        }];

        let error = open_pr_for_completed_run(
            request(&project_id, None, true),
            store_refs(&stores),
            &MockBr {
                bead: bead_detail(),
            },
            &tools,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, PrOpenError::NoRealCodeChanges));
        assert!(!tools.calls.lock().unwrap().contains(&"commit".to_owned()));
    }

    fn stores(
        project_id: ProjectId,
        status: RunStatus,
    ) -> (MockProjectStore, MockRunStore, MockJournalStore) {
        (
            MockProjectStore {
                record: ProjectRecord {
                    id: project_id,
                    name: "2qlo".to_owned(),
                    flow: FlowPreset::Minimal,
                    prompt_reference: "prompt.md".to_owned(),
                    prompt_hash: "hash".to_owned(),
                    created_at: Utc::now(),
                    status_summary: ProjectStatusSummary::Completed,
                    task_source: Some(TaskSource {
                        milestone_id: "milestone".to_owned(),
                        bead_id: "2qlo".to_owned(),
                        parent_epic_id: None,
                        origin: TaskOrigin::Manual,
                        plan_hash: None,
                        plan_version: None,
                        plan_workstream_index: None,
                        plan_bead_index: None,
                    }),
                },
            },
            MockRunStore {
                snapshot: RunSnapshot {
                    active_run: None,
                    interrupted_run: None,
                    status,
                    cycle_history: vec![],
                    completion_rounds: 3,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: RollbackPointMeta::default(),
                    amendment_queue: AmendmentQueueState::default(),
                    status_summary: status.display_str().to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            },
            MockJournalStore {
                events: vec![
                    journal_event(1, JournalEventType::ProjectCreated, json!({})),
                    journal_event(
                        2,
                        JournalEventType::RunStarted,
                        json!({"run_id":"run-2qlo","first_stage":"plan_and_implement"}),
                    ),
                    journal_event(
                        3,
                        JournalEventType::CompletionRoundAdvanced,
                        json!({"run_id":"run-2qlo","from_round":1,"to_round":2,"amendment_count":1}),
                    ),
                    journal_event(
                        4,
                        JournalEventType::CompletionRoundAdvanced,
                        json!({"run_id":"run-2qlo","from_round":2,"to_round":3,"amendment_count":2}),
                    ),
                    journal_event(
                        5,
                        JournalEventType::RunCompleted,
                        json!({"run_id":"run-2qlo","completion_rounds":3}),
                    ),
                ],
            },
        )
    }

    fn journal_event(
        sequence: u64,
        event_type: JournalEventType,
        details: serde_json::Value,
    ) -> JournalEvent {
        JournalEvent {
            sequence,
            timestamp: Utc::now(),
            event_type,
            details,
        }
    }

    fn bead_detail() -> BeadDetail {
        BeadDetail {
            id: "2qlo".to_owned(),
            title: "Open PR automation".to_owned(),
            status: BeadStatus::InProgress,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Task,
            labels: vec![],
            description: Some(
                "Automate the drain-loop PR exit.\n\nSecond paragraph should not be summary."
                    .to_owned(),
            ),
            acceptance_criteria: vec!["cargo test passes".to_owned(), "nix build".to_owned()],
            dependencies: Vec::<DependencyRef>::new(),
            dependents: Vec::<DependencyRef>::new(),
            comments: vec![],
            owner: None,
            created_at: None,
            updated_at: None,
        }
    }
}
