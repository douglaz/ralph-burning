#[cfg(unix)]
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use tempfile::tempdir;

use ralph_burning::adapters::worktree::WorktreeAdapter;
use ralph_burning::contexts::workflow_composition::checkpoints::{
    checkpoint_commit_message, parse_checkpoint_commit_message, VcsCheckpointPort,
};
use ralph_burning::shared::domain::{ProjectId, RunId, StageId};

fn git_binary() -> &'static Path {
    static GIT_BINARY: OnceLock<PathBuf> = OnceLock::new();
    GIT_BINARY
        .get_or_init(|| {
            let candidates = [
                std::env::var_os("RALPH_BURNING_TEST_GIT").map(PathBuf::from),
                Some(PathBuf::from("/run/current-system/sw/bin/git")),
                Some(PathBuf::from("/usr/bin/git")),
                Some(PathBuf::from("/usr/local/bin/git")),
                Some(PathBuf::from("/opt/homebrew/bin/git")),
            ];
            candidates
                .into_iter()
                .flatten()
                .find(|path| path.is_file())
                .or_else(|| {
                    // Fall back to searching PATH (needed in nix build sandbox)
                    std::env::var_os("PATH").and_then(|paths| {
                        std::env::split_paths(&paths)
                            .map(|dir| dir.join("git"))
                            .find(|p| p.is_file())
                    })
                })
                .unwrap_or_else(|| PathBuf::from("git"))
        })
        .as_path()
}

fn git_command() -> Command {
    Command::new(git_binary())
}

fn run_git(repo_root: &Path, args: &[&str]) -> String {
    let output = git_command()
        .args(args)
        .current_dir(repo_root)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git command");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[cfg(not(unix))]
fn git_output(repo_root: &Path, args: &[&str]) -> std::process::Output {
    git_command()
        .args(args)
        .current_dir(repo_root)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git command")
}

fn assert_checkpoint_omits_path(repo_root: &Path, checkpoint_sha: &str, path: &str) {
    #[cfg(unix)]
    {
        assert_checkpoint_omits_raw_path(repo_root, checkpoint_sha, Path::new(path));
    }
    #[cfg(not(unix))]
    {
        let tree = run_git(repo_root, &["ls-tree", "-r", "--name-only", checkpoint_sha]);
        assert!(
            !tree.lines().any(|line| line == path),
            "checkpoint commit should omit {path}, got:\n{tree}"
        );

        let show_path = git_output(repo_root, &["show", &format!("{checkpoint_sha}:{path}")]);
        assert!(
            !show_path.status.success(),
            "checkpoint commit should not retain {path}"
        );
    }
}

fn checkpoint_file_contents(repo_root: &Path, checkpoint_sha: &str, path: &str) -> String {
    run_git(repo_root, &["show", &format!("{checkpoint_sha}:{path}")])
}

#[cfg(unix)]
fn assert_checkpoint_omits_raw_path(repo_root: &Path, checkpoint_sha: &str, path: &Path) {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let tree = git_command()
        .args(["ls-tree", "-r", "-z", "--name-only", checkpoint_sha])
        .current_dir(repo_root)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git ls-tree");
    assert!(
        tree.status.success(),
        "git ls-tree failed: {}",
        String::from_utf8_lossy(&tree.stderr)
    );
    let tree_has_path = tree
        .stdout
        .split(|byte| *byte == 0)
        .any(|entry| !entry.is_empty() && entry == path.as_os_str().as_bytes());
    let tree = run_git(repo_root, &["ls-tree", "-r", "--name-only", checkpoint_sha]);
    assert!(
        !tree_has_path,
        "checkpoint commit should omit {:?}, got:\n{tree}",
        path
    );

    let mut show_spec = checkpoint_sha.as_bytes().to_vec();
    show_spec.push(b':');
    show_spec.extend_from_slice(path.as_os_str().as_bytes());
    let show_path = git_command()
        .arg("show")
        .arg(OsString::from_vec(show_spec))
        .current_dir(repo_root)
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git show");
    assert!(
        !show_path.status.success(),
        "checkpoint commit should not retain {:?}",
        path
    );
}

fn init_repo() -> tempfile::TempDir {
    let tmp = tempdir().expect("tempdir");
    run_git(tmp.path(), &["init"]);
    fs::write(tmp.path().join("README.md"), "initial\n").expect("write README");
    run_git(tmp.path(), &["add", "."]);
    run_git(tmp.path(), &["commit", "-m", "initial"]);
    tmp
}

fn init_repo_with_tracked_runtime_workspace() -> tempfile::TempDir {
    let tmp = init_repo();
    fs::create_dir_all(tmp.path().join(".ralph-burning/projects/demo"))
        .expect("create runtime workspace");
    fs::write(
        tmp.path().join(".ralph-burning/projects/demo/run.json"),
        "{\"status\":\"running\"}\n",
    )
    .expect("write runtime snapshot");
    run_git(tmp.path(), &["add", ".ralph-burning"]);
    run_git(tmp.path(), &["commit", "-m", "track runtime workspace"]);
    tmp
}

#[test]
fn checkpoint_commit_message_matches_required_format() {
    let project_id = ProjectId::new("proj").expect("project id");
    let run_id = RunId::new("run-123").expect("run id");
    let message = checkpoint_commit_message(&project_id, &run_id, StageId::Implementation, 2, 3);

    assert_eq!(
        message,
        "rb: checkpoint project=proj stage=implementation cycle=2 round=3\n\nRB-Project: proj\nRB-Run: run-123\nRB-Stage: implementation\nRB-Cycle: 2\nRB-Completion-Round: 3"
    );

    let parsed = parse_checkpoint_commit_message(&message).expect("parse checkpoint message");
    assert_eq!(parsed.project_id, "proj");
    assert_eq!(parsed.run_id, "run-123");
    assert_eq!(parsed.stage_id, "implementation");
    assert_eq!(parsed.cycle, 2);
    assert_eq!(parsed.completion_round, 3);
}

#[test]
fn worktree_adapter_creates_finds_and_resets_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let head_sha = run_git(tmp.path(), &["rev-parse", "HEAD"]);
    assert_eq!(head_sha, checkpoint_sha);

    let found_sha = adapter
        .find_checkpoint(tmp.path(), &project_id, StageId::Implementation, 1, 1)
        .expect("find checkpoint");
    assert_eq!(found_sha.as_deref(), Some(checkpoint_sha.as_str()));

    let commit_message = run_git(
        tmp.path(),
        &["show", "--quiet", "--format=%B", &checkpoint_sha],
    );
    assert_eq!(
        commit_message,
        checkpoint_commit_message(&project_id, &run_id, StageId::Implementation, 1, 1)
    );

    fs::write(tmp.path().join("README.md"), "after checkpoint\n").expect("write new content");
    run_git(tmp.path(), &["add", "."]);
    run_git(tmp.path(), &["commit", "-m", "after checkpoint"]);
    let newer_head = run_git(tmp.path(), &["rev-parse", "HEAD"]);
    assert_ne!(newer_head, checkpoint_sha);

    adapter
        .reset_to_checkpoint(tmp.path(), &checkpoint_sha)
        .expect("reset to checkpoint");
    let reset_head = run_git(tmp.path(), &["rev-parse", "HEAD"]);
    assert_eq!(reset_head, checkpoint_sha);
    assert_eq!(
        fs::read_to_string(tmp.path().join("README.md")).expect("read README"),
        "checkpointed content\n"
    );
}

#[test]
fn worktree_adapter_includes_workspace_in_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::create_dir_all(tmp.path().join(".ralph-burning/projects/demo"))
        .expect("create runtime workspace");
    fs::write(
        tmp.path().join(".ralph-burning/projects/demo/run.json"),
        "{\"status\":\"running\"}\n",
    )
    .expect("write runtime snapshot");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    run_git(tmp.path(), &["add", "-A"]);

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert!(
        tree.lines()
            .any(|line| line == ".ralph-burning/projects/demo/run.json"),
        "checkpoint commit should include workspace files, got:\n{tree}"
    );
}

#[test]
fn worktree_adapter_includes_tracked_workspace_in_checkpoint_commits() {
    let tmp = init_repo_with_tracked_runtime_workspace();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(
        tmp.path().join(".ralph-burning/projects/demo/run.json"),
        "{\"status\":\"paused\"}\n",
    )
    .expect("update runtime snapshot");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    run_git(tmp.path(), &["add", "-A"]);

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert!(
        tree.lines()
            .any(|line| line == ".ralph-burning/projects/demo/run.json"),
        "checkpoint commit should include tracked workspace files, got:\n{tree}"
    );

    let show_runtime = git_command()
        .args([
            "show",
            &format!("{checkpoint_sha}:.ralph-burning/projects/demo/run.json"),
        ])
        .current_dir(tmp.path())
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git show");
    assert!(
        show_runtime.status.success(),
        "checkpoint commit should include workspace state"
    );
    assert_eq!(
        String::from_utf8_lossy(&show_runtime.stdout).trim(),
        "{\"status\":\"paused\"}"
    );
}

#[test]
fn worktree_adapter_drops_tracked_gitignored_paths_from_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(tmp.path().join(".gitignore"), "ignored.txt\n").expect("write .gitignore");
    fs::write(tmp.path().join("ignored.txt"), "tracked but ignored\n").expect("write ignored");
    run_git(tmp.path(), &["add", ".gitignore"]);
    run_git(tmp.path(), &["add", "-f", "ignored.txt"]);
    run_git(tmp.path(), &["commit", "-m", "track ignored file"]);

    fs::write(tmp.path().join("ignored.txt"), "modified ignored content\n")
        .expect("modify ignored");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert_checkpoint_omits_path(tmp.path(), checkpoint_sha.as_str(), "ignored.txt");
}

#[test]
fn worktree_adapter_preserves_parent_version_for_assume_unchanged_paths_in_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(tmp.path().join("assume.txt"), "tracked content\n").expect("write assume.txt");
    run_git(tmp.path(), &["add", "assume.txt"]);
    run_git(tmp.path(), &["commit", "-m", "track assume-unchanged file"]);

    run_git(
        tmp.path(),
        &["update-index", "--assume-unchanged", "assume.txt"],
    );
    fs::write(tmp.path().join("assume.txt"), "modified ignored content\n").expect("modify assume");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert!(tree.lines().any(|line| line == "assume.txt"));
    assert_eq!(
        checkpoint_file_contents(tmp.path(), checkpoint_sha.as_str(), "assume.txt"),
        "tracked content"
    );

    adapter
        .reset_to_checkpoint(tmp.path(), &checkpoint_sha)
        .expect("reset to checkpoint");
    assert_eq!(
        fs::read_to_string(tmp.path().join("assume.txt")).expect("read assume.txt"),
        "tracked content\n"
    );
}

#[test]
fn worktree_adapter_drops_git_info_exclude_paths_from_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(tmp.path().join("locally-ignored.txt"), "tracked content\n")
        .expect("write locally ignored file");
    run_git(tmp.path(), &["add", "locally-ignored.txt"]);
    run_git(tmp.path(), &["commit", "-m", "track locally ignored file"]);

    fs::write(
        tmp.path().join(".git/info/exclude"),
        "locally-ignored.txt\n",
    )
    .expect("write info exclude");
    fs::write(
        tmp.path().join("locally-ignored.txt"),
        "modified tracked content\n",
    )
    .expect("modify locally ignored file");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert_checkpoint_omits_path(tmp.path(), checkpoint_sha.as_str(), "locally-ignored.txt");
}

#[test]
fn worktree_adapter_drops_core_excludes_file_paths_from_checkpoint_commits() {
    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");

    fs::write(tmp.path().join("globally-ignored.txt"), "tracked content\n")
        .expect("write globally ignored file");
    run_git(tmp.path(), &["add", "globally-ignored.txt"]);
    run_git(tmp.path(), &["commit", "-m", "track globally ignored file"]);

    let excludes_path = tmp.path().join("global-excludes");
    fs::write(&excludes_path, "globally-ignored.txt\n").expect("write global excludes file");
    run_git(
        tmp.path(),
        &[
            "config",
            "core.excludesFile",
            excludes_path.to_str().expect("utf-8 excludes path"),
        ],
    );
    fs::write(
        tmp.path().join("globally-ignored.txt"),
        "modified tracked content\n",
    )
    .expect("modify globally ignored file");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert_checkpoint_omits_path(tmp.path(), checkpoint_sha.as_str(), "globally-ignored.txt");
}

#[cfg(unix)]
#[test]
fn worktree_adapter_drops_non_utf8_gitignored_paths_from_checkpoint_commits() {
    use std::os::unix::ffi::OsStringExt;

    let tmp = init_repo();
    let adapter = WorktreeAdapter;
    let project_id = ProjectId::new("checkpoint-proj").expect("project id");
    let run_id = RunId::new("run-checkpoint").expect("run id");
    let path = PathBuf::from(OsString::from_vec(b"bad\xff.txt".to_vec()));

    fs::write(tmp.path().join(".gitignore"), "bad*.txt\n").expect("write .gitignore");
    fs::write(tmp.path().join(&path), "tracked but ignored\n").expect("write ignored");
    run_git(tmp.path(), &["add", ".gitignore"]);

    let add_output = git_command()
        .arg("add")
        .arg("-f")
        .arg(&path)
        .current_dir(tmp.path())
        .env("GIT_AUTHOR_NAME", "test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git add -f");
    assert!(
        add_output.status.success(),
        "git add -f failed: {}",
        String::from_utf8_lossy(&add_output.stderr)
    );
    run_git(tmp.path(), &["commit", "-m", "track ignored non-utf8 file"]);

    fs::write(tmp.path().join(&path), "modified ignored content\n").expect("modify ignored");
    fs::write(tmp.path().join("README.md"), "checkpointed content\n").expect("update README");

    let checkpoint_sha = adapter
        .create_checkpoint(
            tmp.path(),
            &project_id,
            &run_id,
            StageId::Implementation,
            1,
            1,
        )
        .expect("create checkpoint");

    let tree = run_git(
        tmp.path(),
        &["ls-tree", "-r", "--name-only", checkpoint_sha.as_str()],
    );
    assert!(tree.lines().any(|line| line == "README.md"));
    assert_checkpoint_omits_raw_path(tmp.path(), checkpoint_sha.as_str(), &path);
}
