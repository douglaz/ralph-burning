use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::tempdir;

use ralph_burning::adapters::worktree::WorktreeAdapter;
use ralph_burning::contexts::workflow_composition::checkpoints::{
    checkpoint_commit_message, parse_checkpoint_commit_message, VcsCheckpointPort,
};
use ralph_burning::shared::domain::{ProjectId, RunId, StageId};

fn run_git(repo_root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
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

fn init_repo() -> tempfile::TempDir {
    let tmp = tempdir().expect("tempdir");
    run_git(tmp.path(), &["init"]);
    fs::write(tmp.path().join("README.md"), "initial\n").expect("write README");
    run_git(tmp.path(), &["add", "."]);
    run_git(tmp.path(), &["commit", "-m", "initial"]);
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

    let commit_message = run_git(tmp.path(), &["show", "--quiet", "--format=%B", &checkpoint_sha]);
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
