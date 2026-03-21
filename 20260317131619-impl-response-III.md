# Implementation Response (Iteration 17)

## Changes Made
1. **RC1 — Repo bootstrap/start path**: Added `bootstrap_repo_checkout()` in `repo_registry.rs` that clones a repo from GitHub (using `GITHUB_TOKEN` for auth when available) if the checkout directory under the data-dir shard is empty. If `.git` already exists, bootstrap is a no-op that only ensures `.ralph-burning` workspace exists. Updated `handle_start_multi_repo()` in `daemon.rs` to call bootstrap before validate, so a fresh `daemon start --data-dir ... --repo owner/repo` can bring up a new repo shard without manual pre-population. Bootstrap failures fail early and explicitly with descriptive error messages.

2. **RC2 — Dedup cursor persistence**: Updated `build_github_meta()` to accept the raw `GithubComment` slice and compute `max(comment.id)` as the `last_seen_comment_id` cursor. Updated `poll_and_ingest_repo()` Phase B to keep full `GithubComment` objects (not just bodies) and pass them through to `build_github_meta()`. Added `update_task_cursor()` helper that updates the cursor on existing tasks during Phase A command polling. The `last_seen_review_id` remains `None` at intake time (slice-9 PR review ingestion responsibility).

3. **RI1 — Worktree adapter alignment**: Updated `WorktreeAdapter::worktree_path()` from `base_dir/.ralph-burning/worktrees/<task_id>` to `base_dir/worktrees/<task_id>`, and `WorktreeAdapter::branch_name()` from `rb/task/<task_id>` to `rb/<task_id>`. Updated all test fixtures, CLI tests, unit tests, and conformance scenarios that referenced the old paths. The multi-repo daemon path continues to use `DataDirLayout` overrides for the fully-qualified data-dir paths, but the adapter defaults now align with the slice-8 contract.

4. **Conformance coverage**:
   - Extended `daemon.github.start_validates_repos_and_data_dir` to verify bootstrap behavior: bootstrap on an empty checkout fails explicitly (no real GitHub in tests), and bootstrap on a pre-existing git checkout succeeds and creates the `.ralph-burning` workspace.
   - Added `daemon.tasks.dedup_cursor_persisted` scenario verifying that `build_github_meta` computes max comment ID as cursor, returns `None` for empty comments, and leaves review cursor as `None`.

## Could Not Address
- None.

## Verification
- `cargo check` — clean
- 45 lib tests passed
- 578 unit tests passed
- 110 CLI tests passed
- 286 conformance scenarios passed
