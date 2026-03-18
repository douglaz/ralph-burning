# Implementation Response (Iteration 20)

## Changes Made
1. **RC1 — Atomic GitHub metadata on task creation**: Added an optional `github_meta: Option<&GithubTaskMeta>` parameter to `create_task_from_watched_issue()` in `task_service.rs`. When provided, `repo_slug`, `issue_number`, `pr_url`, and dedup cursors are populated directly on the initial `DaemonTask` struct before `store.create_task()` is called. The GitHub intake path in `github_intake.rs` now builds the `GithubTaskMeta` *before* calling `create_task_from_watched_issue` and passes it as `Some(&github_meta)`, eliminating the two-step create-then-patch pattern. The idempotent `Ok(None)` early-return path can no longer strand a task without GitHub metadata. All non-GitHub callers (file-based watcher in `daemon_loop.rs`, unit tests, conformance scenarios) pass `None`.

2. **RC2 — Repo bootstrap credentials via environment, not argv**: Rewrote `bootstrap_repo_checkout()` in `repo_registry.rs` to inject `GITHUB_TOKEN` through environment-based Git config (`GIT_CONFIG_COUNT=1`, `GIT_CONFIG_KEY_0=http.extraHeader`, `GIT_CONFIG_VALUE_0=Authorization: Bearer <token>`) instead of top-level `git -c` flags. Environment variables are inherited by the child process but are not visible to other users via `/proc/<pid>/cmdline`, and they are never persisted into the cloned repo's `.git/config`. The clone URL remains a clean `https://github.com/<owner>/<repo>.git`.

3. **RI1 — Tightened idempotent comment detection**: Changed `post_idempotent_comment()` in `GithubClient` to match the exact hidden HTML comment form `<!-- marker -->` instead of bare `contains(marker)`. This prevents false positives on ordinary user text that coincidentally contains the marker string.

4. **Conformance coverage**: Extended `daemon.tasks.dedup_cursor_persisted` to verify that `create_task_from_watched_issue` with `github_meta` atomically persists `repo_slug`, `issue_number`, and `last_seen_comment_id` on the initial task record — confirmed both in-memory and after re-read from the store.

## Could Not Address
- None.

## Pending Changes (Pre-Commit)
- `src/contexts/automation_runtime/task_service.rs`: Added `github_meta` parameter to `create_task_from_watched_issue`, populating GitHub fields atomically.
- `src/contexts/automation_runtime/github_intake.rs`: Build `GithubTaskMeta` before task creation, pass atomically, removed post-creation patching.
- `src/contexts/automation_runtime/daemon_loop.rs`: Updated file-watcher call site with `None` for github_meta.
- `src/contexts/automation_runtime/repo_registry.rs`: Switched from `-c http.extraHeader` to `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_*`/`GIT_CONFIG_VALUE_*` env vars.
- `src/adapters/github.rs`: Tightened idempotent comment detection to match `<!-- marker -->`.
- `src/contexts/conformance_spec/scenarios.rs`: Updated all `create_task_from_watched_issue` call sites with `None`; added atomic metadata persistence test.
- `tests/unit/automation_runtime_test.rs`: Updated all call sites with `None`.

## Verification
- `cargo check` — clean
- 45 lib tests, 578 unit tests, 110 CLI tests, 286 conformance scenarios — all passing
