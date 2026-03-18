# Implementation Response (Iteration 4)

## Changes Made
1. **RC1 — Repo workspace boundary**: Split `process_task_multi_repo` and its callees (`handle_requirements_quick`, `handle_requirements_draft`, `ensure_project`, `drive_dispatch`, `dispatch_in_worktree`, `check_waiting_tasks`) to accept both a `store_dir` (daemon shard for task/lease/journal ops) and a `workspace_dir` (checkout root for project/run/requirements ops). In multi-repo mode, `store_dir` = `<data-dir>/repos/<owner>/<repo>/daemon` and `workspace_dir` = `<data-dir>/repos/<owner>/<repo>/repo`. In single-repo mode, both resolve to `base_dir`. Requirements calls (`req_svc.quick`, `req_svc.draft`, `extract_seed_handoff`, `is_requirements_run_complete`), project creation (`ensure_project`), run snapshots, and engine dispatch now use `workspace_dir`; daemon task store, lease, and journal ops continue using `store_dir`.

2. **RC2 — Production intake path**: `daemon start` without `--data-dir` is now only available with `--single-iteration` (test-only; no production daemon runs single-iteration). Without either flag, the CLI returns a clear error. The doc comment and error message explain that `--data-dir` is required for production. The `handle_start_legacy` function and `FileIssueWatcher` remain in the codebase for test use only and are no longer reachable from a production daemon invocation.

3. **RC3 — GitHub labels and repo-failure handling**:
   - **Startup label ensure**: `ensure_labels_on_repos` now returns `Vec<RepoRegistration>` (the set of repos that succeeded). Repos where label ensure fails are quarantined with a log message. `run_multi_repo` uses only the active registrations for polling; if all repos fail, the daemon errors out.
   - **Intake label correctness**: Intake no longer removes `rb:ready` when creating a Pending task. Since `Pending` maps to `rb:ready` in `label_for_status`, the label stays until the daemon loop calls `sync_label_for_task` at claim time (transitioning to `rb:in-progress`).
   - **Label sync failure propagation**: `sync_label_for_task` now propagates real failures (non-404 remove errors and add errors) instead of swallowing them. Critical-path callers in `process_task_multi_repo` (claim, active, completed, failed) propagate errors with `?`. Error-recovery callers (already-failed/aborted paths) remain best-effort. The per-task loop in `process_cycle_multi_repo` breaks on error, stopping further task mutations for that repo in the cycle.
   - **CLI abort/retry label sync**: `handle_abort_by_issue` and `handle_retry_by_issue` now create a `GithubClient` from env and call `sync_label_for_task` after mutating durable task state. Failures are logged but do not block the CLI command (the durable state is authoritative).

## Could Not Address
None

## Verification
- `cargo check` compiles cleanly
- 733 tests passing (45 lib + 578 unit + 110 CLI), 0 failures, 1 ignored (pre-existing)
- All 274 conformance scenarios pass including the full suite
