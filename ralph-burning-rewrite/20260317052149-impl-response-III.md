# Implementation Response (Iteration 1)

## Changes Made

### RC1: GitHub-backed multi-repo daemon start path
- Added `run_multi_repo<G: GithubPort>()` and `process_cycle_multi_repo<G: GithubPort>()` methods to `DaemonLoop` that poll GitHub via `github_intake::poll_and_ingest_repo()` for each registered repo per cycle.
- `handle_start_multi_repo` in `cli/daemon.rs` now creates a `GithubClient` from environment and calls `run_multi_repo()` instead of the single-repo `run()`.
- Used generic type parameter `G: GithubPort` instead of `&dyn GithubPort` to avoid dyn-compatibility issues with async traits.
- **Files**: `daemon_loop.rs`, `cli/daemon.rs`

### RC2: Daemon state and worktree layout
- Added `FsDataDirDaemonStore` in `adapters/fs.rs` — implements `DaemonStorePort` where `base_dir` IS the daemon directory directly (paths resolve as `base_dir/tasks`, `base_dir/leases`, `base_dir/journal.ndjson`), supporting `<data-dir>/repos/<owner>/<repo>/daemon/` layout.
- Added `process_task_multi_repo()` to `daemon_loop.rs` that separates `store_dir` (daemon dir) from `repo_root` (checkout), with worktree path and branch name overrides computed from `DataDirLayout` helpers.
- Added `worktree_path_override: Option<PathBuf>` and `branch_name_override: Option<String>` parameters to `LeaseService::acquire()` and `DaemonTaskService::claim_task()`.
- Updated all conformance scenarios (`multi_repo_status`, `abort_by_issue_number`, `retry_failed_issue`, `reconcile_stale_leases`) and CLI handlers to use `FsDataDirDaemonStore` with data-dir paths.
- **Files**: `fs.rs`, `daemon_loop.rs`, `lease_service.rs`, `task_service.rs`, `scenarios.rs`, `cli/daemon.rs`, `automation_runtime_test.rs`

### RC3: Explicit GitHub commands (`/rb run`, `/rb retry`, `/rb abort`)
- Updated `github_intake::poll_and_ingest_repo()` to fetch issue comments via `github.fetch_issue_comments()` and process `/rb retry` and `/rb abort` inline during polling (retry looks up failed task and retries; abort looks up non-terminal task and aborts).
- `/rb run` is treated as "run with default routing" — filtered from `routing_command` in `create_task_from_watched_issue` alongside other daemon commands, so label/default routing applies.
- **Files**: `github_intake.rs`, `task_service.rs`

### RC4: Durable repo registration
- Added `FsRepoRegistryStore` in `adapters/fs.rs` — implements `RepoRegistryPort` with JSON registration files at `<data-dir>/repos/<owner>/<repo>/registration.json`.
- `handle_start_multi_repo` now persists registrations via `FsRepoRegistryStore` before entering the daemon loop.
- `handle_status_multi_repo` reads registrations from `FsRepoRegistryStore` instead of rebuilding by directory scan.
- **Files**: `fs.rs`, `cli/daemon.rs`

## Could Not Address

None — all four required changes have been addressed.

## Pending Changes (Pre-Commit)

8 files changed, +1041 / -325 lines across:

| File | Summary |
|------|---------|
| `adapters/fs.rs` | `FsDataDirDaemonStore`, `FsRepoRegistryStore`, shared `release_writer_lock_impl` |
| `cli/daemon.rs` | Multi-repo start uses `GithubClient` + `run_multi_repo()`; status/abort/retry/reconcile use `FsDataDirDaemonStore` |
| `daemon_loop.rs` | `run_multi_repo`, `process_cycle_multi_repo`, `process_task_multi_repo` with per-repo isolation |
| `github_intake.rs` | Comment fetching, inline `/rb retry` and `/rb abort` handling |
| `lease_service.rs` | Worktree path/branch name override parameters |
| `task_service.rs` | Override passthrough + daemon command filtering from flow routing |
| `scenarios.rs` | 4 conformance scenarios updated to data-dir layout |
| `automation_runtime_test.rs` | Updated `claim_task` and `acquire` calls with `None, None` |
