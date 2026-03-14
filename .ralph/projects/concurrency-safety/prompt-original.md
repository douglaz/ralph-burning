# Concurrency Safety: CLI writer lock + remove set_current_dir

## Objective

Fix two concurrency-related safety issues in the `ralph-burning-rewrite/` codebase.

## Issue 1: CLI writer lock lacks lease-backed TTL and self-healing (GitHub #3)

### Problem
`acquire_cli_writer_lock` in `src/cli/run.rs:38-48` acquires a bare writer-lock file with a static owner (`"cli"`) but no lease record, heartbeat, or TTL. If `run start` or `run resume` is interrupted by process termination before normal drop, the lock remains indefinitely and blocks future run/daemon operations for that project with `ProjectWriterLockHeld`. This is not self-healing through `daemon reconcile` because reconcile only scans lease records, not standalone writer-lock files.

### Required Changes
- Use the same lease-backed locking mechanism from `LeaseService::acquire()` (or factor it into a shared `ProjectLockPort` trait), with:
  - A proper lease record so reconcile can detect and clean stale CLI locks
  - RAII guard that releases on drop/panic
  - Optional heartbeat or TTL for crash recovery
- The `WriterLockGuard` in `run.rs:24-36` already has RAII drop semantics — extend it to also manage a lease record
- Ensure `daemon reconcile` can discover and clean stale CLI writer locks

## Issue 2: Remove process-global set_current_dir in daemon dispatch (GitHub #6)

### Problem
`dispatch_in_worktree()` in `src/contexts/automation_runtime/daemon_loop.rs` calls `std::env::set_current_dir(worktree_path)` to change the process-global working directory before invoking the engine. With `rt-multi-thread` tokio runtime:
- Signal handlers on other threads observe the mutated CWD
- If the engine panics, CWD is never restored
- Future concurrent task processing would silently inherit the wrong CWD

The call appears unnecessary — all file operations use explicit `base_dir` with absolute paths, and git operations use `Command::new().current_dir()`.

### Required Changes
- Remove all `std::env::set_current_dir()` calls from `daemon_loop.rs`
- If a future backend adapter needs to execute in the worktree directory, pass `worktree_path` as an explicit parameter on `InvocationRequest` instead
- Verify no code path implicitly relies on CWD being set to the worktree

## Constraints
- Do not change any public CLI behavior
- All existing tests (`cargo test`) and conformance scenarios (`ralph-burning conformance run`) must continue to pass
- Use `nix develop -c cargo test` and `nix develop -c cargo build` to build and test
- Add tests that verify:
  - CLI writer locks create lease records discoverable by reconcile
  - Stale CLI locks are cleaned up by reconcile
  - No `set_current_dir` calls remain in `daemon_loop.rs`
