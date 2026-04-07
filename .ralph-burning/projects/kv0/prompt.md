## Bead ID: ralph-burning-kv0

## Goal

Add a bounded timeout to `spawn_background_reap` so that truly unkillable processes (e.g., uninterruptible I/O state on Linux) don't block the tokio task forever.

## Problem

In `spawn_background_reap` (`src/adapters/process_backend.rs:1557-1563`), `child.wait().await` has no timeout. If a process is unkillable, the tokio task blocks indefinitely and the `Arc<ManagedChild>` is never dropped.

## Fix

Add a bounded timeout (e.g., 30s) after `force_kill`. If it expires, log a warning and drop the handle so the task completes.

## Files

- `src/adapters/process_backend.rs` — `spawn_background_reap`

## Acceptance Criteria

- After force_kill, spawn_background_reap waits at most 30 seconds for the child to exit
- If the timeout expires, a warning is logged and the task completes (dropping the handle)
- Existing tests pass
- New test covers the timeout path
