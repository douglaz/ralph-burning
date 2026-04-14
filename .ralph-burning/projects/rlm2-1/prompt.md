# Make PID liveness safe across platforms and after PID reuse

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Problem

Non-Linux builds only populate `proc_start_marker` but `is_pid_alive` only checks `proc_start_ticks`, making macOS/BSD runs unrecoverable from `run.pid`. Legacy pid files without `proc_start_ticks` are treated as stale even when the process is alive, or conversely treated as live after PID reuse via raw `kill(pid, 0)`.

## Fix

1. Honor `proc_start_marker` on non-Linux platforms when `proc_start_ticks` is unavailable
2. Treat missing start-time fields as unsafe/legacy — don't assume the process is alive or dead
3. Compare live process start time against recorded `started_at` for legacy records to detect PID reuse
4. Search for `is_pid_alive`, `proc_start_ticks`, `proc_start_marker`, `process_identity_is_alive` in `src/adapters/fs.rs` and related files

## Acceptance Criteria
- PID liveness checks work correctly on macOS/BSD (using proc_start_marker)
- Legacy pid files without start-time fields are handled safely
- PID reuse is detected by comparing process start times
- All tests pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
