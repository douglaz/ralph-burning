## Bead ID: ralph-burning-rlm

## Goal

Make run lifecycle robust against process death: track whether the orchestrator is alive, add a `run stop` command for graceful shutdown, and let `run status` and `run resume` automatically detect and recover from stale Running states.

## Problem

When the ralph-burning process is killed (SIGKILL, OOM, terminal closed), the run snapshot stays in `Running` status with `active_run` populated. This creates a stuck state that requires manual JSON surgery to fix:

1. `run resume` rejects `Running` status (engine.rs line ~1358) — it only accepts `Failed` or `Paused`
2. `run status` reports "running" even though nothing is running
3. There is no `run stop` command for graceful shutdown
4. The journal reconciliation (engine.rs line ~1336) only catches the case where `fail_run` wrote a journal event but the snapshot write failed — it cannot detect a killed process

## Changes Required

### 1. PID/liveness file for the orchestrator

**File:** `src/adapters/fs.rs`

Write a `run.pid` file in the live project root when a run starts. Contents: PID + start timestamp as JSON. On `run status` and `run resume`, check if the PID is still alive (`kill(pid, 0)` or reading `/proc/<pid>/stat`). Add helpers: `write_pid_file`, `read_pid_file`, `remove_pid_file`, `is_pid_alive`.

### 2. `run stop` command

**Files:** `src/cli/run.rs`, `src/contexts/workflow_composition/engine.rs`

Add a `run stop` subcommand that:
- Reads the PID file to find the orchestrator process
- Sends SIGTERM and waits briefly (e.g. 5 seconds) for graceful shutdown
- If the process doesn't exit, sends SIGKILL
- Transitions the snapshot from Running to Failed with `interrupted_run` populated
- Cleans up the PID file
- Reports what happened

### 3. Stale-run detection in `run status`

**File:** `src/cli/run.rs`

When status shows Running, check the PID file:
- PID alive → genuinely running, report normally
- PID dead or PID file missing → stale, report "stale (process not found)" and suggest `run resume`

### 4. Automatic recovery in `run resume`

**File:** `src/contexts/workflow_composition/engine.rs` (~line 1332)

When snapshot is Running and the PID is dead (or PID file missing):
- Automatically transition to Failed (move `active_run` to `interrupted_run`)
- Log the reconciliation
- Continue with normal resume flow
- This replaces the current hard error at line ~1358

### 5. Signal handling in the orchestrator

**File:** `src/contexts/workflow_composition/engine.rs` or `src/cli/run.rs`

Register a SIGTERM handler (using `tokio::signal`) that:
- Sets the cancellation token (triggers graceful shutdown of current backend invocation)
- Writes a clean Failed snapshot with `interrupted_run` before exiting
- Removes the PID file
- This makes `run stop` → `run resume` a clean cycle

### 6. PID file cleanup on normal exit

Ensure the PID file is removed on both normal completion and on `fail_run`.

## Acceptance Criteria

- Orchestrator writes a PID file on run start, removes on clean exit
- `run stop` gracefully stops a running orchestrator and transitions to resumable state
- `run status` detects stale Running states (dead PID) and reports accurately
- `run resume` automatically recovers from stale Running states without manual JSON editing
- Signal handler ensures clean snapshot writes on SIGTERM
- Existing tests pass
- `cargo test && cargo clippy && cargo fmt --check`
