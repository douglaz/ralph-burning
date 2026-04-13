# Add run liveness tracking, stop command, and automatic stale-run recovery

## Context

This bead covers making run lifecycle robust against process death. Some parts may already be implemented — check what exists before adding new code.

`run stop` already exists and works (it detects stale processes and transitions to failed). Check the current implementation and identify what's missing from the full bead spec.

## What to check first

1. Does `run stop` already handle the stale case? (Yes — it was used successfully)
2. Is there a PID file mechanism? Check for `run.pid` in the codebase
3. Does `run status` detect stale Running states?
4. Does `run resume` auto-recover from stale Running states?
5. Is there SIGTERM signal handling in the orchestrator?

## What may still be needed

Based on the bead description, verify and implement any missing pieces:

1. **PID/liveness tracking**: Orchestrator writes a PID file on start, checks on status/resume
2. **Stale detection in `run status`**: When Running, check if PID is alive; report stale if dead
3. **Auto-recovery in `run resume`**: When Running with dead PID, auto-transition to Failed and continue resume
4. **Signal handling**: SIGTERM handler for clean snapshot writes on graceful shutdown

Search for existing implementations: `run.pid`, `kill`, `proc`, `signal`, `SIGTERM`, `liveness`, `stale` in the codebase.

## Acceptance Criteria
- Orchestrator writes a PID file on run start, removes on clean exit
- `run stop` gracefully stops a running orchestrator (already works — verify)
- `run status` detects stale Running states and reports accurately
- `run resume` auto-recovers from stale Running without manual intervention
- Signal handler ensures clean snapshot writes on SIGTERM
- cargo test && cargo clippy && cargo fmt --check pass
