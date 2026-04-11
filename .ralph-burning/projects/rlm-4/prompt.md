## Bead ID: ralph-burning-rlm.4

## Goal

Make SIGTERM cleanup durable and kill backend subprocess trees on forced stop

## Description

Signal wrapper only cancels the token and waits; it never writes an interrupted snapshot or removes run.pid itself. PID cleanup is deferred to the outer CLI epilogue. If cancellation hangs or the process dies after writing the failed snapshot but before reaching outer cleanup, the project is left stale. Also: run stop SIGKILL only kills the orchestrator PID but backends are in their own process groups (process_group(0)). After forced stop, orphan backend processes keep running. And on non-Linux Unix, the descendant-group cleanup helper is missing entirely. Fix: move interrupted snapshot + pid cleanup into the signal/fail path, signal backend process groups on forced stop across all Unix platforms.

## Acceptance Criteria

- Existing tests pass
- cargo test && cargo clippy && cargo fmt --check
