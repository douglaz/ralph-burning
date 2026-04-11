## Bead ID: ralph-burning-rlm.5

## Goal

Add daemon-owned run SIGTERM cleanup and surface partial lease cleanup failures

## Description

Daemon dispatch path lacks the forced SIGTERM cleanup that the CLI wrapper now has. If the daemon is SIGKILLed by a supervisor because the backend ignored cancellation, the run is left stale with a lingering run.pid. Also: partial stale-owner cleanup (writer lock released but worktree/lease cleanup fails) is silently ignored before reacquiring the CLI writer lease, allowing resume/stop to continue on contradictory durable state. Fix: add equivalent termination cleanup to daemon loop, surface and block on partial cleanup failures.

## Acceptance Criteria

- Existing tests pass
- cargo test && cargo clippy && cargo fmt --check
