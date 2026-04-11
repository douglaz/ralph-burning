## Bead ID: ralph-burning-rlm.1

## Goal

Make status reconciliation attempt-aware to prevent stale terminal events from overriding live runs

## Description

Status reconciliation uses the journal's global last terminal event, but resumed attempts reuse the same run_id. A live resumed run still has an older run_failed for that run_id, so handle_status can incorrectly patch the current Running snapshot to Failed/Completed. The reconciliation needs to ignore terminal events that occurred before the most recent run_started/run_resumed boundary for the current attempt. Also handle the edge case where run_started is missing but a terminal event exists.

## Acceptance Criteria

- Existing tests pass
- cargo test && cargo clippy && cargo fmt --check
