## Bead ID: ralph-burning-rlm.3

## Goal

Bind stale-run recovery to specific run attempt and revalidate before reclaiming

## Description

RunPidRecord stores process identity but not run_id/attempt metadata. A concurrent run resume can replace run.pid and the writer-lock owner after the stale snapshot was read. In that race, run stop can kill the new resumed orchestrator, and stale run resume can reclaim a fresh writer lease belonging to the new attempt. Fix: persist active run identity in run.pid, abort/retry when snapshot changed before signaling or reclaiming, revalidate that current owner/pid still belongs to the stale attempt before reclaiming.

## Acceptance Criteria

- Existing tests pass
- cargo test && cargo clippy && cargo fmt --check
