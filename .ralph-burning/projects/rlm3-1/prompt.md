# Bind stale-run recovery to specific run attempt and revalidate before reclaiming

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`, and config files.

## Problem

`RunPidRecord` stores process identity but not `run_id`/attempt metadata. A concurrent `run resume` can replace `run.pid` and the writer-lock owner after the stale snapshot was read. In that race, `run stop` can kill the new resumed orchestrator, and stale `run resume` can reclaim a fresh writer lease belonging to the new attempt.

## Fix

1. Persist active run identity (run_id and/or attempt number) in `run.pid` alongside PID and start time
2. Before signaling (in `run stop`), revalidate that the current `run.pid` still belongs to the stale attempt — abort if it changed
3. Before reclaiming a writer lease (in `run resume`), revalidate that the current owner/pid still belongs to the stale attempt
4. Search for `RunPidRecord`, `run.pid`, `writer_lease`, `reclaim` in the codebase

## Acceptance Criteria
- run.pid includes run identity metadata
- run stop aborts if run.pid changed between snapshot read and signal
- run resume aborts lease reclaim if owner changed
- Race condition tests covering concurrent stop/resume
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
