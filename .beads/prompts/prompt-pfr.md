## Bead ID: ralph-burning-pfr

## Goal

Run the final_review panel reviewers concurrently instead of sequentially, and do the same for the voting phase. This cuts wall-clock time roughly in half for the 2-reviewer default panel.

## Context

Currently in `src/contexts/workflow_composition/final_review.rs`, both the proposal phase (~line 282) and the voting phase (~line 892) iterate over panel members sequentially with a `for` loop, `await`ing each reviewer/voter one at a time. Since reviewers are independent (they don't need each other's output), they can run in parallel via `tokio::JoinSet` or `tokio::join!`.

## Changes Required

### 1. Parallelize proposal phase
**File:** `src/contexts/workflow_composition/final_review.rs` (~line 282)

Replace the sequential `for` loop over `panel.reviewers` with concurrent spawning:
- Use `tokio::JoinSet` to spawn each reviewer invocation concurrently
- Collect results as they complete
- Preserve `BackendExhausted` handling per-reviewer (skip exhausted, proceed with rest)
- Preserve journal event ordering (started/completed events per reviewer)
- Ensure `cancellation_token` propagation works across all spawned tasks

### 2. Parallelize voting phase
**File:** `src/contexts/workflow_composition/final_review.rs` (~line 892)

Same pattern for the voting loop — spawn all voters concurrently.

### 3. Preserve event ordering
Journal events (`reviewer_started`, `reviewer_completed`) should still be emitted per-reviewer but may now interleave. Ensure the events remain correct and parseable. Consider collecting events and emitting them in a deterministic order if needed.

### 4. Handle partial failures
If one reviewer fails with `BackendExhausted` while others are still running, the remaining reviewers should still complete. Only cancel outstanding tasks if ALL backends fail or if a fatal error occurs.

### 5. Thread-safety considerations
The current sequential code passes `&mut` references to journal stores and log writers. For concurrent execution, these need to either:
- Be wrapped in `Arc<Mutex<_>>` for shared mutable access
- Or have events collected per-task and merged after all tasks complete
- Or use message passing (channels) to a single writer task

Choose the approach that minimizes code churn while maintaining correctness.

## Acceptance Criteria

- Reviewers run concurrently via `tokio::JoinSet` or equivalent
- Voters run concurrently via `tokio::JoinSet` or equivalent
- `BackendExhausted` handling still works (skip exhausted, proceed with rest)
- Journal events are correct for each reviewer/voter
- Cancellation token cancels all concurrent tasks
- No data races or shared mutable state issues
- Existing tests pass
- `cargo test && cargo clippy && cargo fmt --check`
