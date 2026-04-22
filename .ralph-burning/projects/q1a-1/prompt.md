# Bead q1a: Document shadowed `task` binding in try_reconcile_success

## Problem description

In `src/contexts/automation_runtime/daemon_loop.rs::try_reconcile_success`,
`is_reconciliation_only_retry` reads `task.failure_class`. The `task`
binding at that point is a **pre-claim snapshot**, which shadows the outer
`task` parameter. This is correct (pre-claim retains `failure_class` while
post-claim `active_task` clears it) but subtle — a future refactor that
moves the snapshot or rebinds `task` could silently break the check.

Two use sites exist:
- `is_reconciliation_only_retry = ...` assignment (around line 1145)
- second use within the `match outcome` block (around line 1840 where the
  variable is referenced again)

## Fix

Add a short comment (1–3 lines) at each site clarifying that `task` here
refers to the pre-claim snapshot and why the distinction matters.

Example comment shape (adapt to the surrounding style):

```rust
// `task` here is the pre-claim snapshot; post-claim `active_task` has
// `failure_class` cleared, so this check relies on the shadowed binding.
```

Scope guard:
- Do NOT rename variables.
- Do NOT refactor the logic.
- Do NOT introduce a new field.
- The only change is adding a clarifying comment at each of the two use
  sites identified in the bead.

## Tests

No behavior change → no new tests. Existing tests should continue to pass.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files.

## Acceptance criteria

- Comment added at both use sites (pre-assignment and second reference)
  explaining the shadowing invariant.
- No other code changes.
- `nix build` passes.
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
