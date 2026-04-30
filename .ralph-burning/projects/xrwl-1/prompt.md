# Bead xrwl — end-to-end drain harness test

## Problem

The drain-loop chain (4wdb → d31l → 2qlo → itn1 → gj74 → vl8z) is now
complete. Each piece has unit-level tests. What's missing is an
end-to-end test that exercises the full pipeline as a single unbroken
flow: `br ready` → bead pick → project create → run → gates → squash →
PR → poll → merge → bead close → next cycle.

This bead adds that harness. It's the regression test that proves the
drain-loop chain still works as a whole when any individual piece
changes.

## Required behavior

Add an integration-test-level harness that exercises `ralph drain` with
all dependencies stubbed out:

1. **Pre-create scratch beads** in a temporary `.beads/issues.jsonl`.
   Two or three small synthetic beads is enough — each with a
   different success/failure path (one happy-path, one with a known
   flake the rerun path handles, one with a permanent failure that
   should classify as `Abort`).

2. **Stub the run engine** so each bead's run "completes" via a
   deterministic outcome instead of invoking real backends. The
   existing `test_support` crate has fixtures for this; reuse them.

3. **Stub the PR tools** (`PrToolPort` from 2qlo, `PrWatchPort` from
   itn1) with mock implementations that return canned outcomes. No
   real `gh` invocations.

4. **Stub the git tools** so the harness doesn't actually mutate the
   repo. Reuse 2qlo's `GitPort` with a mock implementation.

5. **Run `ralph drain`** with the stubs wired in.

6. **Assert** the drain produced the expected `DrainOutcome` for each
   scenario:
   - Happy path: `Drained{cycles=N}`, all beads merged, all closed.
   - Known flake: rerun was attempted once before merge.
   - Permanent failure: drain stopped with `Failed`, follow-up bead
     filed.

7. **Exhaustiveness check**: assert each `RecoveryAction` variant from
   gj74 has at least one scenario in the harness.

## Subsumes

This bead replaces the original `9ni.10.2` and `9ni.10.2.2` (the e2e
scenarios filed for the milestone-planning angle). Those were
deprioritized to P3 when planning shifted external. Close them as
"replaced by xrwl" when this lands.

## Reuse

- All five drain modules expose library entry points; tests use those,
  not shell-outs.
- `test_support/fixtures.rs` already has helpers for in-memory
  workspaces; extend if needed but don't duplicate.
- Mock ports for PR/git tools should live in `test_support/` and be
  shared with the existing 2qlo / itn1 unit tests if that's a clean
  refactor.

## Tests

The harness IS the tests. One test per scenario in the "Required
behavior" list. Plus the exhaustiveness check.

Tests must be deterministic — no real time, no real I/O beyond temp
dirs, no real binaries. The polling loops in itn1 should use the
clock-injection mechanism that bead already has.

## Where to look

- `src/contexts/bead_workflow/drain.rs` (vl8z) — the drain function
  this harness exercises.
- `src/contexts/bead_workflow/{project_prompt,create_project,
  pr_open,pr_watch,drain_failure}.rs` — the modules vl8z composes.
- `src/test_support/` — existing test fixtures and mock ports.
- The previously-deferred beads `9ni.10.2` and `9ni.10.2.2` for the
  scope they covered; pick the parts that map to drain-loop e2e.

## Out of scope

- Real-end-to-end tests with real `gh`, `git`, and backends. Those
  belong in a separate higher-cost test layer (CI-only, not run on
  every developer commit).
- Performance tests / benchmarks. The harness measures correctness,
  not throughput.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy --locked -- -D warnings`,
  `cargo fmt --check` all pass.
- The harness has at least one scenario per `RecoveryAction` variant
  exposed by gj74's classifier.
- The harness runs in under 5 seconds (no real network/process).
- 9ni.10.2 and 9ni.10.2.2 are closed with a "replaced by xrwl" note.
