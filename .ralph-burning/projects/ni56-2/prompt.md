# Bead 9ni.5.6: Tests for task-mode metadata, bead-backed task creation, and milestone linkage queries

## Problem description

The task substrate work in beads 9ni-5-* (project records carrying
`milestone_id`/`bead_id`, bead-backed creation, lineage queries) has
landed but lacks dedicated test coverage. This bead adds focused unit
tests verifying:

1. Task-mode metadata persistence and validation.
2. Bead-backed task creation surface (metadata propagation, normal-run
   compatibility, failure paths).
3. Lineage queries (by milestone, by bead).

The goal is **focused** test coverage — not a comprehensive rewrite,
not new public APIs. Use existing test fixtures and helpers.

## Required test cases

Place these in `tests/unit/project_run_record_test.rs` (or the closest
existing test module that already exercises the task substrate; pick
one and put all the new cases there for reviewability).

### A. Task-mode metadata persistence

- **A1.** Create a task-mode project record with non-empty
  `milestone_id` and `bead_id`. Save → load → assert both fields round-trip
  identically.
- **A2.** Create a non-milestone (legacy) project with no `milestone_id`
  / `bead_id`. Save → load → assert it remains a valid project record
  (backward compatibility — existing fields untouched).
- **A3.** Validation: a task-mode record with `milestone_id` set but
  empty `bead_id` (or vice versa) must fail to construct or load with a
  clear error. Tasks have *both or neither* — never one without the
  other.

### B. Bead-backed task creation

- **B1.** Use `execute_create_from_bead` (or the underlying pure
  service function that doesn't invoke the bead-claim subprocess) to
  create a project from a synthetic milestone bundle + bead. Assert the
  resulting project record carries the expected `milestone_id` and
  `bead_id`. Use a stub `BrAdapter` so the test doesn't touch the real
  beads database.
- **B2.** A task created from a bead must be compatible with a normal
  `run start` flow — load the task, assert `RunSnapshot::not_started`
  is consistent and `engine::execute_run` would accept the project (a
  stub-backend smoke test that drives one full run is sufficient).
- **B3.** Failure path — missing milestone: `create_from_bead` with a
  non-existent `milestone_id` produces `AppError::MilestoneNotFound`
  (or whatever the existing error variant is — match the production
  code, don't invent).
- **B4.** Failure path — missing bead: bead doesn't exist in the
  milestone bundle → clear error, no project record left on disk.

### C. Lineage queries

- **C1.** Create three projects: two for milestone `ms-alpha` (beads
  `ms-alpha.b-1`, `ms-alpha.b-2`), one for milestone `ms-beta` (bead
  `ms-beta.b-1`). Query "tasks for `ms-alpha`" — assert exactly the
  first two are returned, in a stable order.
- **C2.** Query "task for bead `ms-alpha.b-2`" — assert the right
  project_id is returned. Verify the helper is the existing one in
  `src/contexts/milestone_record/...` or `src/adapters/fs.rs`; do NOT
  invent a new query API.
- **C3.** Querying for a milestone with no tasks returns an empty list,
  not an error.

## Implementation guidance

- Use `tempfile::TempDir` for filesystem fixtures. Follow the patterns
  already in `tests/unit/project_run_record_test.rs` and the
  create-from-bead tests added in bead 9ni.5.7.
- Where production code exposes both the CLI-wrapper
  (`execute_create_from_bead`) and a pure service function, prefer the
  pure service for unit-test focus; only use the CLI wrapper when the
  test is genuinely about CLI behavior.
- Stub the `BrAdapter` so tests do not invoke the real `br` binary.
- Do NOT add new test-only fields to production types. If existing
  domain types lack a constructor or accessor needed by the test, add
  a focused `#[cfg(test)]` helper near the type, not a public API.

## Scope guard

- No production-code changes other than: (a) optional `#[cfg(test)]`
  helpers for fixture construction, (b) bug fixes that the new tests
  surface (in which case keep the fix narrow and document it in the
  commit).
- Do NOT broaden the test scope to cover the bead lifecycle subprocess
  (`br claim`, etc.) — those have their own beads and tests.
- Keep total new test code under ~600 lines. If you find yourself
  writing more, you're rewriting, not testing.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- All 3 + 4 + 3 = ~10 test cases above pass deterministically.
- Existing tests still pass.
- `nix build`, `cargo fmt --check`, `cargo clippy --locked -- -D warnings`,
  `cargo test --locked --features test-stub` all green.
