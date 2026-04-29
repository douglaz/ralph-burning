# Bead 9ni.3.6 — preserve legacy requirements/project-seed behavior during rollout

## Problem

Phase-1 milestone rollout has been adding milestone-mode behavior to the
requirements pipeline (PRs #201 — bundle backlinks/staleness; #202 — auto-
create milestone on bundle completion). The non-goal is breaking the
*existing* single-project flows that don't go through milestone mode:

- the **quick requirements** path (interactive, no full pipeline)
- the **project-seed** terminal mode of full requirements (no MilestoneBundle)
- the **project bootstrap** entry point (`project bootstrap --idea ...`)
- the manual **project create from requirements** bridge for project-seed runs

Today these are not pinned by tests at the level needed to catch silent
regressions. This bead closes that gap by adding deterministic test
coverage that locks in the legacy behavior, so future milestone work
fails loudly if it touches these paths.

## Required behavior

Add or harden tests that assert each of the following invariants. Where
no clean fixture exists, add one using existing test_support patterns.

1. **Quick requirements path is unchanged.** A run kicked off via
   `requirements draft --idea ...` (the quick path, not full mode) must
   complete to a project seed without producing a `MilestoneBundle`,
   without firing any milestone-store calls, and without setting any
   active milestone. Test should assert:
   - run terminal status reaches `Completed`
   - `run.milestone_bundle` is `None`
   - no `MilestoneCreated` / `MilestoneJournalEvent` is emitted
   - `handle_create_from_requirements()` on this run still produces a
     plain project (no milestone), as it did before milestone mode.

2. **Project-seed full-mode terminal is unchanged.** Full-mode runs that
   reach `RequirementsStatus::Completed` via the project-seed path
   (not via `commit_milestone_bundle()`) must NOT trigger the auto-
   milestone-create added in #202. Test should assert:
   - `MilestoneStore::create_milestone` is not called
   - no `source_requirements_bundle` is persisted anywhere
   - the run produces a usable `RequirementsCreateHandoff::ProjectSeed`
     (not `::MilestoneBundle`).

3. **`project bootstrap` works without milestone state.** A bootstrap
   that does not opt into milestone mode produces a project with no
   active milestone, no milestone snapshot, and the project record
   uses the same shape it did before milestone work landed. Cover both
   `--start` and the deferred-start variant.

4. **Manual `project create --from-requirements` for project-seed runs.**
   The handler path that takes a project-seed handoff (not a
   MilestoneBundle handoff) must continue to materialize a plain
   project record without invoking `materialize_bundle_with_source()`.

## Approach

- Inventory the existing tests for these paths first
  (`tests/unit/requirements_drafting_test.rs`, `tests/cli.rs`,
  `tests/unit/project_run_record_test.rs`). Identify gaps; do not
  duplicate coverage that already exists.
- Reuse the test_support fixtures that materialize_bundle / project
  bootstrap tests already use; do not invent new ones unless needed.
- Tests must be deterministic (no real time/network/process — use the
  in-memory store ports already present in test_support).
- A test that fires `MilestoneStore::create_milestone` when it
  shouldn't is the load-bearing assertion: track call counts on the
  mock port and assert zero where appropriate.

## Where to look

- Quick requirements entry: `src/cli/requirements.rs`,
  `src/contexts/requirements_drafting/service.rs` (the non-full-mode
  branch in answer/commit).
- Project-seed terminal: `RequirementsDraftingService::commit_project_seed()`
  around `service.rs:2077`.
- Bootstrap entry: `src/cli/project.rs` (`handle_bootstrap`) or
  whatever currently dispatches `project bootstrap`.
- Manual bridge for non-bundle handoffs: `handle_create_from_requirements()`
  in `src/cli/project.rs:298-365` — confirm the project-seed branch
  still works.
- Mock stores for assertions: `src/test_support/` —
  `mock_milestone_store.rs` or equivalent. If a call-counting mock
  doesn't exist yet for the milestone store, add a thin one.

## Out of scope

- Changing any production behavior. This bead is test coverage only,
  unless an existing test fails on master because of an actual
  regression — in which case fix the regression and call it out in
  the run summary.
- Removing or refactoring the auto-create-milestone path added in
  #202. That path is correct; we just need to assert it doesn't fire
  on legacy modes.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`
  all pass.
- All four invariants above have at least one new test that asserts
  the behavior with concrete mock-store call-count checks where
  applicable. No silent passes (e.g., a test that compiles but
  doesn't actually run the path).
