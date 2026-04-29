# Bead 9ni.3.5 — auto-create milestone record from completed planner output

## Problem

Today the user has to manually run `project create --from-requirements <run-id>`
to materialize a milestone record from a completed requirements run. This is a
visible bridge step that disrupts the "idea → milestone → execution" flow:
the planner finishes, but nothing happens until the user remembers and runs
the bridge command.

This bead closes that gap. When the requirements pipeline transitions to
`Completed` in milestone mode (full-mode `MilestoneBundle` stage commits
successfully), a milestone record must be materialized automatically with
the source backlinks already wired up by 9ni.3.2.2 (PR #201).

## Current state (verified before this run)

- Full-mode bundle completion: `RequirementsDraftingService::commit_milestone_bundle()`
  in `src/contexts/requirements_drafting/service.rs:1872` sets
  `run.status = RequirementsStatus::Completed` and emits
  `RequirementsJournalEventType::RunCompleted` at line 1925.
- The runtime daemon already detects this transition for `RequirementsMilestone`
  dispatch (`src/contexts/automation_runtime/daemon_loop.rs:3287-3318`) and
  records `requirements_run_id` on the task via `RequirementsHandoff`. It does
  not currently call `materialize_bundle()`.
- `materialize_bundle()` in `src/contexts/milestone_record/service.rs:1371-1430`
  has the source-aware variant `materialize_bundle_with_source()` that
  persists `MaterializeBundleSource { requirements_run_id, milestone_bundle_id,
  schema_version, plan_hash }` on the snapshot
  (`MilestoneSnapshot.source_requirements_bundle`, model.rs:182, 235-240).
- Manual bridge: `handle_create_from_requirements()` in
  `src/cli/project.rs:298-365` extracts the bundle handoff, builds the source
  ref, and calls `materialize_bundle_with_source()`. This continues to be
  the correct path when called explicitly.
- Project-seed completion (`service.rs:2077`) is a *different* terminal mode
  (no MilestoneBundle); it must continue to behave as today.

## Required behavior

1. **Auto-create on milestone-bundle completion.** When
   `commit_milestone_bundle()` finishes successfully and the run reaches
   `RequirementsStatus::Completed`, materialize a milestone record with
   the same source backlinks `handle_create_from_requirements()` would
   have set. Reuse `materialize_bundle_with_source()`; do not duplicate
   its logic.

2. **Idempotent on re-run.** Re-running the planner for the same
   `(requirements_run_id, milestone_bundle_id)` must update the existing
   milestone, not create a duplicate. The natural collision key is the
   pair persisted in `source_requirements_bundle`. If a milestone with the
   same backlink already exists, update it in place; otherwise create.

3. **Failure is loud, not silent.** If the milestone store / journal /
   plan-store is unavailable when auto-create fires, the requirements
   completion must still succeed (it already happened) but the
   auto-create failure must surface as a structured error in the journal
   so the user can recover via `project create --from-requirements`. Do
   not roll back the requirements completion just because materialize
   failed.

4. **Project-seed mode is unchanged.** Only milestone-bundle completion
   triggers auto-create. The project-seed terminal path (service.rs:2077)
   must not call `materialize_bundle()` — it has no bundle to materialize.

5. **Activate the new milestone for downstream commands.** After auto-
   create, set the active milestone to the freshly materialized record
   so downstream commands (bead export, milestone show) work without an
   extra `task select`/`milestone select` step. (This matches what
   `handle_create_from_requirements()` already does.)

## Where to look

- Trigger point: `src/contexts/requirements_drafting/service.rs:1872`
  (commit_milestone_bundle) — wire the auto-create call here, after the
  status transition succeeds and the journal event is appended.
- Materialize entry: `src/contexts/milestone_record/service.rs:1371-1430`
  (`materialize_bundle_with_source`) — call this; do not re-implement.
- Manual bridge for cross-reference:
  `src/cli/project.rs:298-365` (`handle_create_from_requirements`).
- Daemon dispatch detection (informational, do not duplicate):
  `src/contexts/automation_runtime/daemon_loop.rs:3287-3318`.
- Tests: `tests/unit/requirements_drafting_test.rs` for the trigger path,
  `tests/unit/milestone_record_test.rs` for materialize idempotency.

## Tests

Add tests covering:
- A milestone-bundle completion in full mode produces a milestone record
  whose `source_requirements_bundle` matches the run's bundle id.
- Re-running the planner for the same run+bundle updates the existing
  milestone in place (no duplicates; backlink fields stable across runs).
- A project-seed completion does **not** create a milestone record.
- A materialize failure leaves the requirements run in `Completed` and
  surfaces a structured error event the test can assert on.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` all pass.
- The new tests assert all four scenarios listed above.
- Existing `handle_create_from_requirements()` continues to work as a
  manual recovery path; tests for it must not regress.
