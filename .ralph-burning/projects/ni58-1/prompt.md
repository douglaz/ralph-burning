# Bead 9ni.5.8: Use stable bead-plan binding instead of mutable title matching

## Problem description

`execute_create_from_bead` in `src/cli/project.rs` currently calls
`resolve_bead_plan(&milestone_bundle.bundle, &milestone_id, &bead)` to
find plan metadata for a bead. That resolution still leans on exact
title matching in places — if an operator edits a bead's title in the
beads graph after the milestone plan was generated, the plan lookup can
silently miss and downstream flow-override / membership validation
behaves unexpectedly.

The authoritative source for a bead's identity is its stable ID
(`bead.id`, e.g. `ralph-burning-9ni.5.8`), not its title. The milestone
plan should index beads by ID and fall back gracefully when a title
drifts.

## Required changes

### 1. ID-first lookup

In `src/contexts/milestone_record/bundle.rs` (or the `resolve_bead_plan`
call site, whichever owns the lookup), switch the primary lookup to use
`bead.id` against the plan's stable bead identifier. If the plan
entries carry the bead_id as a stable field, use it directly. If the
plan structure currently only has titles, extend it to carry a stable
`bead_id` field per entry and populate it when the plan is generated
(lookup by title-at-plan-time is fine *at plan generation*, but the
stored representation must include the stable ID).

Fallbacks allowed:
- If a plan entry has no `bead_id` (legacy plans generated before this
  change), fall back to title matching with a `tracing::warn!` telling
  operators the plan is legacy and will stop matching once the title
  drifts.

### 2. Narrow `--flow` override scope

Currently an explicit `--flow` override may shortcut past
plan-membership validation in `execute_create_from_bead`. That's wrong:
the operator asked for a specific flow, not to bypass the invariant
that the bead belongs to the referenced milestone's plan. Split the two
concerns so `--flow` affects only flow selection and membership
validation runs unconditionally (unless the bead was deliberately
created outside any milestone, which is a separate branch).

### 3. Preserve operator usability

- Editing a bead's title in-place must not break
  `project create-from-bead` for a valid milestone/bead pair.
- A well-formed milestone plan that predates this change (no stable
  `bead_id` on entries) should still function with the documented
  fallback; emit a one-time `warn!` per milestone, not per invocation.
- If ID lookup succeeds but title-at-plan-time no longer matches the
  bead's current title, log a debug-level message; do NOT fail the
  command.

## Tests

In the relevant unit test module (likely
`tests/unit/project_run_record_test.rs` or a focused
create-from-bead test module):

- **ID match with drifted title.** Plan entry has `bead_id =
  "ms-x.bead-1"` and `title = "Old title"`. Bead in the beads graph has
  the same ID but title `"New title"`. `create-from-bead` resolves the
  plan entry by ID, succeeds, and logs no error.
- **Legacy plan fallback.** Plan entry has no `bead_id` field (simulate
  deserialized-from-legacy-JSON path). Bead title matches the plan
  title. Lookup falls back to title match, succeeds, emits the
  legacy-plan warning.
- **Legacy plan + title drift = helpful error.** Plan entry has no
  `bead_id` AND the title no longer matches. Lookup fails with an
  actionable error pointing the operator at regenerating the milestone
  plan.
- **Flow override doesn't skip membership validation.**
  `create-from-bead --flow minimal` against a milestone where the bead
  isn't in the plan still rejects (the old bypass path must not
  survive).
- **Flow override affects flow only.** `create-from-bead --flow minimal`
  against a valid milestone/bead pair creates the project with
  `FlowPreset::Minimal` regardless of the plan's `default_flow`.

## Scope guard

- Do NOT change the public CLI surface (flag names, argument order).
- Do NOT rename the bead or plan structures' public types.
- Do NOT remove the title field from plan entries; keep it for display
  and for the legacy fallback.
- Plan generation (`milestone plan`) should populate `bead_id` on
  entries going forward; legacy plans stay readable.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- `execute_create_from_bead` resolves plan entries by stable `bead_id`
  primary, with a logged fallback to title matching on legacy plans.
- Bead title edits do not break `project create-from-bead` for a valid
  milestone/bead pair where the plan has a stable `bead_id`.
- `--flow` override changes flow selection but cannot bypass
  plan-membership validation.
- Legacy plans (no `bead_id`) emit a one-time warn per milestone.
- Test coverage above exists and passes.
- `nix build` passes.
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
