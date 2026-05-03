# Bead uh1: Clean up remaining planner references after panel removal

## Problem description

Follow-up from bead `aro` (remove planner from final review panel). After the
planner role was removed from the final-review pipeline, stale references still
linger in source, tests, and docs. They mislead readers and the dead code paths
make it harder to reason about backend role routing.

The following places still mention `planner` and must be cleaned up unless the
reference is genuinely about a non-final-review planner concept (e.g. the
requirements/milestone planner — leave those alone).

1. `src/contexts/agent_execution/policy.rs` — `BackendPolicyRole::Planner`,
   `planner_family_for_cycle`, `planner_backend`, and the related match arms
   and resolution logic are dead. Remove the variant and the planner-specific
   branches; keep only roles still used by the panel
   (Reviewer / Completer / Arbiter / Reasoner / etc.).
2. `src/contexts/workflow_composition/panel_contracts.rs` — doc comments like
   "Per-amendment vote used by both the planner-position step and reviewer
   vote step" describe the old two-step flow. Rewrite to describe the current
   reviewer-votes-directly behavior.
3. `src/contexts/conformance_spec/scenarios.rs` — any `final_review:voter`
   payload sequences that still include `Planner position.` entries should be
   updated to match the new direct-vote flow.
4. `docs/templates.md` — remove `planner_positions` placeholder references in
   the voter / arbiter template docs.
5. `tests/unit/backend_diagnostics_test.rs` — test function names or fixtures
   that still reference `planner` in the final-review context should be
   renamed / updated.

Hints:
- Use `rg -n planner` (and `rg -ni planner`) to scope the cleanup; keep
  `requirements_drafting` planner code (that's the planning/milestone planner,
  not the final-review planner).
- Confirm `BackendPolicyRole::Planner` is truly unused by searching callers
  before deleting the variant.
- If a planner reference is ambiguous, leave a short `//` comment explaining
  which planner it refers to instead of deleting.

## Implementation hints

Relevant code paths:
- `src/contexts/agent_execution/policy.rs` — policy role enum and routing
- `src/contexts/workflow_composition/panel_contracts.rs` — panel contract docs
- `src/contexts/conformance_spec/scenarios.rs` — conformance expectations
- `docs/templates.md` — template authoring docs
- `tests/unit/backend_diagnostics_test.rs` — diagnostics tests

Patterns to follow:
- Keep public API changes minimal; remove the variant only if nothing outside
  the context uses it.
- Update any `match` that exhaustively handles `BackendPolicyRole` once the
  variant is removed.
- Do not touch `src/contexts/requirements_drafting/**` — that planner is the
  requirements/milestone planner and is still valid.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files. The same applies to `.beads/` — that is durable bead state,
not code.

## Acceptance criteria

- `rg -n "BackendPolicyRole::Planner"` returns no matches in `src/` or `tests/`.
- No final-review code or test references a `planner` role (requirements
  planner references in `src/contexts/requirements_drafting/` remain).
- `docs/templates.md` no longer references `planner_positions` in the
  voter/arbiter template sections.
- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` all pass.
