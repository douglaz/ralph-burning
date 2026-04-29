# Bead 9ni.3.7 — requirements tests for cache, invalidation, resume, bundle handoff

## Problem

The requirements pipeline now has milestone-mode behavior layered on top of
the legacy single-project flow (PRs #201, #202, #203). The internal mechanics
that make that pipeline safe — stage-output caching, question-round
invalidation cascade, run resume after partial progress, bundle handoff
to materialize_bundle — need deterministic regression coverage that
isn't already provided by 9ni.3.6 (which pinned the legacy *entry* paths).

This bead fills the gap with internal-mechanism tests.

## Scope

Add or extend tests covering each of the following. Focus on the internals,
not user-facing CLI surfaces (those are covered by 9ni.3.6 and existing
cli.rs tests).

1. **Cache reuse.** When a stage's output is already committed and the
   inputs haven't changed, re-driving the pipeline must reuse the
   cached output without re-invoking the backend. Assert the
   `committed_stages` map is consulted and the backend port is not
   called for unchanged stages.

2. **Downstream invalidation cascade.** When `answer()` invalidates a
   question round, every downstream stage (Synthesis through
   MilestoneBundle per `FullModeStage::question_round_invalidated()`)
   must be removed from `committed_stages`. Test the full set, not
   just one stage. Confirm that *upstream* stages (Ideation,
   Research) are preserved.

3. **Question round resume.** A run that was interrupted mid-question-
   round must resume cleanly: the same question is re-asked (not a
   different one), the partial answers stay in place, and the run
   doesn't lose its `question_round` counter. Cover both the
   in-memory store path and the persisted-snapshot path that
   `RequirementsRun::load()` uses.

4. **Milestone bundle creation handoff.** A successful
   `commit_milestone_bundle()` must produce a `MilestoneBundle`
   whose `plan_hash`, `schema_version`, and bead-spec list match
   the inputs. Assert the bundle round-trips through serialization
   (the type that `materialize_bundle_with_source()` reads back).

5. **Legacy compatibility — internal angle.** Where still supported,
   the legacy non-milestone full-mode terminal (project seed) must
   produce a `RequirementsCreateHandoff::ProjectSeed` whose contents
   match the pre-milestone shape. The handoff type round-trips
   through serialization and `load_requirements_handoff()` returns
   the same variant.

## Approach

- Don't duplicate 9ni.3.6 coverage. 9ni.3.6 pinned mock-store
  call-counts on user-facing entry points; this bead pins internal
  state-machine behavior using direct service-level calls.
- Reuse the in-memory port adapters that exist for these contexts
  (mock requirements store, mock plan store). Where a thin extension
  is needed for assertions (e.g., counting stage-output reads for
  cache reuse), add it minimally.
- All tests must be deterministic — no real time, no real I/O
  beyond temp dirs, no external processes.

## Where to look

- `src/contexts/requirements_drafting/{model,service}.rs` — full-mode
  state machine, stage commits, `answer()`, `question_round_invalidated()`,
  `commit_milestone_bundle()`, `commit_project_seed()`.
- `src/contexts/milestone_record/bundle.rs` — `MilestoneBundle` struct
  and `MILESTONE_BUNDLE_VERSION`.
- `src/contexts/requirements_drafting/contracts.rs` — handoff types
  (`RequirementsCreateHandoff::{ProjectSeed, MilestoneBundle}`).
- Existing tests: `tests/unit/requirements_drafting_test.rs`.
  Inventory first; extend rather than duplicate.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files.

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`
  all pass.
- Each of the five scope areas above has at least one new test.
- Tests are deterministic and assert concrete state transitions
  (no test that compiles but doesn't actually exercise the path).
- Existing tests pass without modification.
