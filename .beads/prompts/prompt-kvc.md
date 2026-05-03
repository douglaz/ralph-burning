## Bead ID: ralph-burning-kvc

## Goal

Make the bootstrap requirements review step opt-in instead of mandatory, so that `project create` goes directly from drafting requirements to project creation by default.

## Context

Project bootstrap currently runs a two-step requirements pipeline (draft + review) before creating the project and starting the run. The review step adds latency (5-10 min for Codex) and can timeout, blocking the entire bootstrap flow. Most beads don't need the extra review pass — it's only valuable for complex, ambiguous, or high-stakes work.

## Changes Required

1. **Add `--enable-review` flag** to the bootstrap/project-create flow. When absent, skip the requirements review step entirely.
2. **Default behavior**: draft requirements → create project → start run (no review).
3. **With `--enable-review`**: draft requirements → review requirements → create project → start run (current behavior).
4. **Preserve the review implementation**: don't delete the review code, just gate it behind the flag.

## Acceptance Criteria

- Default bootstrap skips the requirements review step
- `--enable-review` flag triggers the full draft+review pipeline
- No regressions in existing bootstrap tests
- Existing tests pass (`cargo test && cargo clippy && cargo fmt --check`)
