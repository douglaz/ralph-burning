# Skip ApplyFixes stage when review has no findings or amendments

## Bead: ralph-burning-w8z (P1 Feature)

## Goal

Make the ApplyFixes stage conditional — skip it entirely when the preceding Review stage approved with zero findings and zero amendments. Currently ApplyFixes runs unconditionally, wasting ~2+ minutes and a backend call just to re-run tests when there's nothing to fix.

## Problem

In the quick_dev flow (PlanAndImplement → Review → ApplyFixes → FinalReview), when Review approves with no findings, ApplyFixes does nothing useful — it just re-validates that tests still pass. This was observed across 9+ consecutive rounds where Review always approved with zero amendments and ApplyFixes produced "No fixes required" every time.

## Proposed Solution

In the engine's stage progression logic, after Review completes:
1. Check the Review stage artifact/outcome for findings and amendments
2. If the review outcome is "Approved" with empty findings and no follow-up amendments, skip the ApplyFixes stage and advance directly to the next stage (e.g. FinalReview)
3. If the review has any findings, amendments, or non-approved outcome, run ApplyFixes as normal

This should work generically for any flow that includes both Review and ApplyFixes stages.

## Key Files

- `src/contexts/workflow_composition/engine.rs` — stage progression loop, where stage skipping logic would be added
- `src/contexts/workflow_composition/mod.rs` — flow semantics

## Acceptance Criteria

- ApplyFixes is skipped when Review approved with zero findings and zero amendments
- ApplyFixes still runs when Review has findings or amendments
- Works for any flow that contains Review → ApplyFixes in its stage sequence
- Stage skipping is visible in runtime logs (e.g. "skipping apply_fixes: review approved with no findings")
- No impact on flows that don't include ApplyFixes
- Existing tests pass; new tests cover the skip/no-skip logic
