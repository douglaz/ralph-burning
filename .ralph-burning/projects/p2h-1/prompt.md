# Guard drift_still_satisfies_requirements against PromptReview quorum override

## Bug

`drift_still_satisfies_requirements` in engine.rs applies `effective_min_override` uniformly to all stage types including PromptReview. The PromptReview resume path never sets this override and documents that prompt-review does NOT degrade on BackendExhausted, but the function signature accepts it. A future caller could silently weaken PromptReview quorum.

## Fix

In the PromptReview arm of `drift_still_satisfies_requirements`:
- Either ignore `effective_min_override` (treat it as None)
- Or assert/return an error if a non-None value is passed for PromptReview

Search for `drift_still_satisfies_requirements` in the codebase to find the function and all call sites. Also search for `PromptReview` and `effective_min_override` to understand the context.

Add a regression test that verifies PromptReview quorum cannot be weakened by the override parameter.

## Acceptance Criteria
- PromptReview quorum is never weakened by effective_min_override
- Regression test covers this invariant
- cargo test && cargo clippy && cargo fmt --check pass
