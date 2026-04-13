# Teach plan_and_implement to honor bead scope, non-goals, and milestone context

## Goal

Modify plan_and_implement so it respects the bead's declared scope, non-goals, and milestone context when generating implementation plans.

## Context

The project prompt already contains all bead scope information via the task prompt contract sections:
- **Must-Do Scope**: defines what work belongs to this bead
- **Explicit Non-Goals**: defines what work is out of scope
- **Already Planned Elsewhere**: lists beads that own related work

The plan_and_implement stage uses `build_stage_prompt()` which already renders the full project prompt. However, the stage's role instruction and template don't explicitly instruct the LLM to respect scope boundaries.

## Changes Required

1. Add a `render_scope_guidance()` function in `review_classification.rs` (or a new module) that generates scope-aware planning instructions from the project prompt
2. Inject scope guidance into the `build_stage_prompt()` for `PlanAndImplement` and `Planning` stages via the existing `classification_guidance` template placeholder
3. The guidance should instruct the LLM to:
   - Only include work that falls within the Must-Do Scope
   - Note out-of-scope work as deferred with rationale
   - Not duplicate work listed in Already Planned Elsewhere
   - Use milestone context as read-only background
4. Backward compatible: when no bead scope sections exist in the prompt (non-milestone mode), the guidance is empty

## Files to modify

- `src/contexts/workflow_composition/review_classification.rs` — add `render_scope_guidance()`
- `src/contexts/workflow_composition/engine.rs` — inject scope guidance for PlanAndImplement/Planning stages
- Tests for the new function

## Acceptance Criteria

- plan_and_implement receives scope-aware guidance when bead scope sections exist
- Guidance instructs LLM to respect Must-Do Scope, Non-Goals, and Already Planned Elsewhere
- Out-of-scope work should be noted as deferred, not added to the plan
- Empty guidance in non-milestone mode (backward compat)
- Existing tests pass
