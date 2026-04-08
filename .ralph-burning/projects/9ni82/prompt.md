## Bead ID: ralph-burning-9ni.8.2

## Goal

Ask bv what to do next, then verify it against br's actual ready state before acting.

## Selection Policy

- Record the bv recommendation before claiming anything
- Validate the candidate against br readiness and blocker state
- If bv suggests a blocked or missing bead, do not claim and leave an explicit operator-visible reason

## Context

The controller state model (9ni.8.1) is already implemented with states: idle, selecting, claimed, running, reconciling, blocked, needs_operator, completed. This bead implements the "selecting" state logic.

The controller should:
1. Transition to "selecting" state
2. Call bv --robot-next to get a recommendation
3. Call br ready or br show to validate the recommendation against actual readiness
4. If valid and ready: transition to next state (claimed) with the bead_id
5. If blocked or no ready beads: transition to "blocked" with reason
6. If bv/br tool fails: transition to "needs_operator"

## Acceptance Criteria

- The controller can request and record a next-bead recommendation from bv
- Selection is validated against blockers and actual ready state before any claim happens
- Mismatches or empty-ready situations leave milestone state in a safe blocked or needs-operator path instead of claiming the wrong bead
- Deterministic tests cover single-ready, conflicting-recommendation, and no-ready cases
- Existing tests pass

## Non-Goals

- Claiming the bead (covered by 9ni.8.3)
- Parallel execution or multi-bead selection
