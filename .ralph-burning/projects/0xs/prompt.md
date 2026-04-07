## Bead ID: ralph-burning-0xs

## Goal

Allow operators to configure the final-review planner target explicitly instead of relying on the generic planner role or flow defaults.

## Problem

The final-review pipeline has a hidden planner-positions step that currently inherits the generic planner backend. That makes runs unexpectedly hit Claude even when the visible final-review reviewer panel and arbiter are configured for Codex. This is hard to reason about and prevents fully explicit backend routing.

## Changes Required

### 1. Add explicit config surface
Support a dedicated `final_review_planner_backend` setting in workspace/project config and CLI diagnostics/resolution paths.

### 2. Use explicit planner selection in final review
Have final-review planner resolution prefer the dedicated setting over the generic planner role so the planner-positions step can be routed independently.

### 3. Preserve inspectability
Expose the resolved final-review planner target in status/diagnostics/snapshots consistently with the existing final-review reviewer and arbiter metadata.

## Files

- `src/shared/domain.rs`
- `src/contexts/workspace_governance/config.rs`
- `src/contexts/agent_execution/policy.rs`
- related diagnostics/tests

## Acceptance Criteria

- A dedicated `final_review_planner_backend` config key is supported
- When set, the final-review planner-positions step uses that backend instead of the generic planner
- When not set, falls back to generic planner (backward compatible)
- The resolved target is visible in diagnostics/status output
- Existing tests pass; new tests cover the explicit config path
