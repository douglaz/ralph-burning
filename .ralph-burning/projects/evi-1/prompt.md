# Change default flow from standard to minimal

## Problem

Every project create requires `--flow minimal` explicitly. The minimal flow (plan_and_implement + final_review) is the right default for focused bead work.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Changes

1. Change `FlowPreset::Standard` to `FlowPreset::Minimal` as the default in:
   - `src/contexts/workflow_composition/engine.rs` — search for `default_flow: FlowPreset::Standard`
   - `src/contexts/milestone_record/bundle.rs` — search for `default_flow: FlowPreset::Standard`
   - Any other places that hardcode Standard as default

2. Update tests that assume Standard is the default flow

3. The `--flow standard` flag should still work for explicit override

## Acceptance Criteria
- `project create` without `--flow` uses minimal by default
- `--flow standard` still works
- All tests pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
