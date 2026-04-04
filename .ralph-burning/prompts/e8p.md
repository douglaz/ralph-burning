◐ ralph-burning-e8p · Add minimal flow preset (PlanAndImplement + FinalReview only)   [● P1 · IN_PROGRESS]
Owner: master · Type: feature
Created: 2026-03-30 · Updated: 2026-03-30

## Goal

Add a new `minimal` flow preset that only runs `PlanAndImplement` + `FinalReview`, skipping the intermediate `Review` and `ApplyFixes` stages that `quick_dev` currently includes.

## Motivation

The `quick_dev` flow runs 4 stages: `PlanAndImplement → Review → ApplyFixes → FinalReview`. In practice, the `Review` stage has been a rubber stamp (approved with zero amendments across 9+ rounds observed on bead 9ni.2.4), and `ApplyFixes` does nothing when review has no findings. These two stages add ~7 minutes per round and consume backend credits without catching anything the `final_review` doesn't already catch.

A `minimal` flow cuts per-round cost roughly in half while keeping the adversarial multi-reviewer final review that finds real issues.

## Changes

### 1. Add `Minimal` variant to `FlowPreset` enum
**File:** `src/shared/domain.rs:523-528`

- Add `Minimal` to the enum
- Update `ALL` constant: `[Self; 4]` → `[Self; 5]`, add `Self::Minimal`
- Add `as_str()` branch: `Self::Minimal => "minimal"`
- Add `description()` branch: `"Minimal flow with plan+implement and final review only."`
- Add `FromStr` branch: `"minimal" => Ok(Self::Minimal)`

### 2. Define `MINIMAL_STAGES` and register the flow
**File:** `src/contexts/workflow_composition/mod.rs`

Add stage array:
```rust
const MINIMAL_STAGES: [StageId; 2] = [
    StageId::PlanAndImplement,
    StageId::FinalReview,
];
const MINIMAL_LATE_STAGES: [StageId; 1] = [StageId::FinalReview];
```

Add to `FLOW_DEFINITIONS` (`[FlowDefinition; 4]` → `[FlowDefinition; 5]`):
```rust
FlowDefinition {
    preset: FlowPreset::Minimal,
    description: "Minimal flow with plan+implement and final review only.",
    stages: &MINIMAL_STAGES,
    validation_profile: ValidationProfile {
        name: "minimal-default",
        summary: "Final review only, no intermediate review or fix stages.",
        final_review_enabled: true,
    },
},
```

### 3. Add `flow_definition` match arm
**File:** `src/contexts/workflow_composition/mod.rs:137-142`

Add: `FlowPreset::Minimal => &FLOW_DEFINITIONS[4],`

### 4. Add `flow_semantics` for Minimal
**File:** `src/contexts/workflow_composition/mod.rs:150-181`

```rust
FlowPreset::Minimal => FlowSemantics {
    planning_stage: StageId::PlanAndImplement,
    execution_stage: StageId::PlanAndImplement,
    remediation_trigger_stages: &[],  // no review stage to trigger remediation
    late_stages: &MINIMAL_LATE_STAGES,
    prompt_review_stage: None,
},
```

Key: `execution_stage` points to `PlanAndImplement` since there's no `ApplyFixes`. When `final_review` restarts, the engine jumps back to `execution_stage` — re-running `PlanAndImplement` with amendments is correct.

### 5. Update tests
**File:** `tests/unit/flow_preset_test.rs`
- Update registry test for 5 presets
- Add `minimal_flow_stage_order_matches_spec` test

**File:** `tests/unit/flow_semantics_test.rs`
- Add test for minimal flow semantics

## Files to modify

1. `src/shared/domain.rs` — FlowPreset enum + methods
2. `src/contexts/workflow_composition/mod.rs` — stages, flow definition, semantics
3. `tests/unit/flow_preset_test.rs` — flow registry and stage order tests
4. `tests/unit/flow_semantics_test.rs` — semantics test

## Acceptance Criteria

- `flow = "minimal"` in project.toml is accepted and runs only PlanAndImplement + FinalReview
- Final review restarts correctly jump back to PlanAndImplement
- All existing flow tests continue to pass
- New tests cover minimal flow stage order and semantics
