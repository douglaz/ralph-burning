# Add iterative_minimal workflow: loop implementer until stable, then review

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add a new flow preset. Do NOT modify existing flows (minimal, standard, docs_change, ci_improvement).

## Background — what already exists

### Flow presets (`src/shared/domain.rs`):
```rust
pub enum FlowPreset {
    Minimal,       // plan_and_implement + final_review
    Standard,      // full delivery flow
    DocsChange,    // alias of minimal per current code
    CiImprovement, // CI hardening
    QuickDev,      // removed? verify
}
```

### Flow stage assembly (`src/contexts/workflow_composition/engine.rs`):
- Each preset maps to a list of stages
- `plan_and_implement` calls implementer once per round
- `final_review` runs reviewer panel + voting + arbiter

### Stage dispatch (`src/contexts/workflow_composition/`):
- Stages are resolved to backend invocations
- Journal events emitted per stage completion
- Checkpoint/rollback points created between stages

## What to implement

### 1. Add `FlowPreset::IterativeMinimal` variant

In `src/shared/domain.rs`:
- Add new variant and wire up the Display / FromStr / all_variants() implementations
- Parseable as `iterative_minimal` from CLI

### 2. Define the flow stages

In `src/contexts/workflow_composition/`:
- New flow has the same stages as minimal: `plan_and_implement` + `final_review`
- BUT `plan_and_implement` runs in a loop (see next section)

### 3. Implement the iterative implementer loop

The core change is in how `plan_and_implement` executes for `IterativeMinimal`:

```rust
// Pseudocode
let max_rounds = config.iterative_minimal.max_consecutive_implementer_rounds; // default 10
let stable_required = config.iterative_minimal.stable_rounds_required;        // default 2
let mut stable_count = 0;
let mut iteration = 0;

loop {
    iteration += 1;
    let diff_before = git_diff_head()?;
    let result = call_implementer(prompt, amendments, iteration);
    let diff_after = git_diff_head()?;
    
    journal_event(ImplementerIterationStarted { iteration });
    let diff_changed = diff_before != diff_after;
    journal_event(ImplementerIterationCompleted { 
        iteration, 
        diff_changed, 
        accumulated_diff_size 
    });
    
    if !diff_changed {
        stable_count += 1;
        if stable_count >= stable_required {
            journal_event(ImplementerLoopExited { reason: "stable" });
            break;
        }
    } else {
        stable_count = 0;
    }
    
    if iteration >= max_rounds {
        journal_event(ImplementerLoopExited { reason: "max_rounds" });
        break;
    }
}
```

### 4. Config settings

In `src/contexts/workspace_governance/config.rs`:
- Add `workflow.iterative_minimal.max_consecutive_implementer_rounds` (default 10)
- Add `workflow.iterative_minimal.stable_rounds_required` (default 2)
- Follow existing config pattern for nested settings

### 5. New journal event types

In the journal event enum:
- `ImplementerIterationStarted { iteration, stage_id, cycle, completion_round }`
- `ImplementerIterationCompleted { iteration, diff_changed, outcome }`
- `ImplementerLoopExited { reason: "stable" | "max_rounds" | "error", total_iterations }`

### 6. Integration with final_review

After the implementer loop exits, the stage completes normally and final_review runs as usual. If final_review produces amendments and the workflow restarts, the implementer loop runs again with amendments + original prompt.

### 7. UX updates

- `run status` should show iteration count when in implementer loop (e.g., "running: Plan and Implement (iteration 3/10)")
- `run tail` should print iteration events in a readable way

### 8. Tests

Required tests:
- Unit test: stop-condition logic (stable count reached, max rounds reached)
- Unit test: diff comparison detects empty vs changed
- Integration test: full iterative_minimal run with stubbed implementer
- Test that existing flows (minimal, standard) are unaffected

## Files likely to modify

- `src/shared/domain.rs` — FlowPreset variant
- `src/contexts/workflow_composition/engine.rs` — loop logic
- `src/contexts/workflow_composition/*` — flow stage definitions
- `src/contexts/workspace_governance/config.rs` — config settings
- `src/contexts/project_run_record/journal.rs` — new event types
- `src/cli/run.rs` — status display for iteration count
- Tests in relevant test files

## Acceptance Criteria
- `--flow iterative_minimal` accepted by `project create`
- Implementer loops with correct stop conditions (stable or max)
- Empty-diff detection works via git-based comparison
- Max rounds respected (default 10, configurable via config)
- Final review panel integrates cleanly, same quorum/arbiter as minimal
- Amendments re-enter the implementer loop correctly
- Unit tests for stop-condition logic
- Integration test for full cycle
- Journal events emitted per iteration
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
