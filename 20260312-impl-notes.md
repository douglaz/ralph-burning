# Implementation Notes

## Decisions Made
- Removed the `UnsupportedFlow` error variant entirely from `AppError` since `quick_dev` was the only flow that used it and all four built-in presets are now supported. No other code referenced this variant.
- Removed the unused `FlowPreset` import from `cli/run.rs` since the quick_dev guard was the only consumer.
- The engine is already fully preset-agnostic — the only changes needed were removing the CLI-level guards and updating tests/conformance. No engine modifications were required.
- For the remediation test (`quick_dev_review_request_changes_restarts_from_apply_fixes`): in quick_dev, the execution stage (`apply_fixes`) comes AFTER the remediation trigger stage (`review`) in the stage plan. This means `apply_fixes` is entered only once (after cycle advance), unlike docs_change where the execution stage precedes the trigger and gets re-entered. The test assertions reflect this correct engine behavior.

## Spec Deviations
- SC-QD-START-002 ("apply_fixes stage is entered twice"): The acceptance criterion stated apply_fixes enters twice during remediation, but the engine correctly enters apply_fixes only once because the execution stage comes after the trigger stage in quick_dev's stage plan (`plan_and_implement -> review -> apply_fixes -> final_review`). When review fires request_changes, cycle advances to apply_fixes (index 2), which hasn't been entered yet. The conformance scenario was updated to reflect this.
- No engine changes were needed — the engine already handles quick_dev correctly through its generalized flow semantics. The deferral was purely at the CLI layer.

## Testing
- **Unit tests (319 pass)**: Added 6 new quick_dev engine tests (happy path, review remediation, review rejection, final-review completion round, resume from failed, preflight failure) and 1 new flow semantics test (prompt_review ignorance regression).
- **CLI tests (84 pass)**: Replaced the `run_start_rejects_quick_dev_flow` test with 3 success-path tests: end-to-end completion, status verification, and resume from failed state.
- **Conformance features**: Created `run_start_quick_dev.feature` with 10 scenarios. Updated `run_start_standard.feature` (SC-START-002 now tests success, SC-START-017 covers all four presets). Extended `run_resume_non_standard.feature` with quick_dev resume and paused amendment scenarios.
- **Verification**: `cargo check` passes, `cargo test --test unit` (319/319), `cargo test --test cli` (84/84).
