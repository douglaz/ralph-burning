# Implementation Response (Iteration 4)

## Review Changes Addressed

### 1. Completion panel transition invariant (Required Change 1)

**Problem:** `completion_panel` stage writes aggregate record + `stage_completed` before the post-panel transition is durably recorded. If the transition fails, aggregate/stage_completed leak and resume goes to the wrong stage.

**Fix:** Split `commit_completion_aggregate` into two functions:
- `persist_completion_aggregate_records` (engine.rs) — writes only payload+artifact files (reversible)
- `cleanup_completion_aggregate_records` (engine.rs) — best-effort remove payload+artifact pair on failure

**Complete path** (engine.rs ~line 1157): Writes aggregate records first (reversible), then cursor advance snapshot (reversible), then `stage_completed` LAST as the journal commit point. On failure at any step before `stage_completed`, aggregate records are cleaned up via `cleanup_completion_aggregate_records`.

**ContinueWork path** (engine.rs ~line 1335): Writes aggregate records first (reversible), then `completion_round_advanced` as the journal commit point. Key invariant: ContinueWork no longer writes `stage_completed` at all — if `completion_round_advanced` journal append fails, aggregate records are cleaned up and resume correctly restarts from `completion_panel` (not past it).

### 2. Prompt-review commit ordering (Required Change 2)

**Problem:** `stage_completed` becomes durable before prompt files (prompt.md, prompt.original.md, project.toml) are updated. If file update fails, resume treats prompt_review as completed without the refined prompt.

**Fix:** In `dispatch_prompt_review_panel` (engine.rs), moved `replace_prompt_atomically` BEFORE `stage_completed`:
1. Persist primary payload+artifact (reversible)
2. Write prompt.original.md, replace prompt.md, update hash via `replace_prompt_atomically` (reversible)
3. Append `stage_completed` to journal — this is the commit point (irreversible, LAST)

On `stage_completed` failure: revert prompt replacement via new `revert_prompt_replacement` helper (fs.rs) and clean up primary records. The `revert_prompt_replacement` function (fs.rs) performs best-effort rollback: restores original prompt.md content, removes prompt.original.md, and restores the original hash in project.toml.

### 3. Conformance coverage upgrade (Required Change 3)

**Problem:** 10 of 15 Slice 4 scenario executors validated helpers/serialization instead of driving real `run start`/`run resume` workflow paths with journal/file assertions.

**Fix:** Rewrote 10 scenarios to be behavioral CLI-driven tests using workspace config manipulation, env var test seams, and journal/snapshot assertions:

- **`workflow.prompt_review.min_reviewers_enforced`**: Creates workspace with `min_reviewers=3` and only 2 validators, runs `run start`, verifies failure with `InsufficientPanelMembers`
- **`workflow.prompt_review.optional_validator_skip`**: Configures optional openrouter validator, verifies run succeeds with optional skip
- **`workflow.completion.optional_backend_skip`**: Configures optional completer with `RALPH_BURNING_TEST_BACKEND_UNAVAILABLE`, verifies completion succeeds
- **`workflow.completion.required_backend_failure`**: Uses `RALPH_BURNING_TEST_FAIL_INVOKE_STAGE=completion_panel` to simulate completion failure while other stages succeed normally
- **`workflow.completion.threshold_consensus`**: Sets high consensus_threshold, uses `RALPH_BURNING_TEST_STAGE_OVERRIDES` for ContinueWork then Complete
- **`workflow.completion.insufficient_min_completers`**: Configures `min_completers=3` with only 2 backends, verifies failure
- **`backend.resume_drift.implementation_warns_and_reresolves`**: Fails at implementation via failpoint, changes config, resumes — verifies drift detection and re-resolution
- **`backend.resume_drift.qa_warns_and_reresolves`**: Same pattern at acceptance_qa stage
- **`backend.resume_drift.review_warns_and_reresolves`**: Same pattern at review stage
- **`backend.resume_drift.completion_panel_warns_and_reresolves`**: Same pattern at completion_panel stage

All upgraded scenarios use real `execute_standard_run`/`execute_resume_run` paths, write actual workspace.toml configs, and assert on journal events and run snapshots.

## Unit Test Updates

- **`completion_panel_continue_then_complete_success`** (workflow_engine_test.rs:4294): Changed `stage_completed` count assertion from 2 to 1, since ContinueWork no longer writes `stage_completed`. Only the final Complete round produces the event.
- **`resume_after_completion_round_advanced_failpoint_completes`** (workflow_engine_test.rs): Changed failpoint threshold from 18 to 17 (one fewer journal append in ContinueWork path). Changed assertion to verify `stage_completed` is NOT persisted in ContinueWork path. Resume now restarts from `completion_panel`.

## Additional Fixes Found During Implementation

- **`required_backend_failure` conformance scenario**: Initial implementation used `RALPH_BURNING_TEST_BACKEND_UNAVAILABLE=1` which made ALL backends unavailable, causing failure at the first stage (prompt_review) rather than at completion_panel. Fixed by using `RALPH_BURNING_TEST_FAIL_INVOKE_STAGE=completion_panel` which only fails completion_panel invocations while other stages succeed normally.

## Could Not Address
None

## Verification
- `nix build` succeeds (exit code 0)
- 538 tests pass, 1 ignored, 0 failures
