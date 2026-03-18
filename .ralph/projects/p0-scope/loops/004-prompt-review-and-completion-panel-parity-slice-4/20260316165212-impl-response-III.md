---
artifact: impl-response
loop: 4
iteration: 3
project: p0-scope
backend: claude(opus)
role: implementer
created_at: 2026-03-16T16:52:12Z
---

# Implementation Response (Iteration 3)

## Changes Made

### Required Change 1: Completion failure invariant
Reordered the Complete and ContinueWork paths in `dispatch_completion_panel` so that `commit_completion_aggregate` (which writes the aggregate payload/artifact pair and appends `stage_completed`) is called **before** any transition writes (cursor advance, snapshot persist, `completion_round_advanced`).

- **Complete path** (engine.rs ~L1154-1284): aggregate commit now precedes cursor advance to acceptance_qa and snapshot write. If aggregate commit fails, no transition state leaks.
- **ContinueWork path** (engine.rs ~L1285-1452): aggregate commit now precedes `completion_round_advanced` event and planning cursor snapshot. If aggregate commit fails, the run stays at completion_panel.
- **Cleanup on stage_completed failure** (engine.rs `commit_completion_aggregate` ~L3997): if the `stage_completed` journal append fails after the payload/artifact pair was written, the pair is cleaned up via `remove_payload_artifact_pair` before returning `StageCommitFailed`.

### Required Change 2: Resume drift snapshot persistence
Two-part fix:

1. `emit_resume_drift_warning` (engine.rs ~L4224) now sets `snapshot.last_stage_resolution_snapshot = Some(new.clone())` in addition to updating `active_run.stage_resolution_snapshot`. This ensures the top-level snapshot field is persisted even when `active_run` is `None` during resume.
2. When creating the new `ActiveRun` for a resumed run (engine.rs ~L810), the `stage_resolution_snapshot` field is seeded from `snapshot.last_stage_resolution_snapshot.clone()` instead of `None`, so the resumed run inherits the freshly resolved snapshot.

### Required Change 3: Conformance coverage upgrade
Upgraded 4 key Slice 4 scenario executors from pure-helper assertions to behavioral CLI-driven tests:

- **`workflow.prompt_review.panel_accept`**: runs `run start` on a standard-flow project, verifies journal contains `stage_entered`/`stage_completed` for `prompt_review`, checks `prompt.original.md` exists after accept, and validates payload/artifact count includes prompt-review supporting records.
- **`workflow.prompt_review.panel_reject`**: runs `run start` with `RALPH_BURNING_TEST_STAGE_OVERRIDES` setting `readiness.ready=false` (translated to `accepted=false` by the stub backend), asserts run failure, verifies `failed` status in snapshot, and confirms supporting records (refiner + validators) were still persisted.
- **`workflow.completion.panel_two_completer_consensus_complete`**: runs `run start` on a standard-flow project (stub completers default to `vote_complete=true`), verifies journal has `stage_entered`/`stage_completed` for `completion_panel`, and checks payload/artifact counts.
- **`workflow.completion.panel_continue_verdict`**: runs `run start` with stage overrides providing a two-element array for `completion_panel` (first round: `vote_complete=false`, second round: `vote_complete=true`), verifies journal contains `completion_round_advanced` event, and asserts at least 2 `completion_panel` `stage_entered` events (one per round).

The remaining Slice 4 scenarios (min_reviewers_enforced, optional_validator_skip, threshold_consensus, insufficient_min_completers, optional_backend_skip, required_backend_failure) retain their pure-helper assertions because they test policy enforcement edge cases and consensus math that cannot be easily driven through CLI configuration.

Existing pure assertions in all upgraded scenarios are preserved as baseline contract verification alongside the new behavioral checks.

### Recommended 1: Prompt-review panel timeout roles
Replaced the single `policy_role_for_stage(PromptReview)→Planner` timeout with role-specific lookups:
- Refiner uses `policy.timeout_for_role(family, BackendPolicyRole::PromptReviewer)`.
- Validators use `policy.timeout_for_role(family, BackendPolicyRole::PromptValidator)`.
- `execute_prompt_review` now accepts a separate `refiner_timeout: Duration` parameter.

### Recommended 2: Prompt-review file mutation ordering
Refactored `execute_prompt_review` to return `PromptReviewResult { original_prompt, refined_prompt, ... }` instead of mutating files. The caller (`dispatch_prompt_review_panel`) now calls `FileSystem::replace_prompt_atomically` **after** the primary record and `stage_completed` are durably committed. This prevents prompt file mutations when the subsequent journal append fails.

## Files Changed
- `src/contexts/workflow_composition/engine.rs` — completion commit reordering, resume drift fix, prompt-review timeout roles, prompt replacement after commit
- `src/contexts/workflow_composition/prompt_review.rs` — removed file mutation, added refiner_timeout parameter, returned original/refined prompts
- `src/contexts/conformance_spec/scenarios.rs` — 4 upgraded scenario executors with CLI-driven behavioral tests
- `tests/unit/workflow_engine_test.rs` — updated failpoint thresholds and assertions for new completion commit ordering

## Could Not Address
None. All 3 required changes and 2 recommended improvements have been implemented.

## Verification
- 538 unit tests pass
- 110 CLI/conformance tests pass (including all 15 Slice 4 scenarios)
- `nix build` succeeds with zero compilation errors
- Only pre-existing warning: `field 'rollback_count' is never read` in engine.rs
