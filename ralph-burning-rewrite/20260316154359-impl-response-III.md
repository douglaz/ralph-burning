# Implementation Response (Iteration 2)

## Review Changes Addressed

### 1. Fix completion panel commit ordering (Required Change 1)

**Problem:** Aggregate record and `stage_completed` event were persisted inside `dispatch_completion_panel`, before the post-panel transition. If the transition failed, stale aggregate/stage_completed events leaked into the journal.

**Fix:** Introduced `CompletionCommitData` struct (engine.rs:3540) to capture the aggregate payload, artifact, pre-computed payload/artifact IDs, rollback count, and original completion_round. Removed aggregate persistence and `stage_completed` emission from `dispatch_completion_panel`. Added `commit_completion_aggregate` helper (engine.rs:3917) that writes the aggregate record and emits `stage_completed` using the pre-computed IDs from `CompletionCommitData` (not cursor-derived IDs, which would be wrong in the ContinueWork path where the cursor has an advanced completion_round).

Both the Complete and ContinueWork caller paths (engine.rs:1162/1227/1394) now call `commit_completion_aggregate` only AFTER the transition snapshot write succeeds. If `commit_completion_aggregate` itself fails, both paths call `fail_run_result` to write a Failed snapshot (preventing the run from being left in Running state with no aggregate).

### 2. Fix resume drift ordering (Required Change 2)

**Problem:** Drift detection ran after `preflight_check`, but drift should be caught first so stale resolution snapshots don't influence preflight.

**Fix:** Moved the drift detection block (engine.rs:760-806) to execute BEFORE `preflight_check` in `execute_standard_run`. The stage resolution snapshot comparison now happens as the first validation step after reading the current snapshot.

### 3. Fix per-member timeout resolution (Required Change 3)

**Problem:** A single `panel_timeout` was resolved once and applied uniformly to all panel members, even when members use different backend families with different timeout policies.

**Fix:** Replaced the single `policy_timeout: Duration` parameter with a `timeout_for_backend: &dyn Fn(BackendFamily) -> Duration` closure in:
- `execute_prompt_review` (prompt_review.rs:71) — refiner and each validator now resolve their own timeout via `timeout_for_backend(target.backend.family)`
- `execute_completion_panel` (completion.rs:90) — each completer resolves its own timeout
- `dispatch_prompt_review_panel` (engine.rs) and `dispatch_completion_panel` (engine.rs) build the closure as `|family| policy.timeout_for_role(family, policy_role)`

### 4. Upgrade conformance scenarios to behavioral tests (Required Change 4)

**Problem:** Conformance scenarios were thin wrappers around helper functions, not exercising real I/O paths, serialization round-trips, or record metadata.

**Fix:** Rewrote `register_workflow_panels` (scenarios.rs) with 15 enhanced scenarios:
- **prompt_review.panel_accept/reject**: Verify `PromptReviewDecision` serialization round-trips
- **prompt_review.min_reviewers_enforced**: Verify `InsufficientPanelMembers` error on empty validator list
- **prompt_review.optional_validator_skip**: Verify skip behavior for optional validators
- **prompt_review.prompt_replaced_and_original_preserved**: Real file I/O via `create_project_fixture` + `replace_prompt_atomically`, verifies `prompt.md` content, `prompt.original.md` backup, and hash correctness
- **completion.panel_two_completer_consensus_complete**: Full aggregate payload verification with `record_kind` and `producer` metadata checks
- **completion.panel_below_threshold_continue**: Below-consensus-threshold produces ContinueWork
- **completion.aggregate_serialization_roundtrip**: JSON round-trip for `CompletionAggregatePayload`
- **completion.min_completers_threshold**: Minimum completer enforcement
- **completion.cursor_advancement_produces_correct_round**: `advance_completion_round` correctness
- **completion.requirement_satisfaction_with_real_config**: Config-driven consensus validation
- **backend.resume_drift.implementation_warns_and_reresolves**: Drift detection and requirement satisfaction
- **backend.resume_drift.empty_panel_triggers_drift_failure**: Drift failure on empty panel snapshots

### 5. Un-ignore and update completion panel failure path tests

Replaced 5 `#[ignore]` tests with active panel-dispatch-aware versions:

1. **`resume_after_completion_round_advanced_append_failure_preserves_round`**: Uses `FailingJournalStore::new(18)` to fail CRA event; verifies no CRA in journal, snapshot is Failed, and resume completes.
2. **`completion_stage_completed_append_failure_leaves_supporting_records`**: Uses `ScopedJournalAppendFailpoint(18)` to fail `stage_completed` at the 19th journal append; verifies supporting records are durable, `stage_completed` is absent, snapshot is Failed.
3. **`resume_after_completion_panel_failure_no_duplicate_supporting_records`**: Uses `ScopedJournalAppendFailpoint(17)` to fail CRA; verifies resume produces no duplicate records.
4. **`resume_after_completion_round_advanced_failpoint_completes`**: Uses `ScopedJournalAppendFailpoint(17)` to fail CRA; verifies no CRA in journal and resume completes.
5. **`completion_panel_continue_then_complete_success`**: Full ContinueWork->Complete flow with aggregate assertions.

**Note on failpoint counting:** `ScopedJournalAppendFailpoint::for_project(&pid, N)` uses `current >= N` with a 0-indexed counter, so threshold=N allows N appends and fails the (N+1)th. This is different from `FailingJournalStore::new(N)` which fails the Nth call (1-indexed).

## Additional Fixes Found During Implementation

- **Payload ID mismatch in ContinueWork path:** In the ContinueWork path, `commit_completion_aggregate` was called with `&next_cursor` (completion_round+1), but `persist_aggregate_record` generated IDs from the cursor. This caused files to be written with round N+1 in the name while the journal referenced round N. Fixed by inlining the record construction in `commit_completion_aggregate` using pre-computed IDs from `CompletionCommitData`, and storing the original `completion_round` in the struct.

- **Missing Failed snapshot on aggregate commit failure:** Both Complete and ContinueWork paths used `?` on `commit_completion_aggregate`, which propagated the error without writing a Failed snapshot. Changed to `if let Err` with `fail_run_result` calls to ensure the snapshot is correctly marked as Failed.

## Could Not Address
None

## Verification
- `nix build` succeeds
- 538 tests pass (534 unit + conformance), 1 ignored, 0 failures
- 240 conformance scenarios: 219 passed, 0 failed, 21 not-run
