---
artifact: prompt-review
project: amendment-journal-orphan
backend: codex
role: prompt_reviewer
created_at: 2026-03-14T15:42:15Z
---

# Prompt Review

## Issues Found
- The prompt allows two different remedies (`cleanup` or `idempotent reconciliation`) but does not say whether one is sufficient or whether both are preferred. That ambiguity can lead to incomplete fixes or mismatched expectations.
- The required behavior after a partial journal append is not stated as a precise invariant. Without a clear end state, implementation and tests may optimize for different outcomes.
- “Record the amendment IDs written” is underspecified. It does not define where that record lives, how long it must persist, or whether it is only in-memory for rollback logic.
- The failure model is incomplete. It says “journal append fails mid-batch” but does not define the expected durable state after some events have already been appended and others have not.
- The reconciliation requirement is vague about the source of truth. It does not explicitly state whether journal state or disk state wins when they disagree.
- The test requirements describe outcomes but not the mechanism. Without guidance on how to inject a deterministic mid-batch append failure, the task may be hard to implement and flaky to verify.
- The prompt references one source range in `engine.rs`, which may shift. That can cause downstream loops to anchor too tightly to line numbers instead of behavior.
- “Full batch success still works correctly” is too broad. It should define what to assert so tests are specific and sufficient.
- Validation requirements name `cargo test`, `cargo build`, and conformance, but do not clarify whether all three must be run or whether conformance is required only if affected paths exist in the current environment.
- “Do not change any public CLI behavior” is clear, but the prompt does not explicitly permit internal refactors needed to make the failure path testable.

## Refined Prompt
# Fix amendment journal orphan on mid-batch append failure

## Objective

Fix the amendment persistence bug in `ralph-burning-rewrite/` so that a journal append failure during a batch of `amendment_queued` events cannot cause duplicate amendments after resume.

## Scope

Work in the amendment persistence and recovery flow under `src/contexts/workflow_composition/`. The line references in this prompt are directional only; if the code has moved, update the relevant persistence and reconciliation paths wherever they now live.

## Problem Statement

The current flow writes amendment files to disk first and then appends `amendment_queued` journal events one at a time.

If journal appending fails after some events in the batch have already been written but before the full batch completes, the system can be left in this state:

- Amendment files for the full batch exist on disk.
- Journal entries exist only for the earlier amendments in the batch.
- On resume, `reconcile_amendments_from_disk` reads all amendment files and re-adds them to the snapshot.
- Amendments that already have a durable `amendment_queued` journal entry are added again, producing duplicates.

## Required Outcome

After this fix, the system must satisfy all of the following:

- A mid-batch journal append failure must not cause duplicate amendments after resume.
- Recovery must be deterministic: each amendment may appear at most once in the recovered snapshot.
- The journal must remain append-only.
- Public CLI behavior must not change.

## Acceptable Fix Strategies

Implement at least one complete strategy below. If needed for correctness, you may combine them.

### Strategy A: Cleanup unjournaled amendment files on append failure

- After successfully writing the amendment files for a batch, keep track of which amendment IDs were written in that batch.
- While appending `amendment_queued` events one-by-one, if appending fails at amendment `N`, delete the amendment files for:
  - the failed amendment, and
  - every later amendment in the same batch that does not yet have a successfully appended journal event.
- Do not delete files for amendments whose `amendment_queued` event was already durably appended.
- The result must be that disk state after failure is consistent with the durable journal prefix.

### Strategy B: Make reconciliation idempotent against the journal

- Update `reconcile_amendments_from_disk` so it does not re-add an amendment that already has a durable `amendment_queued` journal event.
- During reconciliation, treat the durable journal as the source of truth for whether an amendment has already been queued.
- If an amendment file exists on disk but its `amendment_queued` event is absent from the journal, reconciliation may recover it only once and must not create duplicates.

## Preferred Invariant

Prefer a fix that enforces this invariant:

- After any append failure, the combination of on-disk amendment files plus journal replay must represent each amendment zero or one times, never more than once.

If Strategy A alone fully guarantees this, that is acceptable. If cleanup cannot guarantee correctness in all recovery paths, add Strategy B as well.

## Implementation Notes

- Preserve existing public interfaces unless an internal test seam is needed.
- Internal refactoring is allowed if it improves correctness or testability.
- Do not introduce journal rewrites, journal compaction, or any non-append mutation of journal history.
- If you add tracking of written amendment IDs, keep it scoped to the batch/failure handling logic unless durable state is clearly required.

## Tests Required

Add deterministic tests covering all of the following:

1. Mid-batch journal append failure
- Simulate a batch where at least one `amendment_queued` append succeeds and a later append fails.
- Verify the post-failure state matches the chosen strategy:
  - for Strategy A, unjournaled amendment files are cleaned up correctly;
  - for Strategy B, reconciliation will not duplicate amendments already represented in the journal.

2. Resume after partial failure
- Start from a partial-failure state and run the normal resume/recovery path.
- Verify no duplicate amendments are present in the recovered snapshot or equivalent recovered state.

3. Full batch success
- Verify the normal success path still persists and recovers the full batch correctly.
- Assert no regressions in amendment count and persisted state.

## Test Design Guidance

Use a deterministic failure injection mechanism for journal append operations. Avoid timing-based or flaky tests. If an existing fake, mock, or failpoint facility exists in the codebase, use that instead of inventing a new external harness.

## Validation

Run these commands after the change:

- `nix develop -c cargo build`
- `nix develop -c cargo test`

Also run `ralph-burning conformance run` if the conformance harness is available in this workspace and the changed code affects those scenarios.

## Acceptance Criteria

The task is complete when all of the following are true:

- Mid-batch append failure no longer produces duplicate amendments after resume.
- Added tests fail before the fix and pass after the fix.
- Existing tests continue to pass.
- The journal remains append-only.
- No public CLI behavior changes.
