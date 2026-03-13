---
artifact: prompt-review
project: cursor-history-integrity
backend: codex
role: prompt_reviewer
created_at: 2026-03-13T21:29:27Z
---

I’m reviewing the prompt against the repo so I can tighten ambiguity around the affected code paths, error handling, and verification requirements before rewriting it.
I’ve confirmed the prompt points to real call sites and there are already tests touching `question_round` and `StageCursor`. I’m checking the exact signatures and existing error variants so the rewrite can name concrete acceptance criteria instead of leaving implementation choices underspecified.
The main ambiguity left is scope: the prompt says “update all call sites,” but the failure mode and expected tests depend on exactly where `StageCursor` transitions are used. I’m mapping those sites now so the rewrite can make the implementation boundary explicit.
I’ve verified the cursor methods are only called in a few engine paths plus one unit test. That means the rewrite can require explicit propagation and targeted test updates, rather than a vague “update all call sites.”
There are already fixture expectations hardcoding `-1` IDs, so the rewrite should explicitly tell the implementer to update affected unit/CLI/conformance assertions, not just “ensure tests still pass.”
I found existing workflow engine tests that assert filenames using `-c1-a1`. The prompt should either define the new ID format including completion round or explicitly allow any equivalent format that preserves uniqueness, otherwise downstream loops may overfit or guess wrong.
The current prompt is feasible, but it leaves two implementation decisions open that will cause drift across loops: which error variant to use for arithmetic overflow, and the exact filename/ID shape once `completion_round` is included. I’m locking those down in the rewritten version.
I’ve confirmed there isn’t an existing test that directly asserts history IDs stay unique across completion rounds. The rewritten prompt will require one concrete regression test in the workflow engine and one overflow unit test, so downstream work has a clear stop condition.
One more gap surfaced: the prompt only mentions `StageCursor` methods, but `engine.rs` also does `cursor.cycle + 1` before calling `advance_cycle()`. If that stays unchanged, the overflow bug still exists even after fixing the methods.
There’s a second unchecked increment in resume logic as well: `current_completion_round + 1` in `engine.rs`. The prompt should include all `StageCursor`-related round/cycle increments, not just the three methods, or the fix stays partial.
The last thing I’m tightening is verification. The original prompt names test commands, but it doesn’t say which directory to run them from or what to do if the `ralph-burning` binary isn’t on `PATH` inside `nix develop`.
# Prompt Review

## Issues Found
- The overflow scope is incomplete. The prompt names `StageCursor` methods, but `engine.rs` also performs unchecked cursor-related increments such as `cursor.cycle + 1` and `current_completion_round + 1`; fixing only the methods leaves real overflow paths behind.
- The error contract is ambiguous. `AppError::RemediationExhausted` describes policy exhaustion, not generic counter overflow, so the prompt invites inconsistent or misleading error handling.
- The new history ID shape is underspecified. “Include `completion_round`” does not say where it appears or how it composes with `-rb{rollback_count}`, which makes test updates and compatibility expectations guesswork.
- The requirements ID change is off-by-one ambiguous. Question-set IDs are created before `run.question_round` is updated from `0` to `1`, so “use `question_round`” can be implemented incorrectly unless the prompt defines which round value to use at each write point.
- The testing requirement is too loose. “Add tests for the round-2+ case” does not require overflow regression coverage or explicit assertions that both payload and artifact files coexist without overwrite.
- Compatibility boundaries are unclear. The prompt says not to change storage layout paths, but it does not say whether existing persisted runs should be migrated or left untouched, which can trigger unnecessary migration work.

## Refined Prompt
# Fix StageCursor overflow and round-scoped history ID collisions

## Goal

In `/root/new-ralph-burning/ralph-burning-rewrite`, fix two integrity bugs without changing CLI surface area or storage directory layout:

1. `StageCursor`-related counters must never panic or wrap on `u32` overflow.
2. History payload/artifact IDs must remain unique across completion rounds and requirements question rounds so append-only history is preserved.

## Working Directory

Run all commands from `/root/new-ralph-burning/ralph-burning-rewrite`.

## Required Changes

### 1. Harden StageCursor and related counter increments

Affected files:
- `src/shared/domain.rs`
- `src/shared/error.rs`
- `src/contexts/workflow_composition/engine.rs`
- directly affected tests

Implementation requirements:
- Change these methods in `src/shared/domain.rs` to return `AppResult<Self>`:
  - `StageCursor::retry()`
  - `StageCursor::advance_cycle()`
  - `StageCursor::advance_completion_round()`
- Replace each unchecked `+ 1` with `checked_add(1)`.
- Add a dedicated overflow error variant in `AppError`, for example:
  - `StageCursorOverflow { field: &'static str, value: u32 }`
- Use that overflow error for counter overflow. Do not reuse `AppError::RemediationExhausted` for generic overflow.
- Implement the three transition methods through `StageCursor::new(...)` after computing checked values so existing `> 0` validation remains centralized.
- Update all call sites to propagate the new `AppResult` with `?`.

Additional overflow sites that must also be fixed in this change:
- `src/contexts/workflow_composition/engine.rs`: the remediation path currently computes `cursor.cycle + 1`
- `src/contexts/workflow_composition/engine.rs`: the resume path currently falls back to `current_completion_round + 1`
- Any other unchecked increment of `cycle`, `attempt`, or `completion_round` discovered while implementing this fix

Required behavior:
- In both debug and release builds, overflow must return an `AppError`; it must never panic and never wrap to `0`.
- Keep `AppError::RemediationExhausted` only for retry-policy exhaustion.

### 2. Make workflow history IDs unique across completion rounds

Affected files:
- `src/contexts/workflow_composition/engine.rs`
- affected workflow engine tests

Implementation requirements:
- Update `history_record_base_id(...)` to include `cursor.completion_round`.
- Use this exact base format before any rollback suffix:

```text
{run_id}-{stage_id}-c{cycle}-a{attempt}-cr{completion_round}
```

- Preserve existing rollback branching behavior by appending `-rb{rollback_count}` only when `rollback_count > 0`.
- Keep artifact IDs derived exactly as they are today from the payload ID:
  - `{payload_id}-artifact`
- Do not change directory names, file extensions, or the payload/artifact write locations under `history/payloads/` and `history/artifacts/`.

Required behavior:
- A round-2+ execution of the same stage/cycle/attempt must create a new payload/artifact pair instead of overwriting round 1.
- Rollback branching must still produce distinct IDs on top of the new round-aware base ID.

### 3. Make requirements history IDs unique across question rounds

Affected files:
- `src/contexts/requirements_drafting/service.rs`
- affected requirements, CLI, and conformance tests

Implementation requirements:
- Replace hardcoded `-1` suffixes for non-seed requirements history IDs with the correct round number.
- Use these exact formats:
  - question set payload: `format!("{run_id}-qs-{round}")`
  - question set artifact: `format!("{run_id}-qs-art-{round}")`
  - draft payload: `format!("{run_id}-draft-{round}")`
  - draft artifact: `format!("{run_id}-draft-art-{round}")`
  - review payload: `format!("{run_id}-review-{round}")`
  - review artifact: `format!("{run_id}-review-art-{round}")`
- Keep seed IDs unchanged:
  - payload: `format!("{run_id}-seed-1")`
  - artifact: `format!("{run_id}-seed-art-1")`
- For question-set generation, use the round being generated, not the pre-update persisted `run.question_round` value if it is still `0`.
- For draft/review generation after answers are submitted, use the already-incremented `run.question_round`.

Required behavior:
- Do not rename or migrate existing files on disk. This change affects only newly generated IDs.
- Do not change `run.json` field names, journal schema, or CLI commands/output structure.

## Non-Goals

- No public CLI surface changes: no new commands, flags, directories, or file extensions
- No storage layout changes beyond the ID strings embedded in existing filenames
- No migration or backfill for previously persisted runs

## Acceptance Criteria

- `StageCursor` retry/cycle/completion-round transitions return `AppResult<Self>` and cannot overflow silently.
- All cursor-related increments in scope are checked and return errors instead of panicking or wrapping.
- Workflow history IDs are unique across completion rounds and across rollbacks.
- Requirements question-set, draft, and review IDs are round-aware.
- Seed IDs remain `-1`.
- Existing behavior outside overflow handling and ID generation remains unchanged.

## Test Requirements

Add or update tests to cover all of the following:

1. `tests/unit/domain_test.rs`
- Update existing `StageCursor` transition tests for `AppResult`
- Add overflow tests for `retry`, `advance_cycle`, and `advance_completion_round` starting from `u32::MAX`

2. `tests/unit/workflow_engine_test.rs`
- Add a regression test that drives a completion-round restart and asserts both round-1 and round-2 payload/artifact files exist simultaneously
- Assert the IDs include `-cr1` and `-cr2`
- Assert that neither the payload file nor the artifact file from round 1 is overwritten by round 2
- Update existing assertions that currently match `-implementation-c1-a1` so they expect the new `-implementation-c1-a1-cr1` shape

3. Requirements drafting tests
- Update expectations that currently hardcode `-qs-1`, `-draft-1`, or `-review-1`
- Add at least one regression test where `question_round == 2` and generated draft/review IDs use `-2`

4. CLI and conformance tests
- Update any fixtures or assertions that depend on the old hardcoded ID format

## Verification

Run these commands from `/root/new-ralph-burning/ralph-burning-rewrite`:

1. `nix develop -c cargo build`
2. `nix develop -c cargo test`
3. `nix develop -c ralph-burning conformance run`

The change is complete only if all three commands succeed. If a test fails because it asserted the old ID shape, update the test to the new explicit format rather than weakening the assertion.
