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