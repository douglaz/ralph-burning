# Fix StageCursor overflow and history record ID collisions

## Objective

Fix two related issues in the `ralph-burning-rewrite/` codebase that both involve `StageCursor` and the append-only history integrity invariant.

## Issue 1: StageCursor arithmetic overflow (GitHub #5)

### Problem
`StageCursor::retry()`, `advance_cycle()`, and `advance_completion_round()` in `src/shared/domain.rs` perform unchecked `u32` arithmetic (`+ 1`). In debug builds, overflow panics. In release builds, it wraps to 0, violating the monotonicity invariant (spec §7) and the constructor invariant that these values must be > 0.

### Required Change
- Change `retry()`, `advance_cycle()`, and `advance_completion_round()` to use `checked_add(1)`
- Return `AppResult<StageCursor>` (or `AppResult<Self>`) instead of plain `StageCursor`/`Self`
- On overflow, return `AppError::RemediationExhausted` or a suitable error variant
- Update all call sites to propagate the error with `?`

## Issue 2: Completion round ID collision in history records (GitHub #2)

### Problem
`history_record_base_id` in `src/contexts/workflow_composition/engine.rs` derives payload/artifact IDs from only `run_id`, `stage_id`, `cycle`, and `attempt`. But completion rounds reuse cycle numbers (`StageCursor::advance_completion_round` keeps `cycle` and resets `attempt` to 1), so any round-2+ execution of the same stage reuses the same filename. `FsPayloadArtifactWriteStore::write_payload_artifact_pair` persists to `history/payloads/{payload_id}.json` and `history/artifacts/{artifact_id}.json`, so later completion rounds overwrite earlier records, breaking append-only history.

Similarly, in `src/contexts/requirements_drafting/service.rs`, payload IDs are hardcoded with `-1` suffixes regardless of `question_round`:
- `format!("{run_id}-qs-1")` at ~line 181
- `format!("{run_id}-draft-1")` at ~line 558
- `format!("{run_id}-review-1")` at ~line 635
- `format!("{run_id}-seed-1")` at ~line 767

### Required Changes
- Include `completion_round` in `history_record_base_id` so the generated ID distinguishes round-2+ executions from round-1
- Include `question_round` in requirements payload IDs: `format!("{run_id}-draft-{}", run.question_round)` (and similarly for `-qs-` and `-review-`). Seed ID can remain `-1` since seeding is terminal.
- Ensure existing tests still pass and add tests for the round-2+ case

## Constraints
- Do not change any public CLI behavior
- Do not change the storage layout paths — only the ID generation within those paths
- All existing tests (`cargo test`) and conformance scenarios (`ralph-burning conformance run`) must continue to pass
- Use `nix develop -c cargo test` and `nix develop -c cargo build` to build and test
