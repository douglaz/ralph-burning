○ ralph-burning-enc · Fix remaining milestone lineage issues from 9ni.2.4 force-completion   [● P1 · OPEN]
Owner: master · Type: task
Created: 2026-03-30 · Updated: 2026-03-30

## Goal

Address the 3 accepted but unimplemented amendments from the force-completed final review of ralph-burning-9ni.2.4 (round 21, restart 20/20).

## Issues

### 1. Duplicate journal detail structs can silently drift

`StartJournalDetails` in `src/adapters/fs.rs` duplicates the field set of `StartJournalDetailsPayload` in `src/contexts/milestone_record/model.rs`. If a new field is added to the model's rendering function without updating the fs adapter's parsing struct, `StartJournalDetails::merge` will silently discard the unrecognized field during repair. The same issue exists for `CompletionJournalDetails`.

**Fix:** Make the model's payload structs public with both `Serialize` and `Deserialize`, and reuse them directly in fs.rs — eliminate the duplicate structs entirely. Do NOT use `#[serde(deny_unknown_fields)]` — journal entries must remain forward-compatible (a newer code version may write fields that an older version doesn't know about). Use `#[serde(default)]` on optional fields so unknown fields are silently ignored during deserialization. For merging two journal detail records, deserialize both to the shared struct and merge field-by-field with explicit conflict rules (not a generic JSON merge helper).

### 2. Run ID must always be known at bead start time — remove runless entry support

~~The old approach tried to backfill run_id later via started_at disambiguation. This created massive complexity (unique_terminal_replay_match, started_at fallbacks, debug_assert-to-runtime-check conversions) and has been the source of most review churn.~~

**New approach:** The milestone controller must generate the run ID BEFORE recording the bead start. The flow should be:
1. Generate run ID (e.g. `RunId::new()`)
2. Call `record_bead_start(run_id=Some(run_id), ...)`
3. Create the engine run with that same ID
4. Call `update_task_run(run_id=Some(run_id), ...)` → exact match

**Fix:** Make `run_id` a required parameter (not `Option`) in `record_bead_start` and `update_task_run` in the service layer ports. Remove all runless matching/backfill code: `unique_terminal_replay_match`, `find_matching_running_task_run` runless fallbacks, `started_at`-based disambiguation for run identity. `TaskRunEntry.run_id` can stay as `Option<String>` in the struct for serde compat, but the service API should require it. Remove tests that validate runless behavior — they test a path that should no longer exist.

### 3. plan_hash must be passed by the caller — remove auto-population from snapshot

~~The old approach tried to auto-populate plan_hash from the milestone snapshot at the adapter level. This created complex backfill/conflict logic (safe_plan_hash_backfill, snapshot_plan_hash_at_creation provenance tracking, different rules for new rows vs existing rows vs terminal replays).~~

**New approach:** The milestone controller already knows the plan hash (it calls `persist_plan(hash)` earlier). It should simply pass it as a parameter to `record_bead_start` and `update_task_run`. No auto-population needed.

**Fix:** Make `plan_hash` a required parameter (not `Option`) in `record_bead_start` in the service layer ports. Remove all snapshot-based auto-population code: `safe_plan_hash_backfill`, `snapshot_plan_hash_at_creation` field, `snapshot_plan_hash()` reads. `TaskRunEntry.plan_hash` can stay as `Option<String>` in the struct for serde compat, but the service API should require it at bead start. For `update_task_run` (completion), plan_hash can remain optional since the entry already has it from the start call.

## Acceptance Criteria

- Journal detail structs cannot drift without a compile-time or test failure
- `run_id` is required in `record_bead_start` — all runless matching/backfill code is removed
- `plan_hash` is required in `record_bead_start` — all snapshot auto-population code is removed
- Existing tests that validate runless or auto-populate behavior should be removed or rewritten to test the new required-parameter paths
- New tests cover each fix

Dependencies:
  -> ralph-burning-9ni.2.4 (blocks) - Track bead-to-task run linkage inside milestone state
