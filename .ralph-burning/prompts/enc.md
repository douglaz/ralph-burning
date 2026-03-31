○ ralph-burning-enc · Fix remaining milestone lineage issues from 9ni.2.4 force-completion   [● P1 · OPEN]
Owner: master · Type: task
Created: 2026-03-30 · Updated: 2026-03-30

## Goal

Address the 3 accepted but unimplemented amendments from the force-completed final review of ralph-burning-9ni.2.4 (round 21, restart 20/20).

## Issues

### 1. Duplicate journal detail structs can silently drift

`StartJournalDetails` in `src/adapters/fs.rs` (line 2280) duplicates the field set of `StartJournalDetailsPayload` in `src/contexts/milestone_record/model.rs` (line 354). If a new field is added to the model's rendering function without updating the fs adapter's parsing struct, `StartJournalDetails::merge` will silently discard the unrecognized field during repair. The same issue exists for `CompletionJournalDetails`.

**Fix:** Either make the model's payload structs public with `Deserialize` so fs.rs can reuse them, or add a compile-time/test assertion that the two structs stay in sync. Address both Start and Completion pairs together.

### 2. Runless-to-named completion path incorrectly rejected

`src/adapters/fs.rs:2984-3024` rejects `update_task_run` whenever the caller supplies a `run_id` but the only open match is the same bead/project attempt stored without a `run_id`. This means a start recorded before the controller knew the run ID cannot be finalized later with the newly-known `run_id` or `plan_hash`, even though `started_at` is available and the row could be backfilled.

**Fix:** When `run_id` is supplied but only a runless match exists, use `started_at` to disambiguate and backfill the `run_id` on the matched row before finalizing.

### 3. plan_hash not auto-populated from milestone snapshot

`persist_plan` stores the canonical plan hash in the milestone snapshot (`src/contexts/milestone_record/service.rs:365-371`), but `record_bead_start` and `update_task_run` ignore that state and just forward an optional caller-supplied `plan_hash`. The FS adapter then writes `TaskRunEntry { plan_hash: None }` whenever the caller omits it.

**Fix:** When `plan_hash` is None at write time, read it from the milestone snapshot (if a plan has been persisted). The system already knows which plan version is executing — lineage should not lose that linkage.

## Acceptance Criteria

- Journal detail structs cannot drift without a compile-time or test failure
- A start recorded without run_id can be completed with a subsequently known run_id
- Lineage entries auto-populate plan_hash from the milestone snapshot when callers omit it
- Existing tests continue to pass; new tests cover each fix

Dependencies:
  -> ralph-burning-9ni.2.4 (blocks) - Track bead-to-task run linkage inside milestone state
