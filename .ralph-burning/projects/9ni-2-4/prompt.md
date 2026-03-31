# Task: Track bead-to-task run linkage inside milestone state

## Bead ID: ralph-burning-9ni.2.4

## Goal

Record which Ralph task/project/run attempted which bead, with enough detail to audit retries and outcomes later.

## Requirements

The `TaskRunEntry` model in `src/contexts/milestone_record/model.rs` must capture:
- milestone_id (currently implicit from storage path — should be explicit in the entry for self-contained queries)
- bead_id (already present)
- current task/project id (already present as `project_id`)
- run_id if available (MISSING — add this field)
- outcome summary (already present as `TaskRunOutcome`, but add optional `outcome_detail` for richer context)
- prompt version or plan hash if relevant (MISSING — add `plan_hash` field)

## Changes Required

1. **Extend `TaskRunEntry`** in `src/contexts/milestone_record/model.rs`:
   - Add `run_id: Option<String>` — the specific run ID within the project
   - Add `plan_hash: Option<String>` — plan version/hash at time of execution
   - Add `outcome_detail: Option<String>` — human-readable outcome summary

2. **Add `update_task_run` to `TaskRunLineagePort`** in `src/contexts/milestone_record/service.rs`:
   - Allow updating an existing entry's outcome (controller needs this after each run)
   - Signature: update by bead_id + project_id, setting new outcome + finished_at

3. **Add `find_runs_for_bead` query** in service layer:
   - Filter task runs by bead_id to answer "what happened to bead X?"
   - Return all attempts in chronological order

4. **Implement FS adapter** for the new port method in `src/adapters/fs.rs`

5. **Update `record_bead_start` and `record_bead_completion`** to accept and pass through the new fields

6. **Add tests** for:
   - TaskRunEntry serialization with new fields
   - find_runs_for_bead query
   - update_task_run flow
   - Multiple retries for same bead are all visible

## Acceptance Criteria

- Milestone state can answer "what happened to bead X?" without reconstructing from unrelated artifacts
- Controller can update linkage after each run outcome
- No backward compatibility with old NDJSON schemas is required — there is no existing milestone data on disk. New fields (run_id, plan_hash, outcome_detail, milestone_id) should be required (non-optional) where they are known at write time. Remove any legacy parsing, backfill-on-read, or repair paths that only exist to handle a hypothetical migration from an older schema.

## Non-Goals

- Controller state machine (covered by 8.1)
- Lifecycle transitions (covered by 2.5)
- Status aggregation queries (covered by 2.3)

## Dependencies (already closed)

- ralph-burning-9ni.2.2: Milestone store and service layer ✅
