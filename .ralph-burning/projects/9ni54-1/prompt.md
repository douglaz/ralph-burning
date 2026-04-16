# Expose milestone and bead lineage in run history and task detail queries

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Goal

Add milestone/bead lineage to queries so CLI and controller can trace which milestone and bead produced each run.

## Changes

1. Run history queries: add milestone_id and bead_id to query results
2. Task detail queries: include milestone name, bead title, acceptance criteria
3. Bead execution history: given bead_id, return all task runs (including retries, outcome, duration)
4. Milestone task listing: given milestone_id, return all linked tasks

## Implementation hints

- Check existing query code in `src/contexts/project_run_record/queries.rs` and `src/contexts/milestone_record/`
- Run history is likely in the journal or run snapshot — add lineage fields
- Make lineage fields optional (null for non-milestone tasks) for backward compat
- Ensure results are serializable for `--json` output
- Add unit tests for each new query

## Acceptance Criteria
- All 4 query types implemented
- Non-milestone tasks return null lineage (backward compatible)
- Serializable for --json
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
