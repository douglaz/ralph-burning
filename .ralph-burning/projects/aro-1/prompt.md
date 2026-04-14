# Remove planner role from final review panel

## Goal

Simplify the final review panel by removing the planner role entirely.

Current flow:
1. Reviewers propose amendments (parallel)
2. **Planner votes on amendments (sequential) ← REMOVE THIS**
3. Reviewers vote with planner context (parallel)
4. Arbiter resolves ties

New flow:
1. Reviewers propose amendments (parallel)
2. Reviewers vote on amendments directly (parallel, no planner context)
3. Arbiter resolves ties only when needed

## Implementation

### 1. Remove planner invocation from final_review.rs

Search for the planner invocation in `src/contexts/workflow_composition/final_review.rs`. The planner is invoked between the proposal phase and the reviewer vote phase. Remove:
- The planner backend resolution
- The planner invocation call
- The planner journal events (reviewer_started/reviewer_completed for role=planner)
- Passing planner_positions to build_voter_prompt

### 2. Update voter prompt template

In `src/contexts/workspace_governance/template_catalog.rs`, update `FINAL_REVIEW_VOTER_DEFAULT`:
- Remove instruction #2 about weighing planner positions
- Remove the `{{planner_positions}}` placeholder from the template body
- Remove `planner_positions` from optional_placeholders in the manifest

### 3. Update stage resolution snapshots

In the stage resolution snapshot (`StageResolutionSnapshot` in model.rs or similar), remove the `final_review_planner` field. Update all places that populate this field.

### 4. Update run.json schema

Remove `final_review_planner` from run.json snapshots. Update serialization/deserialization. Keep backward compat for reading old snapshots (use `#[serde(default)]` or skip_deserializing).

### 5. Update journal event handling

Remove planner-related journal events from the final review flow. The journal should no longer record reviewer_started/reviewer_completed events with role=planner.

### 6. Update config

Remove planner backend configuration from `backend show-effective` and related config resolution code.

### 7. Update tests

Update all tests that reference the planner role, planner_positions, or planner backend. This will be the bulk of the changes — search for "planner" across test files.

## What to keep
- Arbiter role and logic (fires on split votes)
- 3-reviewer proposal phase
- Reviewer voting phase (just without planner context)
- Quorum threshold logic

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state (run.json, journal.ndjson, artifacts, etc.) managed by the ralph-burning runtime itself. They are NOT application code and MUST NOT be reviewed or flagged as findings. Specifically:

- `.ralph-burning/projects/*/run.json` — these contain runtime state from the current run, including stage resolution snapshots that may still reference the old planner field. This is expected and correct — the runtime will stop writing planner fields in NEW runs after this change ships, but the CURRENT run's state was created before the change and cannot be retroactively modified.
- `.ralph-burning/projects/*/journal.ndjson` — runtime event log
- `.ralph-burning/projects/*/history/` — historical artifacts

DO NOT flag any file under `.ralph-burning/` as needing changes. Only review source code under `src/`, `tests/`, `docs/`, and config files like `Cargo.toml`, `flake.nix`.

## Acceptance Criteria
- Final review panel no longer invokes a planner
- Reviewers vote independently without planner positions
- Arbiter still resolves tied votes
- All tests pass with updated expectations
- No planner-related fields in NEW run.json snapshots (existing runtime state from this run is excluded)
- Old snapshots with planner fields can still be read (backward compat)
- cargo test && cargo clippy && cargo fmt --check pass
