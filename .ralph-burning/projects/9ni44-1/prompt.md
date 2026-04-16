# Codify sync/import safety rules for non-invasive br operation

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add safety rules, validation, and documentation. Do NOT delete or restructure existing code.

## Background — what already exists

The codebase already has substantial br sync infrastructure:

- `BrMutationAdapter` in `src/adapters/br_process.rs` with dirty-flag tracking (`has_unsync_mutations`)
- `sync_flush()` method that runs `br sync --flush-only` and clears the dirty flag
- Unconditional sync after `br close` in success_reconciliation.rs (crash-safe idempotency)
- `BrError` enum with `BrNotFound`, `BrTimeout`, `BrExitError`, `BrParseError` variants
- `BeadVerification` enum (Verified/Stale/TransientError) in planned_elsewhere.rs
- Controller state machine guards in milestone_record/controller.rs
- No `sync --import-only` is currently used anywhere

## What to implement

### 1. Add `sync --import-only` support to BrMutationAdapter

In `src/adapters/br_process.rs`:
- Add `BrCommand::sync_import()` that builds `br sync --import-only`
- Add `BrMutationAdapter::sync_import()` method (read-only — does NOT set dirty flag)
- This is needed for safely importing external JSONL changes after `git pull`

### 2. Add sync safety validation in BrMutationAdapter

In `src/adapters/br_process.rs`:
- Add `ensure_synced()` method that checks `has_unsync_mutations` and warns/errors if mutations are pending before certain operations
- Add `sync_if_dirty()` convenience method that runs `sync_flush()` only if dirty flag is set

### 3. Add JSONL health check

In `src/adapters/br_process.rs` or a new `src/adapters/br_health.rs`:
- Add a function `check_beads_health(base_dir)` that:
  1. Checks if `.beads/issues.jsonl` exists
  2. Checks for git conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`) in the file
  3. Checks if `br` is available (already handled by `BrAdapter::check_available()`)
  4. Returns a `BeadsHealthStatus` enum: `Healthy`, `ConflictMarkers`, `MissingFile`, `BrUnavailable`

### 4. Add pre-operation safety checks

In `src/contexts/milestone_record/service.rs`:
- Before milestone operations that mutate beads (create bead, close bead, update status), check beads health
- If conflict markers detected, return an error directing the operator to resolve conflicts
- If JSONL missing, return a clear error

### 5. Document recovery procedures

Add inline documentation (doc comments) to key functions:
- `sync_flush()`: when to call, what happens on failure, crash recovery
- `sync_import()`: when to call (after git pull/merge), safety guarantees
- `BrMutationAdapter`: overall sync lifecycle and invariants

### 6. Unit tests

Add tests for:
- `sync_import()` command construction
- `sync_if_dirty()` behavior (syncs when dirty, no-op when clean)
- `check_beads_health()` detecting conflict markers
- `check_beads_health()` with healthy file
- `check_beads_health()` with missing file

## Files to modify

- `src/adapters/br_process.rs` — add sync_import, ensure_synced, sync_if_dirty
- `src/adapters/br_health.rs` (new, small) — beads health check
- `src/contexts/milestone_record/service.rs` — add pre-operation health checks
- `src/adapters/mod.rs` — register br_health module if new file created

## Acceptance Criteria
- `sync --import-only` support added
- `sync_if_dirty()` convenience method works
- JSONL health check detects conflict markers
- Pre-operation safety checks prevent mutations on corrupted state
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
