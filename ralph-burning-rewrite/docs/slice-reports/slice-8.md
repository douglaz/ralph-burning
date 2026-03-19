# Slice 8: Final Sign-off Hardening and Validation

## Legacy References Consulted

- `src/contexts/project_run_record/service.rs:stage_amendment_batch` -- batch staging rollback behavior for completed vs non-completed projects
- `src/contexts/automation_runtime/pr_review.rs:ingest_reviews` -- cursor advancement and journal event gating
- `src/contexts/conformance_spec/scenarios.rs:daemon.pr_review.transient_error_preserves_staged` -- the failing conformance scenario that defined the expected invariant
- Slice 3 review notes (manual amendment parity) -- codified "accepted amendments must persist before reopening work"

## Contracts Changed

### `stage_amendment_batch` failure semantics (service.rs)

Previously: snapshot/reopen write failure rolled back all amendment files unconditionally, regardless of project status.

Now: when the project is `Completed` and the reopen/snapshot write fails, amendment files that have already been written to disk are preserved. The snapshot stays at its last committed completed state. Cursor advancement and journal events are prevented by the error propagating through `ingest_reviews`.

For non-completed projects, the rollback behavior is unchanged.

### Test target gating (unit.rs, cli.rs)

Stub-dependent CLI tests are now compile-gated behind `#[cfg(feature = "test-stub")]` (previously runtime-skipped via `require_stub_binary!()` macro). Default `cargo test` reports 831 tests that all actually execute. Stub build reports 1028 tests (831 default + 197 stub-only).

### Cache key stability for answer-independent stages (service.rs)

Ideation and research cache keys now use only the idea (and ideation output for research), not the full prompt context that includes answers. This ensures cache reuse works correctly when the pipeline resumes after a question round.

### Label override array support (agent_execution_builder.rs)

`RALPH_BURNING_TEST_LABEL_OVERRIDES` now supports JSON arrays as values for a label, which are interpreted as payload sequences. This enables scenarios that need different responses on successive invocations of the same label within a single process.

### Conformance scenario validation overrides (scenarios.rs)

Ten RD-* scenarios (RD-001, RD-004, RD-010, RD-011, RD-012, RD-013, RD-015, RD-018, RD-021, RD-022) now include explicit `validation` label overrides with `needs_questions` outcome to trigger the question round. Previously, the default canned `validation: pass` payload caused the pipeline to skip questions entirely.

## Tests Run

| Suite | Command | Result |
|-------|---------|--------|
| Default unit tests | `cargo test --test unit` | 640 passed, 0 failed |
| Default CLI tests | `cargo test --test cli` | 123 passed, 0 failed |
| Default full | `cargo test` | 831 passed, 0 failed |
| Stub unit tests | `cargo test --features test-stub --test unit` | 791 passed, 0 failed |
| Stub CLI tests | `cargo test --features test-stub --test cli` | 169 passed, 0 failed |
| Full stub suite | `cargo test --features test-stub` | 1028 passed, 0 failed |
| PR review conformance | `conformance run --filter daemon.pr_review.*` | 4/4 passed |
| Full conformance | `conformance run` | 386/386 passed |

## Results

- `daemon.pr_review.transient_error_preserves_staged` passes (was the primary completion blocker)
- All 386 conformance scenarios pass (was 147/386 before, blocked by RD-001)
- `RD-001` through `RD-022` question-round scenarios all pass
- Default build compiles and all tests pass with no stub-only no-ops
- Cache reuse across question rounds works correctly

## Remaining Known Gaps

- **Backend-specific manual smoke items**: Claude, Codex, and OpenRouter smoke items are marked UNVALIDATED (tested only via stub adapter). Requires live backend access for validation.
