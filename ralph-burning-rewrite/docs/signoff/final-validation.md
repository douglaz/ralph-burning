# Final Validation Report

Recorded: 2026-03-19 (updated iteration 9 — seed fixture fixed, rows 2-3 BLOCKED pending re-run)
Branch: ralph/parity-plan

## Automated Check Results

### 1. Default Build: `cargo test` (no features)

```
cargo test
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| lib.rs | 78 | 0 | 0 |
| main.rs | 0 | 0 | 0 |
| cli.rs | 123 | 0 | 0 |
| run_attach_tmux.rs | 1 | 0 | 0 |
| unit.rs | 640 | 0 | 0 |
| **Total** | **842** | **0** | **0** |

**Result: PASS** -- `cargo test` succeeds in the default build. Includes 15 tests in `process_backend.rs` (7 for `enforce_strict_mode_schema`/`extract_json_from_text` + 4 for `looks_like_claude_envelope` + 4 integration tests for Claude `finish()` fallback paths).

### 2. Stub Build: `cargo test --features test-stub`

```
cargo test --features test-stub
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| unit.rs | 791 | 0 | 1 |
| cli.rs | 169 | 0 | 0 |

**Unit tests: PASS** (791 passed, 0 failed, 1 ignored)

**CLI tests: PASS** (169 passed, 0 failed) -- includes `conformance_full_suite_passes`

### 3. Conformance Suite: `cargo run --features test-stub -- conformance run`

```
cargo run --features test-stub -- conformance run
```

| Metric | Value |
|--------|-------|
| Selected | 386 |
| Passed | 386 |
| Failed | 0 |
| Not run | 0 |

**Result: PASS** -- All 386 conformance scenarios pass.

### 4. PR-Review Conformance Scenarios (targeted)

```
cargo run --features test-stub -- conformance run --filter daemon.pr_review.transient_error_preserves_staged
cargo run --features test-stub -- conformance run --filter daemon.pr_review.completed_project_reopens_with_amendments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.whitelist_filters_comments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.dedup_across_restart
```

All 4 PR-review scenarios: **PASS**

## Live Backend Smoke Results (iteration 9)

### Claude (Row 1): PASS

- **smoke_id**: `smoke-claude-20260319183419`
- **project_id**: `claude-backend-smoke-test`
- **run_id**: `run-20260319183619`
- **run_status**: `completed`
- **Evidence**: Full end-to-end standard flow completed through 3 rounds. All stages executed: prompt_review, planning, implementation, review, qa, completion_panel, acceptance_qa, final_review. Stale session recovery handled transparently. Claude smoke did not use `--from-seed` (quick-requirements succeeded), so the seed fixture bug did not affect this row.
- **Fixes verified**: `enforce_strict_mode_schema()` applied to Claude `--json-schema`, `extract_json_from_text()` fallback decoder, stale session retry in `invoke()`, `looks_like_claude_envelope()` guard on empty-result fallback.

### Codex (Row 2): BLOCKED

- **Prior evidence invalidated**: Iteration 8 claimed `run_status = completed` via `--from-seed` bootstrap, but the committed `scripts/smoke-seed.json` had `source.mode = "seed_file"` (not a valid `RequirementsMode` variant) and omitted the required `question_rounds` field. Running `cargo run -- project bootstrap --from-seed scripts/smoke-seed.json --flow standard` against the committed fixture produced `invalid project seed JSON: unknown variant 'seed_file', expected 'draft' or 'quick'`.
- **Seed fixture fixed** (iteration 9): Removed the invalid `source` field from `smoke-seed.json` (the field is `Option<SeedSourceMetadata>` with `#[serde(default)]`). The `--from-seed` bootstrap path is now verified with both test-stub CLI tests and a manual local run.
- **Next step**: Re-run `./scripts/live-backend-smoke.sh codex` with the corrected seed to produce valid evidence (`project_id`, `run_id`, `run_status = completed`).

### OpenRouter (Row 3): BLOCKED

- **Prior evidence invalidated**: Same seed fixture bug as Codex — iteration 8 evidence was recorded against a seed that cannot be parsed by the committed code.
- **Seed fixture fixed** (iteration 9): Same fix as Codex. Credit preflight logic is in place and unchanged.
- **Next step**: Re-run `./scripts/live-backend-smoke.sh openrouter` with the corrected seed and usable API credits to produce valid evidence (`project_id`, `run_id`, `run_status = completed`).

## Cutover Readiness

- [x] `cargo test` succeeds in the default build (842 tests, 0 failures)
- [x] `cargo test --features test-stub` succeeds (791 unit, 169 CLI, 0 failures)
- [x] `cargo run --features test-stub -- conformance run` passes all 386 scenarios
- [x] `daemon.pr_review.transient_error_preserves_staged` passes
- [x] All 4 PR-review conformance scenarios pass
- [x] Stub-dependent CLI tests are compile-gated behind `#[cfg(feature = "test-stub")]`
- [x] Backend-specific manual smoke items — **Claude PASS, Codex BLOCKED, OpenRouter BLOCKED (iteration 9)**
  - **Claude**: Full end-to-end standard flow `completed` (`run-20260319183619`) — PASS
  - **Codex**: BLOCKED — seed fixture corrected, re-run required with fixed `scripts/smoke-seed.json`
  - **OpenRouter**: BLOCKED — seed fixture corrected, re-run required with fixed `scripts/smoke-seed.json`
- [x] All 16 smoke matrix items recorded with environment, exact command, result, and follow-up evidence
- [ ] Rows 2-3 (Codex, OpenRouter) require re-run with corrected seed fixture

**Cutover status: Not Ready** — all automated checks pass (842+ default tests, 791+169 stub tests, 386 conformance scenarios). Claude live backend smoke PASS. Codex and OpenRouter rows BLOCKED: iteration 8 evidence was recorded against a broken seed fixture (`source.mode = "seed_file"` is not a valid `RequirementsMode`). The seed fixture has been corrected in iteration 9 and verified locally. Live re-runs of `./scripts/live-backend-smoke.sh codex` and `./scripts/live-backend-smoke.sh openrouter` are required to produce valid end-to-end evidence before cutover can be marked Ready.
