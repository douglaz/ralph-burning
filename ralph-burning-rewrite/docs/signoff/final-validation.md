# Final Validation Report

Recorded: 2026-03-20 (updated loop 16 — OpenRouter row 3 remains `DEFERRED`; rerun attempts in loops 15 and 16 both failed at preflight with HTTP 403, so the manual smoke matrix is not yet green)
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

## Live Backend Smoke Results

### Claude (Row 1): PASS

- **smoke_id**: `smoke-claude-20260319183419`
- **project_id**: `claude-backend-smoke-test`
- **run_id**: `run-20260319183619`
- **run_status**: `completed`
- **Evidence**: Full end-to-end standard flow completed through 3 rounds. All stages executed: prompt_review, planning, implementation, review, qa, completion_panel, acceptance_qa, final_review. Stale session recovery handled transparently. *Historical note*: this evidence was captured before `--from-seed` was adopted as the default harness bootstrap path (iteration 8); at that time quick-requirements succeeded natively for Claude, so the `--idea` bootstrap was used. The current harness (`live-backend-smoke.sh`) now always uses `--from-seed` for all backends. A future Claude re-run would use `--from-seed` as well.
- **Fixes verified**: `enforce_strict_mode_schema()` applied to Claude `--json-schema`, `extract_json_from_text()` fallback decoder, stale session retry in `invoke()`, `looks_like_claude_envelope()` guard on empty-result fallback.

### Codex (Row 2): PASS

- **smoke_id**: `smoke-codex-20260319203137`
- **project_id**: `smoke-codex-test`
- **run_id**: `run-20260319203137`
- **run_status**: `completed`
- **Evidence**: Full end-to-end standard flow completed through 2 rounds. All stages executed: prompt_review, planning, implementation, qa, review, completion_panel, acceptance_qa, final_review. Final review requested changes in cycle 1; approved in cycle 2.
- **Fixes verified**: Corrected seed fixture (iteration 9), `--from-seed` bootstrap path, smoke script `SCRIPT_DIR` resolution (iteration 10).
- **Prior history**: Iteration 8 evidence was invalidated due to broken seed fixture. Iteration 9 fixed the seed and added CLI tests. Iteration 10 fixed the smoke script path resolution and produced this valid evidence.

### OpenRouter (Row 3): DEFERRED

- **smoke_id**: `smoke-openrouter-20260319203608`
- **project_id**: `smoke-openrouter-test`
- **run_id**: `run-20260319203614`
- **run_status**: `failed` — deferred due to external credit exhaustion ($40/$40 limit reached)
- **Evidence**: All 8 standard flow stages completed successfully on the first cycle in `execution.mode = "direct"`: prompt_review, planning, implementation, qa, review, completion_panel, acceptance_qa, final_review (10 successful backend invocations). Final review requested changes; re-implementation failed on HTTP 403 (key total limit exceeded) after 3 retries. No code defect.
- **Rerun attempts (2026-03-20)**: Loop 15 (`smoke-openrouter-20260320042526`) and loop 16 (`smoke-openrouter-20260320043644`) both exited at preflight with HTTP 403 `key limit exceeded` ($40/$40 total spending limit). Preserved evidence: `/tmp/smoke-openrouter-20260320043644-preflight-evidence.txt`. No project or active-workspace state was created by either rerun.
- **Fixes verified**: Corrected seed fixture (iteration 9), `max_tokens = 16384` in `openrouter_backend.rs` (iteration 10), credit preflight check (catches HTTP 402 and 403), smoke script `SCRIPT_DIR` resolution (iteration 10).
- **Deferral policy**: Qualifies under [`live-backend-smoke.md#qualifying-deferred-policy`](live-backend-smoke.md#qualifying-deferred-policy) because the adapter was validated end-to-end, the failure is external rather than a code defect, and OpenRouter is disabled in the checked-in workspace config `ralph-burning-rewrite/.ralph-burning/workspace.toml` (`[backends.openrouter] enabled = false`).
- **resolution_path**: rerun `./scripts/live-backend-smoke.sh openrouter` after credit top-up to upgrade this row to `PASS`.

## Cutover Readiness

- [x] `cargo test` succeeds in the default build (842 tests, 0 failures)
- [x] `cargo test --features test-stub` succeeds (791 unit, 169 CLI, 0 failures)
- [x] `cargo run --features test-stub -- conformance run` passes all 386 scenarios
- [x] `daemon.pr_review.transient_error_preserves_staged` passes
- [x] All 4 PR-review conformance scenarios pass
- [x] Stub-dependent CLI tests are compile-gated behind `#[cfg(feature = "test-stub")]`
- [x] All 16 smoke matrix items recorded with environment, exact command, result, and follow-up evidence
- [x] Rows 1-2 PASS with live evidence
- [x] Row 3's `DEFERRED` evidence cites the checked-in production workspace config `ralph-burning-rewrite/.ralph-burning/workspace.toml` (`[backends.openrouter] enabled = false`) and records the required `resolution_path`
- [ ] Manual smoke matrix is green per `parity-plan.md` exit criteria (row 3 is still `DEFERRED`, not `PASS`; loop 15 rerun `smoke-openrouter-20260320042526` and loop 16 rerun `smoke-openrouter-20260320043644` both failed at preflight with HTTP 403)

**Cutover status: Not Ready** — all automated checks still pass, and row 3's `DEFERRED` evidence is now grounded in the checked-in production workspace config (`ralph-burning-rewrite/.ralph-burning/workspace.toml`, `[backends.openrouter] enabled = false`). But the parity-plan exit criterion is explicit: the manual smoke matrix must be green. That criterion is not yet met because OpenRouter row 3 has not been rerun to `PASS`; rerun attempts in loops 15 (`smoke-openrouter-20260320042526`) and 16 (`smoke-openrouter-20260320043644`) both failed at preflight with external HTTP 403 credit exhaustion ($40/$40 key limit). The only remaining action is topping up or raising the OpenRouter API key spending limit and rerunning `./scripts/live-backend-smoke.sh openrouter`.
