# Final Validation Report

Recorded: 2026-03-19 (updated iteration 7 — rows 2-3 corrected to BLOCKED, cutover Not Ready)
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

**Result: PASS** -- `cargo test` succeeds in the default build. Includes 11 tests in `process_backend.rs` (7 for `enforce_strict_mode_schema`/`extract_json_from_text` + 4 new for `looks_like_claude_envelope`).

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

## Live Backend Smoke Results (iteration 7)

### Claude (Row 1): PASS

- **smoke_id**: `smoke-claude-20260319183419`
- **project_id**: `claude-backend-smoke-test`
- **run_id**: `run-20260319183619`
- **run_status**: `completed`
- **Evidence**: Full end-to-end standard flow completed through 3 rounds. All stages executed: prompt_review, planning, implementation, review, qa, completion_panel, acceptance_qa, final_review. Stale session recovery handled transparently.
- **Fixes verified**: `enforce_strict_mode_schema()` applied to Claude `--json-schema`, `extract_json_from_text()` fallback decoder, stale session retry in `invoke()`, `looks_like_claude_envelope()` guard on empty-result fallback.

### Codex (Row 2): BLOCKED

- **smoke_id**: `smoke-codex-20260319172938`
- **project_id**: none (bootstrap did not complete)
- **run_id**: none
- **run_status**: none (no project/run created)
- **Evidence**: Preflight PASS. Schema enforcement verified through 5 successful draft→review cycles without schema errors. Bootstrap exits at `MAX_QUICK_REVISIONS=5` — Codex gpt-5.4 does not approve requirements within 5 cycles. No end-to-end completion evidence exists. The PASS rule requires `run_status = completed`.
- **Fixes verified**: `enforce_strict_mode_schema()` in `process_backend.rs` ensures `#[serde(default)]` fields are included in `required`.
- **Blocker**: Model behavior prevents quick-requirements approval within the revision limit.

### OpenRouter (Row 3): BLOCKED

- **smoke_id**: `smoke-openrouter-20260319175711`
- **project_id**: `smoke-openrouter-ci`
- **run_id**: `run-20260319180229`
- **run_status**: `failed` (HTTP 402 at prompt_review)
- **Evidence**: Preflight PASS. Bootstrap PASS (requirements pipeline completes successfully). Run start fails with HTTP 402 (insufficient credits on test API key). Per spec, a `--start` failure must be recorded as a failure, not treated as validated. The PASS rule requires `run_status = completed`.
- **Fixes verified**: `enforce_strict_mode_schema()` in `openrouter_backend.rs` ensures schema compliance before `strict: true` submission.
- **Blocker**: OpenRouter API account requires credit top-up before re-run.

## Cutover Readiness

- [x] `cargo test` succeeds in the default build (842 tests, 0 failures)
- [x] `cargo test --features test-stub` succeeds (791 unit, 169 CLI, 0 failures)
- [x] `cargo run --features test-stub -- conformance run` passes all 386 scenarios
- [x] `daemon.pr_review.transient_error_preserves_staged` passes
- [x] All 4 PR-review conformance scenarios pass
- [x] Stub-dependent CLI tests are compile-gated behind `#[cfg(feature = "test-stub")]`
- [x] Backend-specific manual smoke items — **Claude PASS, Codex BLOCKED, OpenRouter BLOCKED (iteration 7)**
  - **Claude**: Full end-to-end standard flow `completed` (`run-20260319183619`) — PASS
  - **Codex**: Schema enforcement verified (5 cycles), but bootstrap exits at revision limit — no `run_status = completed` evidence — BLOCKED
  - **OpenRouter**: Bootstrap PASS with schema fix, run fails HTTP 402 (`run_status = failed`) — BLOCKED
- [x] All 16 smoke matrix items recorded with environment, exact command, result, and follow-up evidence
- [ ] No prompt-required smoke item remains FAIL — **rows 2-3 are BLOCKED pending external resolution**

**Cutover status: Not Ready** — all automated checks pass (842 default tests, 791+169 stub tests, 386 conformance scenarios). Claude live smoke PASS. Codex and OpenRouter live smokes are BLOCKED on external factors (model behavior at revision limit, API credit exhaustion). Code fixes are verified, but end-to-end `run_status = completed` evidence is missing for rows 2-3. Cutover requires: (1) Codex smoke to achieve end-to-end completion, (2) OpenRouter API account credit top-up and re-run to completion.
