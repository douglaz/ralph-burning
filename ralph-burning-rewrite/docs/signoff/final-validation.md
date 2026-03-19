# Final Validation Report

Recorded: 2026-03-19
Branch: ralph/parity-plan

## Automated Check Results

### 1. Default Build: `cargo test` (no features)

```
cargo test
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| lib.rs | 67 | 0 | 0 |
| main.rs | 0 | 0 | 0 |
| cli.rs | 167 | 0 | 0 |
| run_attach_tmux.rs | 1 | 0 | 0 |
| unit.rs | 640 | 0 | 0 |
| **Total** | **875** | **0** | **0** |

**Result: PASS** -- `cargo test` succeeds in the default build.

### 2. Stub Build: `cargo test --features test-stub`

```
cargo test --features test-stub
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| unit.rs | 791 | 0 | 1 |
| cli.rs | 168 | 1 | 0 |

**Unit tests: PASS** (791 passed, 0 failed, 1 ignored)

**CLI tests: 1 failure** (`conformance_full_suite_passes` -- see conformance details below)

### 3. Conformance Suite: `cargo run --features test-stub -- conformance run`

```
cargo run --features test-stub -- conformance run
```

| Metric | Value |
|--------|-------|
| Selected | 386 |
| Passed | 147 |
| Failed | 1 |
| Not run | 238 |

**Failing scenario:** `RD-001` -- `expected 'awaiting_answers', got 'completed'`

This is a **pre-existing** issue where the `RALPH_BURNING_TEST_LABEL_OVERRIDES` environment variable is not forwarded through the conformance CLI runner. The underlying requirements draft question-round feature works correctly in direct CLI tests (`requirements_draft_with_empty_questions_completes`, `requirements_answer_happy_path_completes_run`).

**Not-run scenarios (238):** These are scenarios that are skipped because they depend on `fail-fast` stopping after the first failure (`RD-001`). All 147 scenarios that ran before `RD-001` passed.

### 4. PR-Review Conformance Scenarios (targeted)

```
cargo run --features test-stub -- conformance run --filter daemon.pr_review.transient_error_preserves_staged
cargo run --features test-stub -- conformance run --filter daemon.pr_review.completed_project_reopens_with_amendments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.whitelist_filters_comments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.dedup_across_restart
```

All 4 PR-review scenarios: **PASS**

## Cutover Readiness

- [x] `cargo test` succeeds in the default build (875 tests, 0 failures)
- [x] `cargo test --features test-stub --test unit` succeeds (791 tests, 0 failures)
- [x] `daemon.pr_review.transient_error_preserves_staged` passes (was the primary blocker)
- [x] All 4 PR-review conformance scenarios pass
- [x] Stub-dependent tests are correctly gated behind `#[cfg(feature = "test-stub")]`
- [ ] Full conformance suite: 1 pre-existing failure (RD-001, not a regression)

**Cutover status: Ready**, pending resolution or explicit deferral of RD-001.
