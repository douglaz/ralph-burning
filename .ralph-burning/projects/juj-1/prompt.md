# Optimize engine test performance: debug vs release mode

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Hypothesis

Tests run in debug profile by default. The unoptimized debug build likely makes tokio runtime, channel operations, and mutex contention much worse than release mode. The 68% futex overhead seen in profiling may largely disappear with compiler optimizations.

## Step 1: Validate the hypothesis

Before any code changes, measure the difference. Run `cli_run_start_releases_lock_on_error` in both modes:

```bash
# Debug (current) - expected ~67s
time cargo test --locked --features test-stub --test cli cli_run_start_releases_lock_on_error -- --exact

# Release - measure this
time cargo test --locked --features test-stub --test cli cli_run_start_releases_lock_on_error -- --exact --release
```

Also measure the full CLI test suite and conformance in both modes.

## Step 2: Apply the fix

If release mode is significantly faster, add optimization to the test profile in Cargo.toml:

```toml
[profile.test]
opt-level = 2  # or opt-level = 1 for lighter optimization
debug = 0      # already set
```

This enables optimizations for all test builds. The tradeoff is slower compilation but faster test execution. If `opt-level = 2` adds too much compile time, try `opt-level = 1` as a compromise.

## Step 3: Verify

After applying the change:
1. Measure the same tests again to confirm improvement
2. Run full test suite to ensure no test breakage
3. Check that compilation time increase is acceptable

## Acceptance Criteria
- Measurable reduction in stub-backend test execution time
- Target: single CLI lifecycle test under 15 seconds (currently 67-85s)
- All tests pass unchanged
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
