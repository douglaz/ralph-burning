# Speed up test suite and nix build

## Problem

CI takes ~35 minutes and `nix build .` is similarly slow. We need to profile and optimize without disabling or removing any tests.

## Step 1: Profile test durations

Run `cargo test -- --format json -Z unstable-options` or `cargo test -- -Zunstable-options --report-time` to identify the slowest individual tests. Also measure overall compilation time vs test execution time to understand where time is spent.

Alternatively, run tests with timing: `cargo test 2>&1 | head -50` to see the test count and total time, and use `cargo test -- --nocapture` with timing to find slow tests.

You can also check individual test timing by running specific test modules.

## Step 2: Identify bottlenecks

Common causes of slow tests:
- **Sleeps and timeouts**: Tests that use `thread::sleep()` or `tokio::time::sleep()` with long durations
- **Process spawning**: Tests that spawn external processes (codex, claude CLI, etc.)
- **Repeated expensive setup**: Tests that rebuild expensive fixtures from scratch each time
- **Sequential bottlenecks**: Tests that could run in parallel but don't
- **Nix-specific overhead**: Cargo vendor fetching, full rebuilds instead of incremental

Search the codebase for:
- `thread::sleep` and `tokio::time::sleep` in test code
- `std::process::Command` in test code
- Large test fixtures or setup functions
- `#[serial]` or other serialization attributes

## Step 3: Optimize

Apply targeted fixes based on profiling results. Prioritize the highest-impact changes.

## Constraints
- Do NOT disable, skip, ignore, or remove any tests
- Do NOT reduce test coverage
- All existing tests must continue to pass
- Changes must not affect runtime behavior

## Acceptance Criteria
- Measurable reduction in `cargo test` execution time
- All tests pass: `cargo test && cargo clippy && cargo fmt --check`
- `nix build .` succeeds
