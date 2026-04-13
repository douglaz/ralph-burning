# Fix 5 nix-build test failures: tests depend on `ps` binary unavailable in nix sandbox

## Problem

`nix build .` fails because 5 tests call `ps` to enumerate process groups, and `ps` is not available in the nix build sandbox.

### Failing tests
1. `cli::run::tests::kill_stale_backend_process_group_refuses_exited_authoritative_leader_without_safe_proof`
2. `cli::run::tests::repair_missing_interrupted_handoff_run_failed_event_and_reload_snapshot_cleans_stale_daemon_backends`
3. `contexts::automation_runtime::daemon_loop::tests::load_dispatch_run_snapshot_repairs_missing_run_failed_event_from_daemon_handoff`
4. `contexts::automation_runtime::daemon_loop::tests::process_cycle_multi_repo_blocks_repo_after_recovered_terminal_cleanup_failure`
5. `contexts::automation_runtime::daemon_loop::tests::process_cycle_phase0_repairs_persisted_cancelled_handoff_for_aborted_task`

### Error pattern
```
failed to enumerate process groups via ps: No such file or directory (os error 2)
```

## Fix approach

Add `procps` (which provides `ps`) to the nix build inputs so it's available during `cargo test` in the sandbox. This is in `flake.nix`. Look at how `nativeBuildInputs` or `buildInputs` or `checkInputs` are configured and add `procps` there so it's available during the check phase.

If procps alone doesn't resolve all 5 failures, also check whether the tests need any other system utilities and add those too.

## Acceptance Criteria
- `nix build .` succeeds with all tests passing (currently 844 pass, 5 fail)
- No test regressions in `nix develop -c cargo test`
- `cargo clippy && cargo fmt --check` pass
