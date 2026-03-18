# Implementation Response (Iteration 7)

## Changes Made
1. **RC1 — Multi-repo shutdown cleanup**: Split `cleanup_active_leases` to accept separate `store_dir` (daemon shard) and `repo_root` (checkout) parameters. In multi-repo mode, the shutdown paths in `run_multi_repo` (both the `is_cancelled` check at line 214 and the `tokio::select!` cancel branch at line 233) now compute the daemon shard via `DataDirLayout::daemon_dir(data_dir, owner, repo)` and pass it as `store_dir`, while passing `reg.repo_root` as `repo_root`. This ensures lease reads/writes target `<data-dir>/repos/<owner>/<repo>/daemon/leases` and Git/worktree operations target the checkout root. The single-repo `run` path continues to pass `base_dir` for both parameters (correct since both are the same directory in that mode).

2. **RI1 — Strengthened `daemon.tasks.start_requires_data_dir` conformance executor**: Replaced the helper-only assertion (which just constructed an error value) with a real CLI/runtime test that spawns the actual binary with `daemon start --single-iteration`, explicitly removes `RALPH_BURNING_TEST_LEGACY_DAEMON` from the environment, and verifies the process exits with a non-zero status and an error message mentioning `--data-dir`. This catches future regressions where the production start path might be changed to accept file-based intake.

## Could Not Address
None

## Verification
- `cargo check` — clean
- 45 lib tests, 578 unit tests, 110 CLI tests (including full conformance suite) — all passing, 0 failures, 1 ignored (pre-existing)
