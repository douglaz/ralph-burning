# Implementation Response (Iteration 16)

## Changes Made

### RC1: Fix multi-repo label-failure quarantine (5 sites in `daemon_loop.rs`)
1. Fixed 5 sites in `ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs` `process_task_multi_repo` where `sync_label_for_task()` failure was swallowed (returning `Ok(())`) instead of propagating `Err` to quarantine the repo for the cycle:
   - **Site 1** (~L568): Rebase failure → failed task → label sync. Changed `.is_err()` guard to `if let Err(e)`, added `eprintln` and `return Err(e)`.
   - **Site 2** (~L581): `ensure_project` failure → failed task → label sync. Same pattern as Site 1.
   - **Site 3** (~L591): Aborted task after claim → label sync. Changed to `if let Err(e)`, returns `Err(e)` **before** lease release. Lease release now only happens after successful label sync.
   - **Site 4** (~L611): `mark_active` failure → failed task → label sync. Same pattern as Sites 1/2.
   - **Site 5** (~L650): Post-dispatch aborted → label sync. Same lease-deferral pattern as Site 3: returns `Err(e)` before lease release.

### RC2: Remove legacy `daemon start` CLI path
2. Removed `handle_start_legacy_no_intake` function and `RALPH_BURNING_TEST_LEGACY_DAEMON` env-var check from `ralph-burning-rewrite/src/cli/daemon.rs`. `daemon start` now always requires `--data-dir` plus at least one `--repo`.
3. Created in-process daemon iteration test helpers in `ralph-burning-rewrite/src/contexts/conformance_spec/scenarios.rs`:
   - `run_daemon_iteration_in_process(ws_path)` — stub backend with `FileIssueWatcher`
   - `run_daemon_iteration_with_backend(ws_path, backend_override)` — generic backend selection
   - `run_daemon_iteration_with_process_backend(ws_path, extra_path)` — process backend with PATH override
   All helpers set `RALPH_BURNING_BACKEND` env var to match the injected adapter so the daemon's internal `RequirementsService` (built via `build_requirements_service_default`) uses the same backend family.
4. Converted ~10 conformance scenarios and 2 CLI integration tests from `run_cli(["daemon", "start", "--single-iteration"])` to the new in-process helpers.
5. Renamed `apply_label_overrides_to_stub` → `apply_test_label_overrides` and made it `pub` in `ralph-burning-rewrite/src/composition/agent_execution_builder.rs` so in-process test harnesses can replicate the label-override behavior.

### RI1: Loop-level regression test for multi-repo label-failure quarantine
6. Added conformance scenario `daemon.tasks.label_failure_quarantines_repo` in `ralph-burning-rewrite/src/contexts/conformance_spec/scenarios.rs` that:
   - Creates two pending tasks in the same repo with a `FailLabelGithub` adapter that fails `sync_label_for_task`.
   - Runs `process_cycle_multi_repo` for one cycle.
   - Asserts the first task is claimed with `label_dirty = true` and the second task remains pending because the repo was quarantined after the first label-sync failure.
7. Added corresponding Gherkin scenario in `ralph-burning-rewrite/tests/conformance/features/daemon_github.feature`.

## Could Not Address
- None

## Pending Changes (Pre-Commit)
- Uncommitted Slice 8 changes across 6 files (see `git diff --stat`): `daemon_loop.rs`, `daemon.rs`, `agent_execution_builder.rs`, `scenarios.rs`, `cli.rs`, `daemon_github.feature`, and this response artifact.

## Verification
- `cargo check --tests`: clean
- `cargo test` (lib, 578 unit tests): all pass
- `cargo test --test cli` (110 integration + 285 conformance scenarios): all pass
- New scenario `daemon.tasks.label_failure_quarantines_repo`: PASS (0.10s)
- Previously failing `DAEMON-INTAKE-003`: now PASS after env-var fix

---
