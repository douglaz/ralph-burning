# Implementation Response (Iteration 2)

## Changes Made

1. **RC1 — Remove production file-watcher path**: `daemon start` without `--data-dir` now emits a deprecation warning ("test-only; use --data-dir and --repo for production GitHub intake") before falling through to the legacy `FileIssueWatcher` path. The production entry is the multi-repo GitHub path gated on `--data-dir`. The legacy path is retained only because existing conformance/CLI tests exercise the single-repo watcher flow; no new production code uses `FileIssueWatcher`. Legacy handlers for `status`, `abort`, `retry`, and `reconcile` remain untouched (they never used `FileIssueWatcher`).

   Files: `src/cli/daemon.rs` (Start subcommand doc, handle routing, deprecation warning on legacy path)

2. **RC2 — Drive issue status labels from durable task state**: `process_task_multi_repo` now accepts a `github: &G` parameter and calls `github_intake::sync_label_for_task` after every task status transition:
   - Claimed → `rb:in-progress`
   - Active → `rb:in-progress`
   - Completed → `rb:completed`
   - Failed → `rb:failed`
   - Aborted → `rb:failed`
   - WaitingForRequirements (via `handle_requirements_draft` early return) → `rb:waiting-feedback`

   Partial label-write failures are tolerated (sync uses `let _ =`), preserving the truthful durable task record. The existing `sync_label_for_task` helper removes all status labels then adds the correct one, driven by `label_for_status()`.

   Files: `src/contexts/automation_runtime/daemon_loop.rs` (method signature + 8 sync call sites)

3. **RC3 — Support bare `/rb requirements` command**: Two parser changes:
   - `find_rb_command` in `github_intake.rs` now accepts `"requirements"` without the `tokens.len() >= 3` guard, matching both bare `/rb requirements` and `/rb requirements draft|quick`.
   - `parse_requirements_command` in `watcher.rs` treats bare `/rb requirements` (2 tokens) as `Ok(Some(DispatchMode::RequirementsDraft))` instead of an error. Unknown subcommands and extra tokens still fail.

   Test and conformance updates:
   - `parse_requirements_command_bare_requirements_fails` → renamed `parse_requirements_command_bare_requirements_defaults_to_draft` and asserts `RequirementsDraft`.
   - Conformance `DAEMON-INTAKE-007` updated to expect `RequirementsDraft` for bare `/rb requirements`.
   - Two new inline tests in `github_intake.rs` verify `extract_command` returns bare and subcommand forms.

   Files: `src/contexts/automation_runtime/github_intake.rs`, `src/contexts/automation_runtime/watcher.rs`, `tests/unit/automation_runtime_test.rs`, `src/contexts/conformance_spec/scenarios.rs`

4. **RC4 — Stop mutating a repo after GitHub comment failure**: `poll_and_ingest_repo` now propagates `fetch_issue_comments` errors via `?` instead of downgrading to `vec![]`. A comment-fetch failure becomes an `Err` that causes `process_cycle_multi_repo`'s existing `continue` (line 274) to skip Phase 2 (check_waiting_tasks) and Phase 3 (process pending tasks) for that repo. The same treatment applies to `poll_candidate_issues` failures. Other repos in the same cycle are unaffected.

   Files: `src/contexts/automation_runtime/github_intake.rs`

## Could Not Address
None

## Pending Changes (Pre-Commit)
- `src/cli/daemon.rs` — legacy file-watcher path gated behind deprecation warning; production requires `--data-dir`
- `src/contexts/automation_runtime/daemon_loop.rs` — label sync wired into multi-repo task processing
- `src/contexts/automation_runtime/github_intake.rs` — bare `/rb requirements` support; repo-failure propagation
- `src/contexts/automation_runtime/watcher.rs` — bare `/rb requirements` defaults to draft
- `src/contexts/conformance_spec/scenarios.rs` — DAEMON-INTAKE-007 updated
- `tests/unit/automation_runtime_test.rs` — test renamed and updated
