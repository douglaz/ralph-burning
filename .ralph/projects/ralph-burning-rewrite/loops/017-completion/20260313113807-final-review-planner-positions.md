---
artifact: final-review-planner-positions
loop: 17
project: ralph-burning-rewrite
backend: claude(opus)
role: planner
created_at: 2026-03-13T11:38:07Z
---

I've read all the relevant source files. Here is my evaluation.

# Planner Positions

## Amendment: CLI-RUN-WRITER-LOCK

### Position
ACCEPT

### Rationale
Verified in `src/cli/run.rs`: `handle_start()` (lines 102-180) calls `engine::execute_run()` directly, and `handle_resume()` (lines 182-254) calls `engine::resume_run()` directly. Neither acquires a writer lock. Grep for `acquire_writer_lock` in `engine.rs` returns zero matches — the engine has no internal lock acquisition. The daemon path correctly acquires via `LeaseService::acquire()` at `lease_service.rs:44-45`, which calls `store.acquire_writer_lock()`. The CLI path has a check-and-act pattern (read status → check `NotStarted` → proceed) at `run.rs:121-144`, but this is not atomic. Two concurrent `ralph-burning run start` invocations can both pass the `NotStarted` check before either transitions the state, creating competing active runs. The `acquire_writer_lock`/`release_writer_lock` methods are on `DaemonStorePort` (`automation_runtime/mod.rs:43-49`), so factoring them into a shared trait is a reasonable approach to make them available to the CLI path.

## Amendment: DAEMON-PROCESS-GLOBAL-CWD

### Position
ACCEPT

### Rationale
Verified at `daemon_loop.rs:930-931`: `std::env::set_current_dir(worktree_path)` is called, with restoration at line 983. Confirmed `Cargo.toml:13` specifies `rt-multi-thread`. `std::env::set_current_dir` mutates process-global state, which is unsound in a multi-threaded async runtime. Additionally verified: if the engine call panics (unwinding past the restore), the CWD is never reset. Confirmed the `set_current_dir` is unnecessary — the engine receives `base_dir` as an explicit parameter (line 943), and `WorktreeAdapter` uses `Command::new().current_dir()` for git operations. No code path in the engine relies on process CWD. This is a genuine defect: process-global mutable state in a multi-threaded runtime with no panic safety.

## Amendment: STRAY-SCHEDULED-TASKS-LOCK

### Position
ACCEPT

### Rationale
Confirmed `.claude/scheduled_tasks.lock` is tracked in git (`git ls-files --error-unmatch` succeeds). File contents are `{"sessionId":"3df01bda-...","pid":3917503,"acquiredAt":1773337837545}` — a transient session-specific lock with a PID that will be stale on any other machine. This file should not be committed.

## Amendment: AM-001

### Position
ACCEPT

### Rationale
This is substantively the same issue as DAEMON-PROCESS-GLOBAL-CWD and identifies the same defect at the same location (`daemon_loop.rs:930-989`). Verified: the `set_current_dir` calls are process-global, the runtime is `rt-multi-thread`, and restoration is not panic-safe. The proposed fix (pass `worktree_path` explicitly, use RAII guard if CWD mutation is truly needed) is sound. Accepting because the underlying problem is real, though this duplicates DAEMON-PROCESS-GLOBAL-CWD.

## Amendment: AM-002

### Position
ACCEPT

### Rationale
Verified at `task_service.rs:174-196`: after the task is already written as `Claimed` with `lease_id` set (lines 166-172), two `append_journal_event` calls follow. If either fails, the `?` propagates the error up to `process_cycle` (daemon_loop.rs:134-136), which also propagates via `?`, causing `run()` to return `Err` and terminate the entire daemon. The task remains on disk in `Claimed` state with an active lease. While reconciliation would eventually clean up the stale lease after TTL, a transient journal write failure for a single task should not terminate the daemon. The proposal to treat claim failures as per-task failures and continue scanning is a genuine robustness improvement.

## Amendment: AM-003

### Position
ACCEPT

### Rationale
Verified at `lease_service.rs:167-169`: `Self::release()` errors are discarded with `let _ =`, and `clear_lease_reference` errors are also discarded, but `lease.lease_id` is unconditionally pushed to `released_lease_ids` on line 169 regardless of whether release succeeded. This means the `ReconcileReport` can report a lease as released when the release actually failed (worktree not removed, writer lock not freed). Similarly at `daemon_loop.rs:1049-1050`, shutdown cleanup silently ignores all errors. The report inaccuracy is a genuine correctness gap — operators relying on the report would believe cleanup succeeded when it didn't. The fix is straightforward: only add to `released_lease_ids` if release succeeds.

## Amendment: AM-004

### Position
ACCEPT

### Rationale
Verified: `.ralph/tmp/logs/` files are tracked in git (the git status shows them as `M` — modified tracked files — and they appear in `git diff HEAD~5..HEAD`). The `.gitignore` at project root has no `.ralph/tmp` exclusion rule. Runtime log files (`ralph-burning-rewrite-017-planner.log`, `ralph-resume-016.log`, etc.) are ephemeral execution artifacts that should not be version-controlled. Adding `.ralph/tmp/` to `.gitignore` and removing the tracked log files is a genuine housekeeping fix that prevents noise in diffs and accidental commits of runtime state.
