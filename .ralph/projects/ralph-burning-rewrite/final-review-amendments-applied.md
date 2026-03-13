# Final Review Amendments Applied

## Round 1

### Amendment: AM-002

### Problem
`claim_task` can leave a task permanently stuck in a claimed-with-lease state on partial write failure: `append_journal_event` for `TaskClaimed`/`LeaseAcquired` is not cleaned up on error, and the caller propagates the failure to the run loop (`process_cycle` uses `?`) which aborts the daemon run ([`task_service.rs:174-197`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/task_service.rs:174), [`daemon_loop.rs:134-136`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:134), [`daemon_loop.rs:390-403`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:390)).

### Proposed Change
In the claim path, make journal-write failures rollback-atomic:
- on any journal append failure, clear `lease_id` on the task and either:
  - transition to `Failed` with explicit class (e.g., `claim_journal_failed`), or
  - revert task to `Pending` and release the lease cleanly.
- do not let one task claim failure terminate the daemon; treat claim failures as per-task failures and continue scanning subsequent tasks.

### Affected Files
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/task_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/task_service.rs:174)
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:134)
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:390)

### Reviewer
codex

### Amendment: AM-003

### Problem
Error paths in lease cleanup are silently discarded and can mask incomplete cleanup state.  
`LeaseService::reconcile` and daemon shutdown cleanup ignore `release`/`clear_lease_reference` failures but still record success in reports / proceed, which can hide leaked worktrees and incomplete lease tear-down ([`lease_service.rs:155-169`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:155), [`daemon_loop.rs:1046-1051`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:1046)).

### Proposed Change
Propagate cleanup failures with severity:
- make `reconcile` return/report per-lease cleanup failures explicitly,
- do not add a lease to `released_lease_ids` unless cleanup fully succeeds,
- keep best-effort logging only for telemetry and emit a non-run-fatal failure summary so operators can reconcile.

### Affected Files
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:155)
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:1046)

### Reviewer
codex

### Amendment: AM-004

### Problem
The workspace contains stray runtime artifacts outside source control (`.ralph/tmp/logs/...` and untracked loop completion files), which risks leaking execution artifacts and noise into repository state and can be mistaken as project changes.

### Proposed Change
Add explicit ignore rules for generated `.ralph` runtime artifacts (`.ralph/tmp`, per-run logs) at repository root, and clean residual tracked runtime files before finalizing release.
At minimum, remove the current untracked/generated files now and prevent future accumulation.

### Affected Files
- [/root/new-ralph-burning/.gitignore](/root/new-ralph-burning/.gitignore:1)
- [/root/new-ralph-burning/.ralph/tmp/logs/ralph-resume-016.log](/root/new-ralph-burning/.ralph/tmp/logs/ralph-resume-016.log)

### Reviewer
codex

### Amendment: CLI-RUN-WRITER-LOCK

### Problem

The spec's core invariant §7 states: **"One writer lock per project."** The daemon path correctly acquires a writer lock via `LeaseService::acquire()` (`src/contexts/automation_runtime/lease_service.rs:45`) before any state mutations. However, the CLI `run start` and `run resume` paths (`src/cli/run.rs:102-180`, `182-254`) call `engine::execute_run()` / `engine::resume_run()` directly **without acquiring a writer lock**.

This means:
- Two concurrent `ralph-burning run start` invocations for the same project can both pass the `NotStarted` status check at `engine.rs:267-290` and create competing active runs, corrupting `run.json` and `journal.ndjson`.
- A CLI `run start` and a daemon dispatch for the same project can run simultaneously with no mutual exclusion.

The engine itself (`src/contexts/workflow_composition/engine.rs`) has no lock acquisition — it delegates that responsibility to the caller. The daemon caller fulfills this contract; the CLI caller does not.

### Proposed Change

Add writer lock acquisition and release around the engine call in both `handle_start()` and `handle_resume()` in `src/cli/run.rs`. The lock should use the same `acquire_writer_lock` / `release_writer_lock` mechanism from `FsDaemonStore` (or factor it into a shared utility), with a guard that releases on drop or error.

Sketch:
```rust
// In handle_start() and handle_resume(), before engine call:
let lock_store = FsDaemonStore;
let lease_id = format!("cli-{}", uuid::Uuid::new_v4());
lock_store.acquire_writer_lock(&current_dir, &project_id, &lease_id)?;

let result = engine::execute_run(...).await;

lock_store.release_writer_lock(&current_dir, &project_id)?;
result?;
```

Ideally, wrap this in an RAII guard to ensure release on panic/error.

### Affected Files
- `src/cli/run.rs` - add writer lock acquire/release around `engine::execute_run()` and `engine::resume_run()` calls in `handle_start()` and `handle_resume()`
- Optionally factor `acquire_writer_lock`/`release_writer_lock` out of `DaemonStorePort` into a shared `ProjectLockPort` trait so CLI can use it without depending on daemon-specific types

---

### Reviewer
claude

### Amendment: DAEMON-PROCESS-GLOBAL-CWD

### Problem

`dispatch_in_worktree()` (`src/contexts/automation_runtime/daemon_loop.rs:930-989`) calls `std::env::set_current_dir(worktree_path)` to change the process-global working directory before invoking the engine, then resets it afterward.

`std::env::set_current_dir` mutates **process-global** state. The tokio runtime is configured with `rt-multi-thread` (`Cargo.toml`), meaning:
1. The signal handler spawned at line 123 runs on a potentially different thread and could observe the mutated CWD.
2. If the engine panics (not caught here), the CWD is never restored.
3. Any future change that introduces concurrent task processing or additional `tokio::spawn` calls will silently inherit the wrong CWD.

Furthermore, this call appears unnecessary: every file operation in the engine uses the explicit `base_dir` parameter with absolute paths, and git operations in `WorktreeAdapter` use `Command::new().current_dir()` explicitly. No code path in the current implementation relies on process CWD.

### Proposed Change

Remove the `std::env::set_current_dir()` calls from `dispatch_in_worktree()`. If a future real backend adapter needs to execute in the worktree directory, pass `worktree_path` as an explicit parameter to the agent execution adapter (e.g., as a field on `InvocationRequest`) rather than mutating process-global state.

```rust
async fn dispatch_in_worktree(
    &self,
    base_dir: &Path,
    project_id: &ProjectId,
    flow: FlowPreset,
    run_status: RunStatus,
    effective_config: &EffectiveConfig,
    worktree_path: &Path,
    cancellation_token: CancellationToken,
) -> AppResult<()> {
    // Remove: let original_dir = std::env::current_dir()?;
    // Remove: std::env::set_current_dir(worktree_path)?;

    let result = match run_status {
        // ... same as before, engine uses base_dir (absolute) ...
    };

    // Remove: let reset_result = std::env::set_current_dir(original_dir);
    result
}
```

### Affected Files
- `src/contexts/automation_runtime/daemon_loop.rs` - remove `set_current_dir` calls in `dispatch_in_worktree()` (lines 930-931 and 983-989)

---

### Reviewer
claude

### Amendment: STRAY-SCHEDULED-TASKS-LOCK

### Problem

The diff includes `.claude/scheduled_tasks.lock`, a transient session lock file from the Claude Code tool environment. This file contains a specific session ID and PID and should not be committed to the repository.

### Proposed Change

Remove `.claude/scheduled_tasks.lock` from the committed changes (e.g., `git rm --cached .claude/scheduled_tasks.lock`). Optionally add it to `.gitignore`.

### Affected Files
- `.claude/scheduled_tasks.lock` - remove from tracked files

---

### Reviewer
claude

