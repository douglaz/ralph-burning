---
artifact: final-review-votes
loop: 17
project: ralph-burning-rewrite
backend: claude
role: final_reviewer
created_at: 2026-03-13T11:38:59Z
---

# Vote Results

## Amendment: CLI-RUN-WRITER-LOCK

### Vote
ACCEPT

### Rationale
Confirmed by reading `src/cli/run.rs`. Both `handle_start()` (line 102-180) and `handle_resume()` (line 182-254) call the engine directly with no writer lock acquisition. The status check at lines 121-144 is a non-atomic read-then-act pattern — two concurrent CLI invocations can both pass the `NotStarted` check and enter `engine::execute_run()` simultaneously. The engine has no internal locking (it trusts the caller). The daemon path correctly acquires via `LeaseService::acquire()`, but the CLI path entirely bypasses this contract. This is a genuine concurrency defect that can corrupt `run.json` and `journal.ndjson`. The proposed fix — acquiring the same writer lock before the engine call, ideally with an RAII guard — is straightforward and correct.

## Amendment: DAEMON-PROCESS-GLOBAL-CWD

### Vote
ACCEPT

### Rationale
Verified at `daemon_loop.rs:930-931`: `std::env::set_current_dir(worktree_path)` mutates process-global state in a `rt-multi-thread` tokio runtime. The restoration at line 983 is only reached on the normal return path — an engine panic would leave the CWD permanently mutated. Furthermore, the call appears unnecessary: the engine receives `base_dir` as an explicit absolute path (line 943), and `WorktreeAdapter` uses `Command::new().current_dir()` for subprocess invocations. Removing the `set_current_dir` calls is a clean, low-risk fix for a genuine unsoundness issue.

## Amendment: STRAY-SCHEDULED-TASKS-LOCK

### Vote
ACCEPT

### Rationale
Confirmed `.claude/scheduled_tasks.lock` is tracked in git and contains ephemeral session state (`sessionId`, `pid`, `acquiredAt`). This is a transient lock file specific to a single development session. It should be removed from tracked files and ideally added to `.gitignore`. No controversy here.

## Amendment: AM-001

### Vote
ACCEPT

### Rationale
This identifies the same defect as DAEMON-PROCESS-GLOBAL-CWD at the same code location (`daemon_loop.rs:920-989`). The analysis is correct: `set_current_dir` is process-global mutable state in a multi-threaded async runtime with no panic safety. The proposed fix (pass `worktree_path` explicitly, use RAII guard as fallback) is sound. While this is a duplicate of DAEMON-PROCESS-GLOBAL-CWD, the underlying issue is real and accepting both is harmless — they converge on the same fix.

## Amendment: AM-002

### Vote
ACCEPT

### Rationale
Verified at `task_service.rs:174-196`: after the task is written as `Claimed` with a lease attached (lines 166-172), two `append_journal_event` calls follow with `?` propagation. If either fails, the error propagates through `process_cycle` (daemon_loop.rs:134-136, which also uses `?`), causing `run()` to return `Err` and terminate the entire daemon loop. The task remains on disk in `Claimed` state with an active lease. A transient journal write failure for a single task should not be fatal to the entire daemon. The proposal to treat claim failures as per-task errors (fail the individual task, continue scanning) is a proportionate and correct robustness improvement.

## Amendment: AM-003

### Vote
ACCEPT

### Rationale
Verified at `lease_service.rs:167-169`: both `Self::release()` and `clear_lease_reference()` errors are silently discarded with `let _ =`, yet `lease.lease_id` is unconditionally pushed to `released_lease_ids` on line 169. This means `ReconcileReport` can claim a lease was released when the actual cleanup (worktree removal, lock release) failed. Similarly at `daemon_loop.rs:1049-1050`, shutdown cleanup silently ignores all errors from `mark_aborted` and `release_task_lease`. The fix is clear: only add to `released_lease_ids` if release actually succeeds. This is a genuine correctness gap in operational reporting that could mislead operators.

## Amendment: AM-004

### Vote
ACCEPT

### Rationale
The git status confirms `.ralph/tmp/logs/` files are tracked (showing as modified). These are ephemeral runtime logs that should not be version-controlled — they add noise to diffs and risk accidental commits of execution state. Adding `.ralph/tmp/` to `.gitignore` and removing the tracked files is basic housekeeping. The only consideration is whether this should be bundled with the current change set or handled as a separate cleanup commit, but the amendment itself is clearly correct.
