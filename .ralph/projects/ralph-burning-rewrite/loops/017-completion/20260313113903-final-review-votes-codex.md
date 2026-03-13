---
artifact: final-review-votes
loop: 17
project: ralph-burning-rewrite
backend: codex
role: final_reviewer
created_at: 2026-03-13T11:39:03Z
---

# Vote Results

## Amendment: CLI-RUN-WRITER-LOCK

### Vote
ACCEPT

### Rationale
This addresses a real atomicity gap in the CLI paths: `handle_start()`/`handle_resume()` currently mutate run state without a writer lock, unlike the daemon path. Requiring a lock around engine execution is consistent with the stated one-writer invariant and prevents concurrent run starts/resumes from creating conflicting active runs.

## Amendment: DAEMON-PROCESS-GLOBAL-CWD

### Vote
ACCEPT

### Rationale
Mutating process-global CWD in a multi-threaded Tokio runtime is unsound and can race across tasks; panic/unwinding can also leave state unrecovered. The engine already receives absolute root context, so removing global `set_current_dir` is correct and lowers runtime-wide side effects.

## Amendment: STRAY-SCHEDULED-TASKS-LOCK

### Vote
ACCEPT

### Rationale
A session-specific lock file has no place in version control and can carry stale PID/session metadata. Removing it from tracking and optionally ignoring it is a straightforward correctness and hygiene fix.

## Amendment: AM-001

### Vote
REJECT

### Rationale
This is effectively a duplicate of **DAEMON-PROCESS-GLOBAL-CWD** describing the same defect and same target location with overlapping fix intent. It should be merged into that amendment rather than accepted as a separate, independent change.

## Amendment: AM-002

### Vote
ACCEPT

### Rationale
Propagating claim errors as fatal aborts for the whole daemon is too strong for single-task failures, and partial claim persistence can leave tasks in an inconsistent claimed state. Treating journal append failures as per-task failures with rollback/marking is a robust recovery improvement and prevents whole-run shutdowns.

## Amendment: AM-003

### Vote
ACCEPT

### Rationale
Silently swallowing lease cleanup errors while reporting success is a correctness and observability bug. Reporting only truly released leases and surfacing cleanup failures improves operator trust and avoids hidden leaked lease/worktree states.

## Amendment: AM-004

### Vote
ACCEPT

### Rationale
Runtime artifacts under `.ralph/tmp/` are generated and should not be committed or tracked. Ignoring them at repo level and cleaning residual tracked artifacts is valid repo hygiene with low risk and high long-term maintainability benefit.
