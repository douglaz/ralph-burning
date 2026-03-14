# Final Review Amendments Applied

## Round 1

### Amendment: PBACK-REVIEW-001

### Problem
The daemon workflow path is still invoking agents from the shared repo root instead of the leased worktree. `DaemonLoop` creates and rebases a per-task worktree ([daemon_loop.rs:397](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:397), [daemon_loop.rs:414](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:414)), passes `lease.worktree_path` into the dispatch future ([daemon_loop.rs:913](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:913)), then drops it by naming the parameter `_worktree_path` ([daemon_loop.rs:955](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:955)) and calling the engine with `base_dir` ([daemon_loop.rs:968](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:968), [daemon_loop.rs:987](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:987)). The engine then hardcodes `InvocationRequest.working_dir` to that `base_dir` ([engine.rs:1802](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:1802)). Daemon-dispatched subprocess runs will therefore edit the main checkout, not the isolated worktree.

### Proposed Change
Thread a separate execution working directory through the workflow engine and set it to `lease.worktree_path` for daemon-dispatched runs, while keeping `base_dir` for project metadata, journal, and persistence paths.

### Affected Files
- [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) - pass the leased worktree path into workflow execution instead of discarding it.
- [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - accept an execution cwd distinct from the metadata base dir and use it for `InvocationRequest.working_dir`.

### Reviewer
codex

### Amendment: PBACK-REVIEW-002

### Problem
Prompt enrichment ignores rollback boundaries. `build_stage_prompt()` loads prior outputs via `load_prior_stage_outputs_this_cycle()` ([engine.rs:77](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:77)), and that helper reads the raw append-only `journal.ndjson` ([engine.rs:2483](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:2483)) and includes every matching `stage_completed` event ([engine.rs:2498](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:2498)). The codebase already has `visible_journal_events()` to hide rolled-back history ([queries.rs:139](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/project_run_record/queries.rs:139)), but this path does not use it. After a rollback or completion-round restart, discarded branch outputs will still be injected into later prompts.

### Proposed Change
Apply `visible_journal_events()` before selecting prior `stage_completed` events, then preserve order from the visible branch only. Add a regression test covering a rollback branch.

### Affected Files
- [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - filter prior outputs from the visible journal branch instead of the raw append-only journal.
- [prompt_builder_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/prompt_builder_test.rs) - add a rollback-aware prompt-builder test.

### Reviewer
codex

### Amendment: PBACK-REVIEW-003

### Problem
`ProcessBackendAdapter::spawn_and_wait()` can deadlock because it writes the full stdin payload before it starts draining stdout/stderr. The blocking write happens at [process_backend.rs:136](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:136), while stdout/stderr are not taken and read until [process_backend.rs:141](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:141) and [process_backend.rs:145](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:145). If `claude` or `codex` emits enough output before consuming stdin, the child can fill its pipe and block while the parent is still stuck in `write_all()`, leaving the invocation hung until the outer timeout.

### Proposed Change
Start draining stdout/stderr immediately after spawn and perform stdin writing concurrently, with read/write failures surfaced as transport failures instead of being ignored.

### Affected Files
- [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - restructure subprocess I/O so stdin/stdout/stderr are handled concurrently.
- [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add a regression test with a fake backend that writes a large stderr/stdout payload before reading stdin.

### Reviewer
codex


## Round 2

### Amendment: PBA-REV-001

### Problem
The Codex resume argv is not compatible with the installed CLI. In [src/adapters/process_backend.rs:365](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L365) and [src/adapters/process_backend.rs:369](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L369), the adapter builds `codex exec resume ... --output-schema ... --output-last-message ...`. The local `codex` binary rejects that shape: `codex exec resume --output-schema /tmp/schema.json` returns `unexpected argument '--output-schema'`. Any workflow stage that tries to reuse a Codex session will fail before the subprocess starts.

### Proposed Change
Build separate Codex argv layouts for new-session and resume flows. Keep `--output-schema` on `codex exec ...`, but remove it from `codex exec resume ...` and rely on prompt/schema validation after reading the last-message file. Add a test that rejects unsupported resume-only flags so this cannot regress.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - split Codex new-session vs resume command construction.

### Reviewer
codex

### Amendment: PBA-REV-002

### Problem
Timeout/cancellation does not retain a reapable child handle. The adapter stores only bare PIDs in [src/adapters/process_backend.rs:23](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L23) and [src/adapters/process_backend.rs:128](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L128), and `cancel()` only sends `kill -TERM` to that PID in [src/adapters/process_backend.rs:544](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L544). At the service layer, timeout/cancellation returns immediately after calling `cancel()` in [src/contexts/agent_execution/service.rs:196](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196). That drops the future owning `tokio::process::Child`, so there is no remaining wait/reap path for the subprocess. In daemon mode, timed-out or cancelled backends can be left running or become zombies, and the bare-PID approach also leaves a PID-reuse hazard.

### Proposed Change
Track the actual child handle, not just the PID, and make cancellation perform signal + reap before removing the entry from the active-child map. If you need to keep the current service shape, spawn a dedicated cleanup task that `wait()`s the child after cancellation. `kill_on_drop(true)` is also worth enabling as a safety net.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - store child handles and reap them on cancel/timeout.
- [src/contexts/agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs) - keep cancellation wired to a reap-aware adapter path if needed.

`nix develop -c cargo build` and `nix develop -c cargo test` both pass. The first amendment was reproduced directly against the installed `codex` CLI, so it is a real runtime mismatch rather than a test-only concern.

### Reviewer
codex


## Round 3

### Amendment: PB-CANCEL-TIMEOUT-HANG

### Problem
`ProcessBackendAdapter::cancel()` sends `SIGTERM` and then blocks on `wait()` in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L614) and [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L629). That `wait()` is an unbounded poll loop in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L58). `AgentExecutionService` calls `cancel()` on both timeout and explicit cancellation before returning to the caller in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196). If a child traps or ignores `SIGTERM`, the timeout/cancel path never completes, so the “authoritative” timeout is no longer authoritative.

### Proposed Change
Make `cancel()` best-effort and non-blocking: remove the child from the map, send `SIGTERM`, and return immediately, or at most use a short bounded grace period with a forced cleanup path. Add a regression test with a fake backend that ignores `SIGTERM` and verify `InvocationTimeout`/`InvocationCancelled` still returns promptly.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - stop waiting indefinitely inside `cancel()`.
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add a TERM-ignoring child test.

### Reviewer
codex

### Amendment: WF-INVOCATION-ID-COLLISION

### Problem
Workflow invocation IDs are built from only `run_id`, `stage`, `cycle`, and `attempt` in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1956). Completion-round restarts reuse the same `cycle` and often reset `attempt` back to `1`, so the same stage in round 2 gets the same `invocation_id` as round 1. That collides with raw-output persistence in [fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L724) and with Codex temp-file naming in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L433). The result is overwritten backend evidence across completion rounds, and stale temp files can alias later invocations if an earlier run exited before cleanup.

### Proposed Change
Include `completion_round` in `InvocationRequest.invocation_id` for workflow stages, e.g. `...-c{cycle}-a{attempt}-cr{completion_round}`. Add coverage proving backend raw outputs and temp artifacts stay distinct across completion rounds.

### Affected Files
- [src/contexts/workflow_composition/engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - make workflow invocation IDs round-aware.
- [tests/unit/workflow_engine_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/workflow_engine_test.rs) - add coverage for unique backend artifact IDs across completion rounds.
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add temp-file/raw-output collision coverage.

Targeted verification I ran: `nix develop -c cargo test process_backend_test` and `nix develop -c cargo test prompt_builder_test`. Both passed, so these are edge-case/spec regressions that current tests do not catch.

### Reviewer
codex

