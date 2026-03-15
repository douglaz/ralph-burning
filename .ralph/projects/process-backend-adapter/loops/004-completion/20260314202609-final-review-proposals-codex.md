---
artifact: final-review-proposals
loop: 4
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T20:26:09Z
---

Reviewing the diff against `ebc30fb702f2716fc64077440b54b373e7c29ec9` first, then I’ll trace the key adapter, workflow, and CLI files end-to-end to look for correctness and regression risks.
I have the full diff. Next I’m reading the adapter, workflow engine, and CLI wiring files end-to-end so I can trace actual behavior rather than diff fragments.
The adapter and engine entrypoints are loaded. I’m tracing the helper functions now, especially prompt construction, cancellation, and Codex temp-file handling, because that’s where most spec and robustness regressions would hide.
I found one area outside the adapter itself that looks worth tracing carefully: the daemon dispatch path passes a `worktree_path`, but the current implementation may not actually use it when invoking the backend. I’m checking whether that means daemon runs execute in the wrong repository directory.
I’ve got the main code-path traces. I’m running a small targeted test pass now to make sure there isn’t a simple contradiction between the source-level findings and the existing suite.
# Final Review: AMENDMENTS

## Amendment: PBACK-REVIEW-001 [P0]

### Problem
The daemon workflow path is still invoking agents from the shared repo root instead of the leased worktree. `DaemonLoop` creates and rebases a per-task worktree ([daemon_loop.rs:397](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:397), [daemon_loop.rs:414](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:414)), passes `lease.worktree_path` into the dispatch future ([daemon_loop.rs:913](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:913)), then drops it by naming the parameter `_worktree_path` ([daemon_loop.rs:955](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:955)) and calling the engine with `base_dir` ([daemon_loop.rs:968](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:968), [daemon_loop.rs:987](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:987)). The engine then hardcodes `InvocationRequest.working_dir` to that `base_dir` ([engine.rs:1802](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:1802)). Daemon-dispatched subprocess runs will therefore edit the main checkout, not the isolated worktree.

### Proposed Change
Thread a separate execution working directory through the workflow engine and set it to `lease.worktree_path` for daemon-dispatched runs, while keeping `base_dir` for project metadata, journal, and persistence paths.

### Affected Files
- [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) - pass the leased worktree path into workflow execution instead of discarding it.
- [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - accept an execution cwd distinct from the metadata base dir and use it for `InvocationRequest.working_dir`.

## Amendment: PBACK-REVIEW-002 [P1]

### Problem
Prompt enrichment ignores rollback boundaries. `build_stage_prompt()` loads prior outputs via `load_prior_stage_outputs_this_cycle()` ([engine.rs:77](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:77)), and that helper reads the raw append-only `journal.ndjson` ([engine.rs:2483](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:2483)) and includes every matching `stage_completed` event ([engine.rs:2498](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs:2498)). The codebase already has `visible_journal_events()` to hide rolled-back history ([queries.rs:139](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/project_run_record/queries.rs:139)), but this path does not use it. After a rollback or completion-round restart, discarded branch outputs will still be injected into later prompts.

### Proposed Change
Apply `visible_journal_events()` before selecting prior `stage_completed` events, then preserve order from the visible branch only. Add a regression test covering a rollback branch.

### Affected Files
- [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - filter prior outputs from the visible journal branch instead of the raw append-only journal.
- [prompt_builder_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/prompt_builder_test.rs) - add a rollback-aware prompt-builder test.

## Amendment: PBACK-REVIEW-003 [P2]

### Problem
`ProcessBackendAdapter::spawn_and_wait()` can deadlock because it writes the full stdin payload before it starts draining stdout/stderr. The blocking write happens at [process_backend.rs:136](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:136), while stdout/stderr are not taken and read until [process_backend.rs:141](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:141) and [process_backend.rs:145](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs:145). If `claude` or `codex` emits enough output before consuming stdin, the child can fill its pipe and block while the parent is still stuck in `write_all()`, leaving the invocation hung until the outer timeout.

### Proposed Change
Start draining stdout/stderr immediately after spawn and perform stdin writing concurrently, with read/write failures surfaced as transport failures instead of being ignored.

### Affected Files
- [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - restructure subprocess I/O so stdin/stdout/stderr are handled concurrently.
- [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add a regression test with a fake backend that writes a large stderr/stdout payload before reading stdin.
