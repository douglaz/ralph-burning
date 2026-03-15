---
artifact: final-review-votes
loop: 4
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T20:31:27Z
---

I’m verifying the cited code paths against the repository before voting. I’ll check the daemon worktree path, the prompt/journal filtering path, and the subprocess I/O ordering, then I’ll return votes in the requested format.
The daemon and process-backend claims are confirmed in code. I’m checking the prompt-enrichment journal path now to see whether rollback visibility is actually bypassed.
# Vote Results

## Amendment: PBACK-REVIEW-001

### Vote
ACCEPT

### Rationale
The planner’s position is correct. The daemon path passes the leased worktree into [`daemon_loop.rs:913`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L913), then immediately drops it as `_worktree_path` in [`daemon_loop.rs:955`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L955). Execution continues with `base_dir` in the engine entrypoints at [`daemon_loop.rs:968`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L968) and [`daemon_loop.rs:987`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L987). The engine sets `InvocationRequest.working_dir` from `base_dir` in [`engine.rs:1802`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1802), and the subprocess adapter uses that as `current_dir`. That means daemon-launched agents run in the shared checkout, not the leased worktree. The proposed fix matches the actual failure mode.

## Amendment: PBACK-REVIEW-002

### Vote
ACCEPT

### Rationale
The planner is right that rollback boundaries are being ignored. `build_stage_prompt()` pulls prior outputs through `load_prior_stage_outputs_this_cycle()` in [`engine.rs:77`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L77), and that helper reads raw `journal.ndjson` directly in [`engine.rs:2483`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2483) and iterates matching `stage_completed` events without visibility filtering. The codebase already has [`queries.rs:139`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/project_run_record/queries.rs#L139) to trim abandoned history after rollback, but this path does not use it. So discarded branch outputs can leak into later prompts. Filtering through visible journal state and adding a rollback regression test is the correct amendment.

## Amendment: PBACK-REVIEW-003

### Vote
ACCEPT

### Rationale
The planner’s acceptance is justified. In [`process_backend.rs:136`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L136), `spawn_and_wait()` writes the full stdin payload before it starts draining stdout and stderr at [`process_backend.rs:141`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L141) and [`process_backend.rs:145`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L145). That ordering can deadlock if the child emits enough output before consuming stdin. The existing test in [`process_backend_test.rs:907`](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L907) already documents the blocking stdin window. The amendment also correctly notes that several read/write errors are currently discarded. Concurrent stream handling with surfaced transport failures is the right change.
