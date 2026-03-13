---
artifact: final-review-proposals
loop: 17
project: ralph-burning-rewrite
backend: codex
role: final_reviewer
created_at: 2026-03-13T11:35:59Z
---

I’m starting by identifying the correct `base` ref and then reading `git diff <base>...HEAD -- . ':(exclude).ralph'` to scope the review to actual code changes. After that I’ll inspect concurrency, persistence, and test coverage hotspots in the modified files.
I’ve identified the main high-risk area (daemon/workflow orchestration, state persistence, and task scheduling), so I’m now reading those Rust modules directly and then I’ll cross-check the state-transition and failure-path handling in the project/run record layer.
I’ve captured the main orchestration flow and there are some candidate edge cases already (notably task retries/state transitions and lease cleanup). Next I’m reading the daemon models and adapter boundaries to confirm invariants like one writer lock per project and one active task/lease semantics.
I found several meaningful points in daemon/task handling and file-store locking. I’m now checking failure-state transitions and project/run services to see whether errors are persisted consistently and whether retries/state checks are correct.
I’ve now got the full engine path through stage execution, commit, and failover states. I’m going to inspect retry-policy/cursor/domain types next, because those are often where subtle invariants break across resumes and retries.
I’m now checking workspace governance/version handling and conformance/test command paths because regressions there can mask correctness issues that still pass targeted unit tests.
I’ll now do the mandatory `git diff origin/master...HEAD -- . ':(exclude).ralph'`, then inspect the highest-risk areas for concurrency, error persistence, state transitions, and test correctness. I’ll anchor each finding to exact files/line numbers for concrete amendments.
I’ve confirmed the changed surface is large and mostly in the rewrite module. Next I’m scanning the remaining high-risk modules (daemon runtime orchestration, persistence, rollback/requirements paths, and CLI wiring) for concrete, actionable defects that affect correctness and safety.
I’m now checking orchestration/error paths for panic handling, retries, and cleanup semantics in conformance/runtime tests to ensure invariants hold under failure, then I’ll validate CLI command behavior and key adapters.
I’ve identified likely high-risk runtime and persistence areas; now I’m checking project/daemon/routing command flows and state-query implementations for mismatches with spec invariants (especially "active run must resume or fail", "one writer lock per project", alias handling, and cleanup semantics).
I’m now checking domain types and error enums for hidden invariant mismatches (especially state transitions, IDs, writer lock semantics, and CLI-facing failure classes), then I’ll cross-check with tests for assertion coverage.
I’m starting by capturing the exact change set with the required `git diff <base>...HEAD -- . ':(exclude).ralph'`, then I’ll inspect the modified runtime/concurrency and error-handling paths plus tests for proof gaps before drafting any amendments.
I’ve got the full change set; next I’m narrowing to high-risk areas (concurrency/task isolation, state transitions, journaling guarantees, and test assertions) and will flag only concrete, actionable defects.
I’m now auditing daemon/task concurrency paths for isolation/synchronization issues, then I’ll pivot to workspace/run state transitions and journaling edge-case handling.
I’ve found large daemon flow paths; I’ll now inspect routing/resolution and state services for subtle precedence/order and isolation defects that are easy to miss.
I’ve identified a potentially important area: worktree path derivation and shared resource use. I’m checking adapters next to confirm isolation and cleanup semantics for concurrent tasks.
I’m now reviewing `workflow_composition::engine` and `workflow_composition::contracts` because these are where cursor/state corruption, boundary validation order, and journal/snapshot rollback behavior usually fail first.
I found the core execution engine and will read the tail of these helpers to spot any resume/rollback state leak or sequence mismatch issues that unit tests might miss.
I’m switching to agent invocation flow next (adapter service/session/backend selection), since policy/security bugs often appear there (timeout, cancellation, session reuse, structured output handling).
I’ll capture the rest of `requirements` service and then cross-check CLI handlers for command-level edge cases, especially completion, routing, and exit semantics.
Next I’m checking CLI entrypoints for argument validation and state transitions because these can hide correctness bugs even when engine internals are safe.
I’m validating the current diff for concrete correctness and safety risks, then I’ll focus on concurrency/isolation, failure-state transitions, and any test or spec gaps. I’ll report only high-signal amendments with exact file/line references.
I found one likely high-risk area around process-wide `cwd` mutation in daemon dispatch; next I’m drilling into related runtime modules to confirm exact failure cases and whether they violate worker isolation and cleanup guarantees.
I confirmed the `set_current_dir` usage and now checking lease handling and worktree adapter behavior to see whether isolation and one-writer-lock guarantees hold under concurrency or shutdown paths.
I’ve confirmed routing and task lifecycle semantics. Next I’m verifying whether panic/error classes and state transitions cover all required failure modes and whether any error paths leave partially mutated state without durable state update.
I’ve identified one hard correctness risk; now I’m reviewing adapter-level persistence and CLI/diff semantics to catch any additional issues that could break safety or invariants.
I’m now verifying one more critical area: whether failure-class transitions and session policy are correct and whether any path skips structured validation or durable state sequencing.
I’ll follow the requested review flow: first inspect the source diff against `origin/master`, then read the highest-risk changed files (daemon/worktree/workflow/state persistence/routing/tests) for concurrency, error-path, and invariant issues, and report concrete amendment items only.
I’ve identified the changed surface. Next I’m checking workspace cleanliness and then focusing on likely high-risk areas: daemon/worktree isolation, task/lease state transitions, journal/error atomicity, and test intent mismatches.
I confirmed where the daemon does claim/rebase/project lifecycle and where potential race hazards sit. I’ll read the remainder of this file for worktree dispatch and cleanup edges, then inspect task/lease services for state-machine consistency.
I’m continuing into the final part of the daemon loop for lease/task failure ordering, then I’ll inspect task, lease, and worktree implementations to validate one-lock/one-lease behavior and cleanup ordering.
I’ve confirmed daemon worktree creation per task and the dispatch path. I’ll now validate how journal/appends and payload writes are handled under rollback and failure paths in the workflow and requirements engines, plus CLI command semantics around `resume`, `rollback`, and `tail --logs`.
I’ll inspect the rollback execution path and cancellation/hard rollback cleanup for subtle ordering issues, since this area directly affects safety and data consistency.
I’m now inspecting completion/rollback transition sections and the rollback command handler to verify one-worktree-per-task and reset ordering; then I’ll inspect CLI parsing paths.
I need remainder of perform_rollback and related query maybe to inspect bug.
I’ve now isolated the rollback handler; I’ll inspect journal validation, adapter store implementations, and daemon CLI parsing next, focusing on malformed-state handling and potential silent-fail paths.
I’ll pull in the tail of `fs.rs` to inspect runtime log writing, lease list/delete, and daemon store implementation for any missing failure handling in state transitions and file cleanup ordering.
I found no immediate obvious errors in routing model, so next I’ll inspect CLI for daemon commands, task creation from external APIs, and requirement of explicit failure handling, then cross-check against conformance scenarios.
I’ve finished reading daemon/adapter paths and will now inspect domain validation for run status, stage cursor, and invariants to ensure failures are not bypassing `validate()` and that sequence transitions are monotonic as intended.
Starting by re-reading the key changed modules to confirm exact line-level behavior in the required diff and finalize high-signal amendments with concrete references.
I’ve confirmed stray workspace artifacts are untracked/modified outside source control, and next I’m pulling line-accurate snippets around concurrency-critical paths to validate the highest-risk issues before drafting amendments.
I’ve now pinpointed the highest-risk concurrency and cleanup paths. I’m next validating rollback/state transitions around error handling in the project record service for any silent or inconsistent failure states.
I found two likely issues to verify next in daemon task lifecycle cleanup: one in panic/unsafe directory state and one where cleanup failures are intentionally dropped in non-terminal branches. I’m reading the main loop/error handling so we can confirm impact.
I validated another important gap: there is potential hard-to-recover “claimed-without-complete-dispatch” state if task claiming writes fail mid-path. I’m checking where lease/file cleanup is invoked and whether this state is ever reclaimed.
# Final Review: AMENDMENTS

## Amendment: AM-001

### Problem
`DaemonLoop::dispatch_in_worktree` mutates process-wide working directory before invoking engine work and restores it only on the normal return path ([`daemon_loop.rs:920-989`](</root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:920>)).  
This is process-global mutable state and is not true per-worker isolation; concurrent workers (or future async/task parallelism) would race on cwd and can interfere with each other’s file/git operations.

### Proposed Change
Remove global `set_current_dir` usage and pass `worktree_path` as an explicit execution root for all backend/worktree/git-facing commands.  
If any fallback to cwd mutation remains, wrap it in a strict RAII guard and `catch_unwind` boundary so process state is restored deterministically even on panic. This also ensures cancellation/retry paths do not depend on ambient global cwd.

### Affected Files
- [/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs:920)

## Amendment: AM-002

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

## Amendment: AM-003

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

## Amendment: AM-004

### Problem
The workspace contains stray runtime artifacts outside source control (`.ralph/tmp/logs/...` and untracked loop completion files), which risks leaking execution artifacts and noise into repository state and can be mistaken as project changes.

### Proposed Change
Add explicit ignore rules for generated `.ralph` runtime artifacts (`.ralph/tmp`, per-run logs) at repository root, and clean residual tracked runtime files before finalizing release.
At minimum, remove the current untracked/generated files now and prevent future accumulation.

### Affected Files
- [/root/new-ralph-burning/.gitignore](/root/new-ralph-burning/.gitignore:1)
- [/root/new-ralph-burning/.ralph/tmp/logs/ralph-resume-016.log](/root/new-ralph-burning/.ralph/tmp/logs/ralph-resume-016.log)
