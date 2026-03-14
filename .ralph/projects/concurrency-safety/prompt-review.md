---
artifact: prompt-review
project: concurrency-safety
backend: codex
role: prompt_reviewer
created_at: 2026-03-14T02:47:30Z
---

I’m reviewing the prompt against the codebase so I can call out concrete ambiguities instead of generic wording issues. I’ll inspect the referenced files, existing locking/lease code, and current test surfaces before rewriting the prompt.
I found the relevant code and there are already some tests around both areas, which is useful because it exposes where the original prompt is underspecified versus what the code already supports. Next I’m reading the lock acquisition/reconcile flow and the daemon dispatch implementation to pin down the exact ambiguities.
I’ve confirmed two important context points the original prompt misses: `daemon_loop.rs` already has a structural test forbidding `set_current_dir`, and the daemon lock system is task-oriented while the CLI lock path is currently just a raw file with no corresponding lease/task model. I’m tightening the rewrite around those constraints so the implementation target is actually achievable without guesswork.
# Prompt Review

## Issues Found
- The prompt is partially stale relative to the current tree: [`src/contexts/automation_runtime/daemon_loop.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) already contains no `set_current_dir`, and that invariant is already enforced by both [`tests/unit/automation_runtime_test.rs#L2901`](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs#L2901) and [`src/contexts/conformance_spec/scenarios.rs#L6447`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/conformance_spec/scenarios.rs#L6447). Treating issue `#6` as an open implementation task will send downstream loops looking for code that no longer exists.
- The prompt suggests reusing `LeaseService::acquire()`, but that is not feasible as written because [`src/contexts/automation_runtime/lease_service.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) couples lease acquisition to daemon task IDs and worktree creation. CLI `run start` and `run resume` should not create either.
- The prompt does not define a data model for a CLI-held lease. Current reconcile logic assumes every lease maps to a daemon task and worktree, so “make stale CLI locks discoverable by reconcile” is underspecified unless the prompt names a lease kind, separate record type, or other explicit representation.
- The staleness policy is ambiguous. “Optional heartbeat or TTL” does not say how a healthy long-running CLI run avoids being reclaimed as stale, what default TTL to use, or whether heartbeat is required.
- The reconcile acceptance rules are incomplete. The prompt does not say whether stale CLI locks should increment `stale_leases`, `released_leases`, or `failed_tasks`, so equally reasonable implementations could produce different reports and tests.
- “Do not change any public CLI behavior” is too broad for this task. `daemon reconcile` will necessarily gain new observable behavior when stale CLI leases exist, so the prompt should narrow the compatibility requirement to `run start`/`run resume` lock-contention behavior and existing error contracts.
- The backward-compatibility story is missing. If the serialized lease schema changes, existing worktree leases must still deserialize; and if legacy standalone `.lock` files are in scope, the prompt must say so explicitly because they contain no timestamp/lease metadata for safe cleanup.
- The verification instructions are inconsistent. The prompt says conformance must keep passing but only names `cargo build` and `cargo test`; it should specify the exact conformance command to run.
- The test request is uneven: it asks for a new `set_current_dir` assertion even though that coverage already exists, but it does not explicitly require the most important new regression test, which is stale CLI lease cleanup followed by successful reacquisition.

## Refined Prompt
# Concurrency Safety: CLI writer-lock lease recovery + preserve daemon CWD safety

## Objective

Implement stale-lock recovery for CLI-held project writer locks in `ralph-burning-rewrite/`.

Treat the `set_current_dir` concern as a regression guard, not a new feature: the current tree already has no `std::env::set_current_dir` call in `src/contexts/automation_runtime/daemon_loop.rs`, and that must remain true.

## Current State

- `src/cli/run.rs` still acquires a bare project writer-lock file with owner `"cli"` and no lease record.
- `src/contexts/automation_runtime/lease_service.rs` and `daemon reconcile` currently manage daemon task/worktree leases only.
- Existing unit and conformance tests already cover the no-`set_current_dir` invariant in `daemon_loop.rs`.

## Scope

### 1. CLI writer-lock recovery (`#3`)

Replace the bare CLI writer locking used by `run start` and `run resume` with a lease-backed lock that can be cleaned by `daemon reconcile` after a crashed CLI process.

Keep the existing project writer-lock file as the mutual-exclusion primitive so CLI runs and daemon task dispatch still contend on the same project-level lock.

Do not create daemon tasks or worktrees for CLI-held locks.

### 2. Daemon CWD safety (`#6`)

Do not reintroduce `std::env::set_current_dir` anywhere in `src/contexts/automation_runtime/daemon_loop.rs`.

No functional daemon-loop change is required for issue `#6` unless you discover a hidden CWD dependency while implementing or testing issue `#3`.

## Design Requirements

- Do not call `LeaseService::acquire()` directly from CLI run paths. Its current behavior is task/worktree-oriented and is not appropriate for CLI `run start`/`run resume`.
- Extract only the shared “project writer lock + lease record + cleanup” behavior into a smaller helper/service, or add a separate CLI writer-lease type that reconcile can process explicitly.
- Represent CLI-held locks explicitly. Acceptable approaches are:
  - a new lease kind/enum that distinguishes worktree leases from CLI writer leases; or
  - a separate serialized record type for CLI writer leases.
- Do not fake CLI locks by inventing daemon task IDs or fake worktree paths.
- If you change serialized lease schema, keep backward compatibility for existing worktree lease files via serde defaults or equivalent.
- No migration of preexisting standalone `.lock` files is required for this task. This change only guarantees self-healing for CLI locks created after the new lease-backed path lands.

## CLI Lease Behavior

- `run start` and `run resume` must acquire the project writer lock before any run-state mutation.
- The guard in `src/cli/run.rs` must own both the writer-lock file lifecycle and the corresponding CLI lease-record lifecycle.
- Use a staleness policy that does not allow a healthy long-running CLI command to be reclaimed as stale during normal execution.
- Preferred implementation:
  - TTL: `300` seconds
  - heartbeat cadence: `30` seconds while the CLI command is alive
- If you implement heartbeat, the guard must own the cancellation/abort handle and stop heartbeat updates on drop before removing the lease record.
- If you choose a different mechanism, it must still prevent a healthy long-running CLI run from being treated as stale under normal operation.
- On normal exit, error unwind, or panic unwind, the guard must release both the lease record and the writer lock via RAII or best-effort cleanup.
- `run start` and `run resume` must keep the current lock-contention behavior and continue surfacing `ProjectWriterLockHeld` for active lock conflicts.

## Reconcile Behavior

- `daemon reconcile` must scan stale CLI writer leases in addition to existing worktree leases.
- For a stale CLI writer lease, reconcile must:
  - remove the CLI lease record;
  - release the writer-lock file;
  - not require a daemon task record;
  - not mark any daemon task failed;
  - not remove any worktree.
- Reconcile accounting must be explicit:
  - stale CLI writer leases increment `stale_leases`;
  - successfully cleaned stale CLI writer leases increment `released_leases`;
  - stale CLI writer leases do not increment `failed_tasks`.
- Keep strict cleanup semantics:
  - if a cleanup sub-step is already absent or returns an I/O error, report a cleanup failure instead of silently counting the CLI lease as released.

## Non-goals

- No new public CLI flags.
- No new daemon task types.
- No worktree creation for CLI-held locks.
- No process-global CWD mutation.

## Acceptance Criteria

- A live CLI `run start` or `run resume` still blocks competing writers with `ProjectWriterLockHeld`.
- A normal CLI run still releases its writer lock on both success and failure.
- A stale CLI lease is discoverable by `daemon reconcile` and can be cleaned without any daemon task or worktree.
- After stale CLI cleanup, a subsequent `run start` or `run resume` can acquire the writer lock normally.
- `src/contexts/automation_runtime/daemon_loop.rs` still contains no `set_current_dir` call sites.
- Existing daemon CWD-safety tests remain green.

## Tests

- Add or update focused unit coverage for the shared CLI writer-lock/lease helper so it proves a CLI-held lock creates a reconcile-visible lease record.
- Add a reconcile test for a stale CLI writer lease with no task/worktree and assert:
  - `stale_leases == 1`
  - `released_leases == 1`
  - `failed_tasks == 0`
- Add a reconcile test for partial cleanup of a stale CLI writer lease, such as a missing writer-lock file, and assert it becomes a cleanup failure rather than a successful release.
- Add a CLI or conformance test that injects a stale CLI lease plus writer lock, runs `daemon reconcile`, and then verifies `run start` or `run resume` succeeds.
- Preserve the existing structural and runtime tests that enforce the no-`set_current_dir` invariant; only update them if helper locations or file paths move.

## Verification

Run all of the following:

1. `nix develop -c cargo build`
2. `nix develop -c cargo test`
3. `nix develop -c cargo run -- conformance run`
