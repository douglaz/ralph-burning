# Implementation Response (Iteration 14)

## Changes Made
1. **RC1 — Phase 0 recovery truthfulness** (three-site fix across `task_service.rs` and `daemon_loop.rs`):
   - **`revert_to_pending_for_recovery`** (`task_service.rs`): Now checks `resources_released` from `LeaseService::release()`. Only reverts to Pending and clears `lease_id` when cleanup positively succeeds. If partial (`resources_released = false`), returns `LeaseCleanupPartialFailure` and preserves the task in its current state with lease ownership intact.
   - **Phase 0 terminal task release** (`daemon_loop.rs`): Deferred `clear_label_dirty` until after `release_task_lease` succeeds. If lease release fails, `label_dirty` stays `true` so Phase 0 retries the cleanup on the next cycle (the label re-sync is idempotent).
   - **Phase 0 non-terminal revert** (`daemon_loop.rs`): Only clears `label_dirty` after `revert_to_pending_for_recovery` succeeds. If revert fails (partial lease cleanup), `label_dirty` stays `true` for next-cycle retry.

2. **RC2 — Abort/retry command semantics** (four-site fix across `github_intake.rs`, `cli/daemon.rs`, `task_service.rs`):
   - **GitHub `/rb abort`** (`github_intake.rs`): Now runs the same lease/worktree cleanup as CLI abort for Claimed/Active tasks. `handle_explicit_command` accepts a `WorktreePort` and calls `LeaseService::release()` with the same truthfulness contract — only clears `lease_id` on positive cleanup success.
   - **`retry_task` retained-lease guard** (`task_service.rs`): Now rejects retry with `LeaseCleanupPartialFailure` if the task still holds a `lease_id`. This prevents blindly clearing the reference and stranding live resources.
   - **GitHub `/rb retry`** (`github_intake.rs`): Before calling `retry_task()`, attempts cleanup of any retained lease. If cleanup succeeds, clears the reference so retry proceeds. If cleanup fails, `retry_task()` rejects the retry.
   - **CLI `daemon retry`** (`cli/daemon.rs`): Same pre-retry cleanup pattern — attempts lease release before `retry_task()`, clears reference only on positive success.

3. **RI1 — Failure-path conformance coverage**: Updated `daemon.tasks.label_failure_quarantine_and_recovery` scenario:
   - **Scenario A** (positive cleanup): Uses a stub that returns `Removed` and creates a proper writer lock, so all three release sub-steps succeed and `resources_released = true`. Verifies revert to Pending with lease cleared.
   - **Scenario A2** (partial cleanup): Uses a stub that returns `AlreadyAbsent` (no writer lock). Verifies `revert_to_pending_for_recovery` returns error and the task stays Claimed with lease preserved.
   - **Scenario B** (terminal task): Unchanged — verifies terminal task retains lease reference for explicit release.
   - **Scenario C** (terminal rejection): Unchanged — verifies `revert_to_pending_for_recovery` rejects terminal tasks.
   - **Scenario D** (retry with retained lease): New — verifies `retry_task()` rejects a Failed task that still holds a `lease_id`, preserving the task state.

## Could Not Address
None

## Verification
- `cargo check` — clean
- 45 lib tests — all passing
- 578 unit tests — all passing, 0 failures, 1 ignored (pre-existing)
- 110 CLI tests — all passing
- 283 conformance scenarios — all passing (1 pre-existing failure: `backend.requirements.real_backend_path.daemon` due to missing binary, unrelated to this change)
- `daemon.tasks.label_failure_quarantine_and_recovery` — PASS (0.14s)
- All 10 `daemon.tasks.*` scenarios — PASS
- All 5 `daemon.github.*` / `daemon.routing.*` / `daemon.labels.*` scenarios — PASS
