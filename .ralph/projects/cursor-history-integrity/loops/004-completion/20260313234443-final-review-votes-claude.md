---
artifact: final-review-votes
loop: 4
project: cursor-history-integrity
backend: claude
role: final_reviewer
created_at: 2026-03-13T23:44:43Z
---

I've now read all the relevant code. Here is my vote:

# Vote Results

## Amendment: CRI-20260313-01

### Vote
ACCEPT

### Rationale
The code path confirms the defect is real and the fix is warranted. The sequence of events on the failure path is:

1. `snapshot.amendment_queue.pending.extend(amendments)` runs at line 1013, mutating the in-memory snapshot.
2. The `completion_round_advanced` journal append is attempted at lines 1028-1030.
3. If that append fails, `fail_run_result` is called, which invokes `fail_run` — this sets `snapshot.active_run = None` and writes the snapshot to disk at line 2003, preserving the pending amendments but **not** the advanced `completion_rounds` counter (line 1055 runs only after the append succeeds).
4. On resume, `derive_resume_state` seeds `current_completion_round` from `snapshot.completion_rounds` (line 2474) and only advances it when it encounters a durable `CompletionRoundAdvanced` event (lines 2498-2507). Since that event was never persisted, the round stays at the old value.
5. The pending-amendments check at line 2525 correctly rewinds to planning, but with the **old** completion round.
6. `history_record_base_id` at line 114 derives file IDs from `cursor.completion_round`, so the resumed planning pass will produce the same `-cr1` IDs as the previous round.
7. The filesystem writer at lines 813/820 uses `fs::rename` with no existence guard, so on Unix this silently replaces the prior canonical file — a direct violation of append-only history.

The proposed fix (either persisting the advanced round before the fallible append, or teaching `derive_resume_state` to infer `current_completion_round + 1` when pending amendments exist without a matching durable event) is sound. The regression test is well-scoped. This is a genuine data integrity gap in a resumable failure path.

## Amendment: CRI-20260313-02

### Vote
ACCEPT

### Rationale
The inconsistency is clear and confirmed in the code:

- **CLI path** (requirements.rs:40-72): Loads `EffectiveConfig`, derives `BackendSelectionConfig`, builds the service, and calls `.with_workspace_defaults(workspace_defaults)` before invoking `draft`/`quick`.
- **Daemon path** (daemon_loop.rs:520, 672): Calls `build_requirements_service()` which constructs `RequirementsService::new(...)` at line 1171 — returning a service with `workspace_defaults: None` (service.rs:118).
- **Target resolution** (service.rs:888-893): `invoke_stage` passes `self.workspace_defaults.as_ref()` to the resolver. With `None`, workspace-configured backend/model preferences are silently ignored.

The daemon already loads `EffectiveConfig` elsewhere in its task processing (line 368), so the fix is straightforward: thread config into `build_requirements_service()` and call `.with_workspace_defaults(...)`. This ensures CLI and daemon requirements paths resolve to the same backend/model targets for a given workspace configuration. The proposed test coverage is appropriate. The fact that the current backend is a stub adapter does not negate the bug — the resolver still uses workspace defaults to select targets, and the inconsistency would immediately surface when a real backend adapter is wired in.
