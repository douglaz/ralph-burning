---
artifact: final-review-votes
loop: 6
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T21:25:54Z
---

Reviewing the cited adapter and service paths first so the votes are anchored to the current code rather than only the planner summary. After that I’ll decide each amendment against the implementation details and the planner’s rationale.
The command and cancellation paths match the planner’s summary so far. I’m pulling the remaining test coverage around cancellation to see whether either amendment is already partially guarded by existing tests.
The current tests only assert that cancel sends `SIGTERM` and that the spawned task eventually unwinds; they do not cover deterministic reap behavior or the unsupported Codex resume flags. I’m checking the local `codex` surface and the pinned Tokio behavior next because those two runtime points matter to the vote.
The runtime points check out: `codex exec resume` rejects `--output-schema`, and Tokio documents dropped children as only best-effort background reaped unless the caller explicitly waits. I’m doing one last pass on the exact line numbers so the final vote rationale is specific and clean.
I have enough to vote. I’m grabbing the exact Tokio version line only so the second rationale can note the drop semantics precisely without overstating the zombie claim.
# Vote Results

## Amendment: PBA-REV-001

### Vote
ACCEPT

### Rationale
The current implementation does build a single Codex argv shape for both new and resumed sessions. In [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L362), the adapter switches to `exec resume` when a reusable session exists, but the shared argument block in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L369) still appends `--output-schema` and `--output-last-message` unconditionally. That makes the reuse path fail before the subprocess can do useful work.

The planner’s runtime check is consistent with this code. In this environment, `codex exec resume --help` exposes `--output-last-message` but not `--output-schema`, and `codex exec resume --output-schema /tmp/schema.json` fails with `unexpected argument '--output-schema'`. The existing resume test in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L486) only checks for `exec resume` and the session id, and the fake Codex stub in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L132) does not reject unsupported flags, so this regression is currently unguarded. Splitting new-session and resume argv construction is the right fix.

## Amendment: PBA-REV-002

### Vote
ACCEPT

### Rationale
The planner’s core position is correct. The adapter stores only `u32` PIDs in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L21) and registers them after spawn in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L128), while the actual `tokio::process::Child` remains local to `spawn_and_wait` and is only reaped by `child.wait()` inside that future in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L162). On cancellation, [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L544) removes the map entry first and only sends `SIGTERM`. Then the service returns immediately after calling `cancel()` in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196), which drops the invoke future before any project-owned reap step is confirmed.

I agree with the planner’s nuance that the amendment slightly overstates the “no reap path at all” claim. Tokio 1.50.0 is pinned in [Cargo.lock](/root/new-ralph-burning/ralph-burning-rewrite/Cargo.lock#L1408), and Tokio documents only best-effort background reaping for dropped children in [mod.rs](/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-1.50.0/src/process/mod.rs#L647). But that does not rescue the current design: cancellation is still not deterministic, not tied to confirmed shutdown, and still relies on bare PID bookkeeping with a PID-reuse race. The proposed handle-aware cleanup is a real improvement and should be accepted.
