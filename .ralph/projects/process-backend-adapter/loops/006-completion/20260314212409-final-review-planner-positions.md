---
artifact: final-review-planner-positions
loop: 6
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-14T21:24:09Z
---

I’m verifying both amendments against the actual adapter and service code first, then I’ll check the local `codex` CLI behavior where that claim depends on runtime behavior.
I’m pulling the exact code paths cited in the amendments so the decision is tied to what the adapter currently does, not to the amendment summary.
The first amendment also depends on the installed CLI surface, so I’m checking the local `codex` subcommand help and the exact failing invocation shape rather than relying on the amendment’s wording.
`codex exec resume` help already shows `--output-last-message` but not `--output-schema`; I’m running the exact flag shape the amendment cites to confirm it fails the same way in this environment.
I’ve confirmed the resume CLI mismatch. Next I’m checking whether the cancellation path keeps any reapable child handle or whether it really drops ownership after sending `SIGTERM`.
I’ve got the high-level behavior. I’m pulling exact line-numbered snippets now so each accept/reject call is anchored to the current source and tests.
The second amendment turns on `tokio::process::Child` drop semantics. I’m checking the pinned dependency version and local Tokio source so I don’t overstate the zombie/reap behavior.
Tokio 1.50 is vendored locally, so I’m reading its `process` implementation to pin down whether dropped child handles are reaped or simply abandoned after the service returns.
I’ve finished the verification pass. One amendment is a direct CLI/runtime mismatch, and the other is a real cancellation robustness issue, but with one nuance: Tokio still does best-effort orphan reaping after a dropped `Child`, so the defect is lack of deterministic termination/reap, not literally zero cleanup path.
# Planner Positions

## Amendment: PBA-REV-001

### Position
ACCEPT

### Rationale
The bug is real. In [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L359), the Codex path switches to `exec resume` when `SessionPolicy::ReuseIfAllowed` has a prior session, but the shared argv block still unconditionally appends `--output-schema` and `--output-last-message` in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L369). The reuse path is reachable because the service loads and attaches reusable sessions before invoke in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L171).

I also verified the installed CLI: `codex exec resume --output-schema /tmp/schema.json` fails with `unexpected argument '--output-schema'`. The current test setup masks this because the fake Codex stub only looks for `--output-last-message` in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L132), and the resume test only checks for `exec resume` plus the session id in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L486). Splitting new-session vs resume argv construction is technically warranted.

## Amendment: PBA-REV-002

### Position
ACCEPT

### Rationale
The core defect is real. The adapter tracks only bare PIDs in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L21) and registers them after spawn in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L128). The actual `tokio::process::Child` stays local to `spawn_and_wait` and is only `wait()`ed inside that future in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L162). `cancel()` removes the PID from the map first and then only issues `kill -TERM`, with no handle-based wait or retry path, in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L544).

At the service layer, timeout and cancellation both call `cancel()` and immediately return in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196). That drops the in-flight invoke future, so this code has no deterministic project-level path to confirm child termination or reap it before reporting cancellation complete. A child that ignores or delays `SIGTERM` can outlive the cancelled invocation, and the PID-only design does carry a PID-reuse hazard.

One nuance: Tokio 1.50 does provide only best-effort orphan reaping after a dropped `Child`, as documented in [tokio process](/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-1.50.0/src/process/mod.rs#L640) and implemented in [tokio reap](/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-1.50.0/src/process/unix/reap.rs#L117). So the amendment slightly overstates the “no reap path at all” part. That does not change the main technical conclusion: current cancellation is not reap-aware and is not tied to confirmed process shutdown, so the proposed handle-based cleanup is a real robustness improvement.
