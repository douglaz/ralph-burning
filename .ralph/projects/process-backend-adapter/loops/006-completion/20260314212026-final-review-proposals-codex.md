---
artifact: final-review-proposals
loop: 6
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T21:20:26Z
---

Reviewing the diff against `ebc30fb702f2716fc64077440b54b373e7c29ec9` first, then I’ll read the implementation files end-to-end and trace the behavior for correctness, failure handling, and regressions.
The main surface area is `process_backend`, CLI adapter selection, and the workflow prompt builder. I’m reading those end-to-end now, then I’ll run the focused tests that exercise the new subprocess path.
I have the new adapter and prompt builder in view. Next I’m tracing where `working_dir`, session reuse, preflight, and rollback/remediation data are actually threaded into stage invocation, because that’s where correctness issues usually hide.
I’ve finished the first code pass. I’m running the focused unit tests now to see whether the adapter and prompt-builder behavior line up with the intended session, cancellation, and workflow wiring.
The workspace doesn’t have `cargo` on `PATH`, so I’m switching to the project’s `nix develop -c ...` path for test execution. I’ll use that to verify the subprocess adapter and prompt-builder coverage.
The focused tests are green. I’m checking the remaining gaps against the actual runtime surfaces now: real CLI/session semantics and cancellation behavior that the fake binaries can’t fully validate.
I have one runtime mismatch under investigation already. I’m running a full `cargo build` and `cargo test` through `nix develop` now to confirm there aren’t additional regressions outside the focused coverage.
The full suite is still running through the long integration portion. I already have one concrete runtime defect from tracing against the installed `codex` CLI, and I’m letting the rest finish in case it surfaces anything else.
I have the final review points. I’m pulling the exact line spans for the session-resume mismatch and the cancellation lifecycle gap, then I’ll write them up with concrete file references.
# Final Review: AMENDMENTS

## Amendment: PBA-REV-001 [P1]

### Problem
The Codex resume argv is not compatible with the installed CLI. In [src/adapters/process_backend.rs:365](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L365) and [src/adapters/process_backend.rs:369](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L369), the adapter builds `codex exec resume ... --output-schema ... --output-last-message ...`. The local `codex` binary rejects that shape: `codex exec resume --output-schema /tmp/schema.json` returns `unexpected argument '--output-schema'`. Any workflow stage that tries to reuse a Codex session will fail before the subprocess starts.

### Proposed Change
Build separate Codex argv layouts for new-session and resume flows. Keep `--output-schema` on `codex exec ...`, but remove it from `codex exec resume ...` and rely on prompt/schema validation after reading the last-message file. Add a test that rejects unsupported resume-only flags so this cannot regress.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - split Codex new-session vs resume command construction.

## Amendment: PBA-REV-002 [P2]

### Problem
Timeout/cancellation does not retain a reapable child handle. The adapter stores only bare PIDs in [src/adapters/process_backend.rs:23](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L23) and [src/adapters/process_backend.rs:128](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L128), and `cancel()` only sends `kill -TERM` to that PID in [src/adapters/process_backend.rs:544](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L544). At the service layer, timeout/cancellation returns immediately after calling `cancel()` in [src/contexts/agent_execution/service.rs:196](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196). That drops the future owning `tokio::process::Child`, so there is no remaining wait/reap path for the subprocess. In daemon mode, timed-out or cancelled backends can be left running or become zombies, and the bare-PID approach also leaves a PID-reuse hazard.

### Proposed Change
Track the actual child handle, not just the PID, and make cancellation perform signal + reap before removing the entry from the active-child map. If you need to keep the current service shape, spawn a dedicated cleanup task that `wait()`s the child after cancellation. `kill_on_drop(true)` is also worth enabling as a safety net.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - store child handles and reap them on cancel/timeout.
- [src/contexts/agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs) - keep cancellation wired to a reap-aware adapter path if needed.

`nix develop -c cargo build` and `nix develop -c cargo test` both pass. The first amendment was reproduced directly against the installed `codex` CLI, so it is a real runtime mismatch rather than a test-only concern.
