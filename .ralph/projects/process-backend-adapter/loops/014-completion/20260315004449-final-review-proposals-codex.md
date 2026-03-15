---
artifact: final-review-proposals
loop: 14
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-15T00:44:49Z
---

Reviewing the diff against `ebc30fb702f2716fc64077440b54b373e7c29ec9` first, then I’ll read the changed implementation files end-to-end and trace the behavior against the process-backend requirements.
I’ve identified the main code paths: the new process adapter, adapter dispatch/CLI wiring, the workflow engine prompt builder, and the supporting model/runtime changes. Next I’m reading those files with line numbers so I can trace correctness and cite exact locations.
The adapter implementation is the highest-risk area, so I’m reading it in full first, then tracing how `run`/daemon build the service and how the workflow engine now assembles prompts and invocation requests.
I’ve read the adapter and the prompt-building path. Now I’m tracing resume/preflight/session behavior and checking the new tests to see what is and is not actually covered.
I’ve got two candidate robustness issues from code tracing. I’m running the focused unit suites now to confirm the existing tests stay green and to judge whether the gaps are genuinely untested behavior rather than an artifact of partial reading.
The local shell doesn’t have `cargo` directly on `PATH`, so I’m using the project’s `nix develop -c ...` path for any verification commands.
I’ve finished the targeted suites and they’re green. I’m doing one broader `cargo test` pass now so the review isn’t based only on reading plus cherry-picked tests.
The full suite is still running, but the CLI pass is already through 109 tests without failures. I’m waiting for the remaining unit/conformance tail to finish so I can distinguish actual test failures from review-only concerns.
# Final Review: AMENDMENTS

## Amendment: RB-REVIEW-001 [P1]

### Problem
The availability probe treats any regular file named `claude` or `codex` on `PATH` as usable because it only checks `metadata.is_file()` at [process_backend.rs#L596](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L596), [process_backend.rs#L603](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L603), and [process_backend.rs#L604](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L604). `preflight_check()` then trusts that result at [engine.rs#L192](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L192). If `PATH` contains a non-executable placeholder or stale file, preflight passes and the run only fails later at `spawn()`, after run state mutation has already begun.

### Proposed Change
Require the resolved backend binary to be executable, not merely present. On Unix, check execute bits on the candidate file (or use a direct executable-resolution helper) before returning `Ok(())`; otherwise return `AppError::BackendUnavailable`.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - harden `check_availability()` so preflight only succeeds for actually executable backends.

## Amendment: RB-REVIEW-002 [P2]

### Problem
Cancellation shells out to `kill` from `PATH` in [process_backend.rs#L678](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L678). That makes timeout/cancel cleanup depend on whichever `kill` binary the environment resolves first, instead of using a stable OS signal API. In the normal timeout/cancellation paths, the service then discards cancel errors at [service.rs#L198](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L198) and [service.rs#L208](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L208), so a missing or shadowed `kill` can leave child processes running with no surfaced failure.

### Proposed Change
Replace the external `kill` command with an in-process signal API such as `nix::sys::signal::kill` and treat `ESRCH` as success. That removes `PATH` hijacking/misconfiguration from the cancellation path and makes subprocess cleanup deterministic.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - stop using `std::process::Command("kill")` for SIGTERM/SIGKILL delivery.

## Amendment: RB-REVIEW-003 [P3]

### Problem
When Codex exits successfully but the `--output-last-message` file is missing or unreadable, the adapter classifies that as `FailureClass::SchemaValidationFailure` at [process_backend.rs#L517](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L517) and [process_backend.rs#L523](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L523). That is an IO/transport failure, not a schema failure. The distinction matters because retry policy treats transport and schema failures differently at [retry_policy.rs#L36](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L36) and [retry_policy.rs#L38](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L38), and the current mapping obscures whether Codex emitted bad JSON versus failed to emit the file at all.

### Proposed Change
Map unreadable or missing last-message files to `FailureClass::TransportFailure`, and reserve `SchemaValidationFailure` only for JSON parse failures in the subsequent `serde_json::from_str(...)` path.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - reclassify Codex last-message file read failures so retry and diagnostics reflect the actual failure mode.

---
