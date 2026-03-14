---
artifact: final-review-proposals
loop: 8
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T22:25:14Z
---

Reviewing the delta against `ebc30fb702f2716fc64077440b54b373e7c29ec9` first, then I’ll read the implementation files end-to-end and trace the new process backend paths for correctness, failure handling, and test coverage.
I have the change surface. Next I’m reading the new adapter, prompt builder, and wiring paths in full, then I’ll run a few targeted tests to check the risky subprocess and prompt-recovery behaviors.
The core files are loaded. I’m now tracing call sites into `AgentExecutionService`, daemon workflow dispatch, and the prompt/journal helpers to look for mismatches between the new subprocess semantics and the existing persistence model.
I’ve found one likely contract regression in the Codex resume path. I’m checking whether it’s isolated or part of a broader session-handling problem before I write up the review.
I’m validating the high-risk paths now. The current implementation looks largely coherent, but I want test execution to separate real regressions from spec-only nits before I finalize the review.
`cargo` is not on the default PATH in this shell. I’m switching to the project’s Nix entrypoint for any verification commands.
I found a second, more serious workflow collision: invocation IDs are not including `completion_round`. I’m tracing where those IDs are used now, because that can cause state overwrites across amendment loops.
# Final Review: AMENDMENTS

## Amendment: PB-CANCEL-TIMEOUT-HANG [P0]

### Problem
`ProcessBackendAdapter::cancel()` sends `SIGTERM` and then blocks on `wait()` in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L614) and [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L629). That `wait()` is an unbounded poll loop in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L58). `AgentExecutionService` calls `cancel()` on both timeout and explicit cancellation before returning to the caller in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196). If a child traps or ignores `SIGTERM`, the timeout/cancel path never completes, so the “authoritative” timeout is no longer authoritative.

### Proposed Change
Make `cancel()` best-effort and non-blocking: remove the child from the map, send `SIGTERM`, and return immediately, or at most use a short bounded grace period with a forced cleanup path. Add a regression test with a fake backend that ignores `SIGTERM` and verify `InvocationTimeout`/`InvocationCancelled` still returns promptly.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - stop waiting indefinitely inside `cancel()`.
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add a TERM-ignoring child test.

## Amendment: PB-CODEX-RESUME-SCHEMA [P1]

### Problem
The Codex resume argv omits `--output-schema` entirely in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L413), and the resume path skips creating the schema file because schema generation is only done in the non-resume branch at [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L439). That contradicts the required resume command shape and means resumed Codex sessions are no longer constrained by the stage schema. The test suite currently locks in the wrong behavior by asserting resume must not include `--output-schema` in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L553).

### Proposed Change
Always materialize the schema file for Codex stage invocations and include `--output-schema <schema_path>` in both new-session and resume commands. Update the resume test to require the schema arg and cleanup behavior.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - write schema files for resumed Codex invocations and pass `--output-schema`.
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - correct the resume-command expectation.

## Amendment: WF-INVOCATION-ID-COLLISION [P2]

### Problem
Workflow invocation IDs are built from only `run_id`, `stage`, `cycle`, and `attempt` in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1956). Completion-round restarts reuse the same `cycle` and often reset `attempt` back to `1`, so the same stage in round 2 gets the same `invocation_id` as round 1. That collides with raw-output persistence in [fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L724) and with Codex temp-file naming in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L433). The result is overwritten backend evidence across completion rounds, and stale temp files can alias later invocations if an earlier run exited before cleanup.

### Proposed Change
Include `completion_round` in `InvocationRequest.invocation_id` for workflow stages, e.g. `...-c{cycle}-a{attempt}-cr{completion_round}`. Add coverage proving backend raw outputs and temp artifacts stay distinct across completion rounds.

### Affected Files
- [src/contexts/workflow_composition/engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - make workflow invocation IDs round-aware.
- [tests/unit/workflow_engine_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/workflow_engine_test.rs) - add coverage for unique backend artifact IDs across completion rounds.
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add temp-file/raw-output collision coverage.

Targeted verification I ran: `nix develop -c cargo test process_backend_test` and `nix develop -c cargo test prompt_builder_test`. Both passed, so these are edge-case/spec regressions that current tests do not catch.
