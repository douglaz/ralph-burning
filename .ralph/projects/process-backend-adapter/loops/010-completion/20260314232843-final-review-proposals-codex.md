---
artifact: final-review-proposals
loop: 10
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T23:28:43Z
---

Reviewing the diff against `ebc30fb702f2716fc64077440b54b373e7c29ec9` first, then I’ll read the main adapter, CLI, and workflow files end-to-end to trace the behavior against the spec and look for regressions.
I have the change surface now. Next I’m reading the adapter, workflow engine, and CLI wiring end-to-end, then I’ll trace a few execution paths against the acceptance criteria to look for concrete failures rather than just local code smells.
I’ve covered the new adapter surface and the workflow entry points. Now I’m tracing the actual stage invocation path in `engine.rs`, because that’s where most correctness regressions would hide: prompt construction, worktree selection, session reuse, and durable IDs.
I’m checking session lifecycle now. The adapter-level behavior can look correct locally while still failing end-to-end if the service can never persist a reusable session for later stages.
I’ve got three concrete review findings. I’m tightening the line references now so the amendments point to the exact call sites and state transitions rather than broad file-level comments.
# Final Review: AMENDMENTS

## Amendment: RBR-FR-20260314-01 [P1]

### Problem
Resumed Codex invocations are built without `--output-schema`: [ralph-burning-rewrite/src/adapters/process_backend.rs#L424](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L424) only includes `--output-last-message`, and the resume branch in [ralph-burning-rewrite/src/adapters/process_backend.rs#L447](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L447) never writes or passes a schema file before calling `codex exec resume`. The master prompt required `codex exec resume ... --output-schema <schema_path> --output-last-message <message_path> ...`. As implemented, resumed Codex stages lose CLI-side schema enforcement and do not match the required argv shape.

### Proposed Change
Create the schema file for resumed Codex executions too, add `--output-schema <schema_path>` to `codex_resume_args()`, and keep the same best-effort cleanup path for both new and resumed sessions.

### Affected Files
- [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - add schema creation and `--output-schema` to the resume path.
- [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - update the resume argv expectation and cover schema temp-file cleanup on resume.

## Amendment: RBR-FR-20260314-02 [P2]

### Problem
New Codex sessions never become reusable. `invoke_codex()` sets `metadata.session_id` to `None` unless the request is already resuming in [ralph-burning-rewrite/src/adapters/process_backend.rs#L538](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L538). Workflow stages still request reuse in [ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1969](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1969), and Codex is advertised as session-reusable in [ralph-burning-rewrite/src/shared/domain.rs#L137](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L137), [ralph-burning-rewrite/src/shared/domain.rs#L203](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L203), and [ralph-burning-rewrite/src/shared/domain.rs#L252](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L252). But `AgentExecutionService` only persists sessions when `session_id` is present in [ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L248](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L248), and `SessionManager` drops `None` immediately in [ralph-burning-rewrite/src/contexts/agent_execution/session.rs#L89](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/session.rs#L89). So a clean project can never seed a Codex session into `sessions.json`, which makes the `codex exec resume` branch unreachable unless the session file was manually pre-populated.

### Proposed Change
Either extract and persist a real Codex session ID after new-session runs, or mark Codex as non-reusable until the CLI can surface one. The current combination of “supports reuse” plus “never records a first session” is internally inconsistent.

### Affected Files
- [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - return a persistable session identifier for new Codex sessions, or stop claiming reuse support there.
- [ralph-burning-rewrite/src/shared/domain.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs) - if no session ID can be surfaced, disable Codex session reuse in the declared capabilities.
- [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add an end-to-end session persistence/reuse test for real Codex adapter behavior.

## Amendment: RBR-FR-20260314-03 [P2]

### Problem
Availability preflight depends on an external `which` binary instead of checking the target executable directly. In [ralph-burning-rewrite/src/adapters/process_backend.rs#L591](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L591), `check_availability()` shells out to `which <binary>`. On hosts where `claude` or `codex` is on `PATH` but `which` is not installed, preflight incorrectly returns `BackendUnavailable`. The required behavior was to verify the backend binary exists on `PATH`, not to require a second utility.

### Proposed Change
Replace the `which` subprocess with a direct `PATH` search in Rust or a small dependency that resolves executables without shelling out. Add a regression test where the backend binary exists but `which` does not.

### Affected Files
- [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs) - remove the `which` subprocess dependency from availability checks.
- [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) - add a positive availability test that passes without an external `which` command.

---
