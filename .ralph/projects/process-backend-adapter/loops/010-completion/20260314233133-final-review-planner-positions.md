---
artifact: final-review-planner-positions
loop: 10
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-14T23:31:33Z
---

Verifying each amendment against the current code now. I’m reading the process backend, session persistence, capability declarations, and the related tests to check whether each claimed defect is real.
I’ve confirmed the core branches exist where the amendments point: Codex resume args, session persistence, and availability probing. Next I’m reading the surrounding tests and helper logic to check whether the current behavior is intentional, already covered, or actually inconsistent.
I’ve reached the point where the decision turns on behavior rather than just line presence. I’m checking how much validation still happens after Codex returns so I can separate “missing convenience flag” from an actual correctness or robustness defect.
The post-invocation path does still validate returned JSON against the stage contract, so the remaining question is whether the missing resume flags create a real robustness gap or just a mismatch in how strictly the CLI is guided. I’m now tracing session lookup so I can judge the second amendment on reachability rather than assumption.
The session path is becoming clearer: `AgentExecutionService` overwrites any caller-provided `prior_session` with whatever is in persistent storage, so reuse only happens if something has already been recorded. I’m checking whether any other code path ever seeds a Codex session or whether the stored-file path is actually unreachable for clean runs.
The only explicit tests for Codex reuse are adapter-level tests that manually inject a prior session into the request. That matters because the service layer does not honor caller-injected sessions; it reloads from `sessions.json` first. I’m reading the session store and service tests now to see whether any integrated path ever records a first Codex session.
I’ve verified amendment 2’s core claim: the integrated service can only reuse sessions loaded from storage, and Codex new-session runs currently return no session ID to store. I’m pulling exact line numbers now so the final positions can cite the precise branches rather than paraphrase them.
I have enough to decide amendment 2 and amendment 3. For amendment 1 I’m pulling the exact post-parse validation lines, because the acceptance decision hinges on whether the missing resume schema is merely redundant or whether it still creates a meaningful robustness gap despite downstream validation.
# Planner Positions

## Amendment: RBR-FR-20260314-01

### Position
ACCEPT

### Rationale
The code does omit `--output-schema` for resumed Codex invocations. New-session args include it in [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L408), and the new-session branch writes the schema file in [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L457). The resume args in [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L424) only pass `--output-last-message`, and the resume branch at [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L450) never writes a schema file. The current unit test explicitly locks that behavior in by asserting resume args must not contain `--output-schema` in [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L567).

There is still downstream validation after invocation in [ralph-burning-rewrite/src/contexts/agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L233) and [ralph-burning-rewrite/src/contexts/workflow_composition/contracts.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/contracts.rs#L130), so this is not a silent acceptance bug. But it is a real robustness and consistency gap: resumed Codex runs get weaker CLI-side output enforcement than fresh runs.

## Amendment: RBR-FR-20260314-02

### Position
ACCEPT

### Rationale
This inconsistency is real. Workflow stages request reuse via `SessionPolicy::ReuseIfAllowed` in [ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1969). Codex is marked reusable because backend/model capabilities default to `supports_session_reuse = true` for all non-OpenRouter families in [ralph-burning-rewrite/src/shared/domain.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L137), [ralph-burning-rewrite/src/shared/domain.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L164), and that flag is exposed by [ralph-burning-rewrite/src/shared/domain.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L203); implementer/reviewer roles also allow reuse in [ralph-burning-rewrite/src/shared/domain.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L252).

But the integrated execution path only reuses sessions loaded from persistent storage: `AgentExecutionService` replaces `request.prior_session` with `load_reusable_session(...)` in [ralph-burning-rewrite/src/contexts/agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L171). Persistence only happens when `metadata.session_id` is present in [ralph-burning-rewrite/src/contexts/agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L248), and `SessionManager` drops `None` immediately in [ralph-burning-rewrite/src/contexts/agent_execution/session.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/session.rs#L89). `invoke_codex()` sets `session_id` to `None` for non-resume runs in [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L538), which is also asserted by the unit test in [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L484). The only service-level reuse test pre-seeds `sessions.json` manually in [ralph-burning-rewrite/tests/unit/agent_execution_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/agent_execution_test.rs#L434).

So, on a clean project, the real application path cannot bootstrap a reusable Codex session. Either persisting a real first-session ID or disabling Codex reuse claims would fix a genuine defect.

## Amendment: RBR-FR-20260314-03

### Position
ACCEPT

### Rationale
`check_availability()` does depend on an external `which` executable instead of searching `PATH` directly. The implementation shells out to `Command::new("which")` in [ralph-burning-rewrite/src/adapters/process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L591). If `which` itself is absent, that code returns `BackendUnavailable` from the spawn error path even when the target backend binary may be present. That is a real portability defect.

The existing unit test in [ralph-burning-rewrite/tests/unit/process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L287) already shows availability is modeled through `which`, not direct executable lookup. Replacing it with a direct `PATH` search would provide real technical benefit.
