# Bead fyj: Use configured backend timeout for requirements invocations

## Problem description

`RequirementsCliService::invoke_backend` in
`src/contexts/requirements_drafting/service.rs:2125-2170` uses a hardcoded
`Duration::from_secs(300)` for the `timeout` field of `InvocationRequest`
(line 2161). This ignores the operator's configured `timeout_seconds` /
`role_timeouts.<role>` in `workspace.toml`.

Workflow stage invocations already resolve the timeout through
`BackendPolicyService::timeout_for_role(backend, policy_role)`
(`src/contexts/agent_execution/policy.rs:168-175`). Requirements invocations
should do the same, not carry a hardcoded stopgap.

## Required fix

1. Plumb a way for the requirements service to call
   `timeout_for_role`. Two reasonable shapes:
   - Pass an `&BackendPolicyService` (or `&EffectiveConfig`) through the
     existing `agent_service` dependency so we can resolve timeouts without
     widening the public CLI signature, OR
   - Expose a small helper on `AgentService` / its adapter that returns
     `Duration` for a given `ResolvedBackendTarget` + `BackendRole`.
   Prefer the option that requires the smallest public-API change.
2. Replace line 2161:
   ```rust
   timeout: std::time::Duration::from_secs(300),
   ```
   with the resolved per-role timeout. The role passed into
   `invoke_backend` is a `BackendRole` (planner/implementer/reviewer/qa/
   completion_judge). Map it to the appropriate `BackendPolicyRole` (the
   requirements stages themselves are not in `StageId`, so
   `stage_to_policy_role` does not apply — a direct `BackendRole ->
   BackendPolicyRole` mapping is the right choice). A reasonable mapping:
   - `BackendRole::Planner` → `BackendPolicyRole::Planning`
   - `BackendRole::Implementer` → `BackendPolicyRole::Implementer`
   - `BackendRole::Reviewer` → `BackendPolicyRole::Reviewer`
   - `BackendRole::QaValidator` → `BackendPolicyRole::Qa`
   - `BackendRole::CompletionJudge` → `BackendPolicyRole::Completer`
3. Use `target.backend.family` (the already-resolved backend on the same
   request) when calling `timeout_for_role`, so operators who override a
   family or role see that override honored.

If you introduce a `fn BackendRole::to_policy_role(self) ->
BackendPolicyRole`, put it in `src/shared/domain.rs` next to the other
`BackendRole` methods so it is reusable.

## Tests

- Unit test in the nearest existing requirements test module
  (`tests/unit/requirements_drafting_test.rs`) that exercises
  `invoke_backend` with a config that sets a non-default timeout and asserts
  the `InvocationRequest.timeout` passed to the adapter matches.
- If `invoke_backend` is not directly unit-testable, add a smaller unit
  test for the `BackendRole -> BackendPolicyRole` mapping and rely on
  integration coverage for the wiring.
- Do NOT add a blanket "timeout is exactly 3600s" assertion — the whole
  point is that the value comes from config.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files. The same applies to `.beads/` — durable bead state, not
code.

## Acceptance criteria

- No hardcoded `Duration::from_secs(300)` (or any other hardcoded second
  literal used as the primary timeout) left in
  `src/contexts/requirements_drafting/service.rs`.
- `invoke_backend` reads the timeout from the resolved backend/role config,
  matching how workflow stages do.
- `nix build` passes on the final tree (authoritative gate).
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
