---
artifact: final-review-planner-positions
loop: 12
project: p0-scope
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-17T23:02:28Z
---

# Planner Positions

## Amendment: FR-001 `[P1]`

### Position
ACCEPT

### Rationale
The bug is real. In stub_backend.rs the `(StageId::FinalReview, "voter")` translation maps both `approved` and non-`approved` outcomes to `"accept"`, so legacy `{"outcome": ...}` voter payloads can never produce a reject vote.

## Amendment: FR-002 `[P1]`

### Position
REJECT

### Rationale
`session_resuming` is computed directly from the same immutable `request.prior_session.is_some()` that is later read by `expect()`. There is no mutation path between those reads, and upstream service.rs only sets `prior_session` once before invocation, so this panic path is not a real reachable defect in the current code.

## Amendment: FR-003 `[P2]`

### Position
REJECT

### Rationale
The amendment misidentifies the failure point. By the time the relevant line in process_backend.rs runs, `request.contract.json_schema_value()` has already produced a `serde_json::Value` and already swallowed schema-conversion errors upstream in model.rs and panel_contracts.rs. Replacing this `to_string(...).unwrap_or_else("{}")` does not surface the claimed root cause, and on the current `Value` type this serialization step is not the meaningful risk.

## Amendment: FR-004 `[P2]`

### Position
REJECT

### Rationale
The files exist, but this is repository hygiene, not a correctness or safety defect. The amendment also misstates the repo state: there are currently 57 matching `*-impl-*.md` files under ralph-burning-rewrite, not 24, and no code under `src/` or `tests/` depends on them.

## Amendment: FR-005 `[P3]`

### Position
REJECT

### Rationale
The code is dead, but harmless. config.rs computes `base_backend_string` and then discards it; the called helper is a pure formatter in domain.rs. That is cleanup, not a real bug or robustness gap.

## Amendment: RBR-20260317-001

### Position
ACCEPT

### Rationale
The request shape is wrong for structured outputs. openrouter_backend.rs sends `response_format.type = "json_object"` with a raw schema, and the test locks that in. OpenRouter distinguishes JSON mode from structured outputs: `json_object` only guarantees valid JSON, while structured outputs require `type: "json_schema"` with `{ name, strict, schema }`. Current real requests therefore are not asking OpenRouter to enforce the contract schema.

## Amendment: RBR-20260317-002

### Position
ACCEPT

### Rationale
This is a real capability-check gap. Every OpenRouter invocation uses `response_format`, but `check_capability()` only verifies backend family and returns success for any OpenRouter model. OpenRouter documents structured outputs as model-dependent and exposed via model metadata, so the adapter currently reports success for configurations it may not actually support.

## Amendment: RBR-20260317-003

### Position
ACCEPT

### Rationale
The build break is real. `test-stub` is only declared in Cargo.toml, while the library gates stub-only surfaces in adapters/mod.rs, conformance_spec/mod.rs, and daemon_loop.rs. Running `nix develop -c cargo test -q` fails at compile time with unresolved imports from tests/cli.rs, agent_execution_test.rs, conformance_spec_test.rs, automation_runtime_test.rs, and several additional stub-backed tests. The default test path is genuinely broken.

## Amendment: RBR-20260317-004

### Position
REJECT

### Rationale
The current code intentionally preserves newline boundaries. final_review.rs normalizes whitespace per line and rejoins with `\n`, and an explicit test locks that behavior in. No in-repo prompt or spec requires whole-body whitespace collapse, and the render paths simply output the stored normalized body verbatim. This is an intentional canonicalization choice, not a demonstrated correctness bug.
