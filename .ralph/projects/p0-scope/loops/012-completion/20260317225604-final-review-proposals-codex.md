---
artifact: final-review-proposals
loop: 12
project: p0-scope
backend: codex
role: final_reviewer
created_at: 2026-03-17T22:56:04Z
---

Reviewing the diff against `129c17b84ef669c089d446d3701fe9cd67198756` and then tracing the main implementation files end-to-end. No listed skill applies here; this is a project code review, not skill creation or installation.
I have the full file list and the review surface is large, centered under `ralph-burning-rewrite/src`. Next I’m tracing the workflow, daemon, backend, and validation code paths and running tests where they help confirm whether an issue is real.
I’m switching from the file list to execution now. A failing test or compile check is often the fastest way to separate a real defect from a suspicious-looking change.
`cargo` is not on `PATH` in this environment, so I can’t use the standard Rust test loop directly. I’m checking whether the toolchain is installed elsewhere before I rely purely on static tracing.
The Nix shell is building a Rust toolchain, so I’m using the wait time to map file size and prioritize the highest-risk areas: workflow engine/state transitions, daemon/runtime, GitHub ingestion, and backend policy.
I’ve narrowed the review to the new policy and orchestration modules first. Those are where parity regressions usually show up as subtle state-machine mistakes rather than obvious syntax problems.
I’m in the final-review and resume-drift code now. One thing I’m checking carefully is whether the new aggregate results actually drive the workflow the same way the spec says, especially around restart caps and resume cleanup.
The prompt-review commit path is doing the right kind of staged write ordering, so I’m dropping deeper into the filesystem helpers now. If those helpers aren’t actually atomic or reversible, the engine-level invariants are weaker than they look.
The daemon and PR ingestion code is large but the patterns are becoming clear. I’m checking the Git and worktree pieces next, because checkpointing and rebase support can look fine in unit tests while still losing state on real repos.
I found a concrete external-integration issue in the OpenRouter adapter and I’m verifying it against OpenRouter’s own docs before I write it up. That kind of bug can pass local mocks and still fail in production.
The Nix-backed test run finished, and it did not compile. I’m pulling the full compiler log now so I can separate test-only breakage from any production-code issue that caused it.
I’ve got the default test-build failure pinned down. I’m doing one more compile pass with `--features test-stub` to see whether the breakage is just feature plumbing or whether there are additional regressions hiding behind it.
I’ve got enough for several concrete findings already. The extra `--features test-stub` compile pass is just to see whether there’s a second layer of breakage under the default test harness failure.
# Final Review: AMENDMENTS

## Amendment: RBR-20260317-001 [P0]

### Problem
[src/adapters/openrouter_backend.rs:130](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs#L130) builds OpenRouter requests with `response_format.type = "json_object"` and passes the raw schema directly at [src/adapters/openrouter_backend.rs:143](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs#L143). The test at [src/adapters/openrouter_backend.rs:704](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs#L704) locks that wire format in. OpenRouter’s structured-output API expects `type = "json_schema"` plus a `{ name, strict, schema }` wrapper, so the current implementation will not reliably enable or validate structured outputs on real requests. Source: https://openrouter.ai/docs/guides/features/structured-outputs

### Proposed Change
Serialize the documented `json_schema` payload shape and update the adapter tests to assert that exact request body.

### Affected Files
- [src/adapters/openrouter_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs) - fix the OpenRouter request payload and its request-shape tests.

## Amendment: RBR-20260317-002 [P1]

### Problem
[src/adapters/openrouter_backend.rs:306](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs#L306) implements `check_capability()` as a backend-family check that always returns `Ok(())` for any OpenRouter model. That means unsupported models pass preflight even though every OpenRouter call depends on structured outputs, which violates the required early capability checks and pushes failures into live invocation.

### Proposed Change
Make capability checks query or cache OpenRouter model metadata and reject targets that do not advertise structured-output / `response_format` support before a run starts.

### Affected Files
- [src/adapters/openrouter_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/openrouter_backend.rs) - implement real model capability validation instead of unconditional success.

## Amendment: RBR-20260317-003 [P1]

### Problem
The repo’s default test path no longer compiles. `test-stub` is only declared in [Cargo.toml:6](/root/new-ralph-burning/ralph-burning-rewrite/Cargo.toml#L6), while the library gates `StubBackendAdapter` and `BackendAdapter::Stub` behind that feature in [src/adapters/mod.rs:6](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/mod.rs#L6), gates `conformance_spec::scenarios` in [src/contexts/conformance_spec/mod.rs:5](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/conformance_spec/mod.rs#L5), and gates `build_requirements_service_for_test` in [src/contexts/automation_runtime/daemon_loop.rs:2499](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L2499). Integration tests still import those symbols directly at [tests/cli.rs:184](/root/new-ralph-burning/ralph-burning-rewrite/tests/cli.rs#L184), [tests/unit/agent_execution_test.rs:9](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/agent_execution_test.rs#L9), [tests/unit/conformance_spec_test.rs:5](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/conformance_spec_test.rs#L5), and [tests/unit/automation_runtime_test.rs:3310](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs#L3310). `nix develop -c cargo test -q` currently fails at compile time on those unresolved imports.

### Proposed Change
Either make the standard test/check entrypoints build with `test-stub`, or stop feature-gating those symbols out of integration-test builds. The default `cargo test`/Nix check path needs to compile the same surface the tests use.

### Affected Files
- [Cargo.toml](/root/new-ralph-burning/ralph-burning-rewrite/Cargo.toml) - wire `test-stub` into the test harness strategy.
- [flake.nix](/root/new-ralph-burning/ralph-burning-rewrite/flake.nix) - if the fix is build-system based, pass the feature in the check phase.
- [src/adapters/mod.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/mod.rs) - align stub adapter gating with integration-test usage.
- [src/contexts/conformance_spec/mod.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/conformance_spec/mod.rs) - align scenario exposure with integration-test usage.
- [src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) - align the daemon test seam with integration-test usage.

## Amendment: RBR-20260317-004 [P2]

### Problem
[src/contexts/workflow_composition/final_review.rs:79](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/final_review.rs#L79) normalizes amendment text per line and then rejoins with `\n`, and deduplication/ID generation uses that value at [src/contexts/workflow_composition/final_review.rs:119](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/final_review.rs#L119). The master prompt defines normalized bodies by collapsing internal whitespace runs to single spaces, so line-wrapped variants of the same amendment should hash to the same ID. The current implementation preserves newline boundaries, so equivalent amendments can survive as separate IDs and go through duplicate voting/restart paths. The embedded test at [src/contexts/workflow_composition/final_review.rs:1126](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/final_review.rs#L1126) codifies that wrong behavior.

### Proposed Change
Collapse internal whitespace across the whole body before hashing and deduplication, then update the unit test to match the required canonical form.

### Affected Files
- [src/contexts/workflow_composition/final_review.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/final_review.rs) - fix amendment normalization and the embedded normalization test.

---

## Verification
- `nix develop -c cargo test -q` fails during compilation in the default configuration.
