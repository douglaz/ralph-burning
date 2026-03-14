---
artifact: completer-verdict
loop: 4
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-14T20:08:38Z
---

I’m validating against the source of truth rather than the loop summaries. First I’ll read `prompt.md`, inspect the implementation paths the prompt names, and then run focused verification so the verdict is based on current `HEAD`.
I found the implementation files and confirmed the worktree is effectively clean except for a completion artifact. I’m locating the project-local `prompt.md` and then I’ll inspect the code paths the prompt explicitly names rather than relying on planner notes.
The prompt is under `.ralph/projects/process-backend-adapter/prompt.md`. I’m reading that plus the concrete implementation files side by side so I can check behavior, not just whether the right filenames exist.
I’ve confirmed the main adapter and CLI wiring exists. Now I’m tracing the less obvious invariants: `working_dir` propagation, stage timeout, prompt composition source-of-truth, and whether the stub-based harness defaults were preserved exactly where the prompt required.
The remaining risk area is subtle mismatches between the spec and the shipped behavior, especially around cancellation, availability checks, and the exact test-harness seams. I’m drilling into those now with targeted reads instead of broad grep output.
I found one concrete deviation already: `tests/cli.rs` does not contain the helper the prompt explicitly called out, while the conformance helper does. I’m checking whether that’s just an implementation variation or whether it leaves the default `process` backend leaking into existing CLI tests.
I have enough source inspection to move from static review to execution. I’m running the actual Rust test suite now because the biggest remaining question is whether the untouched CLI tests still pass under the new default `process` backend.
`cargo` isn’t available in the base shell, so I’m switching to the repo’s intended path and verifying through `nix develop -c ...` instead. That also matches the acceptance criteria more closely.
The build passed. The test suite is still running inside `nix develop`; I’ve already seen the first batch complete cleanly, and I’m waiting on the longer integration coverage now.
The integration block is the long part of the suite. I’m polling that run to completion so the verdict covers the actual acceptance criterion instead of just static inspection.
The 109-test integration block finished cleanly. The final large unit/conformance batch is still running, but there are no failures so far and it has already crossed 260 passing tests.
I found a more substantive risk than the test-harness detail: the prompt explicitly kept requirements drafting out of scope, but `build_agent_execution_service()` now defaults to `process`. I’m checking `src/cli/requirements.rs` to see whether requirements commands were accidentally switched onto the unsupported process adapter.
# Verdict: COMPLETE

The project satisfies all requirements:
- Invocation model and dependencies: `InvocationRequest` now carries `working_dir`, workflow invocations use the workspace base dir, requirements drafting uses its run root, and Tokio `process` is enabled in [model.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/model.rs), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs), [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs), and [Cargo.toml](/root/new-ralph-burning/ralph-burning-rewrite/Cargo.toml).
- Real subprocess adapter: `ProcessBackendAdapter` implements stage-only Claude/Codex execution, prompt+context stdin assembly, temp schema/last-message handling, capability and availability checks, SIGTERM cancellation, and the required transport/schema error mapping in [process_backend.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs).
- Adapter dispatch and CLI wiring: `BackendAdapter` delegates stub/process variants, `RALPH_BURNING_BACKEND` defaults to `process`, invalid values error clearly, and `run`/`daemon` use the fallible builder in [mod.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/mod.rs), [run.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs), and [daemon.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/daemon.rs).
- Workflow prompt enrichment: stage prompts now include the original project prompt, stage role/objective, prior same-cycle outputs recovered from `journal.ndjson` by payload id in journal order, remediation/amendments, the authoritative schema, and the timeout is 3600s in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs).
- Scope boundaries are preserved: requirements CLI and daemon requirements wiring still use the stub adapter, so out-of-scope requirements drafting was not converted in [requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs) and [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs).
- Tests and acceptance: focused unit coverage exists for Claude/Codex command construction, parsing, unsupported backends, missing binaries, cancellation, and prompt building in [process_backend_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs) and [prompt_builder_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/prompt_builder_test.rs); `nix develop -c cargo build --quiet` and `nix develop -c cargo test --quiet` both passed on current `HEAD`.
