---
artifact: completer-verdict
loop: 10
project: p0-scope
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-17T18:45:42Z
---

Reviewing `prompt.md`, the current codebase, and test/conformance coverage from the implementation side rather than the loop ledger. I’m starting with the repo layout and the prompt source, then I’ll trace each P0 requirement to concrete code and verification evidence.
The wrapper repo contains the actual Rust project under `ralph-burning-rewrite`. I didn’t find a top-level `prompt.md`, so I’m locating the authoritative prompt file inside the workspace metadata and then I’ll validate code against it directly.
I’ve found the authoritative P0 prompt under `.ralph/projects/p0-scope/prompt.md`. Next I’m mapping the definition of done to concrete source files and tests, then I’ll look for gaps where the code or conformance coverage does not actually meet the prompt.
I have the full P0 contract. Now I’m checking whether the implementation is present in the expected modules and whether every required scenario ID from the prompt exists in unit or conformance tests, before I trust the loop approvals.
The test inventory is broad and the full suite enumerates cleanly. I’m now checking the harder part: whether every scenario the prompt explicitly requires is actually present in the conformance/unit suite, and then I’ll spot-check the code paths behind the highest-risk items.
I’ve confirmed the prompt’s scenario IDs are represented in the repo. Next I’m running the actual regression suite, not just listing tests, and then I’ll inspect the highest-risk production paths: backend selection, resume drift/prompt-change, checkpoints, and the daemon/GitHub runtime.
The full suite is still running through the large unit/conformance set. While that finishes, I’m reading the production modules to verify the implementation paths are real and not satisfied only by tests.
I’ve found one concrete architecture-risk already: the shared production builder still contains a live `StubBackendAdapter` branch behind `RALPH_BURNING_BACKEND=stub`. I’m checking whether that is reachable from the normal CLI/daemon paths, because the prompt explicitly says production code must not instantiate the stub backend.
The regression suite completed cleanly. I’m doing one last CLI surface check for the prompt’s required command shapes, then I’ll issue the verdict based on the remaining spec gap rather than the passing tests alone.
# Verdict: CONTINUE

## Missing Requirements
1. `Production code must not directly instantiate StubBackendAdapter`: this non-negotiable rule is explicit in [prompt.md](/root/new-ralph-burning/.ralph/projects/p0-scope/prompt.md#L15), but the shared production builder still accepts `RALPH_BURNING_BACKEND=stub` and constructs `BackendAdapter::Stub(build_stub_backend_adapter())` in [agent_execution_builder.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/composition/agent_execution_builder.rs#L35) and [agent_execution_builder.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/composition/agent_execution_builder.rs#L49). That builder is used by the real CLI requirements path in [requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs#L40), the real run path in [run.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs#L72), and daemon startup in [daemon.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/daemon.rs#L193). `cargo test` passes, but this still leaves a production-reachable stub backend path, so the prompt’s architecture constraint is not fully satisfied.

## Recommended Next Features
1. Remove the `stub` branch from the production `build_backend_adapter()` path and move stub construction behind a test-only API or `#[cfg(test)]` helper, so `run`, `requirements`, and `daemon` can only select real backends at runtime.

---
