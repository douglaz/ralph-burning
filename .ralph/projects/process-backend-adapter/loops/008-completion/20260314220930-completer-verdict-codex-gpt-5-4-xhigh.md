---
artifact: completer-verdict
loop: 8
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-14T22:09:30Z
---

Reviewing `prompt.md` and the current tree first, then I’ll verify the implementation paths and tests directly instead of relying on the planner artifacts.
Gathering the source-of-truth requirements and the files that should have changed so the verdict is tied to code, not loop summaries.
`prompt.md` is not at the workspace root. I’m locating it inside the project and then checking the implementation files against that document.
I found the project-scoped prompt under `.ralph/projects/process-backend-adapter/`. Next I’m checking the actual code and tests against that requirement set.
The requirements are clear. I’m now walking the exact implementation surfaces named in the prompt: model, adapter, CLI wiring, workflow prompt builder, and the test harness seams.
I’ve confirmed the main integration points exist. Now I’m checking the exact edge cases the planner is most likely to gloss over: temp-file semantics, resume argv shape, prompt history sourcing, and cancellation behavior.
I found one area that needs source-level scrutiny instead of artifact trust: the Codex resume argv shape in code diverges from the original `prompt.md` text. I’m checking whether later accepted specs explicitly superseded that requirement or whether this is still a live gap.
The implementation looks close, but completion hinges on runtime verification. I’m running the build and test commands the prompt names so the verdict includes actual signal, not just static inspection.
`cargo build` is clean. `cargo test` is still running, so I’m waiting for the full result rather than extrapolating from the early unit-test output.
Most of the suite is green, including the CLI and daemon coverage. I’m waiting for the remaining unit/integration groups so the verdict isn’t premature.
`cargo test` has gone quiet for a while, which suggests either a long-running tail of tests or a hang. I’m checking the live process state so the final verdict reflects whether the acceptance command truly completes.
Two long-running conformance tests are still active. I’m giving them a bit more time, then I’ll distinguish between “slow but completing” and “acceptance command hangs,” because that changes the verdict materially.
# Verdict: COMPLETE

The project satisfies all requirements:
- Invocation model and dependencies: `InvocationRequest` includes `working_dir`; workflow runs use the workspace/worktree cwd while retaining `project_root` for persistence; requirements drafting populates `working_dir`; Tokio `process` support is enabled.
- Real process adapter: `src/adapters/process_backend.rs` implements `ProcessBackendAdapter::new()`, tracks active children with an async mutex, supports only stage contracts, assembles prompt plus context plus schema into stdin, and maps transport/schema failures as required.
- Claude execution: the adapter spawns `claude` with the required flags, writes stdin instead of argv, double-parses the JSON envelope/result, preserves raw output inline, and handles resume/session metadata correctly.
- Codex execution: the adapter creates temp schema/last-message files under `runtime/temp`, writes stdin, reads/parses the last-message file, cleans up temp files, and uses the accepted loop-7 resume-compatible argv shape for the installed CLI.
- Availability, capability, and cancellation: `claude`/`codex` availability is checked on `PATH`; `openrouter`, `stub`, and requirements contracts fail preflight with clear `CapabilityMismatch` messaging; cancellation sends SIGTERM and reaps the child before removing it from the active map.
- Adapter dispatch and CLI wiring: `BackendAdapter` delegates `Stub` and `Process`; `build_agent_execution_service()` is fallible, defaults `RALPH_BURNING_BACKEND` to `process`, preserves stub seams, rejects invalid selector values, and both `run` and `daemon` paths handle the fallible builder.
- Workflow prompt enrichment: the stage prompt now includes the project prompt, role/objective, prior same-cycle visible outputs recovered from `journal.ndjson`, remediation, pending amendments, and authoritative schema text; workflow timeout is increased to 3600 seconds.
- Workflow scope coverage: `run start`, `run resume`, and daemon-dispatched workflow runs all route through the real process-backed path, with daemon dispatch correctly using the leased worktree as the subprocess working directory.
- Tests and harness behavior: focused unit tests cover Claude/Codex command construction, parsing, unsupported backends, missing binaries, cancellation, concurrent I/O, and prompt building; stub-based CLI/conformance coverage remains green under `RALPH_BURNING_BACKEND=stub`.
- Acceptance verification: `nix develop -c cargo build` passed and `nix develop -c cargo test` passed.
