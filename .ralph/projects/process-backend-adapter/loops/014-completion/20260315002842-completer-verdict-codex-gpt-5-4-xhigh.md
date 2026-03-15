---
artifact: completer-verdict
loop: 14
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-15T00:28:42Z
---

Reviewing the repo against `prompt.md` from an implementation-validation perspective, not the planner’s loop summary. I’m starting with the canonical requirements and the live code/tests, then I’ll verify behavior with build/test evidence where possible.
I’ve confirmed the worktree is effectively clean apart from an untracked completion artifact. Next I’m checking the canonical prompt against the live adapter, workflow engine, CLI wiring, and the targeted tests that should prove each acceptance point.
The canonical `prompt.md` wasn’t at the repository root, so I’m locating the actual project prompt file rather than trusting the planner’s pasted copy. After that I’ll validate the code path-by-path against the literal prompt text.
The core implementation and test files do cover most of the prompt already. I’m now checking the edges the planner could have glossed over: exact command shapes, session metadata semantics, prompt recovery from `journal.ndjson`, and whether the acceptance criteria are actually exercised by the test harness defaults.
I’ve finished the static pass and haven’t found an obvious spec hole yet. I’m running the build and tests now to convert that into evidence, with special attention to the new adapter tests and the legacy stub-based paths that would regress if the backend defaulting is wrong.
`cargo` isn’t on the ambient PATH here, so I’m switching to the repo’s intended toolchain entrypoint and running the checks through `nix develop`. That will tell us whether the acceptance criteria hold in the supported environment, not just in this shell.
The focused adapter and prompt-builder suites are green. I’m doing one full `cargo test` pass now because the real risk here is collateral damage in the legacy CLI/conformance tests after changing the default backend selector.
The full suite is still running in the conformance-heavy section. So far the critical signal is positive: the CLI tests that would catch broken `stub` defaulting and broken workflow execution paths are passing, not just the new unit tests.
The remaining runtime is in the broad conformance tests, which is where a hidden regression would show up. I’m waiting this out rather than inferring from targeted suites, because the completion claim depends on the old test harness still behaving under `RALPH_BURNING_BACKEND=stub`.
# Verdict: COMPLETE

The project satisfies all requirements:
- Invocation model and dependencies: `InvocationRequest` includes `working_dir`, workflow execution uses the workspace/worktree cwd, requirements drafting still compiles with its run root, and Tokio `process` support is enabled.
- Real subprocess adapter: `ProcessBackendAdapter` exists, supports only workflow-stage contracts, assembles stdin from prompt plus context, tracks active children, supports cancellation, and maps transport/schema failures as required.
- Claude backend: command shape, stdin usage, resume handling, double-parse of the outer envelope and inner `result`, and metadata behavior match the prompt.
- Codex backend: new-session and resume command shapes, temp schema/last-message files under `project_root/runtime/temp`, stdin usage, parsed last-message payloads, cleanup, and fresh-vs-resume session metadata all match the prompt.
- Capability and availability behavior: `claude`/`codex` are the only supported process families, requirements contracts are rejected with a stage-only message, and `openrouter`/`stub` fail preflight with the required configuration guidance.
- Adapter dispatch and CLI wiring: `BackendAdapter` delegates to stub or process backends, `build_agent_execution_service()` is fallible, defaults `RALPH_BURNING_BACKEND=process`, supports `stub|process`, and `run start`, `run resume`, and daemon workflow dispatch all use it.
- Workflow prompt enrichment: stage prompts now include the project prompt, stage role/objective, prior same-cycle outputs recovered from `journal.ndjson` in journal order, remediation/amendments, and authoritative schema text; timeout is increased to 3600 seconds.
- Test harness protection: conformance helper defaults `RALPH_BURNING_BACKEND=stub`, legacy stub-based CLI/conformance behavior remains green, and focused unit coverage exists for command construction, parsing, unsupported backends, cancellation, and prompt building.
- Acceptance evidence: `nix develop -c cargo build` passed, and `nix develop -c cargo test` passed in full (`109` CLI tests and `492` unit tests, all green).

---
