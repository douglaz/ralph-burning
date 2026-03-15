---
artifact: completer-verdict
loop: 6
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-14T21:05:12Z
---

Reviewing `prompt.md` and the rewrite tree now. I’m validating from the code and tests rather than the loop status, then I’ll compare the implementation against the acceptance criteria and call out anything missing.
I’ve located the project-specific `prompt.md` under `.ralph/projects/process-backend-adapter/`. Next I’m reading the core implementation paths and test harnesses to compare the code against the stated requirements, not the loop summaries.
I’ve confirmed the main adapter exists and I’m now checking for requirement drift: exact command shapes, error classes, worktree threading, prompt composition inputs, and whether the tests actually cover the required paths instead of just smoke-testing them.
I’ve reached the point where the remaining risk is subtle mismatch, not missing files. I’m now checking behavior-level details: how `AgentExecutionService` wraps adapter results, whether daemon dispatch threads `execution_cwd` correctly, and whether any acceptance criterion is only partially implemented.
The remaining question is whether the current tree actually passes the acceptance checks. I’m running the repo’s required `nix develop` build/test commands now, then I’ll base the verdict on the code and the observed result together.
The build is still running inside `nix develop`. I’m waiting for the exit status before moving on to the test suite so the final verdict stays grounded in the actual acceptance path.
Unit tests are through the first batches cleanly and the larger integration/conformance set is still draining. I’m waiting on the final exit, since that’s the strongest signal for whether there’s any hidden gap left.
The long-running portion is still active with no failure output. I’m continuing to poll for the final status rather than inferring from the earlier loop artifacts.
# Verdict: COMPLETE

The project satisfies all requirements:
- Invocation model and dependencies: `InvocationRequest` now carries `working_dir`, workflow invocations use the workspace or leased worktree as the subprocess cwd, requirements drafting populates `working_dir` only for compile compatibility, and Tokio `process` support is enabled.
- Real subprocess adapter: `src/adapters/process_backend.rs` implements `ProcessBackendAdapter::new()`, stage-only capability checks, active-child tracking behind an async mutex, Claude/Codex subprocess execution, and cancellation via SIGTERM.
- Prompt/context assembly: backend stdin includes contract label, backend role, primary prompt text, pretty-printed `Context JSON` when present, and an explicit schema-only JSON return instruction.
- Claude backend support: the adapter builds the required `claude` command shape, writes stdin instead of argv, supports `--resume`, double-parses the outer envelope plus inner `result` JSON, and returns the required metadata/raw output fields.
- Codex backend support: the adapter builds the required `codex exec` and `codex exec resume` command shapes, writes schema and last-message temp files under `project_root/runtime/temp`, parses the last-message JSON, and best-effort cleans the temp files.
- Error handling and unsupported families: capability checks reject `Requirements`, `openrouter`, and `stub` with clear `CapabilityMismatch` messaging; availability checks detect missing binaries; spawn/non-zero exits map to transport failures; bad Claude/Codex JSON maps to schema validation failures.
- Adapter dispatch and CLI wiring: `BackendAdapter` delegates to stub or process adapters, `build_agent_execution_service()` is now fallible, `RALPH_BURNING_BACKEND` defaults to `process`, accepts only `stub` or `process`, preserves existing stub seams, and all run/daemon call sites handle the fallible builder.
- Workflow prompt enrichment: stage prompts now include the original project prompt, stage role/objective, prior same-run same-cycle outputs recovered from `journal.ndjson` in journal order, remediation and pending amendments when present, and the authoritative schema while preserving `payload.context`.
- Final review amendments are present: daemon-dispatched workflow runs thread the leased worktree through to `working_dir`, prior-output recovery uses visible journal filtering so rolled-back branch outputs are excluded, and subprocess stdin/stdout/stderr are handled concurrently to avoid deadlock.
- Acceptance validation: focused unit tests cover command construction, parsing, resume behavior, unsupported backends, missing binaries, cancellation, and prompt building; existing stub-based paths remain green; `nix develop -c cargo build -q` passed; `nix develop -c cargo test -q` passed.

---
