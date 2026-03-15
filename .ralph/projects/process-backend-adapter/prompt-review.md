---
artifact: prompt-review
project: process-backend-adapter
backend: codex
role: prompt_reviewer
created_at: 2026-03-14T17:19:07Z
---

I’m validating the prompt against the referenced code so the review is grounded in the actual interfaces and call sites. I’ll inspect the adapter trait, the current stub adapter, CLI wiring, and the engine prompt/timeout code first.
The referenced paths are not at repo root; I’m locating the actual crate layout before reviewing the prompt details. Then I’ll reconcile the prompt’s file references with what exists.
I found the crate under `ralph-burning-rewrite/`. Next I’m reading the actual trait and engine code to spot where the original prompt is underspecified or mismatched with the implementation surface.
I have the main integration points. I’m drilling into the request/envelope types and the engine call site now, because several ambiguities depend on exact field names and existing prompt context.
The main remaining question is what “prompt enrichment” can realistically pull from existing data structures without inventing new plumbing. I’m checking the current invocation context and project record model to see what the prompt can actually include.
I’ve confirmed the engine already passes remediation and amendments only in `payload.context`. I’m checking session persistence next, because the original prompt says what to return from the adapter but not how that interacts with the existing session manager.
The missing pieces are mostly around error semantics and unsupported backends. I’m checking the error types now so the rewritten prompt can specify how subprocess failures should map into existing `AppError` variants instead of leaving that to guesswork.
One important ambiguity is whether this adapter must handle requirements-generation calls too, not just workflow stages. The trait is domain-neutral, and `InvocationContract` has a `Requirements` branch, so I’m checking whether that path is exercised through the same service.
A larger integration gap showed up: the default `QaValidator` backend is `OpenRouter`, while the proposed adapter only supports `claude` and `codex`. That means most flows would still fail under the new default unless the prompt either adds `OpenRouter` support or explicitly changes backend selection policy.
I’ve got enough to rewrite the prompt accurately. I’m doing one last pass on how project prompts are read from disk so the refined prompt can name the right inputs and edge cases for the engine prompt builder.
One more structural check: the prompt asks for richer engine prompts, but the current stage execution helper only receives `project_root`, cursor, and remediation context. I’m verifying whether the required data is already available there or if the function signatures must also change.
The test constraint needs tightening too. The prompt says existing tests won’t be affected because they use the stub directly, but CLI and daemon tests may still hit `build_agent_execution_service()` and would start defaulting to `process`. I’m checking whether that assumption is actually true.
I want to sanity-check the backend commands in the prompt itself before I rewrite it. If the cited CLI syntax is wrong, that’s a feasibility problem in the prompt, not just an implementation detail.
The local CLI check found a concrete problem: the Codex command in the original prompt does not match the installed CLI surface. I’m checking the session-resume subcommand too so the refined prompt can describe a viable Codex invocation path instead of a guessed one.
The adapter prompt path also needs to say what happens to `InvocationPayload.context`; otherwise remediation/amendment data never reaches the real backend. I’m checking existing dependencies so the refined prompt can safely require temp files for Codex schema/output handling if needed.
I have enough to write the review. I’m folding in the concrete feasibility findings from the local CLI help and the repo shape so the refined prompt is implementable without hidden follow-up decisions.
A subtle prompt-enrichment issue showed up: `PayloadRecord` does not store `run_id`, so “prior stage outputs from the current cycle” is ambiguous if you just scan `history/payloads/`. I’m checking whether the payload ID format or journal already provides a stable way to recover the current-run records.
I found a reliable way to identify prior outputs: use `stage_completed` journal events for the current `run_id` and cycle, then load the referenced payload IDs. I’m wrapping that into the refined prompt so prompt enrichment is testable instead of hand-wavy.
# Prompt Review

## Issues Found
- The scope says "self-hosting readiness" but only the workflow `run` path is wired; `requirements` CLI and daemon requirements drafting still construct stub-backed services directly. This matters because the prompt overstates the user-visible outcome.
- The prompt ignores that `QaValidator` defaults to `OpenRouter`. A Claude/Codex-only adapter cannot execute most built-in flows unless the prompt explicitly defines unsupported-family behavior or required config overrides.
- The Codex invocation spec does not match the installed CLI surface. In this environment, `codex exec` uses `--output-schema <FILE>` and `--output-last-message <FILE>` and optional `--json` JSONL events, not "stdout is JSON directly".
- The subprocess working directory is undefined. `InvocationRequest.project_root` currently points at `.ralph-burning/projects/<id>`, which is a record directory, not the repo/worktree root where Claude/Codex should edit files.
- The prompt never defines how `InvocationPayload.prompt` and `InvocationPayload.context` are merged before invoking the backend. Without that, remediation and amendment context can be silently dropped.
- Timeout and cancellation are specified at the wrong layer. `AgentExecutionService::invoke()` already owns timeout and cancellation; duplicating that inside the adapter creates races and unclear responsibility.
- Error mapping is underspecified. Missing executables, unsupported backend families, non-zero exits, invalid outer envelopes, invalid inner JSON, and cancellation all need explicit `AppError` and `FailureClass` behavior to keep retries deterministic.
- Prompt enrichment asks for project prompt and prior stage outputs but does not define how to recover "current cycle" data. The durable source is current-run `stage_completed` journal events and their `payload_id`s, not a blind scan of `history/payloads/`.
- The testing claim is inaccurate. Many CLI and conformance tests shell out to `run start`, `run resume`, and `daemon start` without a backend override, so defaulting to `process` will break them unless the harness is updated.
- Required dependency and API changes are missing. `tokio::process::Command` needs the Tokio `process` feature, SIGTERM needs an explicit implementation choice, and Codex session behavior is incomplete for new vs resumed sessions.

## Refined Prompt
### Implement ProcessBackendAdapter for workflow-stage subprocess backends

#### Objective
Implement a real subprocess-backed adapter under `ralph-burning-rewrite/` so workflow stage execution can invoke local `claude` and `codex` CLIs instead of the stub adapter.

This issue covers workflow runs only:
- `run start`
- `run resume`
- daemon-dispatched workflow runs that use `build_agent_execution_service()`

This issue does not convert requirements drafting to real backends yet.

#### Scope and Non-Goals
In scope:
- `InvocationContract::Stage` only
- Claude and Codex backend families only
- workflow prompt enrichment in `src/contexts/workflow_composition/engine.rs`
- CLI wiring in `src/cli/run.rs`
- adapter dispatch in `src/adapters/mod.rs`
- tests for the new adapter and test-harness updates needed to keep existing stub-based tests green

Out of scope:
- `src/cli/requirements.rs`
- daemon requirements-drafting wiring in `src/contexts/automation_runtime/daemon_loop.rs`
- OpenRouter subprocess support

Behavior for out-of-scope backend families:
- If the resolved target family is `openrouter` or `stub`, the process adapter must fail preflight with `AppError::CapabilityMismatch`.
- The error detail must clearly say that `ProcessBackendAdapter` currently supports only `claude` and `codex`, and that self-hosted workflow runs require configuring `default_backend=claude` or `default_backend=codex`.

#### Paths
All paths below are relative to `ralph-burning-rewrite/`.

#### Existing Code Facts
- `AgentExecutionPort` is defined in `src/contexts/agent_execution/service.rs`.
- `InvocationRequest` is defined in `src/contexts/agent_execution/model.rs`.
- `build_agent_execution_service()` currently returns `AgentExecutionService<StubBackendAdapter, ...>` from `src/cli/run.rs`.
- `AgentExecutionService::invoke()` already performs the authoritative timeout and cancellation handling. Do not add a second timeout wrapper inside the adapter.
- `InvocationRequest.project_root` currently points at `.ralph-burning/projects/<project_id>`, which is used for raw-output and session persistence, not as the repo working directory.
- Current workflow defaults include `QaValidator -> OpenRouter`, so unsupported-family behavior must be explicit.
- `Cargo.toml` currently lacks Tokio's `process` feature.

#### Required Changes

1. Update the invocation model and dependencies.
- Add `working_dir: PathBuf` to `InvocationRequest`.
- Keep `project_root` for raw-output persistence and session storage.
- Use `working_dir` as the subprocess current directory.
- In workflow execution, populate `working_dir` with the workspace base dir (`base_dir`), not `.ralph-burning/projects/<id>`.
- Update the requirements-drafting constructor to populate `working_dir` with its run root so the code compiles, but do not otherwise change requirements wiring.
- Enable Tokio's `process` feature in `Cargo.toml`.
- Add a Unix signal helper dependency if needed to send SIGTERM cleanly.

2. Add `src/adapters/process_backend.rs`.
- Implement `ProcessBackendAdapter`.
- Provide `ProcessBackendAdapter::new()`.
- Track active children by `invocation_id` so `cancel()` can signal them.
- Use an async-safe mutex for the active-child map.

3. Define supported contracts and prompt assembly.
- `ProcessBackendAdapter` supports only `InvocationContract::Stage`.
- For `InvocationContract::Requirements`, return `AppError::CapabilityMismatch` with a clear "workflow stages only in this issue" message.
- Build one backend input string from both `request.payload.prompt` and `request.payload.context`.
- The backend input must include:
  - contract label
  - backend role
  - primary instructions from `request.payload.prompt`
  - a pretty-printed `Context JSON` section when `request.payload.context` is not null
  - an explicit instruction to return only JSON that matches the supplied schema
- Do not ignore `request.payload.context`.

4. Implement Claude subprocess execution.
- Backend family: `BackendFamily::Claude`
- Binary: `claude`
- Command:
  - `claude -p --output-format json --model <model_id> --permission-mode acceptEdits --allowedTools Bash,Edit,Write,Read,Glob,Grep --json-schema <schema_json>`
- If `request.session_policy == SessionPolicy::ReuseIfAllowed` and `request.prior_session` is present, add `--resume <session_id>`.
- Write the assembled backend input to stdin rather than argv.
- Parse stdout as the Claude JSON envelope.
- Expected outer envelope shape:
  - `type`
  - `result`
  - `session_id`
  - optional telemetry fields such as `cost_usd` and `duration_ms`
- The `result` field is itself a JSON string. Parse it into `serde_json::Value`.
- Return:
  - `raw_output_reference = RawOutputReference::Inline(stdout_text)`
  - `parsed_payload = <double-parsed result JSON>`
  - `metadata.invocation_id = request.invocation_id`
  - `metadata.token_counts = TokenCounts::default()`
  - `metadata.backend_used = request.resolved_target.backend.clone()`
  - `metadata.model_used = request.resolved_target.model.clone()`
  - `metadata.attempt_number = request.attempt_number`
  - `metadata.session_id = envelope.session_id.or_else(|| request.prior_session.as_ref().map(|s| s.session_id.clone()))`
  - `metadata.session_reused = request.prior_session.is_some() && matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)`
  - `metadata.duration = Duration::from_millis(0)` and let `AgentExecutionService` overwrite it later

5. Implement Codex subprocess execution using the installed CLI shape.
- Backend family: `BackendFamily::Codex`
- Binary: `codex`
- New-session command shape:
  - `codex exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --model <model_id> --output-schema <schema_path> --output-last-message <message_path> -`
- Resume command shape:
  - `codex exec resume --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --model <model_id> --output-schema <schema_path> --output-last-message <message_path> <session_id> -`
- Create the schema file under `<project_root>/runtime/temp/<invocation_id>.schema.json`.
- Create the last-message file under `<project_root>/runtime/temp/<invocation_id>.last-message.json`.
- Write the assembled backend input to stdin.
- After the process exits successfully, read the last-message file and parse it as JSON.
- Return:
  - `raw_output_reference = RawOutputReference::Inline(last_message_text)`
  - `parsed_payload = parsed JSON from the last-message file`
  - `metadata.token_counts = TokenCounts::default()`
  - `metadata.session_reused = true` only when `codex exec resume` was used
  - `metadata.session_id = Some(prior_session.session_id.clone())` when resuming, otherwise `None`
- Best-effort delete the temp schema and last-message files after reading them.

6. Implement availability, capability, cancellation, and error mapping.
- `check_capability`:
  - succeed only for `BackendFamily::Claude` and `BackendFamily::Codex` with `InvocationContract::Stage`
  - reject `OpenRouter`, `Stub`, and `InvocationContract::Requirements` with `AppError::CapabilityMismatch`
- `check_availability`:
  - for Claude, verify `claude` exists on `PATH`
  - for Codex, verify `codex` exists on `PATH`
  - on missing binary, return `AppError::BackendUnavailable`
- `invoke`:
  - do not add another timeout wrapper; `AgentExecutionService` already does that
  - spawn with `tokio::process::Command`
  - pipe stdin, capture stdout and stderr
  - register the child before awaiting completion and remove it from the map afterward
- `cancel`:
  - if the child is still running, send SIGTERM
  - if the invocation is already gone, return `Ok(())`
- Error mapping inside the adapter:
  - spawn failure or non-zero exit -> `AppError::InvocationFailed { failure_class: FailureClass::TransportFailure, ... }`
  - invalid Claude outer envelope JSON -> `AppError::InvocationFailed { failure_class: FailureClass::SchemaValidationFailure, ... }`
  - invalid Claude `result` JSON string -> `AppError::InvocationFailed { failure_class: FailureClass::SchemaValidationFailure, ... }`
  - invalid Codex last-message JSON -> `AppError::InvocationFailed { failure_class: FailureClass::SchemaValidationFailure, ... }`
  - include stderr text in transport-failure details when available

7. Add adapter dispatch in `src/adapters/mod.rs`.
- Export `pub mod process_backend;`
- Add:
  - `pub enum BackendAdapter { Stub(StubBackendAdapter), Process(ProcessBackendAdapter) }`
- Implement `AgentExecutionPort` for `BackendAdapter` by delegating to the active variant.

8. Update CLI wiring in `src/cli/run.rs`.
- Change `build_agent_execution_service()` to return `AppResult<AgentExecutionService<BackendAdapter, FsRawOutputStore, FsSessionStore>>`.
- Read `RALPH_BURNING_BACKEND`.
- Default value: `process`
- Supported values:
  - `stub`
  - `process`
- Any other value must return a clear error.
- `stub` branch:
  - preserve all existing `RALPH_BURNING_TEST_*` seams exactly as they work today
- `process` branch:
  - construct `BackendAdapter::Process(ProcessBackendAdapter::new())`
- Update all call sites in `src/cli/run.rs` and `src/cli/daemon.rs` to handle the fallible builder with `?`.

9. Enrich workflow prompts in `src/contexts/workflow_composition/engine.rs`.
- Replace the placeholder `Execute stage: <Stage>` prompt with a helper that builds a real stage prompt.
- The prompt builder must load:
  - the project prompt file from `<project_root>/<project_record.prompt_reference>`
  - the stage role and a short stage-specific objective
  - prior completed stage payloads from the same `run_id` and same cycle
  - remediation context and pending amendments
  - the JSON schema for the stage contract
- Recover prior same-cycle outputs by:
  - reading `journal.ndjson`
  - selecting `stage_completed` events whose `run_id` matches the current run and whose `cycle` matches `cursor.cycle`
  - using the referenced `payload_id`s to load payload records from `history/payloads/`
  - preserving journal order
- Do not identify current-cycle payloads by scanning `history/payloads/` alone.
- The prompt text should include:
  - the original project prompt
  - a concise role instruction for the current stage
  - a "Prior Stage Outputs This Cycle" section when applicable
  - a "Remediation / Pending Amendments" section when applicable
  - a pretty-printed schema section with a note that the schema is authoritative
- Keep `payload.context` populated as before; prompt enrichment is additive, not a replacement for context plumbing.

10. Increase workflow timeout.
- Change the workflow stage timeout in `engine.rs` from `Duration::from_secs(300)` to `Duration::from_secs(3600)`.

#### Tests
Add focused tests without calling real external services.

Required tests:
- `ProcessBackendAdapter` unit tests using fake `claude` and `codex` executables placed earlier on `PATH`
- Claude command-construction test
- Claude double-parse test
- Claude resume-flag test
- Codex command-construction test using schema and last-message temp files
- Unsupported-family test showing clear `CapabilityMismatch` for `OpenRouter`
- Missing-binary availability test
- Cancellation test proving `cancel()` sends SIGTERM to a long-running fake child
- Prompt-builder test that verifies project prompt, prior same-cycle outputs, remediation, amendments, and schema text are included

Test-harness updates required to keep existing tests green:
- Update `tests/cli.rs` helper `run_cli_with_env()` so it injects `RALPH_BURNING_BACKEND=stub` by default unless the caller overrides it.
- Update the analogous helper in `src/contexts/conformance_spec/scenarios.rs` the same way.
- Do not rewrite existing stub-based CLI/conformance expectations to use real subprocesses.

#### Acceptance Criteria
- `run start`, `run resume`, and workflow daemon paths can use real Claude/Codex subprocesses when `RALPH_BURNING_BACKEND=process`.
- Unsupported `openrouter` targets fail preflight with a clear configuration message instead of falling back silently.
- Existing stub-based tests still pass by running under `RALPH_BURNING_BACKEND=stub`.
- New unit tests cover command construction, parsing, unsupported backends, and cancellation.
- `nix develop -c cargo build` passes.
- `nix develop -c cargo test` passes.
