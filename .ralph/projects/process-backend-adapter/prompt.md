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