# Implement ProcessBackendAdapter for self-hosting readiness

## Objective

Implement a real backend adapter in `ralph-burning-rewrite/` that invokes `claude` and `codex` CLI tools as subprocesses, replacing the stub-only `AgentExecutionPort` implementation. This enables the tool to orchestrate real AI backends and self-host.

## Problem (GitHub #9)

The `agent_execution` context only has a `StubBackendAdapter` — both `run start` and `daemon start` use it via `build_agent_execution_service()` in `src/cli/run.rs:86-132`. The tool cannot invoke real AI backends. Additionally:
- The engine prompt is a placeholder: `"Execute stage: Planning"` (`engine.rs:1633`)
- The invocation timeout is 300s (`engine.rs:1636`) — too short for real backend calls

## Required Changes

### 1. ProcessBackendAdapter (new file: `src/adapters/process_backend.rs`)

Implement `AgentExecutionPort` (defined in `src/contexts/agent_execution/service.rs:103-115`) by shelling out to CLI tools. Use `src/adapters/stub_backend.rs` as the reference pattern for implementing the trait.

**For Claude backend (`BackendFamily::Claude`):**
- Command: `claude -p --output-format json --model <model_id> --permission-mode acceptEdits --allowedTools Bash,Edit,Write,Read,Glob,Grep`
- Pass `--json-schema <schema_json>` using `StageContract::json_schema()` (from `src/contexts/workflow_composition/contracts.rs:101`) serialized to JSON string
- Parse claude's JSON output envelope: `{"type": "result", "result": "...", "session_id": "...", "cost_usd": ..., "duration_ms": ...}`
- Double-parse: the `result` field is the stage payload as a JSON string — parse it into `serde_json::Value`
- Session reuse: if `request.session_policy == SessionPolicy::ReuseIfAllowed` and `request.prior_session` is Some, add `--resume <session_id>` flag
- Return `InvocationEnvelope` with:
  - `raw_output_reference`: `RawOutputReference::Inline(stdout_text)`
  - `parsed_payload`: the double-parsed result JSON
  - `metadata`: `InvocationMetadata` with invocation_id, duration, session_id from claude response, backend/model used

**For Codex backend (`BackendFamily::Codex`):**
- Command: `codex exec --dangerously-bypass-approvals-and-sandbox -`
- Pass prompt via stdin
- Parse stdout as JSON directly

**Subprocess management:**
- Use `tokio::process::Command` for async subprocess spawning
- Pipe stdin (for prompt), capture stdout and stderr
- Timeout via `tokio::time::timeout(request.timeout, ...)`
- Cancellation: use `tokio::select!` between process completion and `request.cancellation_token.cancelled()`
- Track child processes in `Arc<Mutex<HashMap<String, tokio::process::Child>>>` for `cancel()` method
- On cancel, send SIGTERM to the child process

**Trait methods:**
- `check_availability`: verify command exists on PATH (e.g., `which claude`)
- `check_capability`: always succeed for Claude (supports all contract families via --json-schema)
- `invoke`: core method as described above
- `cancel`: look up child process by invocation_id, send SIGTERM

### 2. BackendAdapter enum dispatch (modify `src/adapters/mod.rs`)

Add `pub mod process_backend;` and create an enum:

```rust
pub enum BackendAdapter {
    Stub(StubBackendAdapter),
    Process(ProcessBackendAdapter),
}
```

Implement `AgentExecutionPort` for `BackendAdapter` by delegating to the inner variant.

### 3. Wire into CLI (modify `src/cli/run.rs`)

Change `build_agent_execution_service()` to:
- Check env var `RALPH_BURNING_BACKEND` (default: `"process"`)
- If `"stub"`: build `BackendAdapter::Stub(StubBackendAdapter {...})` with existing env-var overrides
- If `"process"`: build `BackendAdapter::Process(ProcessBackendAdapter::new())`
- Return type changes from `AgentExecutionService<StubBackendAdapter, ...>` to `AgentExecutionService<BackendAdapter, ...>`

`src/cli/daemon.rs` already calls `build_agent_execution_service()` and gets the change for free.

### 4. Prompt enrichment (modify `engine.rs:1633`)

Replace `format!("Execute stage: {}", stage_id.display_name())` with a rich prompt that includes:
- The project's prompt file content (load from project record's prompt_reference path)
- Stage role description and what the AI should produce
- Prior stage outputs from the current cycle (for review/QA stages — load from history/payloads/)
- Remediation context and pending amendments (already in `invocation_context`)
- The JSON schema description (so the AI understands the expected output structure even when --json-schema enforces it)

### 5. Timeout increase (modify `engine.rs:1636`)

Change `Duration::from_secs(300)` to `Duration::from_secs(3600)` (1 hour). Real AI backend calls can take 10-30 minutes for complex implementation tasks.

## Key Types (from `src/contexts/agent_execution/model.rs`)

```rust
pub struct InvocationRequest {
    pub invocation_id: String,
    pub project_root: PathBuf,
    pub contract: InvocationContract,
    pub role: BackendRole,
    pub resolved_target: ResolvedBackendTarget,
    pub payload: InvocationPayload,
    pub timeout: Duration,
    pub cancellation_token: CancellationToken,
    pub session_policy: SessionPolicy,
    pub prior_session: Option<SessionMetadata>,
    pub attempt_number: u32,
}

pub struct InvocationEnvelope {
    pub raw_output_reference: RawOutputReference,
    pub parsed_payload: Value,
    pub metadata: InvocationMetadata,
    pub timestamp: DateTime<Utc>,
}
```

## Constraints
- Do not change any public CLI behavior (except that `run start` now actually invokes real backends instead of the stub)
- All existing tests (`cargo test`) must still pass — they use `StubBackendAdapter` directly, not `build_agent_execution_service()`
- Conformance scenarios use the stub adapter — they should continue to work via `RALPH_BURNING_BACKEND=stub`
- Use `nix develop -c cargo test` and `nix develop -c cargo build` to build and test
- Add tests for ProcessBackendAdapter (can use mock/stub approaches for the subprocess, or integration tests that verify command construction)
