---
artifact: completer-verdict
loop: 4
project: process-backend-adapter
backend: claude(opus)
role: completer
created_at: 2026-03-14T20:02:22Z
---

I have now verified every requirement against the actual code. Here is my verdict:

# Verdict: COMPLETE

The project satisfies all requirements:

- **Req 1 (Model + Dependencies)**: `working_dir: PathBuf` added to `InvocationRequest` (model.rs:112); `project_root` retained separately; Tokio `process` feature enabled in Cargo.toml (line 13); requirements-drafting populates `working_dir` with `run_root` (service.rs:902); workflow engine uses `base_dir` as `working_dir` (engine.rs:1802)
- **Req 2 (ProcessBackendAdapter)**: `src/adapters/process_backend.rs` implements `ProcessBackendAdapter::new()` with async-safe `Arc<Mutex<HashMap<String, u32>>>` for active child tracking by invocation_id
- **Req 3 (Contracts + Prompt Assembly)**: `Stage`-only support enforced; `Requirements` returns `CapabilityMismatch`; `assemble_stdin()` includes contract label, role, prompt, pretty-printed Context JSON (when non-null), and schema instruction
- **Req 4 (Claude subprocess)**: Correct command args (`-p --output-format json --model --permission-mode acceptEdits --allowedTools --json-schema`); session resume via `--resume`; stdin-based input; double-parse (outer envelope → inner result JSON string); correct metadata fields including `session_id` fallback and `duration: 0ms`
- **Req 5 (Codex subprocess)**: `codex exec` / `codex exec resume` command shapes; schema file at `<project_root>/runtime/temp/<id>.schema.json`; last-message file parsed; session handling; trailing `-` for stdin; best-effort temp file cleanup
- **Req 6 (Capability/Availability/Cancel/Error)**: `check_capability` accepts Claude+Codex with Stage, rejects OpenRouter/Stub/Requirements with `CapabilityMismatch` including clear config guidance; `check_availability` uses `which` to probe PATH, returns `BackendUnavailable`; `cancel` sends SIGTERM, returns Ok for unknown invocations; error mapping: spawn/exit→TransportFailure, JSON parse→SchemaValidationFailure, stderr included in details
- **Req 7 (Adapter dispatch)**: `src/adapters/mod.rs` exports `process_backend`, defines `BackendAdapter` enum with Stub/Process variants, implements `AgentExecutionPort` delegation
- **Req 8 (CLI wiring)**: `build_agent_execution_service()` returns `AppResult<AgentExecutionService<BackendAdapter, ...>>`; reads `RALPH_BURNING_BACKEND` with default `process`; stub branch preserves all `RALPH_BURNING_TEST_*` seams; invalid values produce clear error; daemon.rs uses the fallible builder with `?`
- **Req 9 (Prompt enrichment)**: `build_stage_prompt()` loads project prompt file, stage role instruction with objective, prior same-cycle outputs via journal (`stage_completed` events filtered by `run_id` + `cycle`, with payload lookup), remediation context, pending amendments, and authoritative JSON schema; journal order preserved; `payload.context` still populated via `invocation_context()`
- **Req 10 (Timeout)**: `Duration::from_secs(3600)` at engine.rs:1810
- **Tests (ProcessBackendAdapter)**: Claude command-construction, double-parse, resume-flag, Codex command-construction with temp files, OpenRouter `CapabilityMismatch` with detail text, missing-binary availability, SIGTERM cancellation, stdin payload assembly, error mapping tests — all present in `process_backend_test.rs`
- **Tests (Prompt builder)**: Rich prompt with project prompt, prior outputs, remediation, amendments, schema; journal-order verification; empty prior-outputs omission; empty remediation omission; missing payload diagnostic — all in `prompt_builder_test.rs`
- **Test harness**: `tests/cli.rs` injects `RALPH_BURNING_BACKEND=stub` at every CLI test call site; `conformance_spec/scenarios.rs` auto-injects `stub` unless caller overrides
- **Build + test**: `nix develop -c cargo build` passes; `nix develop -c cargo test` passes (485 tests, 0 failures)

---
