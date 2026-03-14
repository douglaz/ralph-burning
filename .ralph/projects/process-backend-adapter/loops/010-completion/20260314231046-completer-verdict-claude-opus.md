---
artifact: completer-verdict
loop: 10
project: process-backend-adapter
backend: claude(opus)
role: completer
created_at: 2026-03-14T23:10:46Z
---

All tests pass. I have independently verified every requirement. Here is my verdict:

# Verdict: COMPLETE

The project satisfies all requirements:
- **Req 1 (InvocationRequest.working_dir)**: `model.rs:112` adds `working_dir: PathBuf`; `engine.rs:1959` populates it with `execution_cwd.unwrap_or(base_dir)`; `requirements_drafting/service.rs:902` populates it with `run_root` for compilation
- **Req 2 (ProcessBackendAdapter struct)**: `process_backend.rs` implements `ProcessBackendAdapter::new()` with `active_children: Arc<Mutex<HashMap<...>>>` using async-safe mutex
- **Req 3 (Supported contracts and prompt assembly)**: `assemble_stdin()` at line 144 builds input with contract label, role, prompt, pretty-printed Context JSON section (when non-null), and JSON schema instruction
- **Req 4 (Claude subprocess execution)**: `invoke_claude()` constructs the exact command shape (`-p --output-format json --model --permission-mode acceptEdits --allowedTools --json-schema`), handles `--resume`, writes stdin, double-parses the Claude envelope, and returns correctly shaped metadata
- **Req 5 (Codex subprocess execution)**: `invoke_codex()` constructs correct new-session (`exec ...`) and resume (`exec resume ...`) commands, creates temp files under `<project_root>/runtime/temp/`, reads the last-message file, and performs best-effort cleanup
- **Req 6 (Availability, capability, cancellation, error mapping)**: `check_capability()` accepts only Claude/Codex + Stage, rejects OpenRouter/Stub/Requirements with `CapabilityMismatch`; `check_availability()` uses `which` to verify binaries; `cancel()` sends SIGTERM with grace period and background reap; errors map to correct `FailureClass` variants with stderr text
- **Req 7 (BackendAdapter enum)**: `adapters/mod.rs` defines `BackendAdapter::Stub | Process` and delegates `AgentExecutionPort` methods
- **Req 8 (CLI wiring)**: `run.rs:build_agent_execution_service()` reads `RALPH_BURNING_BACKEND` (default `process`), returns fallible `AppResult`, preserves stub seams
- **Req 9 (Workflow prompt enrichment)**: `engine.rs:build_stage_prompt()` loads project prompt, stage role/objective, prior same-cycle outputs from `journal.ndjson`, remediation/amendments, and authoritative schema
- **Req 10 (Workflow timeout)**: `engine.rs:1967` uses `Duration::from_secs(3600)`
- **Tests**: 25 process_backend tests + 5 prompt_builder tests covering all required cases (command construction, double-parse, resume flags, unsupported families, missing binaries, cancellation/SIGTERM, prompt composition); CLI tests and conformance scenarios inject `RALPH_BURNING_BACKEND=stub`
- **Acceptance**: `cargo build` passes with no warnings; `cargo test` passes all 610 tests (11 + 109 + 490 unit/integration/conformance)

---
