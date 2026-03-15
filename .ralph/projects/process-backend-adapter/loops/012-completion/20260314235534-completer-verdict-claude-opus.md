---
artifact: completer-verdict
loop: 12
project: process-backend-adapter
backend: claude(opus)
role: completer
created_at: 2026-03-14T23:55:34Z
---

Good — SIGTERM is sent via `kill -TERM <pid>`, which is a valid approach that doesn't require a `nix` crate dependency (the prompt said "if needed").

# Verdict: COMPLETE

The project satisfies all requirements:

- **Req 1 (Model & dependencies)**: `working_dir: PathBuf` added to `InvocationRequest`, `project_root` retained, Tokio `process` feature enabled, SIGTERM handled via `kill` command (no external crate needed)
- **Req 2 (ProcessBackendAdapter)**: `new()`, async-safe `Arc<Mutex<HashMap>>` child tracking, `ManagedChild` abstraction with SIGTERM/SIGKILL/wait support
- **Req 3 (Contracts & prompt assembly)**: Stage-only support enforced; Requirements/OpenRouter/Stub rejected with `CapabilityMismatch`; `assemble_stdin` includes contract label, role, prompt, pretty-printed context JSON, and schema instruction
- **Req 4 (Claude execution)**: Correct command flags (`-p`, `--output-format json`, `--model`, `--permission-mode acceptEdits`, `--allowedTools`, `--json-schema`), resume via `--resume`, stdin input, stdout envelope parsing with double-parse of `result` field, correct metadata assembly
- **Req 5 (Codex execution)**: Correct `exec`/`exec resume` command shapes with all flags, temp schema/last-message files under `project_root/runtime/temp/`, file-based output parsing, best-effort cleanup, correct session_id/session_reused semantics
- **Req 6 (Availability/capability/cancel/errors)**: `check_capability` accepts Claude/Codex+Stage only; `check_availability` uses native PATH lookup; `cancel` sends SIGTERM with grace period and background SIGKILL fallback; error mapping: spawn/exit→TransportFailure (with stderr), invalid JSON→SchemaValidationFailure
- **Req 7 (Adapter dispatch)**: `BackendAdapter` enum with `Stub`/`Process` variants, `AgentExecutionPort` delegation impl
- **Req 8 (CLI wiring)**: `build_agent_execution_service()` returns `AppResult<...BackendAdapter...>`, reads `RALPH_BURNING_BACKEND` (default `process`), `stub`/`process`/error branches, daemon uses `?` propagation
- **Req 9 (Workflow prompt enrichment)**: `build_stage_prompt()` loads project prompt, stage role/objective, prior same-cycle outputs from journal (not payload scanning), remediation/amendments sections, authoritative schema section; `payload.context` still populated
- **Req 10 (Timeout)**: Changed from 300s to 3600s
- **Tests**: 27 process_backend tests (command construction, double-parse, resume, codex temp files, OpenRouter mismatch, missing binary, SIGTERM cancellation), 4 prompt_builder tests (all components verified), CLI/conformance harnesses inject `RALPH_BURNING_BACKEND=stub` by default
- **Acceptance criteria**: `nix develop -c cargo build` passes, `nix develop -c cargo test` passes (492 tests, 0 failures)

---
