---
artifact: final-review-proposals
loop: 6
project: process-backend-adapter
backend: claude
role: final_reviewer
created_at: 2026-03-14T21:11:01Z
---

Good — `working_dir` is populated with `run_root` in the requirements service, keeping it compiling without changing requirements behavior.

# Final Review: NO AMENDMENTS

## Summary

The implementation is correct, complete, and robust against the master prompt's requirements:

1. **ProcessBackendAdapter** (`src/adapters/process_backend.rs`): Correctly implements Claude and Codex subprocess execution with proper double-parse for Claude envelopes, temp file management for Codex, concurrent stdin/stdout/stderr I/O via `tokio::join!` to avoid deadlocks, SIGTERM-based cancellation via PID tracking with async-safe mutex, and appropriate error mapping (`TransportFailure` for spawn/exit failures, `SchemaValidationFailure` for JSON parse failures).

2. **Capability/availability checks**: Correctly reject `OpenRouter`, `Stub`, and `Requirements` with `CapabilityMismatch`; availability probes `which` on PATH for the correct binary name.

3. **BackendAdapter dispatch** (`src/adapters/mod.rs`): Clean enum delegation to `Stub` or `Process` variants.

4. **CLI wiring** (`src/cli/run.rs`): `build_agent_execution_service()` correctly reads `RALPH_BURNING_BACKEND` (defaulting to `process`), preserves all `RALPH_BURNING_TEST_*` seams for the stub branch, and returns a fallible `AppResult`.

5. **Workflow prompt enrichment** (`engine.rs`): `build_stage_prompt()` correctly assembles project prompt, stage role instruction, prior same-cycle outputs (recovered from journal via `visible_journal_events` to handle rollbacks), remediation context, pending amendments, and authoritative JSON schema. Journal ordering is preserved. Sections are omitted when empty.

6. **`working_dir` field**: Added to `InvocationRequest`, populated with `execution_cwd.unwrap_or(base_dir)` for workflow runs and with `run_root` for requirements (compilation only, no behavior change). Daemon paths thread `worktree_path` correctly.

7. **Timeout**: Updated from 300s to 3600s as required.

8. **Cargo.toml**: Tokio `process`, `fs`, and `io-util` features added.

9. **Test harness**: All existing CLI and conformance tests inject `RALPH_BURNING_BACKEND=stub` by default. New tests cover command construction, double-parse, resume flags, unsupported backends, missing binaries, cancellation/SIGTERM, stdin payload assembly, deadlock avoidance with large stdout, and prompt builder output ordering.

10. **Build & test**: `cargo build` and all 487 tests pass.
