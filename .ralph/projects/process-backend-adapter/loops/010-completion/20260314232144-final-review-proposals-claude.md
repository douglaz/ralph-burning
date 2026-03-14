---
artifact: final-review-proposals
loop: 10
project: process-backend-adapter
backend: claude
role: final_reviewer
created_at: 2026-03-14T23:21:44Z
---

Everything checks out. Here is my review:

---

# Final Review: NO AMENDMENTS

## Summary

The implementation is correct, safe, and robust across all the required changes:

1. **`InvocationRequest.working_dir`** is properly added and threaded through all call sites. Workflow engine passes `execution_cwd.unwrap_or(base_dir)` (CLI uses repo root, daemon uses worktree path). Requirements drafting sets it to `run_root`. The existing `project_root` remains for persistence.

2. **`ProcessBackendAdapter`** correctly implements all four `AgentExecutionPort` methods:
   - `check_capability` match arms are exhaustive and ordered correctly (Claude/Codex+Stage OK, any+Requirements rejected, OpenRouter/Stub rejected with clear config guidance).
   - `check_availability` probes PATH via `which`.
   - `invoke` delegates to `invoke_claude`/`invoke_codex` without adding a timeout wrapper.
   - `cancel` sends SIGTERM with a 500ms grace period, then spawns a background SIGKILL task for unresponsive processes.

3. **Claude subprocess**: Command construction includes all spec flags (`-p`, `--output-format json`, `--model`, `--permission-mode`, `--allowedTools`, `--json-schema`). Resume adds `--resume <session_id>`. Double-parse of `result` field works correctly. Session metadata fallback logic is correct.

4. **Codex subprocess**: New-session and resume command shapes are correct (resume intentionally omits `--output-schema` per approved Loop 7 amendment). Schema and last-message temp files are created, read, and cleaned up with best-effort deletion on all paths (success, failure, early return).

5. **Concurrent I/O**: `spawn_and_wait` uses `tokio::join!` for stdin/stdout/stderr before polling the child, preventing pipe-buffer deadlocks even with large payloads (verified by `codex_large_stdout_before_reading_stdin_does_not_deadlock` test).

6. **`ManagedChild` polling design**: Uses `try_wait()` with 10ms polling deliberately so the mutex is released between polls, allowing `cancel()` to acquire the lock and read the PID for signaling. `kill_on_drop(true)` provides a safety net.

7. **`BackendAdapter` dispatch enum** in `src/adapters/mod.rs` delegates all trait methods correctly.

8. **CLI wiring**: `build_agent_execution_service()` reads `RALPH_BURNING_BACKEND`, defaults to `process`, preserves all `RALPH_BURNING_TEST_*` seams in the `stub` branch unchanged.

9. **Prompt enrichment**: `build_stage_prompt` includes all required sections (project prompt, role instruction, prior same-cycle outputs in journal order, remediation context, pending amendments, authoritative JSON schema). Prior outputs correctly use `visible_journal_events` to exclude rolled-back branches and filter by `run_id` + `cycle`.

10. **Timeout**: Changed from 300s to 3600s at `engine.rs:1967`.

11. **Test harness**: `conformance_spec/scenarios.rs::run_cli_with_env` defaults `RALPH_BURNING_BACKEND=stub`. All 29 CLI spawn sites in `tests/cli.rs` inject `RALPH_BURNING_BACKEND=stub`. All required test cases are present (command construction, double-parse, resume flag, codex temp files, capability mismatch, missing binary, cancellation/SIGTERM, prompt builder with prior outputs/remediation/amendments/schema).

12. **Build & tests**: `cargo build` succeeds, all 490 tests pass.
