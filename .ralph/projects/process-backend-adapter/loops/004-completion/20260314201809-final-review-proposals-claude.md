---
artifact: final-review-proposals
loop: 4
project: process-backend-adapter
backend: claude
role: final_reviewer
created_at: 2026-03-14T20:18:09Z
---

The `from_secs(300)` in `requirements_drafting` is expected (out of scope per the master prompt), and the daemon_loop one is for lease TTL, not stage timeout. The engine correctly uses 3600s.

I've now thoroughly reviewed all key implementation files, test files, and verified the build and all 485 tests pass. Let me compile my findings.

---

# Final Review: NO AMENDMENTS

## Summary

The ProcessBackendAdapter implementation is correct, complete against the master prompt, and well-tested. All 485 tests pass, and the project builds cleanly. Specific findings:

**Correctness verified across all 10 required changes:**

1. **InvocationRequest model** — `working_dir: PathBuf` added alongside `project_root`. Workflow execution passes `base_dir` (workspace root) as `working_dir` and `project_root` (`.ralph-burning/projects/<id>`) for persistence. Requirements drafting sets `working_dir = run_root` to compile without changing behavior.

2. **ProcessBackendAdapter** (`src/adapters/process_backend.rs`) — Implements `AgentExecutionPort` with capability checks, availability probing via `which`, `invoke()` dispatch to Claude/Codex, and `cancel()` via SIGTERM. Active children tracked by invocation ID in an async-safe `Arc<Mutex<HashMap>>`. PID registered immediately after spawn, removed on all exit paths.

3. **Claude subprocess** — Correct args (`-p`, `--output-format json`, `--model`, `--permission-mode acceptEdits`, `--allowedTools`, `--json-schema`). Session resume via `--resume`. Stdin-based input. Double-parse of envelope (outer `result` string → inner JSON). Metadata correctly populated including `session_id` fallback from `envelope.session_id.or_else(|| prior_session)`.

4. **Codex subprocess** — Correct `exec [resume]` command shape with `--dangerously-bypass-approvals-and-sandbox`, `--skip-git-repo-check`, temp schema/message files under `runtime/temp/`, best-effort cleanup on all paths.

5. **Capability/availability** — OpenRouter and Stub correctly rejected with `CapabilityMismatch` mentioning `default_backend=claude` or `default_backend=codex`. Requirements contracts rejected. Missing binary → `BackendUnavailable`. Defensive re-check in `invoke()`.

6. **Error mapping** — Spawn/exit failures → `TransportFailure` with stderr text. JSON parse errors → `SchemaValidationFailure`. No duplicate timeout wrapper in adapter. 

7. **BackendAdapter dispatch** (`src/adapters/mod.rs`) — Clean enum delegation to Stub or Process variant.

8. **CLI wiring** (`src/cli/run.rs`, `src/cli/daemon.rs`) — `RALPH_BURNING_BACKEND` env var selects `stub`/`process` (default: `process`). All existing test injection seams preserved in stub branch. Both `run.rs` and `daemon.rs` use fallible `build_agent_execution_service()?`.

9. **Workflow prompt enrichment** (`engine.rs:55-134`) — Builds prompt with: role instruction, project prompt, prior same-cycle outputs (recovered from journal + artifact store in journal order), remediation context, pending amendments, and authoritative JSON schema. Context plumbing preserved via `invocation_context()`.

10. **Timeout** — Changed from 300s to 3600s at `engine.rs:1810`.

**Test harness correctly updated:**
- `tests/cli.rs` injects `RALPH_BURNING_BACKEND=stub` in 29 places
- `src/contexts/conformance_spec/scenarios.rs` auto-injects `stub` unless overridden
- 23 new process backend unit tests covering command construction, double-parse, resume flags, capability/availability checks, cancellation (including during stdin), error classification, cleanup, and stdin payload assembly
- 4 prompt builder tests covering all required sections with order verification

**Minor observations (not rising to amendment level):**
- `cancel()` shells out to `kill -TERM` via `std::process::Command` rather than using a direct syscall (`libc::kill`). This is functional but slightly less efficient; the PID reuse window is negligible in practice.
- `ChildOutput.status` is always `Some` from `spawn_and_wait`, making the `None` arms in `invoke_claude`/`invoke_codex` dead code — but this is defensive and harmless.
- Codex temp directory creation (`create_dir_all`) failure is silently ignored; the subsequent schema write catches and surfaces the error.
