# Implementation Notes — Slice 2: Real Requirements Backend Path

## Decisions Made
- Centralized backend adapter construction into `src/composition/agent_execution_builder.rs` shared builder module. All three call-sites (CLI run, CLI requirements, daemon) now delegate to the same `build_backend_adapter()` / `build_agent_execution_service()` / `build_requirements_service()` functions.
- Default backend is `process` (controlled by `RALPH_BURNING_BACKEND` env var); `stub` is now exclusively a test seam.
- Renamed `daemon_loop.rs::build_requirements_service()` (pub test seam) → `build_requirements_service_for_test()` to disambiguate from the shared builder's `build_requirements_service()`.
- Conformance scenario `backend.requirements.real_backend_path` (RD-034) creates fake `claude` and `codex` shell scripts. The fake codex parses `--output-last-message` from args and writes its payload to that file (matching the real codex protocol), since the `Reviewer` role defaults to `BackendFamily::Codex`.

## Spec Deviations
- None.

## Files Changed

### New
- `src/composition/mod.rs` — module declaration
- `src/composition/agent_execution_builder.rs` — shared builder with `build_backend_adapter()`, `build_agent_execution_service()`, `build_requirements_service()`, type aliases `ProductionAgentService` and `ProductionRequirementsService`, and test-only stub construction

### Modified
- `src/lib.rs` — added `pub mod composition`
- `src/cli/run.rs` — delegated to shared builder, removed inline builder
- `src/cli/requirements.rs` — replaced direct `StubBackendAdapter` construction with shared builder
- `src/cli/daemon.rs` — import changed to shared builder
- `src/contexts/automation_runtime/daemon_loop.rs` — `build_requirements_service_default()` delegates to shared builder; `build_requirements_service()` renamed to `build_requirements_service_for_test()`
- `src/adapters/process_backend.rs` — `check_capability` now accepts `InvocationContract::Requirements` for Claude and Codex families
- `src/contexts/conformance_spec/scenarios.rs` — added `register_backend_requirements()` with `backend.requirements.real_backend_path` scenario
- `tests/conformance/features/requirements_drafting.feature` — added RD-034 scenario
- `tests/unit/process_backend_test.rs` — replaced negative requirements tests with positive coverage for both Claude and Codex
- `tests/unit/automation_runtime_test.rs` — updated to use `build_requirements_service_for_test`
- `tests/cli.rs` — added `RALPH_BURNING_BACKEND=stub` to requirements CLI test invocations

## Testing
- 503 unit tests: all pass
- 110 CLI tests: all pass
- 221 conformance scenarios: all pass (including new RD-034)

---
