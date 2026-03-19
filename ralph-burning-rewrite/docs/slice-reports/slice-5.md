# Slice 5: Backend Operations Parity — Report

## Legacy References Consulted

- Old `ralph` had no dedicated `backend` command; operators inspected backend
  resolution by reading config files and running test invocations.
- The new surface reuses the existing `BackendPolicyService`, `BackendResolver`,
  and `EffectiveConfig` primitives from Slices 0-4.
- `CliBackendOverrides` struct from `run start`/`run resume` is reused for
  diagnostics override simulation.
- Panel resolution behavior (required/optional members, minimum enforcement)
  matches the policy semantics already tested in `backend_policy_test.rs`.

## Contracts Changed

- **New CLI command**: `ralph-burning backend` with subcommands `list`, `check`,
  `show-effective`, and `probe`.
- **New JSON DTOs**: `BackendListEntry`, `BackendCheckResult`, `EffectiveBackendView`,
  `BackendProbeResult` with stable schemas documented in `cli-reference.md`.
- **New error variant**: `AppError::BackendCheckFailed` for non-zero exit from
  `backend check`.
- **New public method**: `BackendPolicyService::backend_enabled_public()` exposes
  the existing private `backend_enabled` check for diagnostics use.
- **No existing CLI contracts changed or regressed**.

## Files Modified

- `src/cli/backend.rs` — new backend CLI surface
- `src/cli/mod.rs` — register `Backend` command
- `src/contexts/agent_execution/diagnostics.rs` — new diagnostics service and DTOs
- `src/contexts/agent_execution/mod.rs` — export diagnostics module
- `src/contexts/agent_execution/policy.rs` — add `backend_enabled_public`
- `src/shared/error.rs` — add `BackendCheckFailed` variant
- `tests/unit/backend_diagnostics_test.rs` — new unit tests
- `tests/unit/backend_policy_test.rs` — extended panel probe tests
- `tests/unit.rs` — register new test module
- `tests/cli.rs` — CLI integration tests for all backend subcommands
- `tests/conformance/features/backend_operations.feature` — Slice 5 conformance
- `src/contexts/conformance_spec/scenarios.rs` — 5 conformance executors
- `docs/cli-reference.md` — backend command documentation
- `docs/slice-reports/slice-5.md` — this report

## Tests Run

- Unit tests: `backend_diagnostics_test` (36 tests), `backend_policy_test` (3 new tests)
- CLI integration tests: 12 new `backend_*` tests
- Conformance scenarios: `parity_slice5_backend_list`, `parity_slice5_backend_check`,
  `parity_slice5_backend_show_effective`, `parity_slice5_backend_probe_completion_panel`,
  `parity_slice5_backend_probe_final_review_panel`

## Remaining Known Gaps

- `backend probe` does not yet support arbitrary stage-level resolution
  (e.g., `--stage planning`). Role and panel targets cover the primary
  operator use cases. Stage-level probing can be added as a follow-up.
