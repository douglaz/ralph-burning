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

- Unit tests: `backend_diagnostics_test` (67 tests), `backend_policy_test` (3 new tests)
- CLI integration tests: 15 new `backend_*` tests
- Conformance scenarios: `parity_slice5_backend_list`, `parity_slice5_backend_check`,
  `parity_slice5_backend_show_effective`, `parity_slice5_backend_probe_completion_panel`,
  `parity_slice5_backend_probe_final_review_panel`

## Remaining Known Gaps

- `backend probe` does not yet support arbitrary stage-level resolution
  (e.g., `--stage planning`). Role and panel targets cover the primary
  operator use cases. Stage-level probing can be added as a follow-up.
- `backend check` scoping is now effective-required: it only validates
  backends that execution would actually use for the active flow,
  excluding generic stage-derived roles that are covered by dedicated
  panel checks and skipping `default_backend` when all effectively-required
  roles have explicit overrides.
- `backend check` now decomposes panel failures to exact member identity
  (e.g. `final_review_panel.arbiter`, `prompt_review_panel.refiner`)
  with the selecting config source field.
- `backend show-effective` now reports per-field source precedence for
  model_id (`model_source`) and timeout (`timeout_source`) in addition
  to the existing backend override source.
- `backend show-effective` now surfaces roles with broken backend
  resolution (e.g. disabled configured backend) with `resolution_error`
  set, instead of silently dropping them from the output.
- `backend check` availability evaluation now correctly treats optional
  panel members as non-blocking: optional unavailable members are omitted
  and only cause failure if their omission drops the panel below minimum.
- `backend probe` config-time panel failures now correctly return
  `InsufficientPanelMembers` when optional-member omission (disabled
  backends) drops the panel below its configured minimum, instead of
  falling through to a generic `BackendUnavailable` with `backend: "unknown"`.
  This applies to `completion_panel`, `final_review_panel`, and
  `prompt_review_panel`.
- `backend probe` config-time and availability-time failures now include
  exact target identity (e.g. `(planner)`, `(refiner)`) and config source
  field, replacing the previous generic `(planner/primary)` label.
- `backend check` now resolves and checks arbiter and refiner availability
  independently of full panel resolution, so all blocking failures are
  aggregated even when reviewer/validator resolution fails first.
- `backend probe` config-time panel failures now identify the exact
  failing target/member (e.g. `completion_panel.member[1]`,
  `final_review_panel.arbiter`), the backend family, and the selecting
  config source field, instead of collapsing all panel errors to the
  primary target (planner).
- `backend show-effective` now correctly reports `model_source` for
  models embedded in `default_backend` (e.g. `default_backend = "codex(model)"`)
  by tracing to the `default_backend` source, not misreporting as `"default"`.
- `backend show-effective` top-level `default_model` field now correctly
  reflects the model embedded in `default_backend` (both value and source),
  instead of falling through to the compile-time family default.
- CLI failure-path coverage expanded: `backend probe` non-zero exit on
  disabled backend, panel minimum violation, and `backend check --json`
  failure contract are now tested.
- Conformance scenarios strengthened with source label assertions and
  probe failure semantics checks.
- Panel member failure identity now uses the original configured-spec
  index, not the post-filtering enumeration index. This means that
  when a disabled optional member at spec[0] is omitted, a failing
  required member at spec[1] correctly reports `member[1]`, not
  `member[0]`. This applies to both `backend check` availability-time
  failures and `backend probe --role` availability-time failures.
- `backend list` `compile_only` field for `stub` is now build-sensitive:
  only `true` when the current binary was built without stub support
  (`test-stub` feature absent). In test-stub builds, `stub` reports
  `null` for `compile_only`, consistent with its actually being
  operational.
