# Slice 4 Report — Operator UX Parity

## Legacy References Consulted

- `rb.md` (lines 364–366) — operator inspection surface owns `run status`, `run history`, and `run tail`
- `rb.md` (lines 1928–1930) — legacy run lifecycle command surface
- `rb.md` (lines 2031–2037) — durable journal history and explicit runtime-log separation, with `run tail` focused on durable history and `--logs` as opt-in
- `rb.md` (lines 2586–2590) — durable history versus runtime logs operator expectation
- `rb.md` (lines 4625–4628) — open design note preserving the “durable history by default, logs opt-in” tail behavior
- `parity-plan.md` (lines 316–321) — Slice 4 target CLI contract for `status --json`, `history --verbose --json`, `tail --last --follow --logs`, `rollback --list`, `show-payload`, and `show-artifact`
- `parity-plan.md` (lines 331–349) — Slice 4 acceptance and conformance targets

## Scope

Slice 4 restores the operator read-only inspection surface on `run`:

- `run status --json`
- `run history --verbose`
- `run history --json`
- `run history --stage <stage>`
- `run tail --last <n>`
- `run tail --follow`
- `run tail --follow --logs`
- `run show-payload <payload-id>`
- `run show-artifact <artifact-id>`
- `run rollback --list`

## Contracts Changed

- `RunSubcommand` now exposes JSON output flags, verbose history, stage filters, tail pagination/follow, rollback listing, and direct payload/artifact inspection subcommands
- `RunStatusJsonView` added as the stable `run status --json` DTO with `amendment_queue_depth`
- `RunStatusView`, `RunHistoryView`, `RunTailView`, and `RunRollbackTargetView` are now serializable
- `filter_by_stage()` added to query-layer durable history filtering
- `tail_last_n()` added to derive last-N visible journal slices with associated payload/artifact records
- `ArtifactStorePort` now supports direct `read_payload_by_id()` and `read_artifact_by_id()` lookup, with `FsArtifactStore` implementing O(1) file reads by canonical path
- `get_payload_by_id()` and `get_artifact_by_id()` added to the service layer, both respecting rollback visibility before lookup
- `list_rollback_targets()` added as the CLI-ready rollback listing service
- `AppError` extended with `PayloadNotFound` and `ArtifactNotFound`
- `docs/cli-reference.md` added to document the new run command contract and JSON schemas

## Files Modified

- `src/cli/run.rs`
- `src/contexts/project_run_record/queries.rs`
- `src/contexts/project_run_record/service.rs`
- `src/adapters/fs.rs`
- `src/shared/error.rs`
- `src/contexts/conformance_spec/scenarios.rs`
- `tests/conformance/features/run_queries.feature`
- `tests/cli.rs`
- `tests/unit.rs`
- `tests/unit/run_queries_test.rs`
- `docs/cli-reference.md`
- `docs/slice-reports/slice-4.md`

## Tests Run

- `cargo fmt --check`
- `cargo test --test unit run_queries_test`
- `cargo test --test cli run_status_json_outputs_stable_fields`
- `cargo test --test cli run_history_json_outputs_parseable_json`
- `cargo test --test cli run_tail_follow_starts_and_interrupts_cleanly`
- `cargo test`

## Results

- Stable JSON output now exists for `run status --json` and `run history --json`
- History supports verbose inspection, stage-aware filtering, and compact/verbose JSON modes
- Tail supports last-N pagination and follow mode with optional runtime log streaming
- Payload and artifact inspection resolve only visible canonical history records
- Rollback target discovery no longer requires filesystem inspection
- New conformance scenarios `SC-RUN-029` through `SC-RUN-046` cover the Slice 4 additions
- Existing `SC-RUN-001` through `SC-RUN-028` remain in the same registry and continue to build with the new surface
