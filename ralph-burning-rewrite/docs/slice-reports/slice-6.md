# Slice 6: Tmux And Streaming Parity — Report

## Legacy References Consulted

- [`../rb.md` line 385](/root/new-ralph-burning/rb.md#L385) — process/tmux management owns external CLI execution, live log capture, and timeout/cancel cleanup
- [`../rb.md` line 4625](/root/new-ralph-burning/rb.md#L4625) — `run tail` keeps durable history as the default surface, with runtime logs as explicit opt-in
- Slice 6 feature spec and review feedback artifacts for the required `run attach`, `[execution]`, tmux readiness, and streaming contracts

## Contracts Changed

- `run attach` now resolves the live tmux session from recorded project runtime state instead of recomputing from the current stage cursor or current config
- Production daemon dispatch now builds workflow/rebase agent services from per-project `EffectiveConfig`, so `execution.mode = "tmux"` is honored in daemon-managed runs
- Slice 6 conformance coverage now includes adapter equivalence, cancel cleanup, timeout cleanup, and watcher-driven follow behavior
- CLI docs now cover `[execution]` config, `run attach`, watcher-driven `run tail --follow --logs`, and `backend check` `tmux_unavailable`

## Files Modified

- `src/adapters/tmux.rs` — recorded active tmux session runtime state and cleanup
- `src/cli/run.rs` — state-backed `run attach` and non-leaking follow watcher lifecycle
- `src/cli/daemon.rs` — production daemon wiring for config-aware agent-service builder
- `src/contexts/automation_runtime/daemon_loop.rs` — per-project config-aware workflow/rebase agent-service dispatch path
- `src/contexts/conformance_spec/scenarios.rs` — real Slice 6 executors for SC-TMUX-006 through SC-TMUX-009 and stronger source assertions for SC-TMUX-001/002
- `tests/unit/tmux_adapter_test.rs` — runtime-state persistence/cleanup assertions
- `tests/run_attach_tmux.rs` — CLI coverage for recorded-session attach resolution
- `docs/cli-reference.md` — user/operator docs for execution config, attach, and watcher semantics
- `docs/slice-reports/slice-6.md` — this report

## Tests Run

- `cargo test tmux_adapter`
- `cargo test --test run_attach_tmux`
- `cargo test --lib SC-TMUX-006`
- `cargo test --lib SC-TMUX-007`
- `cargo test --lib SC-TMUX-008`
- `cargo test --lib SC-TMUX-009`
- `cargo check`

## Remaining Known Gaps

- `run attach` still targets a single currently recorded tmux session per project. That matches the workflow engine’s sequential invocation model today; concurrent tmux-backed invocations would need a multi-session operator surface.
- Watcher-driven `run tail --follow --logs` remains best-effort and still falls back to polling when the platform watcher backend cannot watch the project root.
