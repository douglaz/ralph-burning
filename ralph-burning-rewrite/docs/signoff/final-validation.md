# Final Validation Report

Recorded: 2026-03-19 (updated after review-response iteration 3)
Branch: ralph/parity-plan

## Automated Check Results

### 1. Default Build: `cargo test` (no features)

```
cargo test
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| lib.rs | 67 | 0 | 0 |
| main.rs | 0 | 0 | 0 |
| cli.rs | 123 | 0 | 0 |
| run_attach_tmux.rs | 1 | 0 | 0 |
| unit.rs | 640 | 0 | 0 |
| **Total** | **831** | **0** | **0** |

**Result: PASS** -- `cargo test` succeeds in the default build. Stub-only CLI tests are now excluded via `#[cfg(feature = "test-stub")]` instead of runtime no-ops, so the default lane only reports tests that actually execute.

### 2. Stub Build: `cargo test --features test-stub`

```
cargo test --features test-stub
```

| Target | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| unit.rs | 791 | 0 | 1 |
| cli.rs | 169 | 0 | 0 |

**Unit tests: PASS** (791 passed, 0 failed, 1 ignored)

**CLI tests: PASS** (169 passed, 0 failed) -- includes `conformance_full_suite_passes`

### 3. Conformance Suite: `cargo run --features test-stub -- conformance run`

```
cargo run --features test-stub -- conformance run
```

| Metric | Value |
|--------|-------|
| Selected | 386 |
| Passed | 386 |
| Failed | 0 |
| Not run | 0 |

**Result: PASS** -- All 386 conformance scenarios pass.

Previously failing `RD-001` fixed by adding `validation` label override with `needs_questions` outcome alongside the existing `question_set` override. The stub's default canned validation response returns `pass`, which skipped the question round entirely. Nine additional RD-* scenarios required the same fix.

### 4. PR-Review Conformance Scenarios (targeted)

```
cargo run --features test-stub -- conformance run --filter daemon.pr_review.transient_error_preserves_staged
cargo run --features test-stub -- conformance run --filter daemon.pr_review.completed_project_reopens_with_amendments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.whitelist_filters_comments
cargo run --features test-stub -- conformance run --filter daemon.pr_review.dedup_across_restart
```

All 4 PR-review scenarios: **PASS**

## Cutover Readiness

- [x] `cargo test` succeeds in the default build (831 tests, 0 failures, no stub-only no-ops)
- [x] `cargo test --features test-stub` succeeds (791 unit, 169 CLI, 0 failures)
- [x] `cargo run --features test-stub -- conformance run` passes all 386 scenarios
- [x] `daemon.pr_review.transient_error_preserves_staged` passes
- [x] All 4 PR-review conformance scenarios pass
- [x] Stub-dependent CLI tests are compile-gated behind `#[cfg(feature = "test-stub")]`
- [ ] Backend-specific manual smoke items (Claude, Codex, OpenRouter) — **executed 2026-03-19, fixes applied iteration 5**. Harness at `scripts/live-backend-smoke.sh` correctly: (a) isolates workspace state via `cd` into scratch dir, (b) sets `settings.default_backend` and all role overrides in scratch `workspace.toml` for single-backend smoke, (c) binds explicit `--backend` flags at every CLI phase, (d) records evidence with structural JSON parsing. **All three rows FAIL** during `project bootstrap` (quick requirements phase). **Code fixes applied in iteration 5**:
  - **Codex + OpenRouter schema fix**: `inject_additional_properties_false()` replaced by `enforce_strict_mode_schema()` in `process_backend.rs` — now ensures ALL property keys from `properties` are in `required` (fixing `#[serde(default)]` fields like `follow_ups`). Applied to both Codex (`process_backend.rs:429`) and OpenRouter (`openrouter_backend.rs:134`). This directly addresses the Codex strict-mode rejection and pre-empts the same failure on OpenRouter.
  - **Claude diagnostics**: Claude decoder now provides contract label, stdout length, and session policy in error messages. Root cause corrected: requirements stages use `SessionPolicy::NewSession` (not `--resume`); the original diagnosis was incorrect. The failure is a Claude CLI-side issue with the `project_seed` schema.
  - **OpenRouter**: Transient 502 was upstream; the schema fix ensures compliance when the provider is stable.
  - No project state was created in any case (failures are pre-project-creation). Requirements runs are inspectable in scratch dirs.
  - See `docs/signoff/manual-smoke-matrix.md` rows 1-3 for full evidence and fix details.

**Cutover status: Not Ready** — all automated checks pass (831 tests, 386 conformance scenarios); smoke matrix items 4-16 are PASS. Items 1-3 are FAIL with code fixes applied. Re-run `./scripts/live-backend-smoke.sh <backend>` after fixes to verify: (1) Codex smoke should now pass with corrected `required` array, (2) OpenRouter smoke should pass with corrected schema + stable upstream, (3) Claude requires investigation of CLI-side `project_seed` structured output issue. Update matrix rows to PASS with complete evidence (project_id, run_id, run_status, smoke_id) and change cutover status to Ready when all three pass.
