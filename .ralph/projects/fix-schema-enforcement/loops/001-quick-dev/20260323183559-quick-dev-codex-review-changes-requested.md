---
artifact: quick-dev-codex-review
loop: 1
project: fix-schema-enforcement
backend: codex
role: reviewer
created_at: 2026-03-23T18:35:59Z
---

# Review: CHANGES REQUESTED

- [process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs#L1072) rewrites nullable `type` arrays by moving every remaining key into the non-null branch. That misses real `schemars` 0.8 output for defaulted `Option<T>` fields: `schemars` emits `"default": null` alongside `{"type":["string","null"]}` for defaulted options (see [`default.json`](/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/schemars-0.8.22/tests/expected/default.json#L15), and our actual field at [`panel_contracts.rs`](/root/new-ralph-burning/src/contexts/workflow_composition/panel_contracts.rs#L131)). After this transform, the non-null arm becomes `{"type":"string","default":null}`, which is not the intended strict-mode shape and is not covered by the new synthetic tests at [`process_backend.rs`](/root/new-ralph-burning/src/adapters/process_backend.rs#L1350) and [`process_backend.rs`](/root/new-ralph-burning/src/adapters/process_backend.rs#L1472). Fix: hoist or drop schema-wide annotations before building the non-null variant, at minimum ensuring `default: null` stays off the non-null branch, and add a regression test using `schemars::schema_for!(FinalReviewProposalPayload)` or a fixture that includes `"default": null`.

I traced the modified function through [process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs#L488), [process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs#L542), and [openrouter_backend.rs](/root/new-ralph-burning/src/adapters/openrouter_backend.rs#L145); I did not find an over-broad integration issue there.

I couldn’t run `cargo test` here because `cargo` is not installed in this environment.
