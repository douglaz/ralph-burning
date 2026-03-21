# Reclassify Codex file-read failures as TransportFailure

## Objective

Fix incorrect error classification in `ProcessBackendAdapter` where Codex output file-read failures are tagged as `SchemaValidationFailure` instead of `TransportFailure`.

## Problem (GitHub #15)

In `src/adapters/process_backend.rs`, a missing or unreadable Codex `--output-last-message` file is an IO/transport problem, but the adapter maps the read failure to `SchemaValidationFailure` (around line 517-523). Actual schema/JSON validation only begins at `serde_json::from_str(...)` (around line 529).

This is behaviorally significant because retry handling distinguishes those classes in `src/contexts/workflow_composition/retry_policy.rs` (lines 34-38) — transport failures get 3 retries vs schema failures get 2 retries. A transient IO issue (e.g. NFS glitch, race with cleanup) gets fewer retry attempts than it should.

## Required Changes

1. In `src/adapters/process_backend.rs`, change the error classification for Codex output file-read failures from `SchemaValidationFailure` to `TransportFailure` (around line 523). This should be consistent with how the adapter already classifies pre-read failures at line 503.

2. Add or update tests to verify:
   - Codex file-read failures produce `TransportFailure` errors
   - Codex JSON parse failures still produce `SchemaValidationFailure` errors
   - The distinction is maintained correctly

## Constraints
- Do not change any public CLI behavior
- All existing tests (`cargo test`) must still pass
- Use `nix develop -c cargo test` and `nix develop -c cargo build` to build and test
