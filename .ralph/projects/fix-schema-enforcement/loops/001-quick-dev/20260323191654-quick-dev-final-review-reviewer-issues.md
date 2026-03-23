---
artifact: quick-dev-final-review
loop: 1
project: fix-schema-enforcement
backend: codex
role: reviewer
created_at: 2026-03-23T19:16:54Z
---

# Final Review: AMENDMENTS

## Amendment: [P2] Preserve artifacts for `finish()`-path failures

### Problem
The new failure-preservation path only runs for non-zero exit statuses at [src/adapters/process_backend.rs#L897](/root/new-ralph-burning/src/adapters/process_backend.rs#L897), while the success path returns `prepared.finish(...)` directly at [src/adapters/process_backend.rs#L911](/root/new-ralph-burning/src/adapters/process_backend.rs#L911). As a result, successful-exit failures that happen during output decoding never reach `runtime/failed`:

- Claude parse failures return immediately from `finish()` at [src/adapters/process_backend.rs#L106](/root/new-ralph-burning/src/adapters/process_backend.rs#L106), [src/adapters/process_backend.rs#L123](/root/new-ralph-burning/src/adapters/process_backend.rs#L123), and [src/adapters/process_backend.rs#L147](/root/new-ralph-burning/src/adapters/process_backend.rs#L147).
- Codex read/parse failures both delete the temp artifacts before returning at [src/adapters/process_backend.rs#L195](/root/new-ralph-burning/src/adapters/process_backend.rs#L195) and [src/adapters/process_backend.rs#L207](/root/new-ralph-burning/src/adapters/process_backend.rs#L207).

That leaves no preserved raw output or `last-message` file for some of the most useful debugging cases: malformed Claude envelopes/results and malformed or missing Codex `--output-last-message` output.

### Proposed Change
Route all terminal `finish()` errors through the same preservation flow as non-zero exits. The simplest fix is to stop deleting artifacts inside `finish()` on error, let `invoke()` handle the `Err`, call `preserve_failure_artifacts(&request, &output)`, then perform cleanup. Add regressions for Claude invalid-envelope/invalid-result cases and Codex missing/invalid-last-message cases to assert artifacts are preserved under `runtime/failed`.

### Affected Files
- `src/adapters/process_backend.rs` - preserve failure artifacts for decode/read failures after a successful child exit.
- `tests/unit/process_backend_test.rs` - add regression coverage for Claude and Codex `finish()`-path failures.
