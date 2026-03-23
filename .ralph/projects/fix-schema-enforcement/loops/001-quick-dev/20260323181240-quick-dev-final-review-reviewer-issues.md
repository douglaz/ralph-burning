---
artifact: quick-dev-final-review
loop: 1
project: fix-schema-enforcement
backend: codex
role: reviewer
created_at: 2026-03-23T18:12:40Z
---

# Final Review: AMENDMENTS

## Amendment: [P1] Claude debug logs persist on every successful invocation

### Problem
Claude command construction now always adds a per-invocation `--debug-file` under `runtime/temp` at [src/adapters/process_backend.rs:470](/root/new-ralph-burning/src/adapters/process_backend.rs#L470) and [src/adapters/process_backend.rs:486](/root/new-ralph-burning/src/adapters/process_backend.rs#L486), but Claude cleanup is still a no-op at [src/adapters/process_backend.rs:57](/root/new-ralph-burning/src/adapters/process_backend.rs#L57) and the Claude decoder does not retain any debug-file path at [src/adapters/process_backend.rs:253](/root/new-ralph-burning/src/adapters/process_backend.rs#L253). That means every successful Claude invocation now leaves a `*.claude-debug.log` behind indefinitely. This is a success-path data leak and it pollutes transient runtime state.

### Proposed Change
Remove `--debug-file` from normal executions, or store the path in the Claude decoder state and delete it in `cleanup()`. If the log is needed for debugging, gate it behind explicit debug configuration and preserve it only on real failures.

### Affected Files
- `src/adapters/process_backend.rs` - gate or clean up Claude debug-file lifecycle.

## Amendment: [P2] Failure-artifact preservation still drops malformed-output failures

### Problem
`preserve_failure_artifacts()` is only invoked from the non-zero-exit path in `invoke()` at [src/adapters/process_backend.rs:859](/root/new-ralph-burning/src/adapters/process_backend.rs#L859), [src/adapters/process_backend.rs:887](/root/new-ralph-burning/src/adapters/process_backend.rs#L887), and [src/adapters/process_backend.rs:910](/root/new-ralph-burning/src/adapters/process_backend.rs#L910). If Codex exits `0` but omits or corrupts `--output-last-message`, `finish()` still deletes the temp files via `best_effort_cleanup()` before returning an error at [src/adapters/process_backend.rs:203](/root/new-ralph-burning/src/adapters/process_backend.rs#L203), [src/adapters/process_backend.rs:215](/root/new-ralph-burning/src/adapters/process_backend.rs#L215), and [src/adapters/process_backend.rs:224](/root/new-ralph-burning/src/adapters/process_backend.rs#L224). Claude parse failures likewise just dump truncated payloads to stderr at [src/adapters/process_backend.rs:110](/root/new-ralph-burning/src/adapters/process_backend.rs#L110) and [src/adapters/process_backend.rs:130](/root/new-ralph-burning/src/adapters/process_backend.rs#L130) without writing a failure bundle. So the new preservation path misses the decode/schema-validation failures where retaining artifacts is most useful.

### Proposed Change
Route `finish()` failures through the same preservation path used for transport failures: write `runtime/failed/{invocation_id}.failed.raw`, and for Codex move the schema/last-message files before returning the error. If extra logging is still wanted, make it debug-only rather than unconditional `eprintln!`.

### Affected Files
- `src/adapters/process_backend.rs` - unify artifact preservation across transport and decode/schema failures.

## Amendment: [P2] Stale-session retry leaves failed artifacts behind after a successful recovery

### Problem
In the stale-session recovery branch, the first `"No conversation found with session ID"` failure is immediately written to `runtime/failed` at [src/adapters/process_backend.rs:856](/root/new-ralph-burning/src/adapters/process_backend.rs#L856) and [src/adapters/process_backend.rs:859](/root/new-ralph-burning/src/adapters/process_backend.rs#L859). If the fresh retry then succeeds, control returns from [src/adapters/process_backend.rs:905](/root/new-ralph-burning/src/adapters/process_backend.rs#L905), but cleanup for the original attempt has already been disabled by the `invocation_failed` flag at [src/adapters/process_backend.rs:52](/root/new-ralph-burning/src/adapters/process_backend.rs#L52). The end result is a `runtime/failed/{invocation_id}.*` bundle for an invocation that ultimately succeeded.

### Proposed Change
Treat the first stale-session miss as an internal retry rather than a terminal failure: defer `preserve_failure_artifacts()` until the retry also fails, or write retry-scoped artifacts and remove them if the recovery attempt succeeds.

### Affected Files
- `src/adapters/process_backend.rs` - adjust stale-session retry preservation semantics.

Targeted verification: `nix develop -c cargo test --lib enforce_strict_mode_`, `nix develop -c cargo test --test unit codex_nonzero_exit_moves_temp_files_to_runtime_failed`, and `nix develop -c cargo test --test unit nonzero_exit_writes_failed_raw_output_for_claude` passed.

---
