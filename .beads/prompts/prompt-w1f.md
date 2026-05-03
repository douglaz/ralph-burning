# Bead w1f: Document crash-durability limitations of atomic hint file write

## Problem description

`persist_next_step_hint` in
`src/contexts/automation_runtime/success_reconciliation.rs` uses the
write-tmp + rename atomic-write pattern. That pattern protects against
torn reads (a concurrent reader never sees a partial file), but on many
Linux filesystems the rename is only **durable** after an fsync on the
parent directory. Without that fsync, a crash immediately after the
rename may lose the rename from the journal, effectively losing the new
hint entry.

Because hints are best-effort and non-blocking, this tradeoff is
intentional — losing a hint on crash is acceptable. The issue is that it
is currently undocumented, so a future reader may either (a) spend time
hardening durability unnecessarily, or (b) miss the hazard and assume
durability they don't have.

## Fix

Add a comment (2–4 lines) at the top of `persist_next_step_hint`
explaining the tradeoff. Shape:

```rust
// Atomic write (tmp + rename) prevents torn reads, but the rename is not
// crash-durable on all Linux filesystems without an fsync on the parent
// directory. Hints are best-effort and non-blocking, so losing one on
// crash is acceptable — we intentionally skip the fsync.
```

Scope guard:
- Do NOT add fsync.
- Do NOT change the write logic.
- Do NOT rename variables.
- The only change is a clarifying comment.

## Tests

No behavior change → no new tests.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged.

## Acceptance criteria

- Comment added at the top of `persist_next_step_hint` documenting the
  atomicity-without-durability tradeoff and why it's intentional.
- No other code changes.
- `nix build` passes.
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
