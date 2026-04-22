# Bead hg4: Extract duplicated is_timeout_related pattern into helper method

## Problem description

`src/adapters/process_backend.rs` has the same `is_timeout_related`
match-block duplicated at two sites: once around lines 1517-1528 and again
around lines 1572-1583. Both match the same two variants of `AppError`:

```rust
let is_timeout_related = match &error {
    AppError::InvocationFailed {
        failure_class: FailureClass::Timeout,
        ..
    } => true,
    AppError::InvocationFailed { details, .. }
        if details.contains("exceeded timeout") =>
    {
        true
    }
    _ => false,
};
```

If the timeout-detection heuristic changes (e.g., add another failure
class or another details substring), both sites must be updated in lock-step.

## Fix

Extract a free helper function near the top of the file or as a private
method on the `ProcessBackend` / `ChildOutput` impl block:

```rust
fn is_timeout_related(err: &AppError) -> bool {
    matches!(
        err,
        AppError::InvocationFailed { failure_class: FailureClass::Timeout, .. }
    ) || matches!(
        err,
        AppError::InvocationFailed { details, .. } if details.contains("exceeded timeout")
    )
}
```

Replace both match blocks with a call to this helper. Preserve the exact
same semantics (`InvocationFailed` with `Timeout` class OR details
containing `"exceeded timeout"` → true).

Scope guard:
- Do NOT widen the heuristic. If you see other timeout-ish errors elsewhere,
  leave them alone — that's out of scope for this bead.
- Do NOT rename or move unrelated code.
- No need for a new public API — private helper is fine.

## Tests

An existing test exercising these branches is sufficient if present. If
not, add a small unit test for the helper that covers:
- `InvocationFailed` with `failure_class: Timeout` → true
- `InvocationFailed` with some other class but details containing
  `"exceeded timeout"` → true
- `InvocationFailed` with unrelated details → false
- Any non-`InvocationFailed` error → false

Keep the test small (≤ ~40 lines).

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files.

## Acceptance criteria

- No duplicate `is_timeout_related` match block in
  `src/adapters/process_backend.rs`.
- Both former sites call the new helper.
- `nix build` passes (authoritative gate).
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
- This is a refactor — no behavior change expected.
