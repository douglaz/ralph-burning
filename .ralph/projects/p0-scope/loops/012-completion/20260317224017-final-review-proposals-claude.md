---
artifact: final-review-proposals
loop: 12
project: p0-scope
backend: claude
role: final_reviewer
created_at: 2026-03-17T22:40:17Z
---

# Final Review: AMENDMENTS

## Amendment: FR-001 `[P1]`

### Problem
In `ralph-burning-rewrite/src/adapters/stub_backend.rs:614-618`, the `translate_to_panel_payload` function for `(StageId::FinalReview, "voter")` has a copy-paste bug: both branches of the if/else return `"accept"`, so all outcomes are mapped to acceptance regardless of the actual outcome value.

```rust
if outcome == "approved" {
    "accept"
} else {
    "accept"  // BUG: should be "reject"
}
```

Current tests bypass this path because they provide explicit `"votes"` payloads (triggering the early return at line 607), but any future test using the legacy `"outcome"` format for final-review voters will silently produce incorrect accept votes, masking rejection scenarios.

### Proposed Change
Change line 617 from `"accept"` to `"reject"`.

### Affected Files
- `ralph-burning-rewrite/src/adapters/stub_backend.rs` - fix the else branch at line 617

---

## Amendment: FR-002 `[P1]`

### Problem
In `ralph-burning-rewrite/src/adapters/process_backend.rs:469-472`, production code uses `expect()` which will panic if a logic bug ever desynchronizes the `session_resuming` guard from the `prior_session` field:

```rust
let session = request
    .prior_session
    .as_ref()
    .expect("session_resuming requires a prior session");
```

### Proposed Change
Replace with proper error handling returning `AppError::InvocationFailed` with `FailureClass::TransportFailure`.

### Affected Files
- `ralph-burning-rewrite/src/adapters/process_backend.rs` - replace `expect()` at line 472 with `ok_or_else(|| ...)?`

---

## Amendment: FR-003 `[P2]`

### Problem
In `ralph-burning-rewrite/src/adapters/process_backend.rs:302-303`, if JSON schema serialization fails, the code silently falls back to `"{}"`:

```rust
let schema_json = serde_json::to_string(&request.contract.json_schema_value())
    .unwrap_or_else(|_| "{}".to_owned());
```

This sends an empty schema to Claude, which will cause a confusing downstream invocation failure with no indication that the root cause was schema serialization.

### Proposed Change
Propagate the error instead of silently falling back.

### Affected Files
- `ralph-burning-rewrite/src/adapters/process_backend.rs` - replace `unwrap_or_else` at line 303 with `map_err(...)? `

---

## Amendment: FR-004 `[P2]`

### Problem
24 implementation-response and implementation-notes markdown files are committed inside `ralph-burning-rewrite/`. These are development process artifacts (loop iteration responses), not production code or documentation. Examples:
- `ralph-burning-rewrite/20260316-impl-notes.md`
- `ralph-burning-rewrite/20260317142758-impl-response-III.md`
- (22 more)

### Proposed Change
Delete all 24 files. Add a gitignore pattern to prevent recurrence.

### Affected Files
- `ralph-burning-rewrite/*-impl-*.md` (24 files) - delete
- `ralph-burning-rewrite/.gitignore` or root `.gitignore` - add exclusion pattern

---

## Amendment: FR-005 `[P3]`

### Problem
In `ralph-burning-rewrite/src/contexts/workspace_governance/config.rs:350-353`, the variable `base_backend_string` is computed but explicitly discarded with `let _ = base_backend_string;` at line 419. This is dead code — likely leftover from a debugging or logging pass.

### Proposed Change
Remove the dead variable computation (lines 350-353) and its discard statement.

### Affected Files
- `ralph-burning-rewrite/src/contexts/workspace_governance/config.rs` - remove dead code at lines 350-353 and the `let _ = base_backend_string;` discard

---
