---
artifact: final-review-proposals
loop: 9
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T10:58:27Z
---

# Final Review: AMENDMENTS

## Summary

The implementation is thorough, well-designed, and meets all acceptance criteria from the master prompt. Build, 450 unit tests, 108 CLI integration tests, and 218 conformance scenarios all pass. The core design — `CliWriterLease` model, `CliWriterLeaseGuard` with RAII + heartbeat, owner-aware `release_writer_lock`, two-pass reconcile, `LeaseRecordWire` backward compatibility — is sound. `daemon_loop.rs` contains no `set_current_dir` calls.

One low-severity issue was found in code introduced by this changeset.

---

## Amendment: CONC-001

### Problem

`[P3]` **Mislabeled error in reconcile Pass 1 `Err(e)` arm** — `src/contexts/automation_runtime/lease_service.rs:550-557`

The `release()` method was refactored so that worktree removal errors are now captured in `Ok(ReleaseResult { worktree_error: Some(...) })` (line 267-280) instead of being propagated as `Err`. The only remaining `Err` path from `release()` is `ProjectId::new` at line 255. However, the `Err(e)` match arm in the reconcile worktree-lease loop still carries the old label:

```rust
Err(e) => {
    // Physical release failed (e.g. worktree removal) — ...
    details: format!("worktree_remove: {e}"),
}
```

If `ProjectId::new` ever fails (corrupted lease data), the cleanup failure would be mislabeled as `worktree_remove:` when it is actually a `project_id` validation error. The comment is also stale.

This is very low severity — it requires a corrupted `project_id` in a persisted lease file to trigger, which should never happen under normal operation.

### Proposed Change

Update the error label and comment to reflect the actual error source:

```rust
Err(e) => {
    // release() setup failed (e.g. invalid project_id) — lease
    // remains durable and the task remains terminal but recoverable.
    report.cleanup_failures.push(LeaseCleanupFailure {
        lease_id: lease.lease_id.clone(),
        task_id: Some(task.task_id.clone()),
        details: format!("release_setup: {e}"),
    });
}
```

### Affected Files
- `src/contexts/automation_runtime/lease_service.rs` - update error label at line 556 and comment at lines 551-552

---
