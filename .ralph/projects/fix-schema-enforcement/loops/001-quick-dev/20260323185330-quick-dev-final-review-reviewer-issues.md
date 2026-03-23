---
artifact: quick-dev-final-review
loop: 1
project: fix-schema-enforcement
backend: codex
role: reviewer
created_at: 2026-03-23T18:53:30Z
---

# Final Review: AMENDMENTS

## Amendment: [P1] Claude Debug Logs Persist Full Transcripts

### Problem
`build_command` now unconditionally adds Claude’s `--debug-file` output at [src/adapters/process_backend.rs:494](/root/new-ralph-burning/src/adapters/process_backend.rs#L494), and terminal failures move that file into `runtime/failed` at [src/adapters/process_backend.rs:79](/root/new-ralph-burning/src/adapters/process_backend.rs#L79). That makes failed Claude runs retain full CLI debug traces on disk, including prompt/response material, which is unrelated to the strict-schema fix and is a safety/data-retention regression.

### Proposed Change
Remove the unconditional Claude debug-file wiring and failure-log preservation. If this diagnostic path is needed, gate it behind an explicit opt-in debug setting with a clear retention policy.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs) - remove the always-on Claude debug-file path and the preservation logic tied to it.

## Amendment: [P2] Nullable Multi-Type Arrays Still Violate Strict Mode

### Problem
The new normalizer removes `"null"` from a `type` array, but if more than one non-null member remains it writes another `type` array back into the non-null branch at [src/adapters/process_backend.rs:1086](/root/new-ralph-burning/src/adapters/process_backend.rs#L1086) and [src/adapters/process_backend.rs:1107](/root/new-ralph-burning/src/adapters/process_backend.rs#L1107). That yields shapes like `{"anyOf":[{"type":["string","integer"]},{"type":"null"}]}`, which still violate strict mode’s scalar-`type` requirement. The added tests only cover single-type cases at [src/adapters/process_backend.rs:1359](/root/new-ralph-burning/src/adapters/process_backend.rs#L1359) and [src/adapters/process_backend.rs:1386](/root/new-ralph-burning/src/adapters/process_backend.rs#L1386), so this gap would not be caught.

### Proposed Change
Fully eliminate `type` arrays when normalizing nullable unions. The safe fix is to emit one `anyOf` arm per remaining non-null type and add a regression test for a nullable multi-type schema.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs) - fix `normalize_nullable_type_array` and add a regression test for multi-type nullable unions.

## Amendment: [P2] Parse Failures Dump Model Output To stderr

### Problem
The Claude parse-failure paths now call `eprintln!` with truncated response content at [src/adapters/process_backend.rs:120](/root/new-ralph-burning/src/adapters/process_backend.rs#L120) and [src/adapters/process_backend.rs:144](/root/new-ralph-burning/src/adapters/process_backend.rs#L144). That bypasses the project’s logging controls and leaks model output into parent stderr/log collectors even though the branch already preserves failure artifacts separately.

### Proposed Change
Remove the raw `eprintln!` calls. If extra diagnostics are needed, emit structured logs at an opt-in debug level and avoid including model payload text by default.

### Affected Files
- [src/adapters/process_backend.rs](/root/new-ralph-burning/src/adapters/process_backend.rs) - replace the direct stderr logging in `PreparedCommand::finish`.
