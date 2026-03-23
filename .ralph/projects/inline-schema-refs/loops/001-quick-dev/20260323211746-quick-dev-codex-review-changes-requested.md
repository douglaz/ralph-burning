---
artifact: quick-dev-codex-review
loop: 1
project: inline-schema-refs
backend: codex
role: reviewer
created_at: 2026-03-23T21:17:46Z
---

# Review: CHANGES REQUESTED

1. [src/adapters/process_backend.rs#L1163](/root/new-ralph-burning/src/adapters/process_backend.rs#L1163) does not meet Acceptance Criterion 6. `inline_schema_refs()` removes `definitions` before checking that it is an object, so `{"definitions": 42}` is mutated by dropping the key. The test at [src/adapters/process_backend.rs#L1732](/root/new-ralph-burning/src/adapters/process_backend.rs#L1732) locks in that behavior. The spec says non-object `definitions` must be ignored with no modification. Fix this by reading `definitions` via `map.get("definitions").and_then(Value::as_object).cloned()` first, returning early if it is absent or non-object, and only then removing the top-level `definitions` key before resolving refs. Update the edge-case test to assert the schema is unchanged in that case.

2. [src/adapters/process_backend.rs#L1184](/root/new-ralph-burning/src/adapters/process_backend.rs#L1184) will recurse forever on self-referential or mutually recursive definitions, because each `$ref` expansion resolves against the full `definitions` map again. Since this helper now runs in all three backend emission paths, a recursive schema would blow up Claude, Codex, and OpenRouter request construction. The spec’s out-of-scope note says recursive refs should be left unresolved rather than panic. Fix this by threading a `HashSet<String>` or stack of currently-expanding definition names through `resolve_refs`; if a `$ref` target is already in progress, leave that node unchanged and return. Add a regression test for a self-referential definition.

The new wiring itself is otherwise correctly scoped: the helper is called only in the three strict-schema backend paths the spec requested.

I couldn’t run `cargo test` here because `cargo` is not installed in this environment.
