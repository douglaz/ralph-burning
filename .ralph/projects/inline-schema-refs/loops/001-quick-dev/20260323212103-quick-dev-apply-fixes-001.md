---
artifact: quick-dev-apply-fixes
loop: 1
iteration: 1
project: inline-schema-refs
backend: claude
role: implementer
created_at: 2026-03-23T21:21:03Z
---

Everything looks correct. Here's the response:

# Applied Reviewer-Requested Fixes

## Fix 1: Non-object `definitions` must not mutate the schema (AC 6)

**File:** `src/adapters/process_backend.rs:1163-1179`

Changed `inline_schema_refs` to use `map.get("definitions").and_then(Value::as_object).cloned()` to inspect `definitions` before removing it. If `definitions` is absent or non-object, the function returns early without any mutation. The `map.remove("definitions")` call now only executes after confirming it's an object.

**Test update:** The sub-case 2 test (`"definitions": 42`) now clones the schema before calling `inline_schema_refs` and asserts `schema == original` — confirming the schema is completely unchanged, including the `definitions` key still being present.

## Fix 2: Cycle detection for self-referential/mutually recursive definitions

**File:** `src/adapters/process_backend.rs:1178,1182-1217`

`resolve_refs` now takes an `&mut HashSet<String>` parameter (`expanding`) that tracks which definition names are currently being expanded up the call stack. Before expanding a `$ref` target:
- If the name is already in `expanding`, the `$ref` node is left unchanged (no recursion).
- Otherwise, the name is inserted before recursing and removed after, so the guard only applies to cycles, not to legitimate re-use of the same definition at different points in the tree.

**New tests:**
- `inline_schema_refs_handles_self_referential_definition` — A `Node` definition with a `child` property that `$ref`s itself. Asserts the top-level ref is resolved but the nested self-reference is left as `{"$ref": "#/definitions/Node"}`.
- `inline_schema_refs_handles_mutually_recursive_definitions` — `A` refs `B`, `B` refs `A`. Asserts `A` and `B` are each resolved once, but the back-edge (`A` inside `B`) is left unresolved.
