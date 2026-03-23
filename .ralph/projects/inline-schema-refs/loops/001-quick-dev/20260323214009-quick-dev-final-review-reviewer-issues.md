---
artifact: quick-dev-final-review
loop: 1
project: inline-schema-refs
backend: codex
role: reviewer
created_at: 2026-03-23T21:40:09Z
---

# Final Review: AMENDMENTS

## Amendment: [P2] AC9 is only partially covered by the end-to-end tests

### Problem
The new integration coverage in [tests/unit/process_backend_test.rs:89](/root/new-ralph-burning/tests/unit/process_backend_test.rs#L89), [tests/unit/process_backend_test.rs:436](/root/new-ralph-burning/tests/unit/process_backend_test.rs#L436), and [tests/unit/process_backend_test.rs:541](/root/new-ralph-burning/tests/unit/process_backend_test.rs#L541) still does not verify the actual backend schema payloads the way the prompt requires.

On the Claude side, the test only scans the flat `claude-args.txt` shell log for `"$ref"` / `"definitions"` substrings. That does not isolate the `--json-schema` argument or prove it is valid JSON. On the Codex side, the public integration test still never inspects the schema file contents at all; it only checks that the temp files were cleaned up. As written, a regression in the real CLI payload/file shape can slip through even though AC9 calls for end-to-end verification of both backends.

### Proposed Change
Update the public integration tests to validate the exact schema artifacts sent to each backend:

1. Capture Claude args in a parseable form, extract the `--json-schema` value, parse it as JSON, and recursively assert no `$ref` keys and no top-level `definitions`.
2. Extend the Codex invoke-path test so the fake Codex binary preserves or copies the `--output-schema` file before cleanup, then parse that file and make the same assertions.

### Affected Files
- [tests/unit/process_backend_test.rs](/root/new-ralph-burning/tests/unit/process_backend_test.rs) - strengthen the Claude integration assertion and add real Codex schema-file verification.
