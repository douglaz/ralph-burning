Implement bead `ralph-burning-9ni.10.6` in `ralph-burning`.

Title: Build shared test infrastructure: mock br/bv adapters, fixture builders, and log capture helpers

Goal:
Create reusable test infrastructure that milestone, CLI, integration, and conformance tests can share instead of duplicating setup code.

Scope:
- add a mock `br` adapter with configurable responses, error simulation, call tracking, and optional latency controls
- add a mock `bv` adapter with configurable next-bead/triage responses and call tracking
- add fixture builders for milestone state, bead graph state, and realistic temp workspaces
- add log capture helpers that can assert on structured fields and emit useful failure context
- make the helpers accessible from the common test support module used across the repo

Acceptance criteria:
- mock adapters exist for `br` and `bv` operations with full call tracking
- fixture builders can create realistic milestone and bead state in one call
- log capture can assert on specific structured fields
- temp workspace helpers create valid `.ralph-burning` and `.beads` directory structures
- helpers are documented with usage examples in doc comments
- test infrastructure lives in a dedicated shared module available to all test files

Implementation notes:
- keep the infrastructure deterministic and suitable for CI
- prefer typed helpers over ad hoc JSON/string fixtures
- reduce duplication in existing tests where practical, but keep scope on shared infrastructure first
- add right-sized tests for the new helpers themselves
