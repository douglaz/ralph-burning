# Assemble scenario-specific temp workspace with .beads and milestone artifacts for integration testing

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add a test fixture builder function. Do NOT modify existing test infrastructure or source code.

## Background — what already exists

### Shared test infrastructure (`src/test_support/`):
- `TempWorkspaceBuilder::new().with_milestone().with_bead_graph().build()` — creates temp workspaces
- `MilestoneFixtureBuilder::new().with_name().add_bead().with_task_run().with_journal_event()` — materializes milestones
- `BeadGraphFixtureBuilder::new().with_issue().from_bundle()` — writes .beads/issues.jsonl
- `MockBrAdapter` / `MockBvAdapter` — deterministic mock adapters

### MilestoneBundle (`src/contexts/milestone_record/bundle.rs`):
- Full data model with Workstream, BeadProposal, AcceptanceCriterion
- `render_plan_md()` and `render_plan_json()` for plan artifact generation

### materialize_bundle (`src/contexts/milestone_record/service.rs`):
- Creates MilestoneRecord + plan files from a bundle
- Transitions milestone from Planning → Ready

## What to implement

### 1. Create `build_e2e_milestone_scenario_fixture()` function

In `src/test_support/fixtures.rs` (or a new `src/test_support/e2e_fixtures.rs`):

Create a function that builds a complete, self-contained workspace fixture:

```rust
pub fn build_e2e_milestone_scenario_fixture() -> E2eScenarioFixture {
    // Returns a fixture with all paths and mock configs needed
}
```

The fixture should include:

**Milestone artifacts:**
- A milestone in Ready state (already planned)
- plan.md and plan.json written to milestones/{id}/ directory
- A MilestoneBundle with 2 workstreams, 3 beads total, with dependencies

**Bead graph:**
- Root epic + 2 task beads with parent-child dependencies
- One bead depends on the other (sequencing)
- Realistic titles, descriptions, priorities, labels

**Mock adapter responses:**
- Pre-configured MockBrAdapter responses for: list, show, create, close, sync
- Pre-configured MockBvAdapter responses for: robot-next (returns first ready bead)
- Responses that match the bead graph (correct IDs, titles, statuses)

**Workspace state:**
- `.ralph-burning/` directory structure initialized
- `.beads/issues.jsonl` with the bead graph

### 2. Return struct with all fixture data

```rust
pub struct E2eScenarioFixture {
    pub workspace: TempWorkspace,
    pub milestone_id: MilestoneId,
    pub bundle: MilestoneBundle,
    pub bead_ids: Vec<String>,       // Created bead IDs
    pub mock_br: MockBrAdapter,      // Pre-configured mock
    pub mock_bv: MockBvAdapter,      // Pre-configured mock
}
```

### 3. Ensure fixture creation is fast

- No real br/bv calls — everything uses builders and direct file writes
- No network calls
- Target: <1 second for fixture creation

### 4. Add a smoke test

Add a test that:
- Calls `build_e2e_milestone_scenario_fixture()`
- Verifies the milestone exists and is in Ready state
- Verifies plan.md and plan.json exist
- Verifies .beads/issues.jsonl has the expected beads
- Verifies mock adapters return expected responses

## Files to create/modify

- `src/test_support/e2e_fixtures.rs` (new) or extend `src/test_support/fixtures.rs`
- `src/test_support/mod.rs` — register module if new file
- Tests for the fixture builder

## Acceptance Criteria
- Fixture creates a complete workspace with milestone, plan, bead graph
- MockBr/MockBv adapters pre-configured with matching responses
- Fixture creation is fast (<1 second)
- Smoke test verifies fixture completeness
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
