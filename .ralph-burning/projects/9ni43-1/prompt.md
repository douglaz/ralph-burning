# Materialize a MilestoneBundle into a root epic, epics, beads, and dependencies in .beads

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add bead materialization logic. Do NOT modify existing materialize_bundle() behavior — extend it.

## Background — what already exists

### BrMutationAdapter (`src/adapters/br_process.rs`):
- `create_bead(title, bead_type, priority, labels, description)` — line 1307
- `add_dependency(from_id, depends_on_id)` — line 1368
- `comment_bead(id, text)` — line 1399
- `sync_flush()` — line 1413
- `sync_if_dirty()` — line 1465

### BrCommand::create (`src/adapters/br_process.rs` line 431):
- Builds `br create --title=<title> --type=<type> --priority=<priority>`
- Supports `.kv("label", label)` for each label, `.kv("description", desc)` for description

### BeadProposal (`src/contexts/milestone_record/bundle.rs` ~line 1475):
- `bead_id: Option<String>`, `title: String`, `description: Option<String>`
- `bead_type: Option<String>`, `priority: Option<u32>`, `labels: Vec<String>`
- `depends_on: Vec<String>`, `acceptance_criteria: Vec<String>`

### Workstream (`src/contexts/milestone_record/bundle.rs`):
- `name: String`, `description: Option<String>`, `beads: Vec<BeadProposal>`

### materialize_bundle() (`src/contexts/milestone_record/service.rs` line 1351):
- Creates MilestoneRecord + plan files from bundle
- Does NOT create beads via br — this is the gap to fill

## What to implement

### 1. Add `materialize_beads()` function

In `src/contexts/milestone_record/service.rs` (or a new `src/contexts/milestone_record/bead_export.rs`):

Create a function that takes a MilestoneBundle and a BrMutationAdapter and:

1. **Create root epic**: Call `create_bead()` with the milestone name as title, type "epic", priority P1, and label "milestone-root"

2. **Create workstream epics**: For each workstream in the bundle:
   - Call `create_bead()` with workstream name, type "epic", priority P1
   - Add dependency: workstream epic depends on root epic (parent-child)
   - If workstream has a description, add it as a comment

3. **Create task beads**: For each BeadProposal in each workstream:
   - Call `create_bead()` with:
     - `title`: from BeadProposal.title
     - `bead_type`: from BeadProposal.bead_type or default "task"
     - `priority`: format as "P{n}" from BeadProposal.priority or default "P2"
     - `labels`: from BeadProposal.labels
     - `description`: from BeadProposal.description
   - Add parent-child dependency to workstream epic

4. **Add dependency edges**: For each BeadProposal with `depends_on`:
   - Resolve bead IDs (map proposal IDs to created bead IDs)
   - Call `add_dependency(from_id, depends_on_id)` for each

5. **Attach planning rationale**: For beads with acceptance criteria or description:
   - Call `comment_bead()` with planning context (which ACs this bead covers, rationale)

6. **Sync**: Call `sync_flush()` after all mutations

### 2. Track created bead IDs

The `create_bead()` call returns the output from `br create` which includes the created bead ID. Parse this to build an ID mapping:
- `HashMap<String, String>` mapping proposal bead_id → actual created bead_id
- Use this mapping when resolving `depends_on` references

### 3. Wire into materialize_bundle or add a new CLI command

Either:
- Extend `materialize_bundle()` to optionally call `materialize_beads()` after plan materialization
- Or add a new CLI subcommand like `milestone export-beads <milestone_id>` that loads the bundle and calls `materialize_beads()`

The CLI approach is cleaner for now — it separates plan creation from bead creation.

### 4. Add idempotency guard

Before creating beads, check if beads with matching titles already exist (via `BrAdapter::list_matching_beads()` or similar). Skip creation for beads that already exist.

### 5. Add tests

- Test bead creation from a simple bundle (2 workstreams, 3 beads each)
- Test dependency edge creation
- Test ID mapping resolution
- Test idempotency (running twice doesn't duplicate beads)
- Test error handling (br create failure mid-way)

## Files to modify

- `src/contexts/milestone_record/service.rs` or new `bead_export.rs` — materialization logic
- `src/cli/milestone.rs` — add export-beads subcommand if using CLI approach
- Tests in relevant test modules

## Acceptance Criteria
- MilestoneBundle workstreams and beads are created as br beads
- Root epic and workstream epics are created with proper hierarchy
- Dependency edges match BeadProposal.depends_on
- Planning rationale attached as comments
- Idempotent: running twice doesn't create duplicates
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
