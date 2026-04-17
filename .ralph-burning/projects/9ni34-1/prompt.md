# Generate workstreams, epics, bead specs, and dependency hints from the planning output

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add generation logic for MilestoneBundle content. Do NOT restructure existing models or renderers.

## Background — what already exists

### Data models (all in `src/contexts/milestone_record/bundle.rs`):
- `MilestoneBundle` — top-level container with identity, goals, non_goals, constraints, acceptance_map, workstreams, default_flow, agents_guidance
- `Workstream` — name, description, beads: Vec<BeadProposal>
- `BeadProposal` — bead_id, title, description, bead_type, priority, labels, depends_on, acceptance_criteria, flow_override
- `AcceptanceCriterion` — id, description, covered_by
- `render_plan_md()` and `render_plan_json()` — deterministic renderers already exist
- `validate()` — semantic validation already exists (dependency checks, AC coverage consistency)

### Pipeline (in `src/contexts/requirements_drafting/`):
- Full-mode stages: Ideation → Research → Synthesis → ImplementationSpec → GapAnalysis → Validation → ProjectSeed → **MilestoneBundle**
- `generate_and_commit_milestone_bundle()` in service.rs — orchestrates bundle generation
- `RequirementsContract::milestone_bundle()` in contracts.rs — defines schema+domain validation
- The MilestoneBundle stage is the final stage, generated after all upstream stages complete

### What's missing:
The planner currently produces the MilestoneBundle via LLM generation with a prompt, but the prompt needs to be enhanced so the LLM produces:
1. Well-structured workstreams that group related beads logically
2. BeadProposals with meaningful titles, descriptions, types, priorities, labels
3. Dependency hints that reflect actual sequencing constraints
4. Acceptance criteria mapped bidirectionally (beads → ACs, ACs → beads)
5. Notes about what's intentionally deferred

## What to implement

### 1. Enhance the milestone bundle prompt template

In the requirements drafting pipeline (likely `src/contexts/requirements_drafting/` — look for where the MilestoneBundle stage prompt is assembled):

- Add explicit instructions for the LLM to generate:
  - **Workstreams**: Group beads into cohesive epics by theme/layer (e.g., "data model", "API", "CLI", "testing")
  - **BeadProposals**: Each bead should have a clear title, description with rationale and scope, bead_type, priority (P1 for critical-path, P2 for important, P3 for nice-to-have), and relevant labels
  - **Dependency hints**: Explicit `depends_on` references with the bead IDs that must complete first. Include rationale in the description for why the dependency exists
  - **Acceptance criteria mapping**: Each bead declares which ACs it covers. Each AC's `covered_by` field matches
  - **Deferred work notes**: Include in workstream or bead descriptions what is intentionally NOT included and why

### 2. Add workstream generation helpers

In `src/contexts/milestone_record/bundle.rs` or a new helper module:

- Add `fn infer_workstream_order(workstreams: &[Workstream]) -> Vec<usize>` — suggests execution order based on dependency topology
- Add `fn validate_dependency_graph(bundle: &MilestoneBundle) -> Result<(), Vec<String>>` — checks for cycles, missing targets, and reports issues (if not already covered by `validate()`)
- Add `fn summarize_bundle(bundle: &MilestoneBundle) -> BundleSummary` — produces a summary struct with total beads, beads per workstream, dependency depth, AC coverage percentage

### 3. Add bundle summary to plan.md rendering

Enhance `render_plan_md()` to include:
- A summary section at the top (total beads, workstreams, AC coverage)
- A "Deferred Items" section if any beads or workstreams mention deferred work
- A dependency graph section showing the topological order

### 4. Add tests

- Test workstream ordering with dependencies
- Test dependency cycle detection
- Test bundle summary generation
- Test that render_plan_md includes the new sections

## Files to modify

- `src/contexts/requirements_drafting/contracts.rs` or wherever the MilestoneBundle prompt is assembled — enhance prompt template
- `src/contexts/milestone_record/bundle.rs` — add helper functions (infer_workstream_order, validate_dependency_graph, summarize_bundle)
- `src/contexts/milestone_record/bundle.rs` — enhance render_plan_md with summary sections
- Tests in relevant test modules

## Acceptance Criteria
- Planner prompt instructs LLM to generate structured workstreams with grouped beads
- BeadProposals include title, description, type, priority, labels, dependencies, AC coverage
- Dependency hints are validated (no cycles, valid targets)
- Bundle summary available (total beads, coverage, depth)
- render_plan_md includes summary and deferred items sections
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
