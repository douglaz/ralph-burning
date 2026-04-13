## Objective

Add a scope-guidance block to the `plan_and_implement` stage that instructs the planner to respect bead scope boundaries. Follow the established pattern from `review_classification.rs` for conditional guidance injection.

## Architecture

The implementation touches four files:

### 1. New module: `src/contexts/workflow_composition/plan_scope_guidance.rs`

Create a new module (parallel to `review_classification.rs`) with a single public function:

```rust
pub fn render_plan_scope_guidance(
    pe_bead_ids: &std::collections::HashSet<String>,
) -> String
```

This function renders a markdown guidance block that instructs the planner to:
- **(a)** Plan only items within the "Must-Do Scope" and "Acceptance Criteria" sections.
- **(b)** Note items matching "Explicit Non-Goals" as explicitly deferred (reason: non-goal).
- **(c)** Note items matching "Already Planned Elsewhere" as deferred with the owning bead ID. List the valid PE bead IDs from the `pe_bead_ids` parameter (sorted, like `render_classification_guidance` does).
- **(d)** Note genuinely new out-of-scope items as follow-up candidates rather than absorbing them into the plan.

The guidance text MUST reference canonical section names via the constants from `task_prompt_contract.rs`:
- `SECTION_MUST_DO_SCOPE`
- `SECTION_EXPLICIT_NON_GOALS`
- `SECTION_ACCEPTANCE_CRITERIA`
- `SECTION_ALREADY_PLANNED_ELSEWHERE`

Use `format!()` with the constants — do not hardcode section name strings.

Return an empty string if called when no guidance should be shown (though the caller handles the conditional check).

### 2. Register module: `src/contexts/workflow_composition/mod.rs`

Add `pub mod plan_scope_guidance;` to the module declarations.

### 3. Wire into engine: `src/contexts/workflow_composition/engine.rs`

In `build_stage_prompt`, find the existing block (around lines 166-171):

```rust
let classification_guidance_block = if contract.stage_id == StageId::Review {
    let pe_bead_ids = task_prompt_contract::extract_pe_bead_ids(&project_prompt);
    review_classification::render_classification_guidance(&pe_bead_ids, false)
} else {
    String::new()
};
```

Extend the conditional to also handle `StageId::PlanAndImplement`:

```rust
let classification_guidance_block = if contract.stage_id == StageId::Review {
    let pe_bead_ids = task_prompt_contract::extract_pe_bead_ids(&project_prompt);
    review_classification::render_classification_guidance(&pe_bead_ids, false)
} else if contract.stage_id == StageId::PlanAndImplement
    && task_prompt_contract::prompt_uses_contract(&project_prompt)
{
    let pe_bead_ids = task_prompt_contract::extract_pe_bead_ids(&project_prompt);
    plan_scope_guidance::render_plan_scope_guidance(&pe_bead_ids)
} else {
    String::new()
};
```

This reuses the existing `classification_guidance` optional placeholder slot — no new placeholder needed. The built-in default template already renders `{{classification_guidance}}` after `{{remediation}}`, and the manifest in `template_catalog.rs` already lists it in `STAGE_OPTIONAL`.

### 4. Tests: `src/contexts/workflow_composition/plan_scope_guidance.rs`

Add unit tests at the bottom of the new module (following the pattern in `review_classification.rs`):

- **`test_guidance_rendered_with_contract`**: Call `render_plan_scope_guidance` with a non-empty `pe_bead_ids` set. Assert the result contains the canonical section names (Must-Do Scope, Explicit Non-Goals, Already Planned Elsewhere, Acceptance Criteria). Assert it contains the PE bead IDs.
- **`test_guidance_rendered_without_pe_beads`**: Call with an empty set. Assert the result still contains scope guidance but does not list any specific bead IDs (or shows an appropriate "none" message).
- **`test_no_guidance_without_contract`**: This is tested implicitly by the engine conditional, but add an integration-level test in engine.rs tests (or the existing test module) that builds a stage prompt for `PlanAndImplement` with a project prompt lacking the contract marker and asserts the output does not contain the scope guidance header text.

## Guidance Block Content

The rendered guidance should be concise (aim for ~20-30 lines of markdown). Suggested structure:

```
## Plan Scope Guidance

Your plan must stay within the current bead's declared scope. Use the
following rules when deciding what to include:

**Plan (in-scope):** Items that fall under "<Must-Do Scope>" or are
required by "<Acceptance Criteria>". These are the ONLY items that
should appear as planned work.

**Defer as non-goal:** Items that match "<Explicit Non-Goals>".
Note these in your plan narrative as explicitly deferred (reason: non-goal).
Do NOT plan implementation work for them.

**Defer as planned-elsewhere:** Items that belong to another bead listed
in "<Already Planned Elsewhere>". Note them as deferred with the owning
bead ID. Valid bead IDs: [sorted list or "(none listed)"]

**Defer as follow-up:** Genuinely new items that are out of scope but
worth tracking. Note them as follow-up candidates. Do NOT absorb them
into the plan.

When in doubt, defer rather than include. The review stage will catch
scope violations, but preventing them at plan time produces cleaner diffs.
```

Replace `<Section Name>` placeholders with the actual constant values at render time.

## Constraints

- Do NOT modify `template_catalog.rs` — reuse the existing `classification_guidance` placeholder.
- Do NOT modify `review_classification.rs` — keep plan scope guidance separate.
- Do NOT modify `task_prompt_contract.rs` — reuse existing functions.
- Do NOT add structured fields to `ExecutionPayload` — deferred items are advisory in the plan narrative only.
- Use `anyhow` error handling if any fallible operations are needed (though rendering is infallible).
- Follow existing Rust module conventions (pub use, #[cfg(test)] mod tests, etc.).

## Verification

1. `cargo test` — all existing tests pass.
2. `cargo clippy` — no new warnings.
3. `cargo fmt --check` — properly formatted.
4. New unit tests pass for the three cases described above.