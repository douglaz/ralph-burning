# Bead 9ni.6.4: Inject nearby/future bead context into the working prompt

## Problem description

When a bead-backed task starts, the working prompt currently includes
the bead's own description, acceptance criteria, and (per bead 9ni.5.7)
upstream/downstream dependency lists. It does **not** systematically
include enough surrounding context for the executing agent to classify
review findings as "planned elsewhere" instead of trying to do that
work itself.

This bead adds a focused "Nearby work" section that summarizes:

1. **Direct dependencies** — beads this bead depends on (with status
   and a one-line outcome summary if closed).
2. **Direct dependents** — beads that depend on this one (title +
   one-line scope summary).
3. **Siblings** — beads at the same level in the epic hierarchy (title
   + one-line scope).
4. **Related work** — beads sharing labels or subsystem ownership
   (title + one-line scope).

Budget: each nearby bead gets at most 2–3 lines, and the entire
"Nearby work" section is capped at roughly **500 tokens** (≈ 2000
characters as a safe approximation — exact tokenization isn't needed,
just a hard byte cap).

Priority order when budget would be exceeded:
**direct deps > direct dependents > siblings > label overlap.** Drop
items from the bottom of the priority list first. Already closed or
deferred beads are omitted entirely (their resolution is in git
history; the bead graph itself doesn't need to repeat it here unless
the closure outcome is load-bearing).

## Required changes

1. **Builder.** In `src/cli/project.rs` (or wherever
   `BeadProjectContext` is assembled — currently
   `execute_create_from_bead` populates `upstream_dependencies` and
   `downstream_dependents`), add construction of a new
   `nearby_bead_context: NearbyBeadContext` field (or extend an
   existing context struct, whichever requires fewer signature
   changes).

2. **Domain type.** Add `NearbyBeadContext` to
   `src/contexts/project_run_record/task_prompt_contract.rs` (or its
   nearest domain home) with:
   ```rust
   pub struct NearbyBeadContext {
       pub direct_dependencies: Vec<NearbyBeadEntry>,
       pub direct_dependents: Vec<NearbyBeadEntry>,
       pub siblings: Vec<NearbyBeadEntry>,
       pub related_work: Vec<NearbyBeadEntry>,
   }
   pub struct NearbyBeadEntry {
       pub bead_id: String,
       pub title: String,
       pub scope_summary: String, // one-line, derived from description
       pub status: String, // open / in_progress / closed / deferred
   }
   ```

3. **Builder logic.** Implement a function (e.g.
   `build_nearby_bead_context(bead, all_beads_summaries) ->
   NearbyBeadContext`) that:
   - Reads dependency/dependent IDs from the focal bead (already on
     `BeadDetail`).
   - Scans `bead_summaries` for siblings (same parent_epic_id; reuse
     `infer_parent_epic_id` already present in
     `execute_create_from_bead`) — limit to ~5 siblings.
   - Scans for related work via label overlap — limit to ~3 entries,
     prefer beads sharing the most labels.
   - Filters out closed/deferred beads.
   - Truncates the assembled section to the ~500-token byte cap, in
     priority order.

4. **Prompt template.** In the renderer (most likely
   `src/contexts/project_run_record/task_prompt_contract.rs` or
   wherever the bead-task prompt is rendered), add a "## Nearby work"
   section after the existing dependency/dependent sections. Use 4
   subsections matching the categories above. If a category is empty,
   omit its subsection rather than printing an empty header.

5. **Token budget enforcement.** A pure function
   `enforce_nearby_context_budget(ctx: NearbyBeadContext, byte_cap:
   usize) -> NearbyBeadContext` is the right shape. Drop entries from
   the lowest-priority list first, then trim within the next-lowest,
   etc. Document the byte cap on `NearbyBeadContext` so the renderer
   does NOT need to re-truncate.

## Tests

In the appropriate test module (likely
`tests/unit/project_run_record_test.rs` or a focused
`prompt_builder_test.rs`):

- **Direct deps + dependents only.** Bead with 2 deps and 1
  dependent. Assert all three appear, in the correct subsections,
  with one-line scope summaries truncated from longer descriptions.
- **Siblings inferred.** Bead with parent_epic_id = `epic-x`. Two
  other open beads share that parent. Assert both appear in
  `siblings`, in stable order.
- **Closed siblings filtered.** Same as above but one sibling is
  `status=closed` — assert it's omitted.
- **Label overlap.** Bead with labels `["a", "b"]`. Two other open
  beads have labels `["b"]` and `["a", "b", "c"]`. Assert the second
  comes first (more overlap), and only top-3 are kept.
- **Budget enforcement.** Construct a context that would exceed the
  byte cap. Assert priority order: deps fully retained, dependents
  retained, siblings/related work trimmed first.
- **Renderer.** Render a bead with empty `direct_dependents` —
  assert the rendered prompt has NO empty "## Direct dependents:"
  header.

Keep total new test code under ~500 lines.

## Scope guard

- Do NOT change the existing `upstream_dependencies` /
  `downstream_dependents` plumbing if it's already working — extend,
  don't replace.
- Do NOT make `bead_summaries` more expensive to load. If the existing
  `load_bead_summaries` is what we need, reuse it. If we need a per-bead
  detail fetch, do it for the focal bead's deps only, not all 200+
  beads.
- Do NOT include AGENTS.md guidance generation here — that's bead
  9ni.6.3.
- Do NOT modify the prompt template structure beyond adding the
  "Nearby work" section.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- Bead-backed task prompts now include a "Nearby work" section with up
  to 4 subsections (deps, dependents, siblings, related).
- Each entry is at most 2–3 lines (bead_id + title + one-line scope).
- Entire section is capped at ~500 tokens (~2000 bytes); priority
  drops siblings/related first when over budget.
- Closed/deferred beads are filtered out.
- Renderer omits empty subsections cleanly.
- Tests above pass.
- `nix build`, `cargo fmt --check`, `cargo clippy --locked -- -D warnings`,
  `cargo test --locked --features test-stub` all green.
