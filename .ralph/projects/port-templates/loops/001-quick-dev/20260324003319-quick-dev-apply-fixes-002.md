---
artifact: quick-dev-apply-fixes
loop: 1
iteration: 2
project: port-templates
backend: claude
role: implementer
created_at: 2026-03-24T00:33:19Z
---

# Apply reviewer-requested fixes to template_catalog.rs

All three reviewer findings addressed with minimal, scoped edits:

## Fix 1 — Refiner template (line 135): markdown-section rule replaced with JSON guidance
Removed `"## Refined Prompt section MUST be the final section"` which conflicted with the JSON-only contract (`PromptRefinementPayload` at `panel_contracts.rs:19`). Replaced with explicit JSON field guidance: put the rewritten prompt in `refined_prompt`, summarize changes in `refinement_summary`, list improvements in `improvements` — matching the schema fields exactly.

## Fix 2 — Validator template (line 157): removed impossible original-intent comparison
The validator only receives `prompt_text` (confirmed at `prompt_review.rs:150` and `:293`); it never gets the original prompt. Rewrote instructions to assess the provided prompt for clarity, completeness, feasibility, and actionability on its own merits — checking for explicit acceptance criteria, no vague requirements, and nothing under-specified.

## Fix 3 — Reviewer template (line 215): added JSON-aware AMENDMENTS/NO AMENDMENTS mapping
Added diff-inspection instruction ("use your tools to inspect the current diff, then read each implicated file end-to-end"). Mapped both branches to the JSON contract:
- Issues found: `amendments` array with `body` containing labeled **Problem**, **Proposed Change**, and **Affected Files** sections
- No issues: `amendments: []` with `summary` as a NO AMENDMENTS rationale
