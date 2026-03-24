---
artifact: quick-dev-codex-review
loop: 1
project: port-templates
backend: codex
role: reviewer
created_at: 2026-03-24T00:32:09Z
---

# Review: CHANGES REQUESTED

1. [template_catalog.rs:135](/root/new-ralph-burning/src/contexts/workspace_governance/template_catalog.rs#L135) still carries a legacy markdown-output rule: "`## Refined Prompt` section MUST be the final section in your output." That directly conflicts with the enforced JSON-only contract for the refiner at [panel_contracts.rs:19](/root/new-ralph-burning/src/contexts/workflow_composition/panel_contracts.rs#L19) and with the template’s own “Return only JSON” instruction. This can push the refiner toward invalid markdown instead of schema-conforming JSON. Fix: remove the markdown-section requirement and replace it with JSON-specific guidance, e.g. put the rewritten prompt in `refined_prompt`, summarize the changes in `refinement_summary`, and list the key improvements in `improvements`.

2. [template_catalog.rs:157](/root/new-ralph-burning/src/contexts/workspace_governance/template_catalog.rs#L157) tells validators to “Compare the refined prompt against the original intent,” but every validator call path only passes the refined prompt text through `prompt_text` at [prompt_review.rs:150](/root/new-ralph-burning/src/contexts/workflow_composition/prompt_review.rs#L150) and [prompt_review.rs:293](/root/new-ralph-burning/src/contexts/workflow_composition/prompt_review.rs#L293). No validator ever receives the original prompt, so this instruction is impossible to follow on every execution path. Fix: rewrite the validator template to assess the provided prompt for clarity, completeness, feasibility, and actionability based on the text it actually receives. If you want original-vs-refined comparison semantics, the caller would need to compose both into `prompt_text`, but that is outside this spec.

3. [template_catalog.rs:215](/root/new-ralph-burning/src/contexts/workspace_governance/template_catalog.rs#L215) does not fully implement the required final-review reviewer port. The spec asked for explicit AMENDMENTS vs NO AMENDMENTS guidance and the legacy template also told reviewers to inspect the diff before reading touched files end-to-end. The current text only says “report NO AMENDMENTS” and “provide a clear Problem description,” without mapping those branches into the current JSON contract. Fix: add JSON-aware instructions such as: use your tools to inspect the current diff, then read implicated files end-to-end; when no issues exist, return `amendments: []` and make `summary` a clear NO AMENDMENTS rationale; when issues exist, structure each `amendments[].body` with labeled `Problem`, `Proposed Change`, and `Affected Files` sections.

I could not verify the spec’s `cargo fmt`, `cargo test`, or `cargo clippy` requirement here because `cargo` is not installed in this environment (`cargo: command not found`).
