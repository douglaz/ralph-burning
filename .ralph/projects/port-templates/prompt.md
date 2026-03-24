# Port legacy ralph prompt templates to ralph-burning (#49)

## Goal

Replace the minimal built-in default templates in `src/contexts/workspace_governance/template_catalog.rs` with rich templates ported from the legacy ralph orchestrator at `~/multibackend-orchestration/src/prompts/templates.rs`.

The current templates are just headings + placeholders + JSON schema with no role instructions, review criteria, or format guidance. This causes reviewers to approve with "acknowledged risks" instead of requesting changes, and final reviewers to produce 0 amendments for real issues.

## What to change

**File:** `src/contexts/workspace_governance/template_catalog.rs`

### 1. `STAGE_DEFAULT_TEMPLATE` — the shared stage template

The current template is:
```
# Stage Execution Prompt

{{role_instruction}}

## Original Project Prompt

{{project_prompt}}

{{prior_outputs}}

{{remediation}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

\```json
{{json_schema}}
\```
```

This is actually fine as a **wrapper** — it delegates role-specific guidance to `{{role_instruction}}`. The problem is that the callers pass minimal one-line role instructions. But since we're only changing template_catalog.rs, we can't change caller code here — the `role_instruction` content is provided at render time by the engine.

**Keep `STAGE_DEFAULT_TEMPLATE` as-is.** The issue is in the panel templates, not the stage template.

### 2. `FINAL_REVIEW_REVIEWER_DEFAULT` — the final review proposal template

**Current:** Just heading + project_prompt + json_schema.

**Port from legacy `default_final_reviewer_template()`:**

Add role identity ("You are a code reviewer. Review for correctness, safety, and robustness"), instructions to read key implementation files end-to-end, cite specific files and line numbers, and explicit format guidance for AMENDMENTS vs NO AMENDMENTS with Problem/Proposed Change/Affected Files structure.

The template must keep the existing required placeholders (`project_prompt`, `json_schema`) and not add new ones that aren't in the manifest.

### 3. `FINAL_REVIEW_VOTER_DEFAULT` — the voter template

**Current:** Just title + amendments + planner_positions + json_schema.

**Port from legacy `default_vote_template()`:**

Add role identity ("You are a reviewer voting on proposed amendments"), instructions to consider each amendment and the planner's position, and vote ACCEPT or REJECT with rationale. Include guidance: "Do NOT reject amendments because they are 'out of scope'" and "Do NOT dismiss concurrency/isolation issues as 'theoretical'".

Must keep existing placeholders: `title`, `amendments`, `json_schema`, and optional `planner_positions`.

### 4. `FINAL_REVIEW_ARBITER_DEFAULT` — the arbiter template

**Current:** Just heading + amendments + planner_positions + reviewer_votes + json_schema.

**Port from legacy `default_arbiter_template()`:**

Add role identity ("You are the arbiter resolving disputed amendments"), instructions to make final ruling with clear rationale.

Must keep existing placeholders: `amendments`, `planner_positions`, `reviewer_votes`, `json_schema`.

### 5. `COMPLETION_PANEL_COMPLETER_DEFAULT` — the completion vote template

**Current:** Just heading + prompt_text + json_schema.

**Port from legacy `default_completer_template()`:**

Add role identity ("You are a project completion validator"), instructions to review requirements, check all implemented features, verify nothing is missing. Add "You MUST use a DIFFERENT perspective than the Planner."

Must keep existing placeholders: `prompt_text`, `json_schema`.

### 6. `PROMPT_REVIEW_REFINER_DEFAULT` and `PROMPT_REVIEW_VALIDATOR_DEFAULT`

**Port from legacy `default_prompt_reviewer_template()` and `default_prompt_review_validator_template()`:**

Add role identity for the refiner ("You are a prompt reviewer evaluating for clarity, completeness, feasibility, and testability") and for the validator ("You are a prompt review validator deciding whether the refined prompt is acceptable").

Must keep existing placeholders: `role_label`, `prompt_text`, `json_schema`.

## Important constraints

1. **Do NOT add new placeholders** — only use placeholders already declared in the manifest for each template. The manifests are defined in `manifest_for()` in the same file.
2. **Do NOT change the template resolution/rendering machinery** — only change the `const` template strings.
3. **Do NOT change test expectations** unless a test hardcodes the exact template content — tests that check for placeholder presence should still pass.
4. **Keep the `{{json_schema}}` block** at the end of each template — the models need the schema to produce valid JSON output.
5. The templates should be **self-contained guidance** — since they're the built-in defaults, they must work without any external context about what the role should do.
6. Port the **spirit and quality** of the legacy templates, adapting to ralph-burning's placeholder system. Don't copy verbatim — the legacy templates use different placeholders (`{{system_guardrails}}`, `{{master_prompt}}`, `{{feature_spec}}`, etc.) that don't exist in ralph-burning.
7. Run `cargo test` and `cargo clippy` to verify everything compiles and tests pass.
8. Run `cargo fmt` to ensure formatting.
