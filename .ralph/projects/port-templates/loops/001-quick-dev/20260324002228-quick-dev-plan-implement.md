---
artifact: quick-dev-plan-implement
loop: 1
project: port-templates
backend: claude
role: implementer
created_at: 2026-03-24T00:22:28Z
---

Single file changed, only the template constants. Here's a summary of all changes:

## Templates Updated

| Template | What changed |
|---|---|
| `STAGE_DEFAULT_TEMPLATE` | **Kept as-is** per spec |
| `FINAL_REVIEW_REVIEWER_DEFAULT` | Added role identity ("code reviewer"), instructions to read files end-to-end, cite files/lines, focus on correctness/safety/robustness not cosmetics, Problem/Proposed Change/Affected Files format guidance |
| `FINAL_REVIEW_VOTER_DEFAULT` | Added role identity ("reviewer voting on amendments"), instructions to consider each amendment + planner position, explicit "Do NOT reject as out of scope" and "Do NOT dismiss concurrency issues as theoretical" guidance |
| `FINAL_REVIEW_ARBITER_DEFAULT` | Added role identity ("arbiter resolving disputed amendments"), instructions for final ruling with clear rationale citing evidence |
| `COMPLETION_PANEL_COMPLETER_DEFAULT` | Added role identity ("project completion validator"), verification instructions, "MUST use a DIFFERENT perspective than the Planner" directive |
| `PROMPT_REVIEW_REFINER_DEFAULT` | Added role identity ("prompt reviewer evaluating for clarity, completeness, feasibility, testability"), gap identification and rewrite instructions |
| `PROMPT_REVIEW_VALIDATOR_DEFAULT` | Added role identity ("prompt review validator"), comparison instructions, actionable rejection guidance |

All templates use only their declared manifest placeholders, keep `{{json_schema}}` at the end, and all 20 tests pass with no clippy warnings.
