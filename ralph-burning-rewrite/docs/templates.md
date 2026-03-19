# Prompt Templates

All prompt surfaces (workflow stages, panels, requirements) are routed through a shared template catalog. Templates use `{{placeholder}}` syntax and follow a deterministic three-tier precedence resolution.

## Resolution Order

1. **Project override**: `.ralph-burning/projects/<project-id>/templates/<template-id>.md`
2. **Workspace override**: `.ralph-burning/templates/<template-id>.md`
3. **Built-in default** (compiled into the binary)

Resolution is singular — layers are never merged. The first match wins.

## Template IDs

### Workflow Stages

These IDs correspond to `StageId::as_str()`:

| Template ID | Required Placeholders | Optional Placeholders |
|---|---|---|
| `planning` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `implementation` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `qa` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `review` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `acceptance_qa` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `prompt_review` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `completion_panel` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `final_review` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `plan_and_implement` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `apply_fixes` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `docs_plan` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `docs_update` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `docs_validation` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `ci_plan` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `ci_update` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |
| `ci_validation` | `role_instruction`, `project_prompt`, `json_schema` | `prior_outputs`, `remediation` |

### Panels

| Template ID | Required Placeholders | Optional |
|---|---|---|
| `prompt_review_refiner` | `role_label`, `prompt_text`, `json_schema` | — |
| `prompt_review_validator` | `role_label`, `prompt_text`, `json_schema` | — |
| `completion_panel_completer` | `prompt_text`, `json_schema` | — |
| `final_review_reviewer` | `prompt_text`, `json_schema` | — |
| `final_review_voter` | `prompt_text`, `prior_reviews`, `json_schema` | — |
| `final_review_arbiter` | `prompt_text`, `prior_reviews`, `json_schema` | — |

### Requirements

| Template ID | Required Placeholders | Optional |
|---|---|---|
| `requirements_draft` | `idea` | `answers` |
| `requirements_review` | `idea`, `draft_artifact` | — |
| `requirements_question_set` | `idea`, `draft_artifact`, `review_artifact` | — |
| `requirements_project_seed` | `synthesis_artifact`, `impl_spec_artifact` | — |
| `requirements_ideation` | `base_context` | — |
| `requirements_research` | `base_context`, `ideation_artifact` | — |
| `requirements_synthesis` | `base_context`, `ideation_artifact` | `answers` |
| `requirements_implementation_spec` | `synthesis_artifact` | — |
| `requirements_gap_analysis` | `synthesis_artifact`, `impl_spec_artifact` | — |
| `requirements_validation` | `synthesis_artifact`, `impl_spec_artifact`, `gap_artifact` | — |

## Creating Overrides

1. Create the override file at the appropriate path:
   - Workspace: `.ralph-burning/templates/<template-id>.md`
   - Project: `.ralph-burning/projects/<project-id>/templates/<template-id>.md`

2. Include all required placeholders using `{{placeholder_name}}` syntax.

3. Optional placeholders may be omitted — they will be collapsed (empty string) during rendering.

## Validation Rules

Override files are validated at resolution time. The following cause a hard failure (`MalformedTemplate` error):

- **Unknown placeholder**: A `{{name}}` not listed in the template's manifest.
- **Missing required placeholder**: A required placeholder absent from the override text.
- **Non-UTF-8 content**: The override file cannot be read as valid UTF-8.

Malformed overrides never silently fall back to a lower-precedence source. A malformed project override does not fall back to the workspace override or built-in default.

## Example

```markdown
# My Custom Planning Prompt

{{role_instruction}}

## Task

{{project_prompt}}

## Output Format

```json
{{json_schema}}
```
```
