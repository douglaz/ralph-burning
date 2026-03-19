Feature: Template Override Parity (Slice 7)
  Workspace and project prompt template overrides for workflow and requirements paths.

  @parity_slice7_workspace_override
  Scenario: Workspace template override is used for workflow prompts
    Given a workspace with a template override for "planning"
    When a planning stage prompt is built
    Then the prompt uses the workspace override content

  @parity_slice7_project_override
  Scenario: Project template override is used and takes precedence
    Given a workspace with a template override for "planning"
    And a project with a template override for "planning"
    When a planning stage prompt is built for the project
    Then the prompt uses the project override content

  @parity_slice7_project_over_workspace
  Scenario: Project override takes precedence over workspace override
    Given both workspace and project overrides exist for "requirements_ideation"
    When a requirements ideation prompt is resolved
    Then the project override is selected

  @parity_slice7_malformed_workflow_rejection
  Scenario: Malformed override is rejected on a workflow prompt
    Given a workspace with a malformed template override for "planning"
    When a planning stage prompt is built
    Then the build fails with a malformed template error
    And no payload or artifact is written

  @parity_slice7_malformed_requirements_rejection
  Scenario: Malformed override is rejected on a requirements prompt
    Given a workspace with a malformed template override for "requirements_draft"
    When a requirements draft prompt is built
    Then the build fails with a malformed template error
    And no payload or artifact is written

  @parity_slice7_no_silent_fallback
  Scenario: Malformed project override does not silently fall back
    Given a valid workspace override for "requirements_ideation"
    And a malformed project override for "requirements_ideation"
    When the template is resolved for the project
    Then resolution fails with a malformed template error
    And the workspace override is not used as a fallback

  @parity_slice7_built_in_default_preserved
  Scenario: Built-in default is used when no overrides exist
    Given a workspace with no template overrides
    When any prompt surface resolves a template
    Then the built-in default is used
    And the output is structurally equivalent to the previous hardcoded behavior

  @parity_slice7_placeholder_validation
  Scenario: Override with unknown placeholder is rejected
    Given a workspace override that contains an unknown placeholder
    When the template is resolved
    Then resolution fails citing the unknown placeholder

  @parity_slice7_non_utf8_rejection
  Scenario: Non-UTF-8 override file is rejected
    Given a workspace override file with non-UTF-8 content
    When the template is resolved
    Then resolution fails citing non-UTF-8 content

  @parity_slice7_all_ids_have_manifests
  Scenario: All frozen template IDs have manifests
    Given the template catalog
    When all stage, panel, and requirements IDs are checked
    Then every ID has a corresponding manifest
