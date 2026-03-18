Feature: Bootstrap and Requirements-to-Project Creation
  Slice 2 restores the convenience path from a completed requirements run into
  a fully initialized project, plus an inline bootstrap flow that can chain
  requirements quick, project creation, active-project selection, and optional
  run start.

  # parity_slice2_create_from_requirements
  Scenario: Create project directly from a completed requirements run
    Given a completed requirements quick run
    When the user runs "project create --from-requirements <run-id>"
    Then the project.toml fields match the seed
    And prompt.md contains the seed prompt_body
    And the project_created journal event records source "requirements"

  # parity_slice2_bootstrap_standard
  Scenario: Bootstrap a standard project from an idea string
    Given a workspace with an initialized project
    When the user runs "project bootstrap --idea 'Bootstrap standard project'"
    Then quick requirements runs inline
    And a project is created and selected as active

  # parity_slice2_bootstrap_quick_dev
  Scenario: Bootstrap a quick_dev project with explicit flow override
    Given a workspace with an initialized project
    When the user runs "project bootstrap --idea 'Bootstrap quick dev project' --flow quick_dev"
    Then the created project uses the quick_dev flow
    And the project layout matches a manually created quick_dev project

  # parity_slice2_bootstrap_with_start
  Scenario: Bootstrap and immediately start the created run
    Given a workspace with an initialized project
    When the user runs "project bootstrap --idea 'Bootstrap and immediately start' --start"
    Then the created project's run status is no longer "not_started"

  # parity_slice2_bootstrap_from_file
  Scenario: Bootstrap from a file-backed idea input
    Given a workspace with an initialized project
    When the user runs "project bootstrap --from-file ./requirements-idea.md --flow quick_dev"
    Then the requirements run idea matches the file contents
    And the created project honors the quick_dev override

  # parity_slice2_failure_before_creation
  Scenario: Seed extraction failure leaves no project state behind
    Given an incomplete requirements run
    When the user runs "project create --from-requirements <run-id>"
    Then no project directory is created
    And the active project selection is unchanged

  # parity_slice2_failure_after_creation_before_start
  Scenario: Run-start preflight failure preserves a valid created project
    Given bootstrap run-start preflight is configured to fail
    When the user runs "project bootstrap --idea 'Bootstrap should fail at run start' --start"
    Then the project still exists and is selected as active
    And the run snapshot remains "not_started"
    And the error tells the user to retry with "ralph-burning run start"

  # parity_slice2_duplicate_seed_project_id
  Scenario: Creating twice from the same seed rejects the duplicate project ID
    Given a completed requirements quick run
    When the user runs "project create --from-requirements <run-id>" twice
    Then the second command fails with a duplicate-project error
