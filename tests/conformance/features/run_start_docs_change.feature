Feature: Docs Change Run Start Orchestration
  The `run start` command keeps `docs_change` as an accepted preset name while
  routing it through the same minimal stage plan and semantics as `minimal`.

  # SC-DOCS-START-001
  Scenario: Happy path docs_change run completes the minimal stage plan
    Given an initialized workspace with project "docs-alpha" using flow "docs_change"
    And project "docs-alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run

  # SC-DOCS-START-002
  Scenario: Flow show for docs_change reports the minimal stage plan
    When the user runs "flow show docs_change"
    Then the command exits successfully
    And the reported stages include "plan_and_implement" and "final_review"

  # SC-DOCS-START-003
  Scenario: Flow show for docs_change no longer lists legacy docs_* stages
    When the user runs "flow show docs_change"
    Then the command exits successfully
    And the reported stages do not include "docs_update" or "docs_validation"

  # SC-DOCS-START-004
  Scenario: Run status works on a freshly created docs_change project
    Given an initialized workspace with project "docs-retry" using flow "docs_change"
    When the user runs "run status"
    Then the command exits successfully

  # SC-DOCS-START-005
  Scenario: docs_change stage list equals the minimal stage list
    When the user runs "flow show docs_change"
    And the user runs "flow show minimal"
    Then both commands exit successfully
    And the docs_change stage list equals the minimal stage list
