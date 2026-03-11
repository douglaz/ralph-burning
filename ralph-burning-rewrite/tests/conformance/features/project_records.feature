Feature: Project Records
  Canonical project CRUD and immutable flow enforcement.

  # SC-PROJ-001
  Scenario: Project creation writes all canonical files
    Given an initialized workspace
    And a readable prompt file "prompt.md"
    When the user runs "project create --id alpha --name Alpha --prompt prompt.md --flow standard"
    Then the command succeeds
    And the project directory "alpha" contains "project.toml", "prompt.md", "run.json", "journal.ndjson", "sessions.json"
    And the project directory "alpha" contains subdirectories "history/payloads", "history/artifacts", "runtime/logs", "runtime/backend", "runtime/temp", "amendments", "rollback"
    And "journal.ndjson" contains a "project_created" event with sequence 1
    And "run.json" shows status "not_started" with no active run

  # SC-PROJ-002
  Scenario: Duplicate project ID is rejected
    Given an initialized workspace with project "alpha"
    When the user runs "project create --id alpha --name Again --prompt prompt.md --flow standard"
    Then the command fails with error containing "already exists"
    And the existing project "alpha" is unchanged

  # SC-PROJ-003
  Scenario: Flow is immutable after project creation
    Given an initialized workspace with project "alpha" using flow "standard"
    Then "project.toml" for "alpha" contains flow "standard"
    And there is no CLI or store path to change the flow after creation

  # SC-PROJ-004
  Scenario: Project list reads canonical records not directory names
    Given an initialized workspace with projects "alpha" and "beta"
    And "alpha" is the active project
    When the user runs "project list"
    Then the output includes "alpha" marked as active
    And the output includes "beta" not marked as active
    And each entry shows flow, status, and name from canonical records

  # SC-PROJ-005
  Scenario: Project show displays explicit data
    Given an initialized workspace with project "alpha" using flow "standard"
    When the user runs "project show alpha"
    Then the output includes flow "standard", prompt hash, created timestamp, run summary, and rollback summary

  # SC-PROJ-006
  Scenario: Project show resolves active project when no ID given
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "project show"
    Then the output includes project "alpha" marked as active

  # SC-PROJ-007
  Scenario: Project delete succeeds when no active run
    Given an initialized workspace with project "alpha" and no active run
    When the user runs "project delete alpha"
    Then the command succeeds
    And the project directory "alpha" no longer exists
    And "project list" does not include "alpha"

  # SC-PROJ-008
  Scenario: Project delete clears active-project pointer if selected
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "project delete alpha"
    Then the command succeeds
    And the active-project pointer is cleared

  # SC-PROJ-009
  Scenario: Project delete fails when project has active run
    Given an initialized workspace with project "alpha" that has an active run
    When the user runs "project delete alpha"
    Then the command fails with error containing "active run"
    And the project "alpha" remains addressable

  # SC-PROJ-010
  Scenario: Project creation is atomic
    Given an initialized workspace
    And a prompt file that will cause journal initialization to fail
    When the user runs "project create --id partial --name Partial --prompt bad-prompt.md --flow standard"
    Then the command fails
    And no partial project directory "partial" is visible to "project list"
    And the active-project pointer is unchanged

  # SC-PROJ-011
  Scenario: Project create validates workspace version
    Given a workspace with unsupported version
    When the user runs "project create --id x --name X --prompt prompt.md --flow standard"
    Then the command fails with error containing "unsupported workspace version"

  # SC-PROJ-012
  Scenario: Project create validates flow preset
    Given an initialized workspace
    When the user runs "project create --id x --name X --prompt prompt.md --flow unknown"
    Then the command fails with error containing "unknown flow preset"

  # SC-PROJ-013
  Scenario: Project create does not implicitly select the project
    Given an initialized workspace
    When the user runs "project create --id alpha --name Alpha --prompt prompt.md --flow standard"
    Then the active-project pointer does not exist

  # SC-PROJ-014
  Scenario: Project delete is transactional
    Given an initialized workspace with project "alpha" and no active run
    When the filesystem delete partially fails after validation
    Then the project "alpha" remains addressable
    And the active-project pointer is unchanged

  # SC-PROJ-015
  Scenario: Run.json contains full canonical run snapshot schema
    Given an initialized workspace with project "alpha"
    Then "run.json" for "alpha" contains cycle_history, completion_rounds, rollback_point_meta, and amendment_queue fields

  # SC-PROJ-016
  Scenario: Missing project.toml fails fast with corruption error on show
    Given an initialized workspace with project "alpha"
    And "project.toml" for project "alpha" has been deleted
    When the user runs "project show alpha"
    Then the command fails with error referencing "project.toml" and "missing"

  # SC-PROJ-017
  Scenario: Missing project.toml fails fast with corruption error on list
    Given an initialized workspace with project "alpha"
    And "project.toml" for project "alpha" has been deleted
    When the user runs "project list"
    Then the command fails with error referencing "project.toml" and "missing"

  # SC-PROJ-018
  Scenario: Missing project.toml fails fast with corruption error on delete
    Given an initialized workspace with a project directory "alpha" that has no project.toml
    When the user runs "project delete alpha"
    Then the command fails with error referencing "project.toml" and "missing"
