@active-project
Feature: Active project resolution
  The workspace tracks the currently selected project through the
  ".ralph-burning/active-project" pointer file.

  @active-project-select-existing
  Scenario: select an existing project
    Given the current directory contains an initialized ".ralph-burning" workspace
    And the project "alpha" exists under ".ralph-burning/projects/"
    When I run "ralph-burning project select alpha"
    Then the output should include "Selected project alpha"
    And the file ".ralph-burning/active-project" should include "alpha"

  @active-project-select-missing
  Scenario: select a non-existing project
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning project select missing"
    Then the command should fail with a clear project-not-found error

  @active-project-missing
  Scenario: resolve when no active project is set
    Given the current directory contains an initialized ".ralph-burning" workspace
    And the file ".ralph-burning/active-project" does not exist
    When a command requires the active project
    Then the command should fail with a clear no-active-project error naming "project select"

  @active-project-resolve-valid
  Scenario: resolve after a valid select
    Given the current directory contains an initialized ".ralph-burning" workspace
    And the project "alpha" exists under ".ralph-burning/projects/"
    And I run "ralph-burning project select alpha"
    When a command requires the active project
    Then the active project should resolve to "alpha"
