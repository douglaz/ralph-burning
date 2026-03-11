@workspace-init
Feature: Workspace initialization
  The CLI initializes a v1 ralph-burning workspace in the current directory.

  @workspace-init-fresh
  Scenario: init fresh workspace
    Given the current directory does not contain a ".ralph-burning" workspace
    When I run "ralph-burning init"
    Then the file ".ralph-burning/workspace.toml" should exist
    And the command should exit successfully

  @workspace-init-existing
  Scenario: init already-initialized workspace
    Given the current directory already contains a ".ralph-burning/workspace.toml" file
    When I run "ralph-burning init"
    Then the command should fail with a clear already-initialized error

  @workspace-init-layout
  Scenario: init verifies directory structure
    Given the current directory does not contain a ".ralph-burning" workspace
    When I run "ralph-burning init"
    Then the directory ".ralph-burning/projects" should exist
    And the directory ".ralph-burning/requirements" should exist
    And the directory ".ralph-burning/daemon/tasks" should exist
    And the directory ".ralph-burning/daemon/leases" should exist
