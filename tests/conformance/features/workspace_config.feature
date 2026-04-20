@workspace-config
Feature: Workspace effective configuration
  The CLI resolves workspace settings through compiled defaults and
  workspace.toml overrides, and validates mutations before persisting them.

  @workspace-config-show
  Scenario: show effective config
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config show"
    Then the output should include "[settings]"
    And the output should include "prompt_review.enabled"
    And the output should include "source: default"

  @workspace-config-get-known
  Scenario: get a known key
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config get default_flow"
    Then the output should include "minimal"
    And the command should exit successfully

  @workspace-config-get-unknown
  Scenario: get an unknown key
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config get unknown.key"
    Then the command should fail with a clear unknown-config-key error

  @workspace-config-set-valid
  Scenario: set a valid key
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config set default_flow quick_dev"
    Then the output should include "Updated default_flow = quick_dev"
    And the file ".ralph-burning/workspace.toml" should include "default_flow = \"quick_dev\""

  @workspace-config-set-invalid-value
  Scenario: set an invalid value
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config set default_flow unknown_flow"
    Then the command should fail with a clear invalid-config-value error

  @workspace-config-set-invalid-key
  Scenario: set an invalid key
    Given the current directory contains an initialized ".ralph-burning" workspace
    When I run "ralph-burning config set unknown.key value"
    Then the command should fail with a clear unknown-config-key error

  @workspace-config-edit-valid
  Scenario: edit workspace config with a valid result
    Given the current directory contains an initialized ".ralph-burning" workspace
    And EDITOR writes a valid workspace.toml update
    When I run "ralph-burning config edit"
    Then the command should exit successfully
    And the file ".ralph-burning/workspace.toml" should include "default_backend = \"claude\""

  @workspace-config-edit-invalid
  Scenario: edit workspace config with an invalid result
    Given the current directory contains an initialized ".ralph-burning" workspace
    And EDITOR writes an invalid workspace.toml update
    When I run "ralph-burning config edit"
    Then the command should fail with a clear invalid workspace.toml error
