@flow-discovery
Feature: Flow discovery
  The CLI enumerates built-in flow presets and shows their stage sequences.

  @flow-list-all-presets
  Scenario: list all presets
    When I run "ralph-burning flow list"
    Then the output should include "standard"
    And the output should include "quick_dev"
    And the output should include "docs_change"
    And the output should include "ci_improvement"

  @flow-show-each-preset
  Scenario Outline: show each preset
    When I run "ralph-burning flow show <flow_id>"
    Then the output should include "Stage count"
    And the output should include "<stage_1>"
    And the command should exit successfully

    Examples:
      | flow_id        | stage_1            |
      | standard       | prompt_review      |
      | quick_dev      | plan_and_implement |
      | docs_change    | docs_plan          |
      | ci_improvement | ci_plan            |

  @flow-show-invalid-preset
  Scenario: show invalid preset
    When I run "ralph-burning flow show unknown_flow"
    Then the command should fail with a clear invalid-flow error
