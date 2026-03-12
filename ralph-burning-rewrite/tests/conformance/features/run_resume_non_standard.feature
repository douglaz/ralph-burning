Feature: Resume for Docs and CI Presets
  The `run resume` command reconstructs docs_change and ci_improvement runs
  from durable stage boundaries using the same shared engine semantics as
  standard-flow resume.

  # SC-NONSTD-RESUME-001
  Scenario: Resume a failed docs_change run from docs_update
    Given an initialized workspace with project "docs-resume" using flow "docs_change"
    And project "docs-resume" is selected as active
    And a previous "run start" failed after "docs_plan" completed and "docs_update" exhausted retries
    When the user runs "run resume"
    Then the command exits successfully
    And the resumed run keeps the original run_id
    And the docs_plan stage is not re-executed
    And the first resumed stage is "docs_update" with attempt 1

  # SC-NONSTD-RESUME-002
  Scenario: Resume a failed ci_improvement run from ci_update
    Given an initialized workspace with project "ci-resume" using flow "ci_improvement"
    And project "ci-resume" is selected as active
    And a previous "run start" failed after "ci_plan" completed and "ci_update" exhausted retries
    When the user runs "run resume"
    Then the command exits successfully
    And the resumed run keeps the original run_id
    And the ci_plan stage is not re-executed
    And the first resumed stage is "ci_update" with attempt 1

  # SC-NONSTD-RESUME-003
  Scenario: Resume a paused docs_change snapshot with pending amendments
    Given an initialized workspace with project "docs-paused" using flow "docs_change"
    And project "docs-paused" is selected as active
    And the run snapshot is "paused" with a durable pending amendment for the planning stage
    When the user runs "run resume"
    Then the command exits successfully
    And the first resumed stage is "docs_plan" with attempt 1
    And the pending amendment is drained after docs_plan completes

  # SC-NONSTD-RESUME-004
  Scenario: Resume a paused ci_improvement snapshot with pending amendments
    Given an initialized workspace with project "ci-paused" using flow "ci_improvement"
    And project "ci-paused" is selected as active
    And the run snapshot is "paused" with a durable pending amendment for the planning stage
    When the user runs "run resume"
    Then the command exits successfully
    And the first resumed stage is "ci_plan" with attempt 1
    And the pending amendment is drained after ci_plan completes
