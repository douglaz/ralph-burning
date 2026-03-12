Feature: CI Improvement Run Start Orchestration
  The `run start` command executes the `ci_improvement` preset on the shared
  workflow engine, reusing the same retry, remediation, and persistence rules
  as the standard preset.

  # SC-CI-START-001
  Scenario: Happy path ci_improvement run completes all stages
    Given an initialized workspace with project "ci-alpha" using flow "ci_improvement"
    And project "ci-alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the journal records "ci_plan", "ci_update", "ci_validation", and "review" in sequence
    And payload and artifact records exist for all 4 ci_improvement stages

  # SC-CI-START-002
  Scenario: CI validation request_changes restarts from ci_update
    Given an initialized workspace with project "ci-bravo" using flow "ci_improvement"
    And project "ci-bravo" is selected as active
    And the ci_validation stage first returns "request_changes" with follow-up amendment "tighten the workflow assertion"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "cycle_advanced" event from cycle 1 to cycle 2 resuming at "ci_update"
    And the ci_update stage is entered twice across the run
    And the second ci_update invocation includes the ci_validation follow-up amendment in its remediation context

  # SC-CI-START-003
  Scenario: Review rejected fails the ci_improvement run
    Given an initialized workspace with project "ci-charlie" using flow "ci_improvement"
    And project "ci-charlie" is selected as active
    And the review stage returns "rejected"
    When the user runs "run start"
    Then the command fails with error containing "review"
    And the journal ends with a "run_failed" event referencing "review"
    And the run snapshot shows status "failed" with no active run

  # SC-CI-START-004
  Scenario: Retryable ci_update transport failure succeeds on a later attempt
    Given an initialized workspace with project "ci-delta" using flow "ci_improvement"
    And project "ci-delta" is selected as active
    And the ci_update stage fails once with a transport failure before succeeding
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "stage_failed" event for "ci_update" with will_retry true
    And the ci_update stage is entered twice with attempts 1 and 2
