Feature: Run Rollback
  A paused or failed run can be rewound to a durable stage boundary using a
  persisted rollback point. Logical rollback updates canonical run state first;
  hard rollback then attempts to reset the repository to the recorded git SHA.

  # SC-ROLLBACK-001
  Scenario: Soft rollback rewinds to a visible checkpoint
    Given an initialized workspace with project "rb-alpha" using flow "standard"
    And project "rb-alpha" is selected as active
    And the project has visible rollback points for "planning" and "implementation"
    And the current run snapshot status is "failed"
    When the user runs "run rollback --to planning"
    Then the command exits successfully
    And run.json is restored to the planning rollback snapshot
    And the run snapshot shows status "paused" with no active run
    And the journal ends with a "rollback_performed" event for "planning"

  # SC-ROLLBACK-002
  Scenario: Hard rollback resets canonical state before the repository
    Given an initialized workspace with project "rb-bravo" using flow "standard"
    And project "rb-bravo" is selected as active
    And the target rollback point for "implementation" records a git SHA
    And the current run snapshot status is "paused"
    When the user runs "run rollback --to implementation --hard"
    Then the logical rollback is committed before the git reset is attempted
    And the repository reset targets the git SHA stored in the rollback point

  # SC-ROLLBACK-003
  Scenario: Rollback rejects non-resumable run statuses
    Given initialized workspace projects with snapshots in statuses "not_started", "running", and "completed"
    When the user runs "run rollback --to planning" for each project
    Then each command fails with a clear rollback-status error
    And none of the commands mutate run.json

  # SC-ROLLBACK-004
  Scenario: Rollback rejects a stage outside the project flow
    Given an initialized workspace with project "rb-charlie" using flow "standard"
    And project "rb-charlie" is selected as active
    And the current run snapshot status is "failed"
    When the user runs "run rollback --to ci_plan"
    Then the command fails with error containing "not part of flow"

  # SC-ROLLBACK-005
  Scenario: Rollback rejects a stage with no visible checkpoint
    Given an initialized workspace with project "rb-delta" using flow "standard"
    And project "rb-delta" is selected as active
    And the current run snapshot status is "paused"
    And no visible rollback point exists for "review"
    When the user runs "run rollback --to review"
    Then the command fails with error containing "no persisted rollback point exists"

  # SC-ROLLBACK-006
  Scenario: Multiple sequential rollbacks keep rollback metadata monotonic
    Given an initialized workspace with project "rb-echo" using flow "standard"
    And project "rb-echo" is selected as active
    And the current run has visible rollback points for "planning", "implementation", and "qa"
    When the user rolls back to "implementation" and then to "planning"
    Then rollback_point_meta.rollback_count increases on each rollback
    And rollback_point_meta.last_rollback_id matches the most recent rollback point
    And run history excludes the abandoned branch after each rollback

  # SC-ROLLBACK-007
  Scenario: Resume after rollback continues from the restored boundary
    Given an initialized workspace with project "rb-foxtrot" using flow "standard"
    And project "rb-foxtrot" is selected as active
    And the project has a rollback point at "planning" whose snapshot resumes at "implementation"
    When the user runs "run rollback --to planning" and then "run resume"
    Then the resumed run keeps the original run_id
    And the first resumed stage is "implementation"
    And the rolled-back implementation history from the abandoned branch remains hidden

  # SC-ROLLBACK-008
  Scenario: Hard rollback failure preserves the logical rollback
    Given an initialized workspace with project "rb-golf" using flow "standard"
    And project "rb-golf" is selected as active
    And the current run snapshot status is "failed"
    And the target rollback point for "implementation" records a git SHA
    And the repository reset will fail
    When the user runs "run rollback --to implementation --hard"
    Then the command fails with a git-reset error
    And run.json remains in the logically rolled-back paused state
    And the journal still contains the "rollback_performed" event
