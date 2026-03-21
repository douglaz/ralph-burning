Feature: Standard Preset Run Start Orchestration
  The `run start` command executes all stages of the standard flow preset
  end-to-end using the workflow engine, persisting validated payloads,
  rendered artifacts, journal events, and run snapshots at durable stage
  boundaries.

  # SC-START-001
  Scenario: Happy path standard run completes all stages
    Given an initialized workspace with project "alpha" using flow "standard"
    And project "alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the journal contains "run_started", "stage_entered", "stage_completed", and "run_completed" events
    And payload and artifact records exist for all 8 standard stages

  # SC-START-002
  Scenario: Run start succeeds for quick_dev flow preset
    Given an initialized workspace with project "beta" using flow "quick_dev"
    And project "beta" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the journal records "plan_and_implement", "review", "apply_fixes", and "final_review" in sequence
    And payload and artifact records exist for all 4 quick_dev stages

  # SC-START-003
  Scenario: Run start rejects already-running project
    Given an initialized workspace with project "gamma" using flow "standard"
    And project "gamma" is selected as active
    And the run snapshot shows status "running" with an active run
    When the user runs "run start"
    Then the command fails with error containing "not_started"
    And the run snapshot is unchanged (still "running")

  # SC-START-004
  Scenario: Run start rejects already-completed project
    Given an initialized workspace with project "delta" using flow "standard"
    And project "delta" is selected as active
    And the run snapshot shows status "completed" with no active run
    When the user runs "run start"
    Then the command fails with error containing "not_started"
    And the run snapshot is unchanged (still "completed")

  # SC-START-005
  Scenario: Run start validates workspace version before proceeding
    Given a workspace with unsupported version
    When the user runs "run start"
    Then the command fails with error referencing workspace version
    And no run state is mutated

  # SC-START-006
  Scenario: Run start requires an active project
    Given an initialized workspace with no active project
    When the user runs "run start"
    Then the command fails with error referencing active project
    And no run state is mutated

  # SC-START-007
  Scenario: Stage plan derives from flow definition with prompt_review enabled
    Given an initialized workspace with project "echo" using flow "standard"
    And prompt_review.enabled is true (default)
    When the stage plan is derived
    Then the plan includes all 8 standard stages in canonical order
    And the first stage is "prompt_review"

  # SC-START-008
  Scenario: Stage plan skips prompt_review when disabled
    Given an initialized workspace with project "foxtrot" using flow "standard"
    And prompt_review.enabled is false
    When the stage plan is derived
    Then the plan includes 7 stages (all except prompt_review)
    And the first stage is "planning"

  # SC-START-009
  Scenario: Preflight checks validate all stages before any mutation
    Given an initialized workspace with project "golf" using flow "standard"
    And the backend is unavailable
    When the user runs "run start"
    Then the command fails with error containing "preflight"
    And the run snapshot is unchanged (still "not_started")
    And the journal has no new events beyond project_created

  # SC-START-010
  Scenario: Stage-to-role mapping is deterministic
    Given the standard flow stage sequence
    Then each stage maps to exactly one backend role
    And the mapping is consistent across invocations

  # SC-START-011
  Scenario: Atomic stage commit persists payload, artifact, journal event, and snapshot
    Given an initialized workspace with project "hotel" using flow "standard"
    And the run has started and is executing stages
    When a stage completes successfully
    Then a payload record is written to history/payloads
    And a matching artifact record is written to history/artifacts
    And a stage_completed event is appended to the journal
    And the run snapshot is updated with the next stage cursor

  # SC-START-012
  Scenario: Run failure records failure in journal and snapshot
    Given an initialized workspace with project "india" using flow "standard"
    And a stage invocation fails during execution
    When the engine handles the failure
    Then a run_failed event is appended to the journal
    And the run snapshot shows status "failed" with no active run
    And the status_summary includes the failed stage name

  # SC-START-013
  Scenario: Runtime logs are best-effort and do not affect durable state
    Given an initialized workspace with project "juliet" using flow "standard"
    And runtime log writes are failing
    When the user runs "run start"
    Then the command completes successfully despite log failures
    And all durable state (journal, payloads, artifacts, snapshot) is correct

  # SC-START-014
  Scenario: Journal sequence numbers are monotonically increasing
    Given an initialized workspace with project "kilo" using flow "standard"
    When the user runs "run start" and it completes
    Then all journal events have strictly increasing sequence numbers
    And no sequence gaps exist

  # SC-START-015
  Scenario: Run ID is unique per run invocation
    Given an initialized workspace with project "lima" using flow "standard"
    When the user runs "run start"
    Then the run_started event contains a run_id matching "run-YYYYMMDDHHMMSS" format
    And the run_id is consistent across all events for this run

  # SC-START-016
  Scenario: Post-run queries work correctly after successful run
    Given an initialized workspace with project "mike" using flow "standard"
    And the user has completed "run start" successfully
    When the user runs "run status"
    Then the output shows "completed"
    When the user runs "run history"
    Then the output includes all stage events and payload/artifact records

  # SC-START-017
  Scenario: Run start supports all four built-in flow presets
    Given an initialized workspace with projects using flows "standard", "quick_dev", "docs_change", "ci_improvement"
    When the user runs "run start" for each project
    Then all four flow presets complete successfully

  # SC-START-018
  Scenario: Prompt-review-disabled run start completes with 7 stages
    Given an initialized workspace with project "november" using flow "standard"
    And prompt_review.enabled is false
    And project "november" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And payload and artifact records exist for 7 stages (all except prompt_review)
    And the journal contains no "stage_entered" or "stage_completed" events for "prompt_review"
    And the first stage_entered event is for "planning"

  # SC-START-019
  Scenario: Preflight failure leaves all canonical state unchanged
    Given an initialized workspace with project "oscar" using flow "standard"
    And project "oscar" is selected as active
    And the backend is unavailable for preflight
    When the user runs "run start"
    Then the command fails with error containing "preflight"
    And the run snapshot is byte-identical to its pre-run state
    And the journal is byte-identical to its pre-run state
    And no payload or artifact files exist
    And no sessions were created or modified

  # SC-START-020
  Scenario: Mid-stage failure exposes no partial durable history
    Given an initialized workspace with project "papa" using flow "standard"
    And project "papa" is selected as active
    And the run snapshot shows status "not_started" with no active run
    And persistence will fail during the first stage commit
    When the user runs "run start"
    Then the command fails
    And the run snapshot shows status "failed" with no active run
    And no payload or artifact files exist for any stage
    And no "stage_completed" event exists in the journal
    And the journal ends with a "run_failed" event referencing the failed stage
