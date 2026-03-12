Feature: Completion Rounds, Late-Stage Acceptance, and Durable Amendments
  When a late stage (completion_panel, acceptance_qa, final_review) returns
  a non-approved outcome, the engine queues durable amendments and advances
  the completion round, restarting from planning. The completion guard
  blocks run_completed when pending amendments remain.

  # SC-CR-001
  Scenario: Late-stage conditionally_approved triggers completion round advancement
    Given an initialized workspace with project "cr-alpha" using flow "standard"
    And project "cr-alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user starts a run where completion_panel returns "conditionally_approved" with amendments
    Then the engine queues durable amendment files under projects/cr-alpha/amendments/
    And the journal contains an "amendment_queued" event with the amendment body
    And the journal contains a "completion_round_advanced" event with from_round=1 to_round=2
    And the engine restarts from planning (stage_entered for planning appears a second time)
    And the planning invocation context includes pending_amendments
    And after successful planning commit the amendment files are removed from disk
    And the run snapshot shows completion_rounds=2 after completion
    And the run snapshot amendment_queue is empty

  # SC-CR-002
  Scenario: Late-stage request_changes triggers completion round advancement
    Given an initialized workspace with project "cr-beta" using flow "standard"
    And project "cr-beta" is selected as active
    When the user starts a run where acceptance_qa returns "request_changes" with amendments
    Then the engine follows the same completion-round path as conditionally_approved
    And the journal contains a "completion_round_advanced" event with source_stage="acceptance_qa"

  # SC-CR-003
  Scenario: Late-stage rejected causes terminal failure
    Given an initialized workspace with project "cr-gamma" using flow "standard"
    And project "cr-gamma" is selected as active
    When the user starts a run where completion_panel returns "rejected"
    Then the run fails with failure_class "qa_review_outcome_failure"
    And no "completion_round_advanced" event exists in the journal
    And the run snapshot shows status "failed"

  # SC-CR-004
  Scenario: Late-stage approved advances to the next late stage
    Given an initialized workspace with project "cr-delta" using flow "standard"
    And project "cr-delta" is selected as active
    When the user starts a run where all late stages return "approved"
    Then the run completes normally with completion_rounds=1
    And completion_panel transitions to acceptance_qa transitions to final_review

  # SC-CR-005
  Scenario: Non-late-stage conditionally_approved does not queue amendments
    Given an initialized workspace with project "cr-epsilon" using flow "standard"
    And project "cr-epsilon" is selected as active
    When the user starts a run where review returns "conditionally_approved"
    Then the run completes normally
    And the amendment_queue in run.json is empty
    And no "amendment_queued" or "completion_round_advanced" events exist

  # SC-CR-006
  Scenario: Multiple completion rounds accumulate correctly
    Given an initialized workspace with project "cr-zeta" using flow "standard"
    And project "cr-zeta" is selected as active
    When the user starts a run where completion_panel triggers round 2 and acceptance_qa triggers round 3
    Then the journal contains two "completion_round_advanced" events
    And the final run snapshot shows completion_rounds=3
    And the run completes normally

  # SC-CR-007
  Scenario: Completion guard blocks run_completed when disk amendments exist
    Given an initialized workspace with project "cr-eta" using flow "standard"
    And project "cr-eta" is selected as active
    And a durable amendment file exists under projects/cr-eta/amendments/
    When the user runs "run start"
    Then the run fails with error containing "completion blocked" or "pending amendments"
    And the amendment file remains on disk

  # SC-CR-008
  Scenario: Completion guard checks both snapshot queue and disk
    Given an initialized workspace with project "cr-theta" using flow "standard"
    And the run snapshot has non-empty amendment_queue.pending
    When the engine reaches the completion boundary
    Then the completion guard fires and blocks run_completed
    And the run fails with "completion blocked"

  # SC-CR-009
  Scenario: Resume with pending late-stage amendments
    Given an initialized workspace with project "cr-iota" using flow "standard"
    And a previous run failed after completion_round_advanced but before planning in round 2
    And durable amendment files exist on disk
    When the user runs "run resume"
    Then the engine reconciles amendments from disk into the run snapshot
    And the engine restarts from planning with amendments in context
    And the amendments are drained after planning commit
    And the run completes normally

  # SC-CR-010
  Scenario: Cycle advancement emitted when entering implementation from completion round
    Given an initialized workspace with project "cr-kappa" using flow "standard"
    When a completion round restarts from planning and advances to implementation
    Then a "cycle_advanced" event is emitted for the new work cycle
    And the implementation invocation runs in the new cycle

  # SC-CR-011
  Scenario: Amendment queue drain is idempotent
    Given amendments are queued in both snapshot and disk
    When planning commit triggers the drain
    Then all amendments are removed from disk
    And the snapshot amendment_queue.pending is empty
    And amendment_queue.processed_count is incremented

  # SC-CR-012
  Scenario: Amendment persistence is atomic with batch rollback
    Given a late stage returns conditionally_approved with follow_ups
    When the engine persists amendment files
    Then each amendment file is written atomically (temp + rename)
    And if any amendment write fails, already-written files from the same batch are rolled back
    And the run fails without partial amendments visible
    And the failure invariant holds: no queue entry becomes visible without a matching file

  # SC-CR-013
  Scenario: Completion guard leaves snapshot in resumable state
    Given an initialized workspace with project "cr-lambda" using flow "standard"
    And a durable amendment file exists under projects/cr-lambda/amendments/
    When the user runs "run start" and all stages return approved
    Then the completion guard fires because pending amendment files exist on disk
    And the run snapshot shows status "failed" with active_run == null
    And the amendment files remain untouched on disk
    And the user can run "run resume" after removing orphaned amendments

  # SC-CR-014
  Scenario: Same-batch amendments are ordered deterministically by batch_sequence
    Given a late stage returns conditionally_approved with multiple follow_ups
    When the engine persists amendments
    Then each amendment has a stable batch_sequence field
    And list_pending_amendments returns them sorted by (created_at, batch_sequence)
    And the planning invocation receives amendments in deterministic order
