Feature: Final Review Panel

  @workflow.final_review.no_amendments_complete
  Scenario: Final review completes immediately when no amendments remain
    Given the final-review panel proposes no amendments
    When the workflow reaches final review
    Then the run completes without a restart

  @workflow.final_review.restart_then_complete
  Scenario: Accepted amendments restart planning once and then complete
    Given the final-review panel accepts an amendment set
    When the workflow restarts planning with those amendments
    Then the next final-review pass completes without another restart

  @workflow.final_review.planner_completion_with_pending_amendments_fails
  Scenario: Pending amendments block planner completion
    Given pending final-review amendments still exist
    When completion is evaluated
    Then completion is blocked until the amendments are processed

  @workflow.final_review.disputed_amendment_uses_arbiter
  Scenario: Disputed amendments invoke the arbiter
    Given reviewers disagree on a final-review amendment
    When consensus is computed
    Then only the disputed amendment is sent to the arbiter

  @workflow.final_review.no_amendments_complete_at_restart_cap
  Scenario: No-amendment final review still completes normally at the restart cap
    Given the final-review restart cap is already reached
    And the current final-review pass proposes no amendments
    When the workflow re-enters final review
    Then the run completes normally without force-completing

  @workflow.final_review.restart_cap_force_complete
  Scenario: Final-review restart cap forces completion
    Given the final-review restart cap is reached
    And the current final-review pass still accepts amendments
    When the workflow re-enters final review
    Then the run force-completes with an explicit aggregate explanation
