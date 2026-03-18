Feature: Independent Iteration Caps

  @workflow.iteration_caps.qa_cap_enforced
  Scenario: QA iteration cap is enforced independently
    Given QA requests changes
    When the configured QA cap is zero
    Then the run fails with a QA-cap error

  @workflow.iteration_caps.review_cap_enforced
  Scenario: Review iteration cap is enforced independently
    Given review requests changes
    When the configured review cap is zero
    Then the run fails with a review-cap error

  @workflow.iteration_caps.final_review_cap_enforced
  Scenario: Final-review restart cap is enforced independently
    Given final review restarts once and hits its cap
    When the workflow reaches final review again
    Then the run force-completes without affecting QA or review caps
