Feature: Stage Contracts
  Stage contracts define schema validation, semantic validation, and
  deterministic Markdown rendering for every stage in every built-in
  flow preset. Contract evaluation follows a fixed order: schema first,
  semantics second, rendering last. Failures at any step short-circuit
  subsequent steps.

  @SC-EVAL-001
  Scenario: Successful planning contract evaluation
    Given a planning stage contract
    When a valid planning payload is submitted
    Then schema validation passes
    And semantic validation passes
    And a deterministic Markdown artifact is produced
    And the result carries no outcome failure

  @SC-EVAL-002
  Scenario: Successful execution contract evaluation
    Given an execution stage contract
    When a valid execution payload is submitted
    Then schema validation passes
    And semantic validation passes
    And a deterministic Markdown artifact is produced
    And the result carries no outcome failure

  @SC-EVAL-003
  Scenario: Successful validation contract evaluation with passing outcome
    Given a validation stage contract
    When a valid validation payload with outcome "approved" is submitted
    Then schema validation passes
    And semantic validation passes
    And a deterministic Markdown artifact is produced
    And the result carries no outcome failure

  @SC-EVAL-004
  Scenario: Schema validation failure prevents semantic validation
    Given a planning stage contract
    When an invalid JSON payload missing required fields is submitted
    Then schema validation fails with failure class "schema_validation_failure"
    And semantic validation does not run
    And rendering does not run
    And no success bundle is returned

  @SC-EVAL-005
  Scenario: Domain validation failure prevents rendering
    Given an execution stage contract
    When a schema-valid payload with empty change_summary and no steps is submitted
    Then schema validation passes
    And semantic validation fails with failure class "domain_validation_failure"
    And rendering does not run
    And no success bundle is returned

  @SC-EVAL-006
  Scenario: QA/review outcome failure with valid payload
    Given a validation stage contract
    When a valid validation payload with outcome "rejected" is submitted
    Then schema validation passes
    And semantic validation passes
    And a deterministic Markdown artifact is produced
    And the result carries outcome failure class "qa_review_outcome_failure"
    And the validated bundle is still returned

  @SC-EVAL-007
  Scenario: Every stage in every built-in flow has contract coverage
    Given the set of all built-in flow presets
    Then every stage in every preset resolves to exactly one contract
    And no stage is left without a contract

  @SC-EVAL-008
  Scenario: Deterministic rendering produces identical output
    Given a validated planning payload
    When the payload is rendered twice
    Then both renders are byte-identical

  @SC-EVAL-009
  Scenario: Non-passing review outcomes are not treated as schema or domain failures
    Given a validation stage contract
    When a valid validation payload with outcome "request_changes" is submitted
    Then schema validation passes
    And semantic validation passes
    And the failure class is "qa_review_outcome_failure"
    And the failure class is not "schema_validation_failure"
    And the failure class is not "domain_validation_failure"
