Feature: Workflow Panels: Prompt Review, Completion Panel, and Resume Drift
  Prompt review uses a refiner-plus-validator panel. Completion uses a
  multi-completer consensus panel. Resume drift detects backend resolution
  changes and either warns or fails depending on whether minimum requirements
  are still met.

  @workflow-panels-prompt-review-accept
  Scenario: Prompt review panel accepts refined prompt
    Given an initialized workspace with project "wp-alpha" using flow "standard" with prompt_review enabled
    And the stub backend is configured to return accepting validation results
    When the user starts a run
    Then the prompt_review stage produces StageSupporting records for the refiner and each validator
    And the prompt_review stage produces one StagePrimary record with decision "accepted"
    And prompt.original.md is written with the original prompt
    And prompt.md contains the refined prompt
    And the run advances to planning

  @workflow-panels-prompt-review-reject
  Scenario: Prompt review panel rejects refined prompt
    Given an initialized workspace with project "wp-beta" using flow "standard" with prompt_review enabled
    And the stub backend is configured to return a rejecting validation result
    When the user starts a run
    Then the prompt_review stage produces StageSupporting records for the refiner and each validator
    And prompt.md is unchanged from the original prompt
    And prompt.original.md is not written
    And the run fails with prompt review rejection

  @workflow-panels-prompt-review-min-reviewers
  Scenario: Prompt review enforces min_reviewers
    Given an initialized workspace with project "wp-gamma" using flow "standard" with prompt_review enabled
    And min_reviewers is set to 3
    And only 2 validators resolve
    When the user starts a run
    Then the run fails with insufficient panel members error

  @workflow-panels-prompt-review-optional-skip
  Scenario: Optional validator skip does not count as executed reviewer
    Given an initialized workspace with project "wp-delta" using flow "standard" with prompt_review enabled
    And one optional validator is unavailable
    When the user starts a run
    Then the unavailable optional validator is skipped
    And the executed reviewer count reflects only available validators

  @workflow-panels-prompt-review-prompt-replaced
  Scenario: Prompt replaced and original preserved on accept
    Given an initialized workspace with project "wp-epsilon" using flow "standard" with prompt_review enabled
    And the stub backend returns a refined prompt different from the original
    When the user starts a run
    Then prompt.original.md contains the exact original prompt text
    And prompt.md contains the refined prompt text
    And the project prompt hash is updated to match the refined prompt

  @workflow-panels-completion-consensus-complete
  Scenario: Completion panel two completer consensus yields complete
    Given an initialized workspace with project "wp-zeta" using flow "standard"
    And completion panel resolves 2 completers both voting complete
    And min_completers is 1 and consensus_threshold is 0.5
    When the run reaches completion_panel
    Then a StageAggregate record is persisted with verdict "complete"
    And the run advances to acceptance_qa

  @workflow-panels-completion-continue-verdict
  Scenario: Completion panel continue verdict reopens work
    Given an initialized workspace with project "wp-eta" using flow "standard"
    And completion panel resolves 2 completers both voting continue_work
    When the run reaches completion_panel
    Then a StageAggregate record is persisted with verdict "continue_work"
    And completion_round is advanced
    And the run restarts from the planning stage

  @workflow-panels-completion-optional-skip
  Scenario: Completion optional backend skip
    Given an initialized workspace with project "wp-theta" using flow "standard"
    And one optional completer backend is unavailable
    When the run reaches completion_panel
    Then the unavailable optional completer is skipped
    And the aggregate only counts executed voters

  @workflow-panels-completion-required-failure
  Scenario: Completion required backend failure aborts stage
    Given an initialized workspace with project "wp-iota" using flow "standard"
    And a required completer backend is unavailable
    When the run reaches completion_panel
    Then the run fails with backend unavailable error

  @workflow-panels-completion-threshold-boundary
  Scenario: Completion threshold consensus boundary
    Given an initialized workspace with project "wp-kappa" using flow "standard"
    And completion panel resolves 3 completers with 2 voting complete
    And consensus_threshold is 0.75
    When the run reaches completion_panel
    Then the aggregate verdict is "continue_work" because 2/3 < 0.75

  @workflow-panels-completion-insufficient-min
  Scenario: Insufficient min_completers fails completion
    Given an initialized workspace with project "wp-lambda" using flow "standard"
    And min_completers is 3
    And only 2 completers resolve
    When the run reaches completion_panel
    Then the run fails with insufficient panel members error

  @workflow-panels-resume-drift-implementation
  Scenario: Resume drift on implementation warns and re-resolves
    Given an initialized workspace with project "wp-mu" using flow "standard"
    And a run failed at implementation with a stage_resolution_snapshot
    And the backend resolution changes but still meets requirements
    When the user resumes the run
    Then a durable warning event is written to the journal
    And the stage_resolution_snapshot is updated
    And the run continues with the new resolution

  @workflow-panels-resume-drift-qa
  Scenario: Resume drift on QA warns and re-resolves
    Given an initialized workspace with project "wp-nu" using flow "standard"
    And a run failed at acceptance_qa with a stage_resolution_snapshot
    And the backend resolution changes but still meets requirements
    When the user resumes the run
    Then a durable warning event is written to the journal
    And the run continues with the new resolution

  @workflow-panels-resume-drift-review
  Scenario: Resume drift on review warns and re-resolves
    Given an initialized workspace with project "wp-xi" using flow "standard"
    And a run failed at validation with a stage_resolution_snapshot
    And the backend resolution changes but still meets requirements
    When the user resumes the run
    Then a durable warning event is written to the journal

  @workflow-panels-resume-drift-completion
  Scenario: Resume drift on completion panel warns and re-resolves
    Given an initialized workspace with project "wp-omicron" using flow "standard"
    And a run failed at completion_panel with a stage_resolution_snapshot
    And the backend resolution changes but still meets panel requirements
    When the user resumes the run
    Then a durable warning event is written to the journal
    And the stage_resolution_snapshot is updated with new panel resolution
