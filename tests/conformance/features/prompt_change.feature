Feature: Prompt Change On Resume

  @workflow.resume.prompt_change_continue_warns
  Scenario: Continue policy warns and resumes
    Given the prompt changes after a failed run
    When resume is configured with the continue policy
    Then resume emits warnings and proceeds

  @workflow.resume.prompt_change_abort_fails
  Scenario: Abort policy rejects resume
    Given the prompt changes after a failed run
    When resume is configured with the abort policy
    Then resume fails with a prompt-hash mismatch error

  @workflow.resume.prompt_change_restart_cycle
  Scenario: Restart-cycle policy returns to planning
    Given the prompt changes after a failed run
    When resume is configured with the restart_cycle policy
    Then the current cycle is reset to planning before work continues

  @workflow.resume.backend_drift_warns
  Scenario: Final-review backend drift warns on resume
    Given final-review resolution changes after a failed run
    When the run resumes
    Then resume emits a durable backend-drift warning and continues
