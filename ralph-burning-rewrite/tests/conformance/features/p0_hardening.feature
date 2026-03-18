@p0-hardening
Feature: Slice 0 hardening and sign-off
  Slice 0 closes the remaining P0 correctness gaps before broader parity work.

  @parity_slice0_executable_permission_required
  Scenario: Backend availability requires executable permission
    Given a backend binary exists on PATH without executable permission
    When the process backend checks availability
    Then it fails with BackendUnavailable that names the non-executable path

  @parity_slice0_permission_check_success
  Scenario: Executable backend binary passes availability checks
    Given an executable backend binary exists on PATH
    When the process backend checks availability
    Then the availability check succeeds

  @parity_slice0_cancel_no_orphans
  Scenario: Cancel reaps the child process without leaving orphans
    Given a long-running process backend child is active
    When the invocation is cancelled
    Then the child receives SIGTERM and exits without an orphan

  @parity_slice0_timeout_cleanup
  Scenario: Timeout cleanup escalates and leaves no orphan child process
    Given a long-running process backend child ignores SIGTERM
    When the invocation times out
    Then timeout cleanup removes the child process and clears active state

  @parity_slice0_final_review_planner_in_snapshot
  Scenario: Final-review resolution snapshots include the planner target
    Given a run fails during final review after the panel is resolved
    When the run snapshot is persisted
    Then the saved final-review resolution includes final_review_planner

  @parity_slice0_final_review_planner_drift_detected
  Scenario: Resume warns when only the final-review planner drifts
    Given a suspended final-review run with a saved planner resolution
    When the planner backend changes before resume
    Then resume records durable drift for the final-review planner

  @parity_slice0_ref_encoding_reserved_chars
  Scenario: GitHub compare refs encode reserved characters in request paths
    Given compare refs contain percent signs and slashes
    When the GitHub adapter checks whether the head is ahead
    Then the compare request path percent-encodes the refs safely
