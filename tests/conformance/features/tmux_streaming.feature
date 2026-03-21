Feature: Tmux and streaming parity (Slice 6)
  Optional tmux-backed execution and live runtime-log streaming for active
  backend invocations while preserving durable-history semantics.

  # SC-TMUX-001
  Scenario: execution mode resolves with standard precedence
    Given workspace and project execution-mode overrides
    When the effective config is loaded with a CLI execution-mode override
    Then the effective execution mode matches the CLI override
    And the config source for execution.mode is cli override

  # SC-TMUX-002
  Scenario: stream_output resolves with standard precedence
    Given workspace and project stream_output overrides
    When the effective config is loaded with a CLI stream_output override
    Then the effective stream_output value matches the CLI override
    And the config source for execution.stream_output is cli override

  # SC-TMUX-003
  Scenario: tmux session naming is deterministic
    Given a project id and invocation id
    When the tmux session name is derived
    Then the session name matches the rb-<project-id>-<invocation-id> pattern

  # SC-TMUX-004
  Scenario: backend check reports tmux availability when tmux mode is configured
    Given execution.mode is tmux and tmux is unavailable on PATH
    When the operator runs backend check logic
    Then the result includes a tmux_unavailable failure

  # SC-TMUX-005
  Scenario: run attach exits cleanly when no active tmux session exists
    Given an initialized workspace with project "alpha" configured for tmux mode
    When the operator runs `ralph-burning run attach`
    Then the command exits successfully
    And the output explains that no active tmux session exists

  # SC-TMUX-006
  Scenario: tmux adapter preserves direct-mode parsed payloads
    Given the same backend response is returned in direct and tmux modes
    When both adapters invoke the planning contract
    Then the parsed payloads are identical
    And the raw output references are identical

  # SC-TMUX-007
  Scenario: tmux adapter cancel cleans up the active session
    Given a long-running tmux-backed invocation
    When cancellation is requested
    Then the tmux session no longer exists

  # SC-TMUX-008
  Scenario: tmux adapter timeout cleans up the active session
    Given a long-running tmux-backed invocation
    When the agent execution service times out the invocation
    Then the invocation returns a timeout error
    And the tmux session no longer exists

  # SC-TMUX-009
  Scenario: tail follow with logs can use watcher-driven updates
    Given stream_output is enabled
    When run tail follow logic builds a watcher
    Then watcher setup succeeds when the project root is watchable

  # SC-TMUX-010
  Scenario: direct mode remains the default when execution mode is unset
    Given no execution settings are configured
    When the effective config is loaded
    Then the effective execution mode is direct
