Feature: Backend operations parity (Slice 5)
  Operator-facing backend diagnostics commands for inspecting backend
  resolution, readiness, and panel configuration outside a live run.

  # parity_slice5_backend_list
  Scenario: backend list shows all supported families
    Given an initialized workspace with default config
    When the operator runs `ralph-burning backend list`
    Then the output includes claude, codex, openrouter, and stub families
    And each family shows its enabled state and transport mechanism

  # parity_slice5_backend_check
  Scenario: backend check aggregates readiness failures
    Given an initialized workspace with a disabled required backend
    When the operator runs `ralph-burning backend check`
    Then the command exits non-zero
    And each blocking failure identifies the exact role, backend family, and config source
    And the command does not create or modify any project state, run snapshots, or journals

  # parity_slice5_backend_show_effective
  Scenario: backend show-effective exposes source precedence
    Given an initialized workspace with workspace and CLI backend overrides
    When the operator runs `ralph-burning backend show-effective --json`
    Then the JSON output includes base backend, default model, per-role resolution, and timeout
    And each field reports its source as default, workspace.toml, project config.toml, or cli override

  # parity_slice5_backend_probe_completion_panel
  Scenario: backend probe resolves completion panel targets
    Given an initialized workspace with explicit completion panel backends
    When the operator runs `ralph-burning backend probe --role completion_panel --flow standard`
    Then the output shows each panel member with required/optional status
    And optional members that are disabled are listed as omitted without failing the probe
    And the probe fails only when the configured minimum cannot be met

  # parity_slice5_backend_probe_final_review_panel
  Scenario: backend probe resolves final review panel targets
    Given an initialized workspace with default config
    When the operator runs `ralph-burning backend probe --role final_review_panel --flow standard`
    Then the output shows reviewers and arbiter
    And required-member failures identify the exact role, backend family, and config source
