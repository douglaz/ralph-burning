Feature: Validation Runner and Pre-Commit Parity (Slice 6)
  Docs and CI validation stages run local commands instead of invoking agent
  backends. Standard flow injects local validation evidence into review
  context. Pre-commit checks gate reviewer approval in the standard flow.

  @validation.docs.commands_pass
  Scenario: Docs validation with passing commands completes
    Given an initialized workspace with project "vd-pass" using flow "docs_change"
    And docs_commands is configured to ["true"]
    When the user starts a run
    Then the docs_validation stage runs commands locally
    And all docs commands pass
    And the run completes successfully

  @validation.docs.command_failure_requests_changes
  Scenario: Docs validation with failing command triggers remediation
    Given an initialized workspace with project "vd-fail" using flow "docs_change"
    And docs_commands is configured to ["false"]
    When the user starts a run
    Then the docs_validation stage runs commands locally
    And the failing command causes a request_changes outcome
    And local validation evidence is persisted as StageSupporting records
    And the run returns to docs_update for remediation

  @validation.ci.commands_pass
  Scenario: CI validation with passing commands completes
    Given an initialized workspace with project "vc-pass" using flow "ci_improvement"
    And ci_commands is configured to ["true"]
    When the user starts a run
    Then the ci_validation stage runs commands locally
    And all CI commands pass
    And the run completes successfully

  @validation.ci.command_failure_requests_changes
  Scenario: CI validation with failing command triggers remediation
    Given an initialized workspace with project "vc-fail" using flow "ci_improvement"
    And ci_commands is configured to ["false"]
    When the user starts a run
    Then the ci_validation stage runs commands locally
    And the failing command causes a request_changes outcome
    And local validation evidence is persisted as StageSupporting records
    And the run returns to ci_update for remediation

  @validation.standard.review_context_contains_local_validation
  Scenario: Standard flow review stage includes local validation evidence
    Given an initialized workspace with project "vs-ctx" using flow "standard"
    And standard_commands is configured to ["echo validation-evidence-marker"]
    And pre-commit checks are disabled
    When the user starts a run
    Then local validation evidence from standard_commands is persisted as StageSupporting records
    And the review stage prompt includes local validation context
    And the run completes successfully

  @validation.pre_commit.disabled_skips_checks
  Scenario: Pre-commit checks are skipped when all disabled
    Given an initialized workspace with project "vp-disabled" using flow "standard"
    And pre_commit_fmt, pre_commit_clippy, and pre_commit_nix_build are all false
    When the user starts a run
    Then no pre-commit checks run after review approval
    And the run completes successfully

  @validation.pre_commit.no_cargo_toml_skips_cargo_checks
  Scenario: Cargo-based pre-commit checks are skipped without Cargo.toml
    Given an initialized workspace with project "vp-nocargo" using flow "standard"
    And pre_commit_fmt and pre_commit_clippy are true
    And the repo root does not contain Cargo.toml
    When the user starts a run
    Then cargo fmt and cargo clippy checks are skipped
    And the run completes successfully

  @validation.pre_commit.fmt_failure_triggers_remediation
  Scenario: cargo fmt failure triggers remediation
    Given a failed pre-commit group result from cargo fmt --check
    Then the remediation context source_stage is "pre_commit"
    And findings_or_gaps contains the failure details
    And follow_up_or_amendments contains a fix instruction

  @validation.pre_commit.fmt_auto_fix_succeeds
  Scenario: cargo fmt auto-fix succeeds on rerun
    Given pre_commit_fmt_auto_fix is true
    And cargo fmt --check initially fails
    When cargo fmt is run to auto-fix
    And cargo fmt --check is rerun
    Then the recheck passes
    And the pre-commit group result reflects all three commands
    And the group is marked as passed

  @validation.pre_commit.nix_build_failure_records_feedback
  Scenario: nix build failure records feedback
    Given a failed pre-commit group result from nix build
    Then the remediation context findings_or_gaps contains nix build error details
    And follow_up_or_amendments contains a fix instruction for nix build
