Feature: Backend policy foundation
  Backend role overrides and timeout config round trips are resolved through
  the new policy service and config model.

  @backend.role_overrides.per_role_override_beats_default
  Scenario: Per-role override beats default
    Given backend policy config defines a reviewer override
    When reviewer resolution is computed
    Then the explicit reviewer backend wins over the default family policy

  @backend.role_timeouts.config_roundtrip
  Scenario: Role timeouts round trip through config
    Given backend role timeouts are defined in project policy
    When the project config is serialized and deserialized
    Then the role timeout values are preserved exactly
