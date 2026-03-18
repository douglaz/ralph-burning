Feature: Backend stub gating
  Production builds must reject the test-only stub selector while feature-enabled
  test builds continue to expose the stub seam.

  @backend.stub.production_rejects_stub_selector
  Scenario: Production build rejects the stub selector
    Given the binary is built without the test-stub feature
    When RALPH_BURNING_BACKEND is set to stub
    Then backend adapter construction fails with an invalid config error
