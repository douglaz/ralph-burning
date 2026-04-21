Feature: OpenRouter backend parity
  OpenRouter-backed execution is available through the shared agent-execution
  builder, enforces explicit model injection, and respects backend enablement
  policy during target resolution.

  @backend.openrouter.model_injection
  Scenario: OpenRouter request injects the resolved model ID
    Given an OpenRouter invocation target with model "anthropic/claude-3.5-sonnet"
    When the shared agent execution service invokes the target
    Then the OpenRouter API request body contains exactly "model": "anthropic/claude-3.5-sonnet"

  @backend.openrouter.disabled_default_backend
  Scenario: Disabled OpenRouter default backend fails policy resolution
    Given workspace policy resolves the primary cycle family to "openrouter"
    And backends.openrouter.enabled is false
    When planning target resolution is computed
    Then resolution fails with BackendUnavailable

  @backend.openrouter.requirements_draft
  Scenario: Requirements draft contract runs through OpenRouter
    Given an OpenRouter invocation targeting the "requirements:requirements_draft" contract
    When the shared agent execution service invokes the target
    Then the returned payload satisfies the requirements draft contract
