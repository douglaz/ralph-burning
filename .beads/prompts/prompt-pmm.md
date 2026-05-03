## Bead ID: ralph-burning-pmm

## Goal

Allow each panel member (reviewer, voter, completer) to specify its own model independently, so panels like final_review can mix different models from the same backend family.

## Problem

Panel backend specs are plain backend family names: `["codex", "codex"]`. The model for each role is resolved from a single `backends.<family>.role_models.<role>` setting. When two panel members use the same backend family, they always get the same model — there is no way to say "codex slot 1 uses gpt-5.4, codex slot 2 uses gpt-5.3-codex-spark".

This became a real problem when Claude credits ran out: replacing the Opus reviewer with a second codex entry forced both reviewers to the same model, overwriting the gpt-5.4 reviewer with spark.

## Proposed Solution

Extend `PanelBackendSpec` to support an optional inline model override using `family/model` syntax:

```toml
# Current (family only — both get same model from role_models config)
final_review.backends = ["codex", "codex"]

# Proposed (family with optional model override)
final_review.backends = ["codex/gpt-5.4-xhigh", "codex/gpt-5.3-codex-spark-xhigh"]

# With the optional marker
final_review.backends = ["codex/gpt-5.4-xhigh", "?codex/gpt-5.3-codex-spark-xhigh"]

# Bare family names still work (resolved from role_models config as before)
final_review.backends = ["claude", "codex/gpt-5.4-xhigh"]
```

## Changes Required

### 1. Extend PanelBackendSpec parsing
**File:** `src/shared/domain.rs` (~line 494, `FromStr` impl for `PanelBackendSpec`)

Parse `family/model` syntax. The `PanelBackendSpec` enum variants (`Required`, `Optional`) wrap a `BackendSelection` which already has a `model_override: Option<String>` field. Wire the inline model from the parsed spec into `BackendSelection::model_override`.

### 2. Update Display impl
**File:** `src/shared/domain.rs` (~line 485)

`PanelBackendSpec::Display` should render the model when present: `codex/gpt-5.4` or `?codex/gpt-5.3-codex-spark`.

### 3. Update panel resolution
**File:** `src/contexts/agent_execution/policy.rs` (~line 131)

When resolving panel members to concrete `BackendTarget`, if `BackendSelection::model_override` is `Some`, use it instead of looking up `role_models` config. The existing `resolve_panel_member` or equivalent function should already respect `model_override` — verify this is the case.

### 4. Update config set CLI validation
**File:** `src/contexts/workspace_governance/config.rs`

The `apply_string_list` function and `config set` validation should accept the new `family/model` format in backend list values.

### 5. Serde round-trip
The `Serialize` and `Deserialize` impls for `PanelBackendSpec` use `Display`/`FromStr` via string serialization. Once Display and FromStr handle the `/model` syntax, serde should work automatically.

## Key Files
- `src/shared/domain.rs` — `PanelBackendSpec`, `BackendSelection`
- `src/contexts/agent_execution/policy.rs` — panel member resolution
- `src/contexts/workspace_governance/config.rs` — config set validation

## Acceptance Criteria

- Panel specs support `family/model` syntax: `["codex/gpt-5.4", "codex/gpt-5.3-codex-spark"]`
- Inline model overrides take precedence over role_models config
- Bare family names (no `/`) continue to work exactly as before
- Optional marker `?` works with the new syntax: `?codex/gpt-5.4`
- `config show` displays the model override when present
- Serde JSON/TOML round-trip works
- Existing tests pass
- `cargo test && cargo clippy && cargo fmt --check`
