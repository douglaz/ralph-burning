## Bead ID: ralph-burning-TBD

## Goal

Make artifact producer metadata unambiguous by always including both the requested (resolved target) and actual (adapter-reported) backend/model, so readers can tell at a glance what was planned vs what ran.

## Problem

Currently `RecordProducer::Agent` in `src/contexts/workflow_composition/panel_contracts.rs:252-261` has:
- `backend_family` / `model_id` — always set to the **resolved target** (what was requested), overwritten in `src/contexts/agent_execution/service.rs:235-236`
- `adapter_reported_backend_family` / `adapter_reported_model_id` — only populated when the actual backend **differs** from the target, and skipped during serialization when None

This means artifact JSON always shows the requested model as the producer, with no indication of whether it actually ran on that model. When they match (the common case), the adapter_reported fields are absent, making it impossible to distinguish "matched" from "not tracked."

## Changes Required

1. **Always populate `adapter_reported_backend_family` and `adapter_reported_model_id`** in `src/contexts/agent_execution/service.rs:219-234` — remove the conditional that only sets them on mismatch. The adapter always reports what it used; pass it through unconditionally.

2. **Rename fields in `RecordProducer::Agent`** for clarity:
   - `backend_family` → `requested_backend_family`
   - `model_id` → `requested_model_id`
   - `adapter_reported_backend_family` → `actual_backend_family`
   - `adapter_reported_model_id` → `actual_model_id`

3. **Remove `skip_serializing_if`** on the actual fields — they should always be present in serialized artifacts.

4. **Update `agent_record_producer()`** in `src/contexts/workflow_composition/mod.rs:197-219` to pass both requested and actual values unconditionally.

5. **Update Display impl** for `RecordProducer` to show actual model (since that's what readers care about), e.g. `agent:claude/claude-opus-4-6` when they match, `agent:claude/claude-opus-4-6 (requested codex/gpt-5.4)` when they differ.

6. **Update tests** that assert on producer fields to use the new field names.

## Acceptance Criteria

- Artifact JSON always contains both requested and actual backend/model
- When they match, both fields have the same value (no ambiguity)
- When they differ, both are visible without needing to cross-reference raw backend files
- Existing artifact deserialization still works (serde defaults for renamed fields)

## Non-Goals

- Changing the raw backend output format
- Retroactively fixing old artifacts
