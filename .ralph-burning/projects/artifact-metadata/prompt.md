## Problem

Stage artifacts (history/artifacts/*.json) don't record which backend family or model was used for the invocation. The `producer` field exists but `invocation_metadata` doesn't include `backend_family` or `model_id` from the `ResolvedBackendTarget`.

## Fix

In the artifact persistence code, populate `backend_family` and `model_id` in the artifact's metadata from `InvocationEnvelope.metadata.backend_used` and `InvocationEnvelope.metadata.model_used`. The `InvocationMetadata` struct already has `backend_used: BackendSpec` and `model_used: ModelSpec` fields.

**Files:** Look at how artifacts are persisted in `src/contexts/workflow_composition/engine.rs` and `src/adapters/fs.rs`.
