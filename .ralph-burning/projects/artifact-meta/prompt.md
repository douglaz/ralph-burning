## Problem

Stage artifacts (history/artifacts/*.json) have a `producer` field but don't include `backend_family` or `model_id` from the ResolvedBackendTarget used for the invocation.

## Fix

When persisting artifacts, populate `producer.backend_family` and `producer.model_id` from `InvocationEnvelope.metadata.backend_used` and `InvocationEnvelope.metadata.model_used`.

**Files:** `src/contexts/workflow_composition/engine.rs` — artifact persistence, `src/adapters/fs.rs` — the ArtifactRecord struct
