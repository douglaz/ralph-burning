## Problem

When a backend invocation fails (non-zero exit), `prepared.cleanup()` deletes temp files (schema, last-message) and raw output is never written.

## Fix

On failure, move temp files to `runtime/failed/` instead of deleting. Write stdout+stderr to a `.failed.raw` file so operators can inspect what the backend returned.

**File:** `src/adapters/process_backend.rs` — the error paths in `invoke()` and `cleanup()`
