## Problem

When a backend invocation fails (non-zero exit), `prepared.cleanup()` deletes all temp files (schema, last-message) before returning the error. The raw output is also never written on failure.

## Fix

1. On non-zero exit, move temp files (schema, last-message) to a `runtime/failed/` directory instead of deleting them
2. Write stdout+stderr to a `.failed.raw` file on non-zero exit so operators can inspect what the backend returned
3. The `cleanup()` method should check if the invocation failed and skip deletion in that case

**File:** `src/adapters/process_backend.rs` — the `cleanup()` method and the error paths in `invoke()`
