#!/usr/bin/env bash
# live-backend-smoke.sh — Creates an isolated smoke project, runs a standard
# flow against a selected backend, and emits evidence for sign-off docs.
#
# Usage:
#   ./scripts/live-backend-smoke.sh <backend>
#
# Where <backend> is one of: claude, codex, openrouter
#
# Environment variables:
#   RALPH_BURNING      — path to ralph-burning binary (default: cargo run --)
#   SMOKE_DIR          — scratch directory for smoke state (default: /tmp/rb-smoke-$$)
#   OPENROUTER_API_KEY — required when backend=openrouter
#
# The script runs all CLI commands from inside SMOKE_DIR so that current_dir()
# resolves to the scratch workspace, not the real repo.  This guarantees that
# preflight and run failures cannot touch the checked-in .ralph-burning state
# or active-project selection.
#
# Exit codes:
#   0  — smoke passed
#   1  — smoke failed (evidence recorded)
#   2  — preflight failed (no project state mutated)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

BACKEND="${1:?Usage: $0 <claude|codex|openrouter>}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/rb-smoke-$$}"
SMOKE_ID="smoke-${BACKEND}-$(date +%Y%m%d%H%M%S)"
EVIDENCE_FILE="${SMOKE_DIR}/${SMOKE_ID}-evidence.txt"

# Resolve the ralph-burning binary.  When the caller sets RALPH_BURNING we
# honour it as-is; otherwise we build an absolute path to `cargo run` so that
# it works after we cd into the scratch workspace.
if [ -n "${RALPH_BURNING:-}" ]; then
    IFS=' ' read -ra RB <<< "$RALPH_BURNING"
else
    RB=( cargo run --manifest-path "$(cd "$(dirname "$0")/.." && pwd)/Cargo.toml" -- )
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { printf '[smoke] %s\n' "$*" >&2; }
fail() { printf '[smoke] FAIL: %s\n' "$*" >&2; }
evidence() { printf '%s\n' "$*" | tee -a "$EVIDENCE_FILE"; }

# json_field <field_name> — extract a top-level string field from JSON on stdin.
# Uses jq if available; otherwise falls back to a whitespace-tolerant sed pattern
# that handles both compact and pretty-printed serde_json output.
json_field() {
    local field="$1"
    if command -v jq >/dev/null 2>&1; then
        # Recursively find the first string value for the named field,
        # handling both top-level fields and nested ones like events[].details.run_id.
        jq -r --arg f "$field" '[.. | .[$f]? | strings] | first // empty' 2>/dev/null
    else
        # Match "field" : "value" with optional whitespace around the colon
        sed -n 's/.*"'"$field"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1
    fi
}

# Preflight evidence directory — evidence captured during preflight is saved
# here so it survives scratch-dir cleanup on preflight failure.
PREFLIGHT_EVIDENCE_DIR="${SMOKE_DIR%/*}"  # parent of SMOKE_DIR (e.g. /tmp)
PREFLIGHT_EVIDENCE_COPY="${PREFLIGHT_EVIDENCE_DIR}/${SMOKE_ID}-preflight-evidence.txt"

cleanup_on_preflight_fail() {
    # Preflight failures must not leave any project or workspace state.
    # Preserve the evidence file outside the scratch dir so the operator
    # can inspect the readiness error, then remove the scratch dir.
    if [ -d "$SMOKE_DIR" ] && [ -f "$EVIDENCE_FILE" ]; then
        cp "$EVIDENCE_FILE" "$PREFLIGHT_EVIDENCE_COPY" 2>/dev/null || true
        log "Preflight evidence preserved at: $PREFLIGHT_EVIDENCE_COPY"
    fi
    if [ -d "$SMOKE_DIR" ]; then
        rm -rf "$SMOKE_DIR"
    fi
}

# ── Preflight ─────────────────────────────────────────────────────────────────

preflight_passed=false
trap 'if ! $preflight_passed; then cleanup_on_preflight_fail; fi' EXIT

log "Preflight: backend=$BACKEND  smoke_id=$SMOKE_ID"

# Validate backend argument
case "$BACKEND" in
    claude|codex|openrouter) ;;
    *) fail "Unknown backend: $BACKEND (expected claude, codex, or openrouter)"; exit 2 ;;
esac

# Create isolated smoke workspace directory and initialise a minimal
# workspace.toml so that the CLI accepts it as a valid workspace root.
mkdir -p "$SMOKE_DIR/.ralph-burning"

evidence "# Smoke Evidence: $SMOKE_ID"
evidence "backend: $BACKEND"
evidence "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
evidence "hostname: $(hostname)"
evidence "smoke_dir: $SMOKE_DIR"
evidence ""

# Write backend-specific workspace config inside the scratch workspace.
case "$BACKEND" in
    claude)
        cat > "$SMOKE_DIR/.ralph-burning/workspace.toml" <<TOML
version = 1
created_at = "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

[settings]
default_backend = "claude"

[execution]
mode = "direct"

[workflow]
implementer_backend = "claude"
qa_backend = "claude"

[completion]
backends = ["claude", "claude"]

[final_review]
backends = ["claude", "claude"]

[prompt_review]
refiner_backend = "claude"
validator_backends = ["claude", "claude"]
TOML
        ;;
    codex)
        cat > "$SMOKE_DIR/.ralph-burning/workspace.toml" <<TOML
version = 1
created_at = "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

[settings]
default_backend = "codex"

[execution]
mode = "direct"

[workflow]
planner_backend = "codex"
reviewer_backend = "codex"

[completion]
backends = ["codex", "codex"]

[final_review]
backends = ["codex", "codex"]
arbiter_backend = "codex"

[prompt_review]
refiner_backend = "codex"
validator_backends = ["codex", "codex"]
TOML
        ;;
    openrouter)
        cat > "$SMOKE_DIR/.ralph-burning/workspace.toml" <<TOML
version = 1
created_at = "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

[settings]
default_backend = "openrouter"

[backends.openrouter]
enabled = true

[execution]
mode = "direct"

[workflow]
planner_backend = "openrouter"
implementer_backend = "openrouter"
reviewer_backend = "openrouter"
qa_backend = "openrouter"

[completion]
backends = ["openrouter", "openrouter"]

[final_review]
backends = ["openrouter", "openrouter"]
arbiter_backend = "openrouter"

[prompt_review]
refiner_backend = "openrouter"
validator_backends = ["openrouter", "openrouter"]
TOML
        # OpenRouter requires the dedicated adapter, selected via env var.
        export RALPH_BURNING_BACKEND=openrouter
        ;;
esac
evidence "workspace_config: $SMOKE_DIR/.ralph-burning/workspace.toml written"

# Backend-specific preflight checks
case "$BACKEND" in
    claude)
        if ! command -v claude >/dev/null 2>&1; then
            fail "claude CLI not found on PATH"
            evidence "preflight: FAIL — claude CLI not found"
            exit 2
        fi
        log "Preflight: claude CLI found at $(command -v claude)"
        evidence "preflight: claude CLI at $(command -v claude)"
        ;;
    codex)
        if ! command -v codex >/dev/null 2>&1; then
            fail "codex CLI not found on PATH"
            evidence "preflight: FAIL — codex CLI not found"
            exit 2
        fi
        log "Preflight: codex CLI found at $(command -v codex)"
        evidence "preflight: codex CLI at $(command -v codex)"
        ;;
    openrouter)
        if [ -z "${OPENROUTER_API_KEY:-}" ]; then
            fail "OPENROUTER_API_KEY is not set"
            evidence "preflight: FAIL — OPENROUTER_API_KEY not set"
            exit 2
        fi
        log "Preflight: OPENROUTER_API_KEY is set"
        evidence "preflight: OPENROUTER_API_KEY present (${#OPENROUTER_API_KEY} chars)"

        # Credit check: send a minimal completion request to verify the key
        # has usable credits before committing to a full smoke run.
        log "Preflight: verifying OpenRouter credit availability..."
        CREDIT_CHECK_RESPONSE=$(curl -s -w "\n%{http_code}" \
            -H "Authorization: Bearer $OPENROUTER_API_KEY" \
            -H "Content-Type: application/json" \
            -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"say ok"}],"max_tokens":3}' \
            "https://openrouter.ai/api/v1/chat/completions" 2>/dev/null || echo "000")
        CREDIT_CHECK_HTTP=$(echo "$CREDIT_CHECK_RESPONSE" | tail -1)
        if [ "$CREDIT_CHECK_HTTP" = "402" ]; then
            fail "OpenRouter API key has insufficient credits (HTTP 402)"
            evidence "preflight: FAIL — OpenRouter credit check returned HTTP 402 (insufficient credits)"
            evidence "preflight: Top up the OpenRouter account and re-run."
            exit 2
        elif [ "$CREDIT_CHECK_HTTP" = "401" ]; then
            fail "OpenRouter API key is invalid (HTTP 401)"
            evidence "preflight: FAIL — OpenRouter API key rejected (HTTP 401)"
            exit 2
        elif [ "$CREDIT_CHECK_HTTP" -ge 200 ] && [ "$CREDIT_CHECK_HTTP" -lt 300 ] 2>/dev/null; then
            log "Preflight: OpenRouter credit check PASS (HTTP $CREDIT_CHECK_HTTP)"
            evidence "preflight: OpenRouter credit check PASS (HTTP $CREDIT_CHECK_HTTP)"
        else
            log "Preflight: OpenRouter credit check inconclusive (HTTP $CREDIT_CHECK_HTTP), proceeding"
            evidence "preflight: OpenRouter credit check inconclusive (HTTP $CREDIT_CHECK_HTTP)"
        fi
        ;;
esac

# All subsequent CLI calls run from the scratch workspace so that
# current_dir() resolves to SMOKE_DIR, isolating all project/workspace state.
cd "$SMOKE_DIR"

# Run ralph-burning backend check with explicit --backend to validate
# the specific backend under test.
log "Running backend check (--backend $BACKEND)..."
if "${RB[@]}" backend check --backend "$BACKEND" --json \
       > "$SMOKE_DIR/backend-check.json" 2>&1; then
    log "Backend check passed"
    evidence "backend_check: PASS"
else
    bc_exit=$?
    fail "backend check exited $bc_exit"
    evidence "backend_check: FAIL (exit $bc_exit)"
    cat "$SMOKE_DIR/backend-check.json" >&2 || true
    exit 2
fi

# Run backend probe for the standard flow with explicit --backend.
log "Running backend probe for standard flow (--backend $BACKEND)..."
if "${RB[@]}" backend probe --role planner --flow standard \
       --backend "$BACKEND" --json \
       > "$SMOKE_DIR/probe-planner.json" 2>&1; then
    evidence "probe_planner: PASS"
else
    probe_exit=$?
    fail "backend probe (planner) exited $probe_exit"
    evidence "probe_planner: FAIL (exit $probe_exit)"
    exit 2
fi

if "${RB[@]}" backend probe --role implementer --flow standard \
       --backend "$BACKEND" --json \
       > "$SMOKE_DIR/probe-implementer.json" 2>&1; then
    evidence "probe_implementer: PASS"
else
    probe_exit=$?
    fail "backend probe (implementer) exited $probe_exit"
    evidence "probe_implementer: FAIL (exit $probe_exit)"
    exit 2
fi

preflight_passed=true
evidence ""
evidence "--- Preflight complete ---"
evidence ""

# ── Project Creation ──────────────────────────────────────────────────────────

log "Creating smoke project..."

# Resolve the seed fixture path relative to the script directory.  Using
# --from-seed bypasses the quick-requirements pipeline, which avoids the
# model-behaviour blocker where some backends (e.g. Codex) cannot approve
# requirements within the MAX_QUICK_REVISIONS limit.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SEED_FILE="${SMOKE_SEED:-${SCRIPT_DIR}/smoke-seed.json}"

if [ ! -f "$SEED_FILE" ]; then
    fail "smoke seed fixture not found: $SEED_FILE"
    evidence "bootstrap: FAIL — seed file not found: $SEED_FILE"
    exit 2
fi

# Copy seed into scratch workspace so paths stay isolated.
cp "$SEED_FILE" "$SMOKE_DIR/smoke-seed.json"

# Override project_id in the seed to include the backend name, ensuring
# each backend smoke creates a distinct project.
if command -v jq >/dev/null 2>&1; then
    jq --arg id "smoke-${BACKEND}-test" '.project_id = $id | .project_name = "Smoke Test (\($id))"' \
        "$SMOKE_DIR/smoke-seed.json" > "$SMOKE_DIR/smoke-seed-tmp.json" \
        && mv "$SMOKE_DIR/smoke-seed-tmp.json" "$SMOKE_DIR/smoke-seed.json"
fi

BOOTSTRAP_ARGS=(
    project bootstrap
    --from-seed "$SMOKE_DIR/smoke-seed.json"
    --flow standard
)
evidence "bootstrap_command: ${RB[*]} ${BOOTSTRAP_ARGS[*]}"

if "${RB[@]}" "${BOOTSTRAP_ARGS[@]}" \
       > "$SMOKE_DIR/bootstrap-stdout.txt" 2>"$SMOKE_DIR/bootstrap-stderr.txt"; then
    evidence "bootstrap: PASS"
    log "Project bootstrapped successfully"
else
    boot_exit=$?
    fail "project bootstrap exited $boot_exit"
    evidence "bootstrap: FAIL (exit $boot_exit)"
    # Project may exist in a valid but not-started state — leave it for inspection
    evidence "bootstrap_stderr: $(cat "$SMOKE_DIR/bootstrap-stderr.txt" 2>/dev/null || echo '(empty)')"
    exit 1
fi

# ── Run Start ─────────────────────────────────────────────────────────────────

log "Starting standard flow run (--backend $BACKEND)..."

# Pass --backend so that the run explicitly targets the backend under test.
START_ARGS=(
    run start
    --backend "$BACKEND"
)
evidence "start_command: ${RB[*]} ${START_ARGS[*]}"

if "${RB[@]}" "${START_ARGS[@]}" \
       > "$SMOKE_DIR/run-stdout.txt" 2>"$SMOKE_DIR/run-stderr.txt"; then
    evidence "run_start: PASS"
    log "Run completed successfully"
else
    run_exit=$?
    fail "run start exited $run_exit"
    evidence "run_start: FAIL (exit $run_exit)"
    evidence "run_stderr: $(cat "$SMOKE_DIR/run-stderr.txt" 2>/dev/null || echo '(empty)')"
    # Run history remains canonical and inspectable
    "${RB[@]}" run status --json > "$SMOKE_DIR/final-status.json" 2>/dev/null || true
    "${RB[@]}" run history --json > "$SMOKE_DIR/final-history.json" 2>/dev/null || true
    FAIL_PROJECT_ID=$(json_field project_id < "$SMOKE_DIR/final-status.json" 2>/dev/null || echo "(unknown)")
    FAIL_RUN_STATUS=$(json_field status < "$SMOKE_DIR/final-status.json" 2>/dev/null || echo "(unknown)")
    FAIL_RUN_ID=$(json_field run_id < "$SMOKE_DIR/final-history.json" 2>/dev/null || echo "(unknown)")
    evidence "project_id: $FAIL_PROJECT_ID"
    evidence "run_id: $FAIL_RUN_ID"
    evidence "run_status: $FAIL_RUN_STATUS"
    evidence "final_status_json: $(cat "$SMOKE_DIR/final-status.json" 2>/dev/null || echo '(unavailable)')"
    exit 1
fi

# ── Evidence Collection ───────────────────────────────────────────────────────

log "Collecting final evidence..."

"${RB[@]}" run status --json > "$SMOKE_DIR/final-status.json" 2>/dev/null || true
"${RB[@]}" run history --json > "$SMOKE_DIR/final-history.json" 2>/dev/null || true

FINAL_STATUS=$(cat "$SMOKE_DIR/final-status.json" 2>/dev/null || echo '{}')

# Extract project_id and canonical status for sign-off evidence.
# Uses json_field() which handles both pretty-printed (serde_json::to_string_pretty)
# and compact JSON output from the CLI.
PROJECT_ID=$(printf '%s' "$FINAL_STATUS" | json_field project_id || echo "(unknown)")
RUN_STATUS=$(printf '%s' "$FINAL_STATUS" | json_field status || echo "(unknown)")

# Extract run_id from journal events in run history.  The run_started event
# stores run_id in details.run_id (journal.rs:107).
FINAL_HISTORY=$(cat "$SMOKE_DIR/final-history.json" 2>/dev/null || echo '{}')
RUN_ID=$(printf '%s' "$FINAL_HISTORY" | json_field run_id || echo "(unknown)")

evidence ""
evidence "--- Final Evidence ---"
evidence "project_id: $PROJECT_ID"
evidence "run_id: $RUN_ID"
evidence "run_status: $RUN_STATUS"
evidence "final_status_json: $FINAL_STATUS"
evidence "smoke_result: PASS"
evidence "smoke_id: $SMOKE_ID"

log "Smoke PASSED for $BACKEND"
log "  project_id: $PROJECT_ID"
log "  run_id: $RUN_ID"
log "  run_status: $RUN_STATUS"
log "  Evidence file: $EVIDENCE_FILE"

exit 0
