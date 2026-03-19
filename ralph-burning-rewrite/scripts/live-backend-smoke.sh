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
#   RALPH_BURNING   — path to ralph-burning binary (default: cargo run --)
#   SMOKE_DIR       — scratch directory for smoke state (default: /tmp/rb-smoke-$$)
#   OPENROUTER_API_KEY — required when backend=openrouter
#
# Exit codes:
#   0  — smoke passed
#   1  — smoke failed (evidence recorded)
#   2  — preflight failed (no project state mutated)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

BACKEND="${1:?Usage: $0 <claude|codex|openrouter>}"
RALPH_BURNING="${RALPH_BURNING:-cargo run --}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/rb-smoke-$$}"
SMOKE_ID="smoke-${BACKEND}-$(date +%Y%m%d%H%M%S)"
EVIDENCE_FILE="${SMOKE_DIR}/${SMOKE_ID}-evidence.txt"

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { printf '[smoke] %s\n' "$*" >&2; }
fail() { printf '[smoke] FAIL: %s\n' "$*" >&2; }
evidence() { printf '%s\n' "$*" | tee -a "$EVIDENCE_FILE"; }

cleanup_on_preflight_fail() {
    # Preflight failures must not leave any project or workspace state.
    if [ -d "$SMOKE_DIR" ] && [ ! -f "$EVIDENCE_FILE" ]; then
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

# Create isolated smoke workspace
mkdir -p "$SMOKE_DIR"
evidence "# Smoke Evidence: $SMOKE_ID"
evidence "backend: $BACKEND"
evidence "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
evidence "hostname: $(hostname)"
evidence "smoke_dir: $SMOKE_DIR"
evidence ""

# Backend-specific preflight
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
        ;;
esac

# Run ralph-burning backend check to validate readiness
log "Running backend check..."
if $RALPH_BURNING backend check --json > "$SMOKE_DIR/backend-check.json" 2>&1; then
    log "Backend check passed"
    evidence "backend_check: PASS"
else
    bc_exit=$?
    fail "backend check exited $bc_exit"
    evidence "backend_check: FAIL (exit $bc_exit)"
    cat "$SMOKE_DIR/backend-check.json" >&2 || true
    exit 2
fi

# Run backend probe for the standard flow
log "Running backend probe for standard flow..."
if $RALPH_BURNING backend probe --role planner --flow standard --json > "$SMOKE_DIR/probe-planner.json" 2>&1; then
    evidence "probe_planner: PASS"
else
    probe_exit=$?
    fail "backend probe (planner) exited $probe_exit"
    evidence "probe_planner: FAIL (exit $probe_exit)"
    exit 2
fi

if $RALPH_BURNING backend probe --role implementer --flow standard --json > "$SMOKE_DIR/probe-implementer.json" 2>&1; then
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

SMOKE_PROJECT_NAME="smoke-${BACKEND}-$(date +%s)"

# For OpenRouter, create an isolated config that enables the backend
if [ "$BACKEND" = "openrouter" ]; then
    SMOKE_CONFIG_DIR="${SMOKE_DIR}/.ralph-burning"
    mkdir -p "$SMOKE_CONFIG_DIR"
    cat > "$SMOKE_CONFIG_DIR/workspace.toml" <<TOML
[workspace]
version = "0.1.0"

[backends.openrouter]
enabled = true

[execution]
mode = "direct"
TOML
    evidence "openrouter_config: isolated workspace.toml with enabled=true, mode=direct"
    export RALPH_BURNING_WORKSPACE="$SMOKE_DIR"
fi

# Bootstrap a standard-flow project
BOOTSTRAP_CMD="$RALPH_BURNING project bootstrap --idea \"Smoke test: validate ${BACKEND} backend end-to-end\" --flow standard"
evidence "bootstrap_command: $BOOTSTRAP_CMD"

if eval "$BOOTSTRAP_CMD" > "$SMOKE_DIR/bootstrap-stdout.txt" 2>"$SMOKE_DIR/bootstrap-stderr.txt"; then
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

log "Starting standard flow run..."

START_CMD="$RALPH_BURNING run start"
evidence "start_command: $START_CMD"

if eval "$START_CMD" > "$SMOKE_DIR/run-stdout.txt" 2>"$SMOKE_DIR/run-stderr.txt"; then
    evidence "run_start: PASS"
    log "Run completed successfully"
else
    run_exit=$?
    fail "run start exited $run_exit"
    evidence "run_start: FAIL (exit $run_exit)"
    evidence "run_stderr: $(cat "$SMOKE_DIR/run-stderr.txt" 2>/dev/null || echo '(empty)')"
    # Run history remains canonical and inspectable
    $RALPH_BURNING run status --json > "$SMOKE_DIR/final-status.json" 2>/dev/null || true
    evidence "final_status: $(cat "$SMOKE_DIR/final-status.json" 2>/dev/null || echo '(unavailable)')"
    exit 1
fi

# ── Evidence Collection ───────────────────────────────────────────────────────

log "Collecting final evidence..."

$RALPH_BURNING run status --json > "$SMOKE_DIR/final-status.json" 2>/dev/null || true
$RALPH_BURNING run history --json > "$SMOKE_DIR/final-history.json" 2>/dev/null || true

FINAL_STATUS=$(cat "$SMOKE_DIR/final-status.json" 2>/dev/null || echo '{}')
evidence ""
evidence "--- Final Evidence ---"
evidence "final_status: $FINAL_STATUS"
evidence "smoke_result: PASS"
evidence "smoke_id: $SMOKE_ID"

log "Smoke PASSED for $BACKEND"
log "Evidence file: $EVIDENCE_FILE"

exit 0
