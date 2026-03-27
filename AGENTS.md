# AGENTS.md

<!-- bv-agent-instructions-v2 -->

Repository-specific guidance for agents lives in this section. Keep workflow and policy content here so the same instructions are visible to both humans and `br`/`bv`.

## Project Working Norms

- Follow the current bead descriptions and comments; they contain the roadmap, assumptions, testing expectations, and deferred-scope notes.
- Treat `.beads/issues.jsonl` as the authoritative backlog export. Keep active task knowledge in the beads; use `docs/` only for reference material such as prior scientific review or spec notes.
- Do not recreate parallel backlog scripts or mirror plan docs unless explicitly requested.
- When revising or adding beads, preserve feature scope while making verification concrete: specify deterministic unit tests for local logic, integration tests for cross-system changes, and e2e/scenario scripts for long-horizon or player-visible behavior.
- Run `br lint` after backlog edits.
- Keep criteria content only in the structured field.
- For tasks and spikes, keep the required `## Acceptance Criteria` heading in the description only as an empty template marker for `br lint`; do not repeat bullets or pointer text there.
- Treat structured tracing/logging as part of the deliverable for scientific-core work. Reuse shared regression/e2e harness conventions rather than inventing one-off scenario runners per bead.
- Keep tests deterministic and add right-sized coverage for any code change: unit tests for local logic, integration tests for cross-system behavior, and scenario/e2e coverage when long-horizon behavior changes.
- Prefer explicit, inspectable behavior over hidden tuning. If diagnostics or tracing are needed, wire them in cleanly rather than relying on ad hoc prints.
- If a change affects persisted state or action semantics, update save/migration handling and the relevant bead context.

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`) for issue tracking and [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) (`bv`) for graph-aware triage. Active planning state lives in the beads; `bv` reads from `.beads/` as a read-only sidecar.

**Note:** `br` is non-invasive and never executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/ && git commit`.

### Workflow

1. Run `bv --robot-triage` to find the highest-impact actionable work, or `br ready` for a minimal ready list.
2. Claim work with `br update <id> --status=in_progress`.
3. Read full task context with `br show <id>`.
4. Use `br graph` or `br dep tree <id>` when dependency shape matters.
5. Implement the task and update beads as needed.
6. Close completed work with `br close <id>`.
7. Run `br sync --flush-only`, then stage and commit `.beads/`.

### Essential Commands

```bash
# Interactive viewer (manual inspection only)
bv

# Agent-friendly triage
bv --robot-triage
bv --robot-next
bv --robot-triage --format toon

# Issue inspection and management
br ready
br show <id>
br list --pretty
br list --status=open
br graph
br dep tree <id>
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>
br lint
br sync --flush-only

# After sync
git add .beads/
git commit -m "sync beads"
```

### bv Guidance

Use `bv --robot-*` flags in agent or CI workflows instead of the TUI. `bv` handles triage, prioritization, and dependency-aware planning; `br` handles creating, modifying, and closing beads.

Useful robot commands:

- `bv --robot-triage`: at-a-glance counts, ranked recommendations, quick wins, blockers, and copy-paste next commands.
- `bv --robot-plan`: parallel execution tracks with unblock lists.
- `bv --robot-priority`: priority misalignment detection with confidence.
- `bv --robot-insights`: graph metrics including PageRank, betweenness, HITS, eigenvector, critical path, cycles, and k-core.
- `bv --robot-alerts`: stale issues, blocking cascades, and priority mismatches.
- `bv --robot-suggest`: duplicates, missing dependencies, label suggestions, and cycle-break ideas.
- `bv --robot-diff --diff-since <ref>`: changes since a git ref.
- `bv --robot-graph [--graph-format=json|dot|mermaid]`: dependency graph export.

Useful scoping patterns:

```bash
bv --robot-plan --label backend
bv --robot-insights --as-of HEAD~30
bv --recipe actionable --robot-plan
bv --recipe high-impact --robot-triage
```

### Key Concepts

- Dependencies block downstream work; `br ready` shows only unblocked issues.
- Priority uses numeric levels: P0 critical, P1 high, P2 medium, P3 low, P4 backlog.
- Supported issue types include `task`, `bug`, `feature`, `epic`, `chore`, `docs`, and `question`.
- Add dependencies with `br dep add <issue> <depends-on>`.

### Session Protocol

```bash
git status
git add <files>
br sync --flush-only
git add .beads/
git commit -m "..."
git push
```

<!-- end-bv-agent-instructions -->
