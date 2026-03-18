# Requirements Drafting

The `requirements` command family drives idea-to-seed workflows. Two modes are available: **full** (staged, multi-pass) and **quick** (single-pass with revision loop).

## Full Mode (`requirements draft`)

Full mode runs a seven-stage pipeline:

1. **Ideation** — themes, key concepts, initial scope
2. **Research** — findings, constraints, prior art, technical context
3. **Synthesis** — problem summary, goals, non-goals, constraints, acceptance criteria
4. **Implementation Spec** — architecture overview, components, integration points
5. **Gap Analysis** — coverage assessment, blocking/non-blocking gaps
6. **Validation** — pass/fail/needs-questions outcome with evidence
7. **Project Seed** — versioned seed (`project.json` + `prompt.md`) for project creation

Each stage produces a validated JSON payload and a deterministic Markdown artifact. Stages are committed atomically — if a stage fails, the run rolls back to the last committed stage.

### Question Rounds

The validation stage may return a `needs_questions` outcome, indicating missing information. When this happens:

- Synthesis and all downstream stages are invalidated (removed from `committed_stages`)
- Ideation and research committed stages are preserved
- A question round is opened with the missing information items
- The run transitions to `awaiting_answers` status
- After the user answers via `requirements answer`, the pipeline resumes from synthesis

### Cache Reuse

Each stage computes a deterministic cache key from its input (stage name + upstream outputs). When resuming a run, stages with a matching cache key in `committed_stages` are reused without re-invocation. The `last_transition_cached` flag in `run.json` indicates whether the most recent transition was a cache hit.

## Quick Mode (`requirements quick`)

Quick mode skips the staged pipeline and runs a writer/reviewer revision loop:

1. An initial draft is generated
2. A reviewer evaluates the draft
3. If the reviewer returns `request_changes`, the draft is revised with the feedback
4. Steps 2–3 repeat until the reviewer approves or `MAX_QUICK_REVISIONS` (5) is reached
5. On approval, a project seed is generated

The `quick_revision_count` field in `run.json` tracks how many revisions were performed.

## Versioned Project Seed

The project seed (`seed/project.json`) carries a `version` field:

- **v2** (current): includes `source` metadata with `mode`, `run_id`, `question_rounds`, and `quick_revisions` (quick mode only)
- **v1**: supported for backward compatibility (no source metadata)
- Unsupported versions are rejected at contract validation

## CLI Commands

| Command | Description |
|---|---|
| `requirements draft --idea "..."` | Start a full-mode requirements pipeline |
| `requirements quick --idea "..."` | Start a quick-mode requirements pipeline |
| `requirements show <run-id>` | Display run state, stage progress, pending questions |
| `requirements answer <run-id>` | Submit answers to pending questions |

### Show Output

For full-mode runs, `show` displays:
- Current Stage (if in progress)
- Completed Stages (comma-separated list of committed stage names)
- Quick Revisions (for quick-mode runs)
- Last Transition status (cached/reused indicator)
- Pending Questions count (if applicable)
- Suggested `project create` command (if seed is available)

## Run State

Run state is persisted in `.ralph-burning/requirements/<run-id>/run.json`. Key fields:

- `status`: drafting, awaiting_answers, completed, failed
- `mode`: draft (full) or quick
- `current_stage`: the stage currently being executed (full mode only)
- `committed_stages`: map of stage name → `CommittedStageEntry` with payload ID, artifact ID, and cache key
- `question_round`: number of question rounds completed
- `quick_revision_count`: number of revisions in quick mode
- `last_transition_cached`: whether the last stage transition was a cache reuse
