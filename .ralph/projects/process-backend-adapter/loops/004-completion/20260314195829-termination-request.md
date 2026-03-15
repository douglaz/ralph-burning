---
artifact: termination-request
loop: 4
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-14T19:58:29Z
---

I’m checking `prompt.md`, the completed loop specs, and recent git checkpoints to see which requirements are already covered and whether anything from the master prompt is still unplanned.
The branch history aligns with the three completed loops. I’m now reading the master prompt and the implemented areas directly to verify whether anything from the original scope is still uncovered.
The planning artifacts aren’t at repo root. I’m locating the actual `prompt.md` and loop directories before I decide whether there’s any remaining feature to spec.
The repo stores the planning artifacts under its internal project state, not at top level. I’m reading the active project’s `prompt.md` and loop specs from there now.
The loop artifacts indicate the planned scope is already exhausted. I’m doing one direct pass over the shipped code to confirm the master prompt’s requirements are actually present in `HEAD`, not just recorded in the loop notes.
I’m checking one remaining detail against the master prompt: whether the stub-test harness behavior was implemented in the exact helper seams the prompt called out, or only patched around at call sites.
The remaining question is whether the current implementation merely approximates the prompt or actually covers the required failure messaging and invariants. I’m spot-checking those exact paths now.
I have enough to decide the planning outcome. I’m doing a final repo-state check so the completion request is based on the current branch state, not just historical artifacts.
# Project Completion Request

## Rationale
The in-scope requirements from `prompt.md` are already satisfied by the completed loop sequence and the current branch state. Loop 1 delivered the foundation work: `working_dir`, Tokio `process`, adapter dispatch, fallible `RALPH_BURNING_BACKEND` selection, stub-preserving harness updates, and the 3600-second workflow timeout. Loop 2 implemented real Claude/Codex stage subprocess execution in `ProcessBackendAdapter`, including prompt/context stdin assembly, session reuse, temp-file handling, capability and availability checks, transport/schema error mapping, and SIGTERM cancellation. Loop 3 replaced the placeholder workflow prompt with the journal-backed stage prompt builder and added prompt-builder coverage. The git checkpoint history also lines up with that completion path through `af9c24d`, `427bf43`, and `ee25df7`, with no additional in-scope feature left after loop 3.

## Summary of Work
Workflow `run start`, `run resume`, and daemon-dispatched workflow runs can now use real local `claude` and `codex` CLIs when `RALPH_BURNING_BACKEND=process`, while unsupported `openrouter` and `stub` targets fail preflight with the required configuration guidance. The workflow engine now builds enriched stage prompts from the original project prompt, prior same-cycle outputs, remediation and amendments, and the authoritative JSON schema. Stub-based CLI and conformance tests remain isolated behind default stub harness behavior, and the branch contains the expected adapter, CLI, workflow, and unit-test changes with a clean worktree.

## Remaining Items
None

---
