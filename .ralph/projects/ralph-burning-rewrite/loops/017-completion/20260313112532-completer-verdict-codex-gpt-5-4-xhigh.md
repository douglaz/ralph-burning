---
artifact: completer-verdict
loop: 17
project: ralph-burning-rewrite
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-13T11:25:32Z
---

Reviewing `prompt.md`, the Rust rewrite under `ralph-burning-rewrite/`, and the current test/conformance state from an independent validation angle. I’ll check the contract surface first, then verify the implementation and coverage against it.
I’m mapping the repo and locating the source-of-truth files now so the validation stays tied to the actual implementation rather than the planner summary.
The repo has the rewrite and its test corpus. I haven’t seen `prompt.md` in the first pass, so I’m locating the exact requirements file and then checking the implemented contract surface against it.
`prompt.md` does not appear to exist as a file in the checkout. I’m treating the “Master Prompt” embedded in your message as the operative requirements unless I find a local copy with different content.
I found the project-local prompt copy under the orchestrator state directory. Next I’m comparing that source of truth with the actual CLI and context implementation, not just the completion note.
I’ve confirmed the binary shape and top-level command groups. Now I’m drilling into whether each promised subcommand and state behavior is actually wired, especially the edge cases that tend to get overclaimed in completion notes.
The test corpus is broad, but I’m not relying on scenario titles. I’m now checking the storage and engine code directly for invariants that are easy to miss even when the happy-path suite passes.
I’ve confirmed the workflow/requirements contracts are real and not markdown-driven. Next I’m auditing the daemon/runtime slice, because completion claims often gloss over routing precedence, task uniqueness, and resource cleanup invariants.
The daemon slice is mostly there. I’m checking two things now: whether the compiled CLI actually exposes the promised surface, and whether any prompt requirements are only claimed in tests rather than enforced in code.
I can’t invoke `cargo` in this environment, but the compiled `target/release/ralph-burning` is present. I’m using that binary to run the project’s own conformance gate and spot-check real CLI behavior rather than just reading the source.
The compiled conformance gate does not pass. It fails immediately with a registry drift error: `DAEMON-INTAKE-010` is defined in Gherkin but has no registered executor. That alone blocks a completion verdict, and I’m confirming whether the source has the same gap or just the built artifact.
The source tree and the compiled binary are out of sync: `scenarios.rs` does register `DAEMON-INTAKE-010`, but the checked-in release binary does not. I’m verifying whether the current source can actually be built and whether that stale artifact is masking any additional gaps.
I found a usable Rust toolchain through the repo’s Nix flake. I’m rebuilding from source now so the verdict is based on the current checkout, not the stale release artifact.
The source does build enough to start, but the conformance runner fails under `cargo run` because its subprocess re-invocation path is wrong in that mode. I’m separating “build/tests” from “conformance execution environment” now so I can tell whether this is a real product gap or just a runner invocation issue.
The current source test suite does pass: `383 passed; 0 failed`. I still need a clean conformance run from the freshly built binary, because `cargo run` was a bad harness for a self-spawning CLI.
The fresh build clears the main risk: current source passes both the 383-test suite and all 212 conformance scenarios. I’m doing a final pass over prompt-specific invariants that aren’t captured by simple command counts, then I’ll issue the verdict.
# Verdict: COMPLETE

The project satisfies all requirements:
- Product definition: implemented as a Rust modular monolith with one `ralph-burning` binary, `.ralph-burning/` workspace root, fixed built-in presets `standard`, `quick_dev`, `docs_change`, and `ci_improvement`, and no user-defined flow DSL.
- Bounded contexts and adapters: the required packages exist under `src/contexts/{workspace_governance,project_run_record,workflow_composition,agent_execution,requirements_drafting,automation_runtime,conformance_spec}` plus `src/adapters/*`, with the expected dependency direction.
- Canonical state and storage layout: project metadata, `run.json`, `journal.ndjson`, `sessions.json`, payload/artifact history, runtime logs/backend/temp, amendments, rollback points, requirements runs, and daemon task/lease storage are all implemented under `.ralph-burning/`.
- Core invariants: immutable per-project flow, single active run/stage cursor, stage membership validation, monotonic cycle/completion-round handling, pre-mutation payload validation, durable-boundary resume rules, writer-lock and lease enforcement, routing precedence, hard-rollback ordering, and workspace-version fail-fast behavior are enforced in code and covered by tests.
- CLI contract: all required command groups and subcommands are present and wired, including config, flow discovery, projects, run lifecycle, requirements, daemon, and conformance commands, with invalid state/policy failures returning non-zero.
- Flow presets and stage contracts: all four preset stage sequences are defined in code; every stage resolves to a `StageContract` with schema-first validation, domain validation, deterministic Markdown rendering, atomic payload+artifact persistence, raw backend output capture, and explicit failure-class handling.
- Project/run record rules: `journal.ndjson` is treated as authoritative, run state is read from canonical run/journal data rather than artifact scans, and history/tail split durable history from ephemeral runtime logs correctly.
- Agent execution and automation routing: backend/model resolution, capability/availability preflight, timeout/cancellation, session-reuse gating, normalized invocation envelopes, daemon task routing precedence, task uniqueness, worktree leases, watcher ingestion, and requirements handoff paths are implemented.
- Testability and conformance: the public behavior is defined in Gherkin under `tests/conformance/features/`; scenario filtering and fail-fast are implemented; current source passes `383` tests via `nix develop -c cargo test --quiet` and `212/212` conformance scenarios via a rebuilt binary.

Validated against a fresh rebuild of the current source. The checked `target/release` artifact was stale relative to source, so the verdict is based on the rebuilt checkout rather than that prebuilt binary.

---
