---
artifact: completer-verdict
loop: 15
project: ralph-burning-rewrite
backend: codex(gpt-5.4-xhigh)
role: completer
created_at: 2026-03-13T03:11:09Z
---

Reviewing the repo against `prompt.md` from a validation perspective, not the planner’s implementation narrative. I’m starting with the requirements file and the current project surface, then I’ll spot-check CLI behavior, bounded contexts, storage layout, and test coverage against the prompt.
I have the repo shape now. Next I’m locating the actual `prompt.md` and key implementation entrypoints so I can compare the code and tests directly to the requirements, not to the planner’s summary.
I found the source-of-truth prompt under the project record rather than the crate root. I’m reading that plus the crate manifest and entrypoints next, because the main failure mode here is “tests pass but the shipped surface doesn’t actually match the v1 contract.”
I’ve confirmed the formal prompt. The next pass is adversarial: I’m looking for legacy path leakage, missing CLI surface, and places where tests might be asserting the prompt text rather than the actual runtime behavior.
The environment doesn’t have `cargo` on `PATH`, so I can’t rely on a fresh local test run yet. I’m compensating by reading the compiled crate surface, existing test files, and the implementation itself for contract mismatches.
I can likely recover a real test run via the repo’s Nix shell even though `cargo` isn’t on the base `PATH`. I’m checking the flake now, because a green local run is useful but not sufficient; I still need to verify the requirements the tests might not cover.
I’ve moved from surface checks into the domain code. The likely incompleteness points now are slice-11 infrastructure behaviors, session semantics, and whether the tests actually exercise the promised public commands instead of only internal helpers.
I’ve found some likely gaps in the daemon/integration story, but I want one real build/test signal before I conclude. I’m using the flake dev shell to run the crate’s tests since `cargo` isn’t available in the base environment.
The unit/CLI suite is green inside the dev shell: `94` CLI tests and `352` unit/integration tests passed. I’m validating the conformance surface directly now, because the master prompt makes Gherkin scenarios part of the public contract.
The conformance runner is not green. `cargo run -- conformance run` failed on scenario `SC-PROJ-009`, so by the prompt’s own acceptance bar the project is not complete yet. I’m locating the broken scenario and the mismatch causing it.
I found a second substantive gap beyond the broken scenario: `automation_runtime` never dispatches into `requirements_drafting`, only into the workflow engine. That conflicts directly with the prompt’s context dependency contract and slice 11’s “interactive requirements path.”
# Verdict: CONTINUE

## Missing Requirements
1. `Conformance gate in CI / passing public-behavior suite`: not satisfied. Running `nix develop -c cargo run --quiet -- conformance run` exits non-zero. It fails on `SC-PROJ-009` because [scenarios.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/conformance_spec/scenarios.rs#L667) writes a stale `run.json` fixture without `active_run.stage_cursor`, while the canonical schema in [model.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/project_run_record/model.rs#L90) requires it. Section 15 and roadmap slice 12 are not complete while `conformance run` fails.
2. `automation_runtime dispatches into workflow/requirements`: not satisfied. The prompt requires `automation_runtime` to dispatch into both workflow and requirements, and slice 11 calls for the interactive requirements path. In [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L327), dispatch only calls workflow execution/resume; there is no requirements-drafting path from daemon task processing.
3. `Slice 11 watchers`: not satisfied. The daemon loop only scans already-persisted task files via [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L135); there is no watcher/ingestion adapter that creates tasks from an external issue source. That leaves the roadmap slice partially implemented.

## Recommended Next Features
1. Fix `SC-PROJ-009` to use the current canonical run snapshot shape, then rerun `cargo run -- conformance run` until the full suite passes in CI.
2. Add a daemon requirements-dispatch path that invokes `requirements_drafting` before workflow execution when a task needs interactive requirements handling.
3. Implement the missing watcher/issue-ingestion adapter and add Gherkin scenarios covering watcher creation, routing, and requirements handoff.
