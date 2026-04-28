# ralph-burning

AI-assisted code execution with durable runs, review loops, and bead-aware recovery.

ralph-burning is a multi-stage code-execution orchestrator. It turns a problem prompt into a durable run: plan the work, ask agents to implement and review it, record each stage and payload on disk, and resume from saved state after interruption instead of starting over.

## Why it exists

Ad hoc LLM CLI sessions are easy to start and hard to operate: state is scattered, review loops are manual, and crash recovery depends on terminal history. ralph-burning adds durable project/run records, rollback points, multi-reviewer panels with arbiter behavior, and `.beads` integration as the canonical work graph. The most-used flow is `iterative_minimal`, which cycles `plan_and_implement` until the implementation is stable, then runs `final_review`.

## Quick start

Run the released CLI directly with Nix, or build the local checkout:

```bash
nix run github:douglaz/ralph-burning -- --help
nix develop -c cargo build --locked
```

After a local build, replace `ralph-burning` below with `target/debug/ralph-burning` unless the binary is already on `PATH`.
For a deterministic local smoke without live model credentials, build with `--features test-stub`, set `RALPH_BURNING_BACKEND=stub`, and pass `--backend stub` to `backend check`.

Initialize a workspace and check backend readiness:

```bash
ralph-burning init
ralph-burning backend check
```

Create a project, start it, watch it, and resume if a backend, process, or host failure interrupts the run:

```bash
ralph-burning project bootstrap --idea "Fix the failing parser tests" --flow iterative_minimal
ralph-burning run start
ralph-burning run status
ralph-burning run tail --follow
ralph-burning run resume
```

`backend check` is a readiness gate, not a credential bootstrapper; configure the required Claude, Codex, or OpenRouter tools/API keys before expecting real backend runs to pass.

## Concept cheat sheet

- Project: a named unit of work with prompt, flow, config, journal, payloads, artifacts, and run snapshot in the live workspace printed by `ralph-burning init`, mirrored for audit under `.ralph-burning/projects/<id>/`.
- Run: one execution attempt for the active project, started with `run start` and continued with `run resume`.
- Cycle: one pass through the flow's implementation/review loop before either completion or another remediation pass.
- Completion round: a late-stage stabilization pass used by completion/final-review logic, bounded by `workflow.max_completion_rounds`.
- Stage: a typed step in a flow, such as `plan_and_implement`, `review`, or `final_review`; stage payload families are defined in [contracts.rs](src/contexts/workflow_composition/contracts.rs#L26-L35).
- Bead: a `br` issue in `.beads` that represents canonical backlog work; see [AGENTS.md](AGENTS.md) and [beads_rust](https://github.com/Dicklesworthstone/beads_rust).
- Milestone: a planned bundle of related beads with durable state under `.ralph-burning/milestones/`.
- Flow preset: a built-in stage sequence selected by `--flow`; see [mod.rs](src/contexts/workflow_composition/mod.rs#L89-L143).
- Panel: a group of backend reviewers/completers plus minimum quorum and arbiter settings.
- Amendment: operator-supplied change request queued onto an existing project with `project amend`.
- Classification: review/final-review routing for findings, such as `fix_current_bead`, `covered_by_existing_bead`, `propose_new_bead`, or `informational_only`.
- Rollback point: a saved run checkpoint listed or restored with `run rollback`.

Deeper operator notes live in [AGENTS.md](AGENTS.md), [docs/cli-reference.md](docs/cli-reference.md), [docs/bootstrap.md](docs/bootstrap.md), and [docs/amendments.md](docs/amendments.md).

## Flows

| Preset | Stages | When to pick |
| --- | --- | --- |
| `minimal` | `plan_and_implement` -> `final_review` | Small tasks where one implementer pass and final validation are enough. |
| `iterative_minimal` | `plan_and_implement` -> `final_review` | Default choice for implementation work that may need repeated implementer stabilization before final review. |
| `quick_dev` | `plan_and_implement` -> `review` -> `apply_fixes` -> `final_review` | Small code changes that need an explicit review/fix pass. |
| `standard` | `prompt_review` -> `planning` -> `implementation` -> `qa` -> `review` -> `completion_panel` -> `acceptance_qa` -> `final_review` | Larger changes that need separated planning, QA, review, and acceptance. |
| `docs_change` | `plan_and_implement` -> `final_review` | Documentation-recognizable alias of `minimal`; kept for CLI UX. |
| `ci_improvement` | `ci_plan` -> `ci_update` -> `ci_validation` -> `review` | CI or automation updates where validation should focus on pipeline behavior. |

List and inspect flows with `ralph-burning flow list` and `ralph-burning flow show <preset>`.

## Backend support

The supported backend families are Codex, Claude, OpenRouter, and `stub`. The stub backend is for tests and local deterministic scenarios; production runs should use real backends. Backend selection is configurable globally, by role, by panel, and through CLI overrides such as `--backend`, `--implementer-backend`, and `--reviewer-backend`. See [backend.rs](src/cli/backend.rs) and [docs/cli-reference.md](docs/cli-reference.md#backend-commands).

`BackendExhausted` is treated as a non-retryable quota/credit exhaustion class. Review and completion panels can degrade gracefully by skipping exhausted optional members and recomputing effective quorum; if all required capacity is exhausted, the run fails clearly for operator action.

## Bead-driven workflow

ralph-burning can create a task project from an existing bead with `project create-from-bead` or the newer `task create` alias. Milestone plans materialize bundles of beads, carry bead lineage into task prompts, and let terminal reconciliation close, sync, or route work through `br`.

Final review classifications drive bead-aware routing: `fix_current_bead` restarts current work, `covered_by_existing_bead` records that another bead owns the concern, and `propose_new_bead` can create a new bead automatically when policy thresholds are met. The routing vocabulary is enforced in [review_classification.rs](src/contexts/workflow_composition/review_classification.rs#L452-L459). The implementation lineage is documented in the 9ni series, especially [9ni.7.2](prompt-9ni-7-2.md), [9ni.8.5.2](prompt-9ni852.md), [9ni.8.6](prompt-9ni-8-6.md), and related 9ni.4.x bead/milestone work in the closed backlog.

## Configuration

Workspace config is resolved from the live workspace path printed by `ralph-burning init`, typically `.git/ralph-burning-live/workspace.toml` in a Git checkout. `.ralph-burning/workspace.toml` is the audit mirror and fallback for older workspaces, so do not treat it as the normal manual edit target. Project-specific overrides live in the active project's live workspace `config.toml` and are mirrored under `.ralph-burning/projects/<id>/config.toml`. Use `ralph-burning config show`, `ralph-burning config get <key>`, `ralph-burning config set <key> <value>`, or `ralph-burning config edit`; add `--project` for active-project overrides.

Common workflow knobs include `workflow.max_completion_rounds`, the stable-rounds setting for the `iterative_minimal` loop, and `workflow.parsimonious_bead_creation`. The shipped stable-rounds key is `workflow.iterative_minimal.stable_rounds_required`:

```toml
[workflow]
max_completion_rounds = 25

[workflow.iterative_minimal]
max_consecutive_implementer_rounds = 10
stable_rounds_required = 2

[workflow.parsimonious_bead_creation]
enabled = true
existing_bead_match_threshold_score = 0.65
proposal_threshold = 2
```

See [config.rs](src/contexts/workspace_governance/config.rs#L30-L38) for defaults and [accepted keys](src/contexts/workspace_governance/config.rs#L1830-L1846).

## Development

Use the same gates that CI and reviewers expect:

```bash
nix develop -c cargo test --features test-stub --locked
nix develop -c cargo clippy --locked -- -D warnings
nix develop -c cargo fmt --check
nix build
```

`nix build` is the authoritative verification gate for this repo because the Nix sandbox differs from the local developer environment.

## License and contributing

No license file is currently checked in, so treat redistribution rights as unspecified until maintainers add one. For contribution workflow, read [AGENTS.md](AGENTS.md) and [CLAUDE.md](CLAUDE.md). Backlog and graph workflows use [beads_rust](https://github.com/Dicklesworthstone/beads_rust) and `.beads/issues.jsonl` as described in AGENTS.
