# Bootstrap Workflow

Slice 2 adds two project-ingestion paths that remove the manual copy/paste step
between a completed requirements run and `project create`.

## `project create --from-requirements`

Use this when a requirements run already exists and you want to turn its seed
into a project without rerunning requirements:

```sh
ralph-burning requirements quick --idea "Build a release dashboard"
ralph-burning project create --from-requirements req-20260318-221500
```

Behavior:

- Reads the completed requirements run's versioned seed via
  `extract_seed_handoff()`
- Creates the project from seed-derived `project_id`, `project_name`, `flow`,
  and `prompt_body`
- Writes the project's canonical `prompt.md` from `prompt_body`
- Stores `prompt_reference = "prompt.md"` in `project.toml`
- Selects the created project as the active project

Validation failures are distinct:

- Missing run ID: the run does not exist under `.ralph-burning/requirements/`
- Incomplete run: the run exists but is not in `completed` status
- Unsupported seed version: the run completed but the seed version is not
  supported
- Duplicate project: the seed resolves to a `project_id` that already exists

## `project bootstrap`

Use bootstrap when you want the full convenience chain in one command:

```sh
ralph-burning project bootstrap --idea "Build a release dashboard"
ralph-burning project bootstrap --idea "Tighten CI linting" --flow quick_dev
ralph-burning project bootstrap --from-file ./requirements.txt --flow standard
```

Bootstrap performs:

1. `requirements quick`
2. Seed extraction
3. Project creation from the seed
4. Active-project selection
5. Optional `run start` when `--start` is supplied

Exactly one input source is required:

- `--idea "..."` uses inline text
- `--from-file <path>` reads the file contents and uses them as the idea input

## Flow Override

`--flow` is an explicit override. When present, it overrides the seed's `flow`
for the created project.

When `--flow` is omitted, the created project uses the seed's `flow`. The
seed's `recommended_flow` remains informational only and is logged in the
`ProjectCreated` journal event metadata.

## `--start`

`--start` is equivalent to running `ralph-burning run start` immediately after
bootstrap selects the project:

```sh
ralph-burning project bootstrap --idea "Ship release dashboard" --start
```

Success path:

- The project is created and selected as active
- A writer lease is acquired
- The workflow engine starts the run

Failure semantics:

- If requirements, seed extraction, seed validation, or atomic project creation
  fails, no project directory is left behind and the active project pointer is
  unchanged
- If project creation succeeds but `--start` fails before the run begins, the
  project remains valid, selected as active, and its `run.json` remains
  `not_started`
- In that case the CLI reports:

```text
Project '<id>' created successfully but run failed to start: <reason>. Use `ralph-burning run start` to retry.
```

## Quick Examples

Standard bootstrap:

```sh
ralph-burning project bootstrap --idea "Add release notes generation"
```

Quick-dev bootstrap:

```sh
ralph-burning project bootstrap --idea "Fix flaky CI timeout" --flow quick_dev
```

Create later from an existing requirements run:

```sh
ralph-burning requirements quick --idea "Add release notes generation"
ralph-burning project create --from-requirements req-20260318-221500
```
