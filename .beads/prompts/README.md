# Operator scratchpad for ralph-burning project prompts

This directory holds `prompt-<bead-id>.md` files used as input to
`ralph-burning project create --prompt <path>` during the manual
ralph-burning loop. The corresponding beads have already closed and
shipped — these files are point-in-time snapshots, not authoritative.

## Why here, not at the repo root?

Originally these lived at the repo root and were tracked by git. Bead
`x11` moved them here because:

- They aren't authoritative — the bead body and the merged PR are.
- They cluttered the repo root with 32 historical files.
- A ralph-burning workflow naturally roots prompt inputs near other
  bead state (`.beads/`).

## Why not delete them outright?

Deleting them would force operators back to writing prompt files when
they want to repro a past run. The current workflow is:

1. Operator writes a prompt for a new bead under `.beads/prompts/`
2. Runs `ralph-burning project create --prompt .beads/prompts/prompt-<id>.md ...`
3. The drain loop picks the project up

## What replaces this directory long-term?

Bead `9ni.4.1` will close the loop so ralph-burning consumes bead
bodies directly via the br adapter — no `--prompt path/file.md` needed.
Once that lands, this directory can be deleted entirely (the bead body
becomes the prompt). Until then this stays as an operator scratchpad.

## Why is the directory gitignored?

The files here are local work-in-progress, not durable project
history. Tracking them again would re-introduce the clutter `x11`
removed.
