# Bead itn1 — ralph pr watch (drain loop)

## Problem

After `ralph pr open` (bead 2qlo) creates a PR, the operator manually:
1. Polls `gh pr view <n>` every few minutes for CI completion.
2. Polls the codex bot's reactions on the PR body for `+1` (approval) or
   line-comments (findings).
3. On CI green + bot `+1` → squash-merge with `--delete-branch`.
4. On CI failure that matches a known-flake list → `gh run rerun --failed`
   once.
5. On bot line-comments → stop, surface the findings, let the human decide.

This is the part of the drain loop that watches a PR through to merge. It
costs ~10–30 minutes of low-attention polling per bead. This bead automates
it.

`ralph pr watch` is the success-path completion of a drain cycle. It does
NOT decide what to do with bot rejections (just stop and report) — that's
deferred to bead `gj74`'s failure-mode policies. It does NOT loop over
beads — that's `vl8z`'s drain orchestration.

## Required behavior

Add a CLI subcommand. Natural shape: `ralph pr watch <pr-number>
[--max-wait <duration>] [--poll-interval <seconds>]`. Defaults: 30s poll
interval, 60min max wait.

Each poll cycle:

1. **Check CI status** via `gh pr view <n> --json statusCheckRollup,mergeable`.
2. **Check the codex bot's reaction on the PR body** via
   `gh api repos/<owner>/<repo>/issues/<pr>/reactions` and filter for
   `chatgpt-codex-connector[bot]`. The skill notes that the bot's
   reactions on the PR body are the authoritative approval signal in
   this repo: `eyes` = reading, `+1` = approved, `-1` = rejected.
3. **Check for new line-comments** since the most recent push via
   `gh api repos/<owner>/<repo>/pulls/<pr>/comments`. Line-comments
   from the bot are findings that block automatic merge.

State machine:

- **CI in progress + bot eyes/none** → poll again.
- **CI green + bot +1, no new line-comments** → `gh pr merge --squash
  --delete-branch`. Return `Merged(commit_sha)`.
- **CI failure + test names match known-flake list** → `gh run rerun
  --failed` once per PR (track per-invocation rerun count). Then poll
  again. If second failure → return `BlockedByCi(reason)`.
- **CI failure + non-flake** → return `BlockedByCi(reason)` with the
  failing test/job names included.
- **Bot left line-comments since the latest push** → return
  `BlockedByBot(reasons)` listing the comments. Do NOT auto-amend
  (that's the operator's call).
- **Bot reacted -1 (thumbs down)** → return `BlockedByBot("rejected")`.
- **Timeout reached** → return `Timeout(elapsed_minutes)`.

Output: a single structured `WatchOutcome` enum the caller can match on:

```rust
pub enum WatchOutcome {
    Merged { sha: String, pr_url: String },
    BlockedByCi { failing: Vec<String>, after_rerun: bool },
    BlockedByBot { reasons: Vec<String> },
    Timeout { elapsed: Duration },
    Aborted { reason: String },
}
```

## Known-flake list

This repo currently has one documented flake:
`adapters::br_process::tests::check_available_times_out_when_version_probe_hangs`
(a 50ms-timeout race tracked as bead `zh55`). The known-flake list should
be a small static slice in this module (or sourced from a config file
later); just hardcode `zh55`-class failures for now.

## Reuse vs duplicate

- The `gh` and `git` shell-outs go through a port — possibly the same
  `PrToolPort` 2qlo introduced. Extending it with poll/merge/rerun
  methods is natural. Tests use a mocked port; no real `gh` invocations.
- Use the same orchestration-state-exclusion constant 4wdb owns; it's
  not relevant here (no PR body rendering) but document the import if
  helpful.

## Tests

- Happy path: CI completed success + bot +1 → merge called.
- CI in progress → state advances on poll.
- CI fail with known-flake test name → rerun, then merge if rerun
  passes.
- CI fail twice → BlockedByCi.
- CI fail with non-flake test → BlockedByCi without rerun attempt.
- Bot line-comment after push → BlockedByBot with the comment text
  in `reasons`.
- Bot -1 reaction → BlockedByBot("rejected").
- Timeout → Timeout outcome.

Tests should not actually sleep between polls — inject a
deterministic clock or a hook that fast-forwards time so the
polling interval doesn't slow them down.

## Where to look

- `src/cli/pr.rs`, `src/contexts/bead_workflow/pr_open.rs` (2qlo) — the
  existing `ralph pr open` command and its `PrToolPort`.
- `src/cli/bead.rs`, `src/contexts/bead_workflow/create_project.rs`
  (d31l) — the bead-create-project command for CLI shape.
- `src/contexts/bead_workflow/project_prompt.rs` (4wdb) — the
  orchestration-state-exclusion constant if needed.
- The skill `~/.claude/skills/pr-with-codex-bot-review/SKILL.md` (in
  the operator's environment, not this repo) documents the bot's
  reaction-driven approval semantics. Replicate the relevant bits in
  inline comments so the code is self-documenting.

## Out of scope

- Auto-amending on bot findings. The bot's findings are surfaced in
  the `BlockedByBot` outcome; the operator (or the drain loop's
  failure-mode policy in bead `gj74`) decides whether to amend.
- Loop orchestration over multiple PRs / beads. That's `vl8z`.
- Filing follow-up beads for permanent CI failures. That's
  `gj74`'s policy decision.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy --locked -- -D warnings`,
  `cargo fmt --check` all pass.
- Each state-machine outcome listed in "Required behavior" has at
  least one test.
- `gh` invocations go through a mockable port; tests do not shell
  out.
- The known-flake list is centralized (a single slice, not
  scattered match arms).
