# Bead t7j: Drop gitignored paths from rb checkpoint commits

## Problem description

`rb: checkpoint ...` commits stage and commit the top-level `result` symlink
(the nix build output). `result` is already in `.gitignore` (line 5), but
because it was accidentally added to the git index in the past, every nix
build flips the symlink target and that flip gets captured by the
checkpoint tree, leaking into PR diffs.

Concrete evidence from recent PRs (#168, #169): multiple
`rb: checkpoint project=... stage=...` commits changed `result` on the
feature branch. Reviewers see unrelated symlink changes in the diff.

## Required fix

This has two parts. Both must land in this PR.

### Part A — Stop leaking gitignored paths into checkpoint trees

The checkpoint tree is built in `WorktreeVcs::build_checkpoint_tree` (see
`src/adapters/worktree.rs:124-158`). After `read-tree` and `add -A -- .`,
any path that git considers ignored (either because it matches `.gitignore`
or because it is `assume-unchanged`) should be removed from the temporary
index before `write-tree`. This keeps checkpoint trees clean for all
tracked-but-gitignored paths, not just `result`.

Implementation sketch:
- After `git add -A -- .` (line 138), run
  `git ls-files -i -c --exclude-standard` (in the temporary index) to list
  tracked paths that are gitignored.
- If the list is non-empty, run
  `git update-index --force-remove -- <path> ...` in the same temporary
  index for each path so they drop out of the tree.
- Use the temp-index-scoped `git_in_index` helper — do not touch the real
  repo index.

Alternative acceptable implementations (pick the simplest that tests prove
correct):
- Use `git add -A -- .` but filter the list of staged paths through
  `git check-ignore --stdin --no-index` against `.gitignore`, then
  `update-index --force-remove` anything ignored.
- Pre-compute the ignore list with `git check-ignore --stdin --verbose`
  before staging and use `--pathspec-file` to exclude them up front.

Whichever path you pick, add a unit test in `tests/unit/worktree_test.rs`
(or wherever `worktree.rs` is tested) that:
- creates a temp repo with a file matching `.gitignore` that is already
  tracked (simulate via `git add -f` then commit),
- modifies the tracked-but-gitignored file,
- builds a checkpoint via the public VCS port API,
- asserts the checkpoint tree does NOT contain the modified blob for that
  path.

### Part B — Stop tracking `result`

The `result` symlink is still in the index today. Part A stops future leaks,
but the symlink must also be removed from the tree so master is clean:
- `git rm --cached -- result` and commit the removal on this branch.
- Do NOT also delete the physical symlink — just untrack it.
- After this PR merges, a fresh clone + `nix build` will create a new
  symlink that is correctly ignored.

If any other files are both tracked and gitignored (check with
`git ls-files -i -c --exclude-standard` on a clean master), untrack them
too.

## Implementation hints

Relevant code:
- `src/adapters/worktree.rs::build_checkpoint_tree` (lines 124-158).
- `src/adapters/worktree.rs::git_in_index` helper for running git with a
  custom index file.
- Existing pattern `TemporaryGitIndex` at the top of the same file.

Testing:
- `tests/unit/worktree_test.rs` exists — follow its fixture style.
- Prefer a real tempdir + real git, not mocks, so the behavior matches
  production.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files. The same applies to `.beads/` — that is durable bead state,
not code.

## Acceptance criteria

- `rb: checkpoint` commits no longer include tracked-but-gitignored paths.
  After this PR, a new ralph-burning run on a repo where `nix build` produces
  a `result` symlink does NOT introduce `result` changes in checkpoint commits.
- `result` is no longer tracked on `master` (post-merge).
- `git ls-files -i -c --exclude-standard` on a fresh clone of master prints
  nothing.
- Unit test in `worktree` tests covering the "tracked + gitignored" case.
- `nix build` passes on the final tree (authoritative gate).
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.
