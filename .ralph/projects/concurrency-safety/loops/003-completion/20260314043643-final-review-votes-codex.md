---
artifact: final-review-votes
loop: 3
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T04:36:43Z
---

Reviewing the referenced cleanup paths and store APIs first so the votes are based on source, not just the planner summary.
The core blind-unlink and cleanup ordering issues are present. I’m checking the surrounding reconcile semantics and existing tests to confirm the proposed fixes fit the current invariants.
I’ve confirmed the current tests only cover missing-file cases, not owner mismatch or release-failure durability. I’m reading the mock-store hooks next to verify those regressions are testable without broad changes.
There’s one more call site in the acquisition rollback path. I’m checking whether the owner-aware change should cover that path too before I cast the vote.
The store change touches more than the two CLI cleanup sites. I’m checking the worktree lease paths now because blind `release_writer_lock` appears shared across the daemon lease lifecycle too.
The CLI-only ordering bug also appears suspiciously similar to the worktree lease release path. I’m checking whether the proposed amendment is fixing the whole class of bug or just one manifestation.
I have enough context for the votes. I’m pulling exact line numbers for the final rationale so the review stays concrete.
# Vote Results

## Amendment: CSR-20260314-01

### Vote
ACCEPT

### Rationale
The core bug is real. The lock owner token is written into the writer-lock file in [ralph-burning-rewrite/src/adapters/fs.rs:1228](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1228), but release still does a blind path unlink in [ralph-burning-rewrite/src/adapters/fs.rs:1245](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1245). The shared store API in [ralph-burning-rewrite/src/contexts/automation_runtime/mod.rs:78](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/mod.rs#L78) also has no way to pass the expected `lease_id`, so both the CLI guard drop in [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:165](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L165) and stale-CLI reconcile in [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:466](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L466) can delete a replacement lock they no longer own.

Owner-aware release is the right fix. The only caveat is scope: once the API is owner-aware, the same protection should be applied to the other `release_writer_lock` callers too, not just the CLI cleanup sites.

## Amendment: CSR-20260314-02

### Vote
ACCEPT

### Rationale
The planner’s ordering concern is correct. The CLI guard drop deletes the durable lease before releasing the writer lock in [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:162](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L162), and stale-CLI reconcile does the same in [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:446](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L446) before it even validates `project_id` in [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:454](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L454). If anything after `remove_lease` fails, the lock can be stranded with no reconcile-visible CLI lease.

Reordering to validate first, then release the lock, then delete the lease record preserves recoverability and is the correct direction. One follow-up detail to handle carefully is the case where lock release succeeds but lease deletion later fails; reconcile still needs a path to clear that leftover lease record on a later pass instead of leaving it permanent.
