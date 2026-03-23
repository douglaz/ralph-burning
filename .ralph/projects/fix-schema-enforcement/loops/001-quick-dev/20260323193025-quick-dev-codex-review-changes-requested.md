---
artifact: quick-dev-codex-review
loop: 1
project: fix-schema-enforcement
backend: codex
role: reviewer
created_at: 2026-03-23T19:30:25Z
---

# Review: CHANGES REQUESTED

- High: [src/adapters/tmux.rs:509](/root/new-ralph-burning/src/adapters/tmux.rs#L509) still drops failure artifacts on non-zero exits. The new preservation flow was wired into [src/adapters/process_backend.rs:877](/root/new-ralph-burning/src/adapters/process_backend.rs#L877) and [src/adapters/process_backend.rs:900](/root/new-ralph-burning/src/adapters/process_backend.rs#L900), and `PreparedCommand::finish()` now preserves artifacts on parse failures, but the tmux caller still goes straight to `prepared.cleanup()` at [src/adapters/tmux.rs:525](/root/new-ralph-burning/src/adapters/tmux.rs#L525). That means the same Claude/Codex failure loses `runtime/failed/*` whenever the tmux backend is used. Fix by mirroring the process backend path: call `prepared.preserve_failure_artifacts(&request, &output).await` before `prepared.cleanup().await` in the non-retry failure branch, and add a tmux regression test for a failing invocation.

The strict-mode schema change itself looks correct: [src/adapters/process_backend.rs:1023](/root/new-ralph-burning/src/adapters/process_backend.rs#L1023) normalizes nullable type arrays without null-wrapping single-type defaulted fields, [src/adapters/process_backend.rs:1145](/root/new-ralph-burning/src/adapters/process_backend.rs#L1145) now recurses through `anyOf`/`oneOf`/`allOf`, and the shared callers in Claude/Codex/OpenRouter still invoke the transformer. I also ran `cargo test enforce_strict_mode --lib` and `cargo test process_backend --tests` via `nix-shell`; both passed.
