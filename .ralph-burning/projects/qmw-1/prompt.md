# Set final review panel defaults: 1 required reviewer, optional backends, Codex 5.4 arbiter

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Goal

Change the default final review panel configuration:

```
final_review.backends = ["codex/gpt-5.4-xhigh", "?claude-opus-4-6", "?codex/gpt-5.3-codex-spark-xhigh"]
final_review.min_reviewers = 1
final_review.arbiter_backend = codex/gpt-5.4-xhigh
```

The `?` prefix means optional — if the backend is unavailable (exhausted, auth failure, etc.), the panel proceeds without it. Only the first backend (codex/gpt-5.4-xhigh) is required.

## Implementation

1. Search for how the final review panel resolves its backends — look for `final_review_reviewers`, `resolve_final_review_panel`, or similar in the engine and config code
2. Add support for the `?` prefix on backend specs to mark them as optional
3. Add `min_reviewers` config setting — minimum number of reviewers required for the panel to proceed
4. Set the new defaults in the config/backend resolution code
5. Make `codex/gpt-5.4-xhigh` the default arbiter backend
6. Ensure explicit config overrides still work

## Acceptance Criteria
- Default panel uses codex/gpt-5.4-xhigh (required), claude and spark (optional)
- Optional backends degrade gracefully when unavailable
- min_reviewers=1 allows panel to proceed with single reviewer
- Arbiter defaults to codex/gpt-5.4-xhigh
- Existing explicit config still works
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree
