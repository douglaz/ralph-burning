# Implementation Notes

## Decisions Made
- Fixed `routing_source` values in DAEMON-LIFECYCLE-007 and DAEMON-LIFECYCLE-008 fixtures from invalid `"default"` to canonical `"default_flow"`, matching the `RoutingSource::DefaultFlow` serde snake_case serialization.
- Added `assert_success(&out)?` to both DAEMON-LIFECYCLE-007 and DAEMON-LIFECYCLE-008 so that a daemon command failure (e.g. from fixture deserialization) cannot silently satisfy the scenario.
- Strengthened DAEMON-LIFECYCLE-007 with an explicit check that `locked-task` acquires no `lease_id` under writer lock contention, enforcing the "no claim-side durable mutation" invariant.
- Strengthened DAEMON-LIFECYCLE-008 to require task status != `"pending"` unconditionally after a successful daemon cycle, rather than the previous fallback of checking output mentions. This ensures a malformed fixture that causes a parse failure cannot count as successful dispatch.
- CLI regression tests assert against stderr (where the conformance report is printed via `eprint!`) rather than stdout, matching the actual `ConformanceReport` Display format (`Passed:    N`, `Failed:    N`).
- No compatibility alias was added to `RoutingSource`; the canonical durable values remain `command`, `label`, and `default_flow`.

## Spec Deviations
- None. All acceptance criteria are addressed as specified.

## Testing
- `nix build` passes all 105 tests (102 existing + 3 new CLI regression tests).
- New CLI tests: `conformance_daemon_lifecycle_007_passes`, `conformance_daemon_lifecycle_008_passes`, `conformance_full_suite_passes`.
- To verify manually: `./result/bin/ralph-burning conformance run` (requires features directory to be accessible; verified via nix build test phase).
