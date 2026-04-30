# Drain failure recovery policy

The source of truth is `classify_drain_failure` in
[`src/contexts/bead_workflow/drain_failure.rs`](../src/contexts/bead_workflow/drain_failure.rs).
`ralph drain` callers should convert observations into `FailureObservation`
values and match on the returned `RecoveryAction`.

| Failure mode | Recovery action | Rationale | Test |
|---|---|---|---|
| CI flake where the failing test is in the authoritative `KNOWN_CI_FLAKES` list | `Rerun { budget: OncePerPr }` | Known flakes often resolve on retry, but the one-rerun budget prevents masking real regressions. | [`known_ci_flake_reruns_once_per_pr`](../tests/unit/drain_failure_policy_test.rs) |
| CI or gate permanent failure, including `Gate` failures from PR open | `FileBead { after_filing: AbortDrain }` | Real test/build failures need operator attention and should not be hidden by the drain loop. | [`ci_permanent_failure_files_bead_and_aborts_drain`](../tests/unit/drain_failure_policy_test.rs) |
| Codex bot line-comments after the latest push | `AbortDrain` | Line findings need human judgment; drain does not auto-amend or try to outsmart review feedback. | [`bot_line_comments_after_latest_push_abort_drain`](../tests/unit/drain_failure_policy_test.rs) |
| Codex bot `-1` reaction | `AbortDrain` | The negative reaction is an explicit rejection signal. | [`bot_negative_reaction_aborts_drain`](../tests/unit/drain_failure_policy_test.rs) |
| Amendment oscillation reaches the configured cap | `ForceCompleteRun` | The workflow engine already enforces `max_completion_rounds`; reaching that cap is treated as normal completion for drain shipping. | [`amendment_oscillation_limit_force_completes_run`](../tests/unit/drain_failure_policy_test.rs) |
| Same bead failed twice in a row | `FileBead { after_filing: AbortDrain }` | Repeated failure on one bead suggests a structural problem worth inspecting before continuing. | [`same_bead_failed_twice_files_bead_and_aborts_drain`](../tests/unit/drain_failure_policy_test.rs) |
| Orchestrator run interrupted or died | `ResumeRun { stop_first: true }` | `run stop` records the interrupted state and the next drain cycle should use `run resume` so checkpoint state is preserved. | [`interrupted_run_stops_and_resumes_on_next_cycle`](../tests/unit/drain_failure_policy_test.rs) |
| Backend exhausted with `FailureClass::BackendExhausted` from the existing backend classifier | `FileBead { after_filing: SkipBeadAndContinueDrain }` | One bead exhausting its backend budget should be captured for later without killing the whole drain. | [`backend_exhausted_files_bead_skips_current_bead_and_continues_drain`](../tests/unit/drain_failure_policy_test.rs) |

The classifier deliberately reuses existing sources instead of redefining
them: known CI flakes come from `pr_watch::KNOWN_CI_FLAKES`, local gate
identity comes from `pr_open::Gate`, and backend exhaustion comes from
`FailureClass::BackendExhausted`, which is produced by the process backend's
existing non-retryable exhaustion classifier.

Amendment oscillation is not detected by a new loop here. The engine's
existing `max_completion_rounds` cap is the mechanism; drain policy only
classifies the observation that the cap was reached.
