use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use ralph_burning::contexts::bead_workflow::drain::{drain_bead_queue, DrainOptions, DrainOutcome};
use ralph_burning::contexts::bead_workflow::drain_failure::{
    classify_drain_failure, FailureObservation, FailureObservationKind, PostBeadAction,
    RecoveryAction,
};
use ralph_burning::shared::domain::FailureClass;
use ralph_burning::test_support::drain_harness::{
    known_flake_observation, DrainHarnessEvent, DrainHarnessScenario, ScratchDrainHarness,
};

#[tokio::test]
async fn drain_harness_happy_path_drains_all_scratch_beads() {
    let mut harness = ScratchDrainHarness::new([
        ("drain-happy-a", DrainHarnessScenario::Happy),
        ("drain-happy-b", DrainHarnessScenario::Happy),
        ("drain-happy-c", DrainHarnessScenario::Happy),
    ]);

    let started = Instant::now();
    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain succeeds");

    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 3 });
    assert_eq!(
        report.landed,
        vec!["drain-happy-a", "drain-happy-b", "drain-happy-c"]
    );
    assert!(harness.events.iter().any(|event| matches!(
        event,
        DrainHarnessEvent::ProjectCreated {
            bead_id,
            branch_name
        } if bead_id == "drain-happy-a" && branch_name.starts_with("feat/")
    )));
    let pr_tool_calls = harness.pr_tool.calls.lock().expect("PR tool calls").clone();
    for expected_call in [
        "cargo fmt --check",
        "cargo clippy --locked -- -D warnings",
        "cargo test",
        "nix build",
        "soft_reset_origin_master",
        "commit",
        "push_branch",
        "create_pr",
    ] {
        assert!(
            pr_tool_calls.iter().any(|call| call == expected_call),
            "expected PR-open mock call {expected_call:?}; calls were {pr_tool_calls:?}"
        );
    }
    assert_eq!(harness.pr_tool.opened.len(), 3);
    assert_eq!(harness.pr_watch.watched.len(), 3);
    assert_eq!(
        harness.closed_bead_ids_on_disk(),
        vec!["drain-happy-a", "drain-happy-b", "drain-happy-c"]
    );
    assert!(harness
        .events
        .contains(&DrainHarnessEvent::GitPrepareBeadMutationBase));
    assert_eq!(harness.git.persisted_mutations, 3);
}

#[tokio::test]
async fn drain_harness_known_flake_reruns_once_before_merge() {
    let mut harness = ScratchDrainHarness::new([(
        "drain-known-flake",
        DrainHarnessScenario::KnownFlakeThenMerge,
    )]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain succeeds");

    assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 1 });
    assert_eq!(report.landed, vec!["drain-known-flake"]);
    assert_eq!(
        harness.pr_watch.known_flake_reruns,
        vec![("drain-known-flake".to_owned(), 1)]
    );
    assert_eq!(harness.run_attempts("drain-known-flake"), 1);
    assert_eq!(harness.closed_bead_ids_on_disk(), vec!["drain-known-flake"]);
}

#[tokio::test]
async fn drain_harness_permanent_failure_stops_and_files_follow_up() {
    let mut harness = ScratchDrainHarness::new([(
        "drain-permanent-failure",
        DrainHarnessScenario::PermanentCiFailure,
    )]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain returns classified failure");

    assert_eq!(
        report.outcome,
        DrainOutcome::Failed {
            bead_id: "drain-permanent-failure".to_owned(),
            reason: "filed follow-up bead and aborted drain".to_owned()
        }
    );
    assert_eq!(report.failed, vec!["drain-permanent-failure"]);
    assert_eq!(harness.follow_up_count_on_disk(), 1);
    assert!(harness.closed_bead_ids_on_disk().is_empty());
    assert!(harness.events.contains(&DrainHarnessEvent::FollowUpFiled {
        bead_id: "drain-permanent-failure".to_owned()
    }));
}

#[tokio::test]
async fn drain_harness_backend_exhausted_files_follow_up_skips_and_continues() {
    let mut harness = ScratchDrainHarness::new([
        (
            "drain-backend-exhausted",
            DrainHarnessScenario::BackendExhaustedSkip,
        ),
        ("drain-after-skip", DrainHarnessScenario::Happy),
    ]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain succeeds after skip");

    assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 2 });
    assert_eq!(report.skipped, vec!["drain-backend-exhausted"]);
    assert_eq!(report.landed, vec!["drain-after-skip"]);
    assert_eq!(harness.follow_up_count_on_disk(), 1);
    assert_eq!(
        harness.closed_bead_ids_on_disk(),
        vec!["drain-backend-exhausted", "drain-after-skip"]
    );
}

#[tokio::test]
async fn drain_harness_interrupted_run_stops_then_resumes_next_cycle() {
    let mut harness = ScratchDrainHarness::new([(
        "drain-interrupted",
        DrainHarnessScenario::InterruptedThenResume,
    )]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain resumes interrupted run");

    assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 2 });
    assert_eq!(report.landed, vec!["drain-interrupted"]);
    assert_eq!(harness.runs.stops, vec!["drain-interrupted"]);
    assert_eq!(harness.runs.resumes, vec!["drain-interrupted"]);
    assert_eq!(
        harness.git.preserving_resume_syncs,
        vec!["drain-interrupted"]
    );
    assert_eq!(report.cycles[0].outcome, "resume queued");
    assert_eq!(report.cycles[1].outcome, "landed after resume");
}

#[tokio::test]
async fn drain_harness_bot_rejection_aborts_without_follow_up() {
    let mut harness =
        ScratchDrainHarness::new([("drain-bot-rejected", DrainHarnessScenario::BotRejected)]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain returns classified abort");

    assert_eq!(
        report.outcome,
        DrainOutcome::Failed {
            bead_id: "drain-bot-rejected".to_owned(),
            reason: "failure policy aborted drain".to_owned()
        }
    );
    assert_eq!(report.failed, vec!["drain-bot-rejected"]);
    assert_eq!(harness.follow_up_count_on_disk(), 0);
    assert!(harness.closed_bead_ids_on_disk().is_empty());
}

#[tokio::test]
async fn drain_harness_force_complete_continues_to_pr_and_merge() {
    let mut harness =
        ScratchDrainHarness::new([("drain-force-complete", DrainHarnessScenario::ForceComplete)]);

    let report = drain_bead_queue(&mut harness, DrainOptions::default())
        .await
        .expect("drain force-completes run");

    assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 1 });
    assert_eq!(report.landed, vec!["drain-force-complete"]);
    assert_eq!(report.cycles[0].convergence_pattern, "force-completed");
    assert_eq!(
        harness.closed_bead_ids_on_disk(),
        vec!["drain-force-complete"]
    );
}

#[test]
fn drain_harness_recovery_action_scenarios_cover_every_variant() {
    let observed = [
        harness_action_variant(DrainHarnessScenario::KnownFlakeThenMerge),
        harness_action_variant(DrainHarnessScenario::PermanentCiFailure),
        harness_action_variant(DrainHarnessScenario::BackendExhaustedSkip),
        harness_action_variant(DrainHarnessScenario::InterruptedThenResume),
        harness_action_variant(DrainHarnessScenario::BotRejected),
        harness_action_variant(DrainHarnessScenario::ForceComplete),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();

    assert_eq!(
        observed,
        BTreeSet::from([
            "AbortDrain",
            "FileBeadAbort",
            "FileBeadSkip",
            "ForceCompleteRun",
            "Rerun",
            "ResumeRun"
        ])
    );
}

fn harness_action_variant(scenario: DrainHarnessScenario) -> &'static str {
    action_variant(classify_drain_failure(&scenario_observation(scenario)))
}

fn scenario_observation(scenario: DrainHarnessScenario) -> FailureObservation {
    match scenario {
        DrainHarnessScenario::KnownFlakeThenMerge => known_flake_observation(),
        DrainHarnessScenario::PermanentCiFailure => FailureObservation::new(
            FailureObservationKind::VerificationFailure {
                source: ralph_burning::contexts::bead_workflow::drain_failure::VerificationFailureSource::Ci,
                failing: vec!["tests::permanent_failure".to_owned()],
                reruns_attempted_for_pr: 1,
            },
        ),
        DrainHarnessScenario::BackendExhaustedSkip => FailureObservation::new(
            FailureObservationKind::BackendFailure {
                bead_id: "skip-after-follow-up".to_owned(),
                failure_class: FailureClass::BackendExhausted,
            },
        ),
        DrainHarnessScenario::InterruptedThenResume => {
            FailureObservation::new(FailureObservationKind::RunInterrupted {
                run_id: "run-resume".to_owned(),
            })
        }
        DrainHarnessScenario::BotRejected => {
            FailureObservation::new(FailureObservationKind::BotRejectedReaction)
        }
        DrainHarnessScenario::ForceComplete => FailureObservation::new(
            FailureObservationKind::AmendmentOscillationLimitReached {
                completion_rounds: 3,
                max_completion_rounds: 3,
            },
        ),
        DrainHarnessScenario::Happy => {
            panic!("happy path does not produce a recovery observation")
        }
    }
}

fn action_variant(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::Rerun { .. } => "Rerun",
        RecoveryAction::FileBead {
            after_filing: PostBeadAction::AbortDrain,
        } => "FileBeadAbort",
        RecoveryAction::FileBead {
            after_filing: PostBeadAction::SkipBeadAndContinueDrain,
        } => "FileBeadSkip",
        RecoveryAction::AbortDrain => "AbortDrain",
        RecoveryAction::ResumeRun { .. } => "ResumeRun",
        RecoveryAction::ForceCompleteRun => "ForceCompleteRun",
    }
}
