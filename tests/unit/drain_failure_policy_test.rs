use ralph_burning::contexts::bead_workflow::drain_failure::{
    classify_drain_failure, FailureObservation, FailureObservationKind, PostBeadAction,
    RecoveryAction, RerunBudget, VerificationFailureSource,
};
use ralph_burning::contexts::bead_workflow::pr_open::Gate;
use ralph_burning::contexts::bead_workflow::pr_watch::KNOWN_CI_FLAKES;
use ralph_burning::shared::domain::FailureClass;

fn observation(kind: FailureObservationKind) -> FailureObservation {
    FailureObservation::new(kind)
}

fn file_bead_abort() -> RecoveryAction {
    RecoveryAction::FileBead {
        after_filing: PostBeadAction::AbortDrain,
    }
}

fn file_bead_skip() -> RecoveryAction {
    RecoveryAction::FileBead {
        after_filing: PostBeadAction::SkipBeadAndContinueDrain,
    }
}

#[test]
fn known_ci_flake_reruns_once_per_pr() {
    let action =
        classify_drain_failure(&observation(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Ci,
            failing: vec![KNOWN_CI_FLAKES[0].to_owned()],
            reruns_attempted_for_pr: 0,
        }));

    assert_eq!(
        action,
        RecoveryAction::Rerun {
            budget: RerunBudget::OncePerPr
        }
    );
}

#[test]
fn ci_permanent_failure_files_bead_and_aborts_drain() {
    let action =
        classify_drain_failure(&observation(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Gate(Gate::NixBuild),
            failing: vec!["nix build failed".to_owned()],
            reruns_attempted_for_pr: 0,
        }));

    assert_eq!(action, file_bead_abort());
}

#[test]
fn bot_line_comments_after_latest_push_abort_drain() {
    let action = classify_drain_failure(&observation(
        FailureObservationKind::BotLineCommentsAfterLatestPush { comment_count: 2 },
    ));

    assert_eq!(action, RecoveryAction::AbortDrain);
}

#[test]
fn bot_negative_reaction_aborts_drain() {
    let action = classify_drain_failure(&observation(FailureObservationKind::BotRejectedReaction));

    assert_eq!(action, RecoveryAction::AbortDrain);
}

#[test]
fn amendment_oscillation_limit_force_completes_run() {
    let action = classify_drain_failure(&observation(
        FailureObservationKind::AmendmentOscillationLimitReached {
            completion_rounds: 20,
            max_completion_rounds: 20,
        },
    ));

    assert_eq!(action, RecoveryAction::ForceCompleteRun);
}

#[test]
fn same_bead_failed_twice_files_bead_and_aborts_drain() {
    let action = classify_drain_failure(&observation(
        FailureObservationKind::SameBeadFailedTwiceInRow {
            bead_id: "gj74".to_owned(),
        },
    ));

    assert_eq!(action, file_bead_abort());
}

#[test]
fn interrupted_run_stops_and_resumes_on_next_cycle() {
    let action = classify_drain_failure(&observation(FailureObservationKind::RunInterrupted {
        run_id: "run-gj74".to_owned(),
    }));

    assert_eq!(action, RecoveryAction::ResumeRun { stop_first: true });
}

#[test]
fn backend_exhausted_files_bead_skips_current_bead_and_continues_drain() {
    let action = classify_drain_failure(&observation(FailureObservationKind::BackendFailure {
        bead_id: "gj74".to_owned(),
        failure_class: FailureClass::BackendExhausted,
    }));

    assert_eq!(action, file_bead_skip());
}

#[test]
fn policy_match_is_exhaustive_over_failure_observation_kinds() {
    fn match_all_kinds(kind: FailureObservationKind) -> &'static str {
        match kind {
            FailureObservationKind::VerificationFailure { .. } => "verification_failure",
            FailureObservationKind::BotLineCommentsAfterLatestPush { .. } => {
                "bot_line_comments_after_latest_push"
            }
            FailureObservationKind::BotRejectedReaction => "bot_rejected_reaction",
            FailureObservationKind::AmendmentOscillationLimitReached { .. } => {
                "amendment_oscillation_limit_reached"
            }
            FailureObservationKind::SameBeadFailedTwiceInRow { .. } => {
                "same_bead_failed_twice_in_row"
            }
            FailureObservationKind::RunInterrupted { .. } => "run_interrupted",
            FailureObservationKind::BackendFailure { .. } => "backend_failure",
        }
    }

    let observed = [
        match_all_kinds(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Ci,
            failing: vec![KNOWN_CI_FLAKES[0].to_owned()],
            reruns_attempted_for_pr: 0,
        }),
        match_all_kinds(FailureObservationKind::BotLineCommentsAfterLatestPush {
            comment_count: 1,
        }),
        match_all_kinds(FailureObservationKind::BotRejectedReaction),
        match_all_kinds(FailureObservationKind::AmendmentOscillationLimitReached {
            completion_rounds: 20,
            max_completion_rounds: 20,
        }),
        match_all_kinds(FailureObservationKind::SameBeadFailedTwiceInRow {
            bead_id: "gj74".to_owned(),
        }),
        match_all_kinds(FailureObservationKind::RunInterrupted {
            run_id: "run-gj74".to_owned(),
        }),
        match_all_kinds(FailureObservationKind::BackendFailure {
            bead_id: "gj74".to_owned(),
            failure_class: FailureClass::BackendExhausted,
        }),
    ];

    assert_eq!(
        observed,
        [
            "verification_failure",
            "bot_line_comments_after_latest_push",
            "bot_rejected_reaction",
            "amendment_oscillation_limit_reached",
            "same_bead_failed_twice_in_row",
            "run_interrupted",
            "backend_failure",
        ]
    );
}
