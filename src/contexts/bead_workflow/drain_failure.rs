#![forbid(unsafe_code)]

//! Deterministic recovery policy for future `ralph drain` failure handling.

use crate::shared::domain::FailureClass;

use super::pr_open::Gate;
use super::pr_watch::is_known_flake_failure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureObservation {
    pub kind: FailureObservationKind,
}

impl FailureObservation {
    pub fn new(kind: FailureObservationKind) -> Self {
        Self { kind }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureObservationKind {
    VerificationFailure {
        source: VerificationFailureSource,
        failing: Vec<String>,
        reruns_attempted_for_pr: u32,
    },
    BotLineCommentsAfterLatestPush {
        comment_count: usize,
    },
    BotRejectedReaction,
    AmendmentOscillationLimitReached {
        completion_rounds: u32,
        max_completion_rounds: u32,
    },
    SameBeadFailedTwiceInRow {
        bead_id: String,
    },
    RunInterrupted {
        run_id: String,
    },
    BackendFailure {
        bead_id: String,
        failure_class: FailureClass,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationFailureSource {
    Ci,
    Gate(Gate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    Rerun {
        budget: RerunBudget,
    },
    /// Discard the current project's run output and start a fresh
    /// plan_and_implement attempt on the same bead, with the failure log
    /// injected into the new project prompt as additional context.
    ///
    /// Reserved for verification failures whose content suggests a clean
    /// re-implementation pass would resolve them (lint warnings, fmt
    /// issues, clippy-only lints) — i.e. deterministic but mechanically
    /// fixable. The drain loop is responsible for applying a per-bead
    /// retry budget; if the budget is exhausted the loop falls through
    /// to `FileBead { AbortDrain }`.
    RetryBeadFresh,
    FileBead {
        after_filing: PostBeadAction,
    },
    AbortDrain,
    ResumeRun {
        stop_first: bool,
    },
    ForceCompleteRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerunBudget {
    OncePerPr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostBeadAction {
    AbortDrain,
    SkipBeadAndContinueDrain,
}

pub fn classify_drain_failure(observation: &FailureObservation) -> RecoveryAction {
    match &observation.kind {
        FailureObservationKind::VerificationFailure {
            source,
            failing,
            reruns_attempted_for_pr,
        } => match source {
            VerificationFailureSource::Ci
                if *reruns_attempted_for_pr == 0 && is_known_flake_failure(failing) =>
            {
                RecoveryAction::Rerun {
                    budget: RerunBudget::OncePerPr,
                }
            }
            VerificationFailureSource::Ci | VerificationFailureSource::Gate(_) => {
                if is_cleanable_verification_failure(failing) {
                    RecoveryAction::RetryBeadFresh
                } else {
                    RecoveryAction::FileBead {
                        after_filing: PostBeadAction::AbortDrain,
                    }
                }
            }
        },
        FailureObservationKind::BotLineCommentsAfterLatestPush { .. } => RecoveryAction::AbortDrain,
        FailureObservationKind::BotRejectedReaction => RecoveryAction::AbortDrain,
        FailureObservationKind::AmendmentOscillationLimitReached { .. } => {
            RecoveryAction::ForceCompleteRun
        }
        FailureObservationKind::SameBeadFailedTwiceInRow { .. } => RecoveryAction::FileBead {
            after_filing: PostBeadAction::AbortDrain,
        },
        FailureObservationKind::RunInterrupted { .. } => {
            RecoveryAction::ResumeRun { stop_first: true }
        }
        FailureObservationKind::BackendFailure { failure_class, .. } => match failure_class {
            FailureClass::BackendExhausted => RecoveryAction::FileBead {
                after_filing: PostBeadAction::SkipBeadAndContinueDrain,
            },
            FailureClass::TransportFailure
            | FailureClass::SchemaValidationFailure
            | FailureClass::DomainValidationFailure
            | FailureClass::Timeout
            | FailureClass::Cancellation
            | FailureClass::QaReviewOutcomeFailure
            | FailureClass::BinaryNotFound => RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain,
            },
        },
    }
}

/// Markers that indicate a real test failure or compile error — not the
/// kind of thing a re-implementation pass on the same bead would clear.
/// If any of these appear in the failure text we treat the failure as
/// non-cleanable and fall through to `FileBead { AbortDrain }`.
const NON_CLEANABLE_MARKERS: &[&str] = &[
    "panicked at",
    "assertion failed",
    "assertion `left ==",
    "assertion `left !=",
    "thread '",
    "test result: FAILED",
    "error[E",
    "expected `",
    "cannot find",
    "no method named",
    "trait bound",
    "borrow checker",
    "use of moved value",
];

/// Markers that indicate a lint-only / formatting / cleanable-style
/// failure — the kind a fresh implementation round on the same bead
/// (with the failure log surfaced as context) would typically clear.
const CLEANABLE_MARKERS: &[&str] = &[
    "unused_imports",
    "unused_variables",
    "unused_mut",
    "unused_assignments",
    "dead_code",
    "unused import",
    "unused variable",
    "unused mut",
    "unused assignment",
    "cargo fmt",
    "rustfmt",
    "Diff in ",
    "clippy::",
    "warning: ",
];

/// Returns `true` when the failure text looks deterministic-but-mechanically-
/// fixable (lint warnings, fmt issues, clippy-only lints). The classifier
/// uses this to route such failures to `RetryBeadFresh` instead of filing
/// a follow-up bead and aborting.
///
/// The check is deliberately conservative: any indicator of a real test
/// panic, compile error, or borrow-checker failure forces non-cleanable,
/// even if cleanable markers are also present (e.g. a panicking test that
/// happens to share a build with an unused-import warning).
pub fn is_cleanable_verification_failure(failing: &[String]) -> bool {
    if failing.is_empty() {
        return false;
    }
    let combined: String = failing.join("\n");
    if NON_CLEANABLE_MARKERS
        .iter()
        .any(|marker| combined.contains(marker))
    {
        return false;
    }
    CLEANABLE_MARKERS
        .iter()
        .any(|marker| combined.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verification_failure(failing: Vec<String>) -> FailureObservation {
        FailureObservation::new(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Gate(Gate::CargoTest),
            failing,
            reruns_attempted_for_pr: 0,
        })
    }

    #[test]
    fn cleanable_unused_import_warning_routes_to_retry_bead_fresh() {
        let failing = vec![
            "cargo test failed: warning: unused import: `std::time::Duration`\n    --> src/cli/run.rs:5031:9\n     |\n5031 |     use std::time::Duration;\n     |         ^^^^^^^^^^^^^^^^^^^"
                .to_owned(),
        ];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::RetryBeadFresh
        );
    }

    #[test]
    fn cleanable_cargo_fmt_failure_routes_to_retry_bead_fresh() {
        let failing =
            vec!["cargo fmt --check failed: Diff in src/cli/run.rs at line 42".to_owned()];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::RetryBeadFresh
        );
    }

    #[test]
    fn clippy_only_lint_routes_to_retry_bead_fresh() {
        let failing = vec![
            "warning: redundant clone\n  --> src/foo.rs:10\n   |\n   = note: `#[warn(clippy::redundant_clone)]` on by default"
                .to_owned(),
        ];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::RetryBeadFresh
        );
    }

    #[test]
    fn real_test_panic_routes_to_file_bead_abort() {
        let failing = vec![
            "test foo::bar::baz ... FAILED\nthread 'foo::bar::baz' panicked at src/foo.rs:42:5:\nassertion failed: expected_value == actual_value"
                .to_owned(),
        ];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain
            }
        );
    }

    #[test]
    fn compile_error_routes_to_file_bead_abort() {
        let failing = vec![
            "error[E0425]: cannot find value `undefined_variable` in this scope\n  --> src/lib.rs:10:5"
                .to_owned(),
        ];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain
            }
        );
    }

    #[test]
    fn mixed_warning_and_panic_routes_to_file_bead_abort() {
        // A run that has both a cleanable warning AND a real test panic
        // must NOT be treated as cleanable — the panic dominates.
        let failing = vec![
            "warning: unused import: `Foo`".to_owned(),
            "test result: FAILED. 1 passed; 1 failed".to_owned(),
            "thread 'tests::foo' panicked at src/foo.rs:1:1".to_owned(),
        ];
        let observation = verification_failure(failing);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain
            }
        );
    }

    #[test]
    fn empty_failing_list_routes_to_file_bead_abort() {
        let observation = verification_failure(vec![]);
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain
            }
        );
    }

    #[test]
    fn cleanable_failure_on_ci_source_also_routes_to_retry_bead_fresh() {
        // Lint failures from CI should be treated the same as gate failures —
        // rerunning the same PR's CI won't help (it's deterministic), but a
        // fresh implementation round on the bead would clear them.
        let observation = FailureObservation::new(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Ci,
            failing: vec!["warning: unused variable: `x`".to_owned()],
            reruns_attempted_for_pr: 0,
        });
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::RetryBeadFresh
        );
    }

    #[test]
    fn known_ci_flake_still_takes_precedence_over_cleanable_check() {
        // If the failure is a known CI flake AND looks cleanable, the
        // existing flake-rerun path should win (it's cheaper than a
        // full bead retry). This test guards against the cleanable
        // check accidentally swallowing the flake-retry path.
        let flake_name =
            "adapters::br_process::tests::check_available_times_out_when_version_probe_hangs";
        let observation = FailureObservation::new(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Ci,
            failing: vec![flake_name.to_owned()],
            reruns_attempted_for_pr: 0,
        });
        assert_eq!(
            classify_drain_failure(&observation),
            RecoveryAction::Rerun {
                budget: RerunBudget::OncePerPr
            }
        );
    }
}
