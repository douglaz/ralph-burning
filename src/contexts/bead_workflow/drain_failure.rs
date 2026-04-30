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
    Rerun { budget: RerunBudget },
    FileBead { after_filing: PostBeadAction },
    AbortDrain,
    ResumeRun { stop_first: bool },
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
                RecoveryAction::FileBead {
                    after_filing: PostBeadAction::AbortDrain,
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
