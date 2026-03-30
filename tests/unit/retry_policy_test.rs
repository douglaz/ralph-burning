use std::time::Duration;

use ralph_burning::contexts::workflow_composition::retry_policy::{RetryPolicy, RetryRule};
use ralph_burning::shared::domain::FailureClass;

#[test]
fn default_retry_policy_matches_specified_attempt_limits() {
    let policy = RetryPolicy::default_policy();

    assert_eq!(policy.max_attempts(FailureClass::TransportFailure), 5);
    assert_eq!(
        policy.max_attempts(FailureClass::SchemaValidationFailure),
        3
    );
    assert_eq!(
        policy.max_attempts(FailureClass::DomainValidationFailure),
        2
    );
    assert_eq!(policy.max_attempts(FailureClass::Timeout), 3);
    assert_eq!(policy.max_attempts(FailureClass::Cancellation), 1);
    assert_eq!(policy.max_attempts(FailureClass::QaReviewOutcomeFailure), 1);
    assert_eq!(policy.max_attempts(FailureClass::BinaryNotFound), 1);
    assert_eq!(policy.max_remediation_cycles(), 3);
}

#[test]
fn default_retry_policy_marks_retryable_and_terminal_classes() {
    let policy = RetryPolicy::default_policy();

    assert!(policy.is_retryable(FailureClass::TransportFailure));
    assert!(policy.is_retryable(FailureClass::SchemaValidationFailure));
    assert!(policy.is_retryable(FailureClass::DomainValidationFailure));
    assert!(policy.is_retryable(FailureClass::Timeout));
    assert!(!policy.is_retryable(FailureClass::Cancellation));
    assert!(!policy.is_retryable(FailureClass::QaReviewOutcomeFailure));
    assert!(!policy.is_retryable(FailureClass::BinaryNotFound));
}

#[test]
fn retry_policy_can_be_customized() {
    let policy = RetryPolicy::default_policy()
        .with_rule(FailureClass::Timeout, RetryRule::retryable(4))
        .with_max_remediation_cycles(5);

    assert_eq!(policy.max_attempts(FailureClass::Timeout), 4);
    assert!(policy.is_retryable(FailureClass::Timeout));
    assert_eq!(policy.max_remediation_cycles(), 5);
}

#[test]
fn default_backoff_uses_exponential_growth_capped_at_60s() {
    let policy = RetryPolicy::default_policy().with_no_jitter();

    assert_eq!(policy.backoff_for_attempt(1), Duration::from_secs(5));
    assert_eq!(policy.backoff_for_attempt(2), Duration::from_secs(10));
    assert_eq!(policy.backoff_for_attempt(3), Duration::from_secs(20));
    assert_eq!(policy.backoff_for_attempt(4), Duration::from_secs(40));
    assert_eq!(policy.backoff_for_attempt(5), Duration::from_secs(60));
    assert_eq!(policy.backoff_for_attempt(6), Duration::from_secs(60));
}

#[test]
fn jitter_enabled_backoff_stays_within_expected_range() {
    let policy = RetryPolicy::default_policy(); // jitter enabled by default
                                                // Default base is 5s, attempt 1 multiplier is 1, so base delay = 5s.
                                                // With ±25% jitter the expected range is [3.75s, 6.25s).
    for _ in 0..20 {
        let backoff = policy.backoff_for_attempt(1);
        assert!(
            backoff >= Duration::from_millis(3750) && backoff < Duration::from_millis(6250),
            "jittered backoff {backoff:?} outside expected [3.75s, 6.25s) for attempt 1"
        );
    }
}

#[test]
fn jitter_never_exceeds_backoff_cap() {
    let policy = RetryPolicy::default_policy(); // jitter enabled, cap = 60s
                                                // At high attempt numbers the pre-jitter delay saturates at the cap.
                                                // After the re-cap fix, jitter must never push the result above 60s.
    for attempt in 1..=10 {
        for _ in 0..20 {
            let backoff = policy.backoff_for_attempt(attempt);
            assert!(
                backoff <= Duration::from_secs(60),
                "jittered backoff {backoff:?} exceeds 60s cap at attempt {attempt}"
            );
        }
    }
}

#[test]
fn no_backoff_returns_zero_for_all_attempts() {
    let policy = RetryPolicy::default_policy().with_no_backoff();

    assert_eq!(policy.backoff_for_attempt(1), Duration::ZERO);
    assert_eq!(policy.backoff_for_attempt(5), Duration::ZERO);
}
