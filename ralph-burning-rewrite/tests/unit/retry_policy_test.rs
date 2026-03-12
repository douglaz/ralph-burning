use ralph_burning::contexts::workflow_composition::retry_policy::{RetryPolicy, RetryRule};
use ralph_burning::shared::domain::FailureClass;

#[test]
fn default_retry_policy_matches_specified_attempt_limits() {
    let policy = RetryPolicy::default_policy();

    assert_eq!(policy.max_attempts(FailureClass::TransportFailure), 3);
    assert_eq!(
        policy.max_attempts(FailureClass::SchemaValidationFailure),
        2
    );
    assert_eq!(
        policy.max_attempts(FailureClass::DomainValidationFailure),
        2
    );
    assert_eq!(policy.max_attempts(FailureClass::Timeout), 2);
    assert_eq!(policy.max_attempts(FailureClass::Cancellation), 1);
    assert_eq!(policy.max_attempts(FailureClass::QaReviewOutcomeFailure), 1);
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
