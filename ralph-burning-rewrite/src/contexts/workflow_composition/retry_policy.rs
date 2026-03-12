use std::collections::HashMap;

use crate::shared::domain::FailureClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryRule {
    pub max_attempts: u32,
    pub retryable: bool,
}

impl RetryRule {
    pub const fn retryable(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            retryable: true,
        }
    }

    pub const fn terminal() -> Self {
        Self {
            max_attempts: 1,
            retryable: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    rules: HashMap<FailureClass, RetryRule>,
    max_remediation_cycles: u32,
}

impl RetryPolicy {
    pub fn default_policy() -> Self {
        let mut rules = HashMap::new();
        rules.insert(
            FailureClass::TransportFailure,
            RetryRule::retryable(3),
        );
        rules.insert(
            FailureClass::SchemaValidationFailure,
            RetryRule::retryable(2),
        );
        rules.insert(
            FailureClass::DomainValidationFailure,
            RetryRule::retryable(2),
        );
        rules.insert(FailureClass::Timeout, RetryRule::retryable(2));
        rules.insert(FailureClass::Cancellation, RetryRule::terminal());
        rules.insert(
            FailureClass::QaReviewOutcomeFailure,
            RetryRule::terminal(),
        );

        Self {
            rules,
            max_remediation_cycles: 3,
        }
    }

    pub fn with_rule(mut self, failure_class: FailureClass, rule: RetryRule) -> Self {
        self.rules.insert(failure_class, rule);
        self
    }

    pub fn with_max_remediation_cycles(mut self, max_remediation_cycles: u32) -> Self {
        self.max_remediation_cycles = max_remediation_cycles.max(1);
        self
    }

    pub fn rule_for(&self, failure_class: FailureClass) -> RetryRule {
        self.rules
            .get(&failure_class)
            .copied()
            .unwrap_or_else(RetryRule::terminal)
    }

    pub fn max_attempts(&self, failure_class: FailureClass) -> u32 {
        self.rule_for(failure_class).max_attempts
    }

    pub fn is_retryable(&self, failure_class: FailureClass) -> bool {
        self.rule_for(failure_class).retryable
    }

    pub fn max_remediation_cycles(&self) -> u32 {
        self.max_remediation_cycles
    }
}
