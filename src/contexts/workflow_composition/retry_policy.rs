use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

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

/// Base backoff delay between retry attempts (default 5 seconds).
const DEFAULT_BACKOFF_BASE_SECS: u64 = 5;

/// Maximum backoff delay between retry attempts (default 60 seconds).
const DEFAULT_BACKOFF_CAP_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    rules: HashMap<FailureClass, RetryRule>,
    max_remediation_cycles: u32,
    backoff_base: Duration,
    backoff_cap: Duration,
    jitter: bool,
}

impl RetryPolicy {
    pub fn default_policy() -> Self {
        let mut rules = HashMap::new();
        rules.insert(FailureClass::TransportFailure, RetryRule::retryable(5));
        rules.insert(
            FailureClass::SchemaValidationFailure,
            RetryRule::retryable(3),
        );
        rules.insert(
            FailureClass::DomainValidationFailure,
            RetryRule::retryable(2),
        );
        rules.insert(FailureClass::Timeout, RetryRule::retryable(3));
        rules.insert(FailureClass::Cancellation, RetryRule::terminal());
        rules.insert(FailureClass::QaReviewOutcomeFailure, RetryRule::terminal());
        rules.insert(FailureClass::BinaryNotFound, RetryRule::terminal());

        Self {
            rules,
            max_remediation_cycles: 3,
            backoff_base: Duration::from_secs(DEFAULT_BACKOFF_BASE_SECS),
            backoff_cap: Duration::from_secs(DEFAULT_BACKOFF_CAP_SECS),
            jitter: true,
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

    pub fn with_no_backoff(mut self) -> Self {
        self.backoff_base = Duration::ZERO;
        self.backoff_cap = Duration::ZERO;
        self.jitter = false;
        self
    }

    pub fn with_no_jitter(mut self) -> Self {
        self.jitter = false;
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

    /// Compute the backoff duration for a given attempt number (1-indexed).
    ///
    /// Uses exponential backoff: `base * 2^(attempt - 1)`, capped at `backoff_cap`,
    /// then applies ±25% jitter to stagger concurrent retries and avoid
    /// thundering-herd amplification on rate-limited backends.
    ///
    /// Returns `Duration::ZERO` when backoff is disabled or `attempt` is 0.
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if self.backoff_base.is_zero() || attempt == 0 {
            return Duration::ZERO;
        }
        let multiplier = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.backoff_base.saturating_mul(multiplier);
        let capped = delay.min(self.backoff_cap);
        if self.jitter {
            Self::apply_jitter(capped).min(self.backoff_cap)
        } else {
            capped
        }
    }

    /// Apply ±25% jitter to a duration using system clock nanos as a
    /// cheap randomness source to stagger concurrent retries.
    fn apply_jitter(base: Duration) -> Duration {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        // Map nanos into [0.75, 1.25) via: 0.75 + (nanos % 1000) / 1000 * 0.50
        let fraction = (nanos % 1000) as f64 / 1000.0; // [0.0, 1.0)
        let factor = 0.75 + fraction * 0.50; // [0.75, 1.25)
        Duration::from_secs_f64(base.as_secs_f64() * factor)
    }
}
