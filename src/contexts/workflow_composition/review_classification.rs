#![forbid(unsafe_code)]

//! Three-way review classification schema for milestone-aware review stages.
//!
//! Each finding produced by review or final_review is classified as one of:
//! - **fix-now**: within the active bead's scope, remediate immediately
//! - **planned-elsewhere**: valid but already owned by another bead
//! - **propose-new-bead**: genuinely missing work not covered anywhere

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// ── Severity ─────────────────────────────────────────────────────────

/// Severity level for review findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Finding Classification ───────────────────────────────────────────

/// Three-way classification for a review finding.
///
/// Every finding must be classified as exactly one variant. Downstream
/// reconciliation (8.5.x) and prompt rendering (7.2.2) depend on this
/// schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "classification", rename_all = "snake_case")]
pub enum FindingClassification {
    /// The finding is within the active bead's scope and should be
    /// remediated immediately.
    FixNow {
        finding_summary: String,
        severity: Severity,
        affected_files: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remediation_hint: Option<String>,
    },
    /// The finding is valid but already owned by another bead in the graph.
    PlannedElsewhere {
        finding_summary: String,
        mapped_to_bead_id: String,
        confidence: f64,
        rationale: String,
    },
    /// The finding represents genuinely missing work not covered anywhere
    /// in the current plan.
    ProposeNewBead {
        finding_summary: String,
        proposed_title: String,
        proposed_scope: String,
        severity: Severity,
        rationale: String,
    },
}

// ── Validation ───────────────────────────────────────────────────────

/// Validates a `FindingClassification` against domain rules.
///
/// Returns a list of validation errors. An empty vec means the
/// classification is valid.
pub fn validate_classification(classification: &FindingClassification) -> Vec<String> {
    let mut errors = Vec::new();

    match classification {
        FindingClassification::FixNow {
            finding_summary,
            severity: _,
            affected_files: _,
            remediation_hint: _,
        } => {
            if finding_summary.trim().is_empty() {
                errors.push("fix_now: finding_summary must not be empty".to_owned());
            }
        }
        FindingClassification::PlannedElsewhere {
            finding_summary,
            mapped_to_bead_id,
            confidence,
            rationale,
        } => {
            if finding_summary.trim().is_empty() {
                errors.push("planned_elsewhere: finding_summary must not be empty".to_owned());
            }
            if mapped_to_bead_id.trim().is_empty() {
                errors.push("planned_elsewhere: mapped_to_bead_id must not be empty".to_owned());
            }
            if !(*confidence >= 0.0 && *confidence <= 1.0) {
                errors.push(format!(
                    "planned_elsewhere: confidence must be in range 0.0..=1.0, got {confidence}"
                ));
            }
            if rationale.trim().is_empty() {
                errors.push("planned_elsewhere: rationale must not be empty".to_owned());
            }
        }
        FindingClassification::ProposeNewBead {
            finding_summary,
            proposed_title,
            proposed_scope,
            severity: _,
            rationale,
        } => {
            if finding_summary.trim().is_empty() {
                errors.push("propose_new_bead: finding_summary must not be empty".to_owned());
            }
            if proposed_title.trim().is_empty() {
                errors.push("propose_new_bead: proposed_title must not be empty".to_owned());
            }
            if proposed_scope.trim().is_empty() {
                errors.push("propose_new_bead: proposed_scope must not be empty".to_owned());
            }
            if rationale.trim().is_empty() {
                errors.push("propose_new_bead: rationale must not be empty".to_owned());
            }
        }
    }

    errors
}

/// Validates a list of classifications, returning all errors with their
/// indices for diagnostics.
pub fn validate_classifications(
    classifications: &[FindingClassification],
) -> Vec<(usize, Vec<String>)> {
    classifications
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            let errs = validate_classification(c);
            if errs.is_empty() {
                None
            } else {
                Some((i, errs))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────

    fn fix_now_valid() -> FindingClassification {
        FindingClassification::FixNow {
            finding_summary: "Missing null check in parser".to_owned(),
            severity: Severity::High,
            affected_files: vec!["src/parser.rs".to_owned()],
            remediation_hint: Some("Add guard clause at line 42".to_owned()),
        }
    }

    fn planned_elsewhere_valid() -> FindingClassification {
        FindingClassification::PlannedElsewhere {
            finding_summary: "Error handling needs improvement".to_owned(),
            mapped_to_bead_id: "m1.error-handling".to_owned(),
            confidence: 0.85,
            rationale: "Bead m1.error-handling covers error handling refactor".to_owned(),
        }
    }

    fn propose_new_bead_valid() -> FindingClassification {
        FindingClassification::ProposeNewBead {
            finding_summary: "No telemetry for retry loops".to_owned(),
            proposed_title: "Add retry-loop telemetry".to_owned(),
            proposed_scope: "Instrument all retry loops with counter and histogram metrics"
                .to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers observability for retry paths".to_owned(),
        }
    }

    // ── Serde round-trip tests ──────────────────────────────────────

    #[test]
    fn fix_now_round_trips_through_json() -> Result<(), serde_json::Error> {
        let original = fix_now_valid();
        let json = serde_json::to_string_pretty(&original)?;
        let restored: FindingClassification = serde_json::from_str(&json)?;
        assert_eq!(original, restored);
        Ok(())
    }

    #[test]
    fn planned_elsewhere_round_trips_through_json() -> Result<(), serde_json::Error> {
        let original = planned_elsewhere_valid();
        let json = serde_json::to_string_pretty(&original)?;
        let restored: FindingClassification = serde_json::from_str(&json)?;
        assert_eq!(original, restored);
        Ok(())
    }

    #[test]
    fn propose_new_bead_round_trips_through_json() -> Result<(), serde_json::Error> {
        let original = propose_new_bead_valid();
        let json = serde_json::to_string_pretty(&original)?;
        let restored: FindingClassification = serde_json::from_str(&json)?;
        assert_eq!(original, restored);
        Ok(())
    }

    #[test]
    fn fix_now_without_remediation_hint_round_trips() -> Result<(), serde_json::Error> {
        let original = FindingClassification::FixNow {
            finding_summary: "Unused import".to_owned(),
            severity: Severity::Low,
            affected_files: vec![],
            remediation_hint: None,
        };
        let json = serde_json::to_string(&original)?;
        assert!(!json.contains("remediation_hint"));
        let restored: FindingClassification = serde_json::from_str(&json)?;
        assert_eq!(original, restored);
        Ok(())
    }

    #[test]
    fn classification_list_round_trips() -> Result<(), serde_json::Error> {
        let list = vec![
            fix_now_valid(),
            planned_elsewhere_valid(),
            propose_new_bead_valid(),
        ];
        let json = serde_json::to_string_pretty(&list)?;
        let restored: Vec<FindingClassification> = serde_json::from_str(&json)?;
        assert_eq!(list, restored);
        Ok(())
    }

    #[test]
    fn severity_round_trips_all_variants() -> Result<(), serde_json::Error> {
        for severity in [
            Severity::Critical,
            Severity::High,
            Severity::Medium,
            Severity::Low,
        ] {
            let json = serde_json::to_string(&severity)?;
            let restored: Severity = serde_json::from_str(&json)?;
            assert_eq!(severity, restored);
        }
        Ok(())
    }

    #[test]
    fn severity_serializes_as_snake_case() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&Severity::Critical)?, "\"critical\"");
        assert_eq!(serde_json::to_string(&Severity::High)?, "\"high\"");
        assert_eq!(serde_json::to_string(&Severity::Medium)?, "\"medium\"");
        assert_eq!(serde_json::to_string(&Severity::Low)?, "\"low\"");
        Ok(())
    }

    #[test]
    fn classification_tag_field_is_present_in_json() -> Result<(), serde_json::Error> {
        let fix_now = fix_now_valid();
        let json: serde_json::Value = serde_json::to_value(&fix_now)?;
        assert_eq!(json["classification"], "fix_now");

        let planned = planned_elsewhere_valid();
        let json: serde_json::Value = serde_json::to_value(&planned)?;
        assert_eq!(json["classification"], "planned_elsewhere");

        let propose = propose_new_bead_valid();
        let json: serde_json::Value = serde_json::to_value(&propose)?;
        assert_eq!(json["classification"], "propose_new_bead");
        Ok(())
    }

    // ── Validation: valid inputs ────────────────────────────────────

    #[test]
    fn valid_fix_now_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let errors = validate_classification(&fix_now_valid());
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn valid_planned_elsewhere_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let errors = validate_classification(&planned_elsewhere_valid());
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn valid_propose_new_bead_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let errors = validate_classification(&propose_new_bead_valid());
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    // ── Validation: empty strings ───────────────────────────────────

    #[test]
    fn fix_now_rejects_empty_finding_summary() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "".to_owned(),
            severity: Severity::High,
            affected_files: vec![],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("finding_summary"));
        Ok(())
    }

    #[test]
    fn fix_now_rejects_whitespace_only_summary() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "   ".to_owned(),
            severity: Severity::Low,
            affected_files: vec![],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("finding_summary"));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_empty_mapped_to_bead_id() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid summary".to_owned(),
            mapped_to_bead_id: "".to_owned(),
            confidence: 0.9,
            rationale: "Some rationale".to_owned(),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("mapped_to_bead_id"));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_empty_rationale() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid summary".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: 0.5,
            rationale: "  ".to_owned(),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("rationale"));
        Ok(())
    }

    #[test]
    fn propose_new_bead_rejects_empty_title() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::ProposeNewBead {
            finding_summary: "Valid summary".to_owned(),
            proposed_title: "".to_owned(),
            proposed_scope: "Valid scope".to_owned(),
            severity: Severity::Medium,
            rationale: "Valid rationale".to_owned(),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("proposed_title"));
        Ok(())
    }

    #[test]
    fn propose_new_bead_rejects_empty_scope() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::ProposeNewBead {
            finding_summary: "Valid summary".to_owned(),
            proposed_title: "Valid title".to_owned(),
            proposed_scope: "".to_owned(),
            severity: Severity::High,
            rationale: "Valid rationale".to_owned(),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("proposed_scope"));
        Ok(())
    }

    #[test]
    fn propose_new_bead_collects_multiple_errors() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::ProposeNewBead {
            finding_summary: "".to_owned(),
            proposed_title: "".to_owned(),
            proposed_scope: "".to_owned(),
            severity: Severity::Critical,
            rationale: "".to_owned(),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 4);
        Ok(())
    }

    // ── Validation: confidence range ────────────────────────────────

    #[test]
    fn planned_elsewhere_rejects_negative_confidence() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: -0.1,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("confidence")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_confidence_above_one() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: 1.01,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("confidence")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_boundary_confidence_zero() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: 0.0,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_boundary_confidence_one() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: 1.0,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_nan_confidence() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: f64::NAN,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("confidence")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_infinity_confidence() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.bead-1".to_owned(),
            confidence: f64::INFINITY,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("confidence")));
        Ok(())
    }

    // ── Batch validation ────────────────────────────────────────────

    #[test]
    fn validate_classifications_returns_errors_with_indices(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let list = vec![
            fix_now_valid(),
            FindingClassification::PlannedElsewhere {
                finding_summary: "".to_owned(),
                mapped_to_bead_id: "".to_owned(),
                confidence: 2.0,
                rationale: "".to_owned(),
            },
            propose_new_bead_valid(),
        ];
        let results = validate_classifications(&list);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert_eq!(results[0].1.len(), 4);
        Ok(())
    }

    #[test]
    fn validate_classifications_returns_empty_for_all_valid(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let list = vec![
            fix_now_valid(),
            planned_elsewhere_valid(),
            propose_new_bead_valid(),
        ];
        let results = validate_classifications(&list);
        assert!(results.is_empty());
        Ok(())
    }

    // ── Display ─────────────────────────────────────────────────────

    #[test]
    fn severity_display_matches_as_str() -> Result<(), Box<dyn std::error::Error>> {
        for severity in [
            Severity::Critical,
            Severity::High,
            Severity::Medium,
            Severity::Low,
        ] {
            assert_eq!(severity.to_string(), severity.as_str());
        }
        Ok(())
    }
}
