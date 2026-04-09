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
#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "classification", rename_all = "kebab-case")]
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

// ── Checked deserialization ─────────────────────────────────────────

/// Private mirror of [`FindingClassification`] with a derived
/// `Deserialize`. The public enum uses a custom `Deserialize` impl
/// that runs [`validate_classification_fields`] first, making
/// cross-variant field contamination a deserialization error instead
/// of a silent field drop.
mod unchecked {
    use super::Severity;
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(tag = "classification", rename_all = "kebab-case")]
    pub(super) enum FindingClassificationUnchecked {
        FixNow {
            finding_summary: String,
            severity: Severity,
            affected_files: Vec<String>,
            #[serde(default)]
            remediation_hint: Option<String>,
        },
        PlannedElsewhere {
            finding_summary: String,
            mapped_to_bead_id: String,
            confidence: f64,
            rationale: String,
        },
        ProposeNewBead {
            finding_summary: String,
            proposed_title: String,
            proposed_scope: String,
            severity: Severity,
            rationale: String,
        },
    }

    impl From<FindingClassificationUnchecked> for super::FindingClassification {
        fn from(raw: FindingClassificationUnchecked) -> Self {
            match raw {
                FindingClassificationUnchecked::FixNow {
                    finding_summary,
                    severity,
                    affected_files,
                    remediation_hint,
                } => super::FindingClassification::FixNow {
                    finding_summary,
                    severity,
                    affected_files,
                    remediation_hint,
                },
                FindingClassificationUnchecked::PlannedElsewhere {
                    finding_summary,
                    mapped_to_bead_id,
                    confidence,
                    rationale,
                } => super::FindingClassification::PlannedElsewhere {
                    finding_summary,
                    mapped_to_bead_id,
                    confidence,
                    rationale,
                },
                FindingClassificationUnchecked::ProposeNewBead {
                    finding_summary,
                    proposed_title,
                    proposed_scope,
                    severity,
                    rationale,
                } => super::FindingClassification::ProposeNewBead {
                    finding_summary,
                    proposed_title,
                    proposed_scope,
                    severity,
                    rationale,
                },
            }
        }
    }
}

impl<'de> Deserialize<'de> for FindingClassification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let field_errors = validate_classification_fields(&value);
        if !field_errors.is_empty() {
            return Err(serde::de::Error::custom(field_errors.join("; ")));
        }
        let unchecked: unchecked::FindingClassificationUnchecked =
            serde_json::from_value(value).map_err(serde::de::Error::custom)?;
        Ok(unchecked.into())
    }
}

// ── Bead ID syntax ───────────────────────────────────────────────────

/// Validates that a string is a syntactically valid bead identifier.
///
/// Bead IDs consist of alphanumeric characters, hyphens, underscores,
/// and dots (for qualified forms like `milestone.bead-name`). They must
/// not start with a dot or hyphen, end with a dot, or contain
/// consecutive dots. This does NOT check whether the bead actually
/// exists — that happens at reconciliation time.
///
/// The character allowlist also prevents CLI injection: since bead IDs
/// are passed as positional args to `br show`, values like `--help`
/// would be misinterpreted as flags without this check.
fn validate_bead_id_syntax(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("must not be empty".to_owned());
    }
    if value != value.trim() {
        return Err(format!(
            "'{value}' must not contain leading or trailing whitespace"
        ));
    }
    if value.starts_with('-') {
        return Err(format!(
            "'{value}' must not start with a hyphen (would be interpreted as a CLI flag)"
        ));
    }
    if value.starts_with('.') {
        return Err(format!("'{value}' must not start with a dot"));
    }
    if value.ends_with('.') {
        return Err(format!("'{value}' must not end with a dot"));
    }
    if value.contains("..") {
        return Err(format!("'{value}' must not contain consecutive dots"));
    }
    // Bead IDs may only contain: a-z A-Z 0-9 . - _
    if let Some(ch) = value
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '.' && *c != '-' && *c != '_')
    {
        return Err(format!(
            "'{value}' contains invalid character '{ch}'; bead IDs may only contain \
             alphanumeric characters, hyphens, underscores, and dots"
        ));
    }
    Ok(())
}

// ── JSON-level field validation ──────────────────────────────────────

/// Expected fields per classification variant (excluding the tag itself).
const FIX_NOW_FIELDS: &[&str] = &[
    "finding_summary",
    "severity",
    "affected_files",
    "remediation_hint",
];
const PLANNED_ELSEWHERE_FIELDS: &[&str] = &[
    "finding_summary",
    "mapped_to_bead_id",
    "confidence",
    "rationale",
];
const PROPOSE_NEW_BEAD_FIELDS: &[&str] = &[
    "finding_summary",
    "proposed_title",
    "proposed_scope",
    "severity",
    "rationale",
];

/// Validates that a JSON object representing a classification does not
/// contain fields from a different variant.
///
/// Serde internally tagged enums silently discard unknown fields, so a
/// payload like `{"classification":"fix-now",...,"mapped_to_bead_id":"x"}`
/// would deserialize as `FixNow` and lose the planned-elsewhere field.
/// This function detects such cross-variant contamination at the JSON
/// level before or after deserialization.
///
/// Returns a list of errors. An empty vec means no unknown fields.
pub fn validate_classification_fields(value: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();

    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            errors.push("classification must be a JSON object".to_owned());
            return errors;
        }
    };

    let tag = match obj.get("classification").and_then(|v| v.as_str()) {
        Some(t) => t,
        None => {
            errors
                .push("classification object must have a 'classification' string field".to_owned());
            return errors;
        }
    };

    let allowed = match tag {
        "fix-now" => FIX_NOW_FIELDS,
        "planned-elsewhere" => PLANNED_ELSEWHERE_FIELDS,
        "propose-new-bead" => PROPOSE_NEW_BEAD_FIELDS,
        other => {
            errors.push(format!("unknown classification variant '{other}'"));
            return errors;
        }
    };

    for key in obj.keys() {
        if key == "classification" {
            continue;
        }
        if !allowed.contains(&key.as_str()) {
            errors.push(format!(
                "{tag}: unexpected field '{key}' (not valid for this classification)"
            ));
        }
    }

    errors
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
            affected_files,
            remediation_hint,
        } => {
            if finding_summary.trim().is_empty() {
                errors.push("fix_now: finding_summary must not be empty".to_owned());
            }
            for (i, file) in affected_files.iter().enumerate() {
                if file.trim().is_empty() {
                    errors.push(format!(
                        "fix_now: affected_files[{i}] must not be empty or whitespace-only"
                    ));
                }
            }
            if let Some(hint) = remediation_hint {
                if hint.trim().is_empty() {
                    errors.push(
                        "fix_now: remediation_hint must not be empty or whitespace-only when present"
                            .to_owned(),
                    );
                }
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
            if let Err(reason) = validate_bead_id_syntax(mapped_to_bead_id) {
                errors.push(format!("planned_elsewhere: mapped_to_bead_id {reason}"));
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
    fn classification_tag_field_uses_kebab_case() -> Result<(), serde_json::Error> {
        let fix_now = fix_now_valid();
        let json: serde_json::Value = serde_json::to_value(&fix_now)?;
        assert_eq!(json["classification"], "fix-now");

        let planned = planned_elsewhere_valid();
        let json: serde_json::Value = serde_json::to_value(&planned)?;
        assert_eq!(json["classification"], "planned-elsewhere");

        let propose = propose_new_bead_valid();
        let json: serde_json::Value = serde_json::to_value(&propose)?;
        assert_eq!(json["classification"], "propose-new-bead");
        Ok(())
    }

    #[test]
    fn classification_deserializes_from_kebab_case_tag() -> Result<(), serde_json::Error> {
        let json = r#"{"classification":"fix-now","finding_summary":"test","severity":"high","affected_files":[]}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::FixNow { .. }));

        let json = r#"{"classification":"planned-elsewhere","finding_summary":"test","mapped_to_bead_id":"m1.b1","confidence":0.5,"rationale":"r"}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::PlannedElsewhere { .. }));

        let json = r#"{"classification":"propose-new-bead","finding_summary":"test","proposed_title":"t","proposed_scope":"s","severity":"low","rationale":"r"}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::ProposeNewBead { .. }));
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

    // ── Validation: affected_files entries ─────────────────────────

    #[test]
    fn fix_now_rejects_empty_affected_file_entry() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::High,
            affected_files: vec!["src/valid.rs".to_owned(), "".to_owned()],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("affected_files[1]"));
        Ok(())
    }

    #[test]
    fn fix_now_rejects_whitespace_only_affected_file_entry(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::Medium,
            affected_files: vec!["  ".to_owned()],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("affected_files[0]"));
        Ok(())
    }

    #[test]
    fn fix_now_accepts_empty_affected_files_vec() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::Low,
            affected_files: vec![],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn fix_now_reports_multiple_bad_affected_files() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::High,
            affected_files: vec!["".to_owned(), "  ".to_owned(), "valid.rs".to_owned()],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("affected_files[0]"));
        assert!(errors[1].contains("affected_files[1]"));
        Ok(())
    }

    // ── Validation: bead ID syntax ──────────────────────────────────

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_leading_whitespace(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: " 9ni.8.5.2".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("whitespace")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_trailing_whitespace(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "9ni.8.5.2 ".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_internal_space(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "bad id".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("invalid character")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_path_traversal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "../bad".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_forward_slash(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "some/path".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("invalid character")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_backslash() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "some\\path".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_starting_with_dot(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: ".hidden-bead".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("dot")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_trailing_dot(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "9ni.".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("end with a dot")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_bare_word_trailing_dot(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "bead.".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_consecutive_dots(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "a..b".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("consecutive dots")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_qualified_bead_id() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "m1.error-handling".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_simple_bead_id() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "bead-42".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_deeply_qualified_bead_id() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "9ni.8.5.2".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    // ── Validation: remediation_hint ───────────────────────────────

    #[test]
    fn fix_now_rejects_empty_remediation_hint() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::High,
            affected_files: vec![],
            remediation_hint: Some("".to_owned()),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("remediation_hint"));
        Ok(())
    }

    #[test]
    fn fix_now_rejects_whitespace_only_remediation_hint() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::Medium,
            affected_files: vec![],
            remediation_hint: Some("   ".to_owned()),
        };
        let errors = validate_classification(&c);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("remediation_hint"));
        Ok(())
    }

    #[test]
    fn fix_now_accepts_none_remediation_hint() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::Low,
            affected_files: vec![],
            remediation_hint: None,
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn fix_now_accepts_valid_remediation_hint() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::FixNow {
            finding_summary: "Valid summary".to_owned(),
            severity: Severity::High,
            affected_files: vec![],
            remediation_hint: Some("Add guard clause".to_owned()),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    // ── JSON-level cross-variant field detection ────────────────────

    #[test]
    fn cross_variant_field_on_fix_now_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"classification":"fix-now","finding_summary":"s","severity":"high","affected_files":[],"mapped_to_bead_id":"bead-2"}"#,
        )?;
        let errors = validate_classification_fields(&json);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("mapped_to_bead_id"));
        assert!(errors[0].contains("not valid for this classification"));
        Ok(())
    }

    #[test]
    fn cross_variant_field_on_planned_elsewhere_is_rejected(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"classification":"planned-elsewhere","finding_summary":"s","mapped_to_bead_id":"m1.b1","confidence":0.5,"rationale":"r","proposed_title":"leaked"}"#,
        )?;
        let errors = validate_classification_fields(&json);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("proposed_title"));
        Ok(())
    }

    #[test]
    fn cross_variant_field_on_propose_new_bead_is_rejected(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"classification":"propose-new-bead","finding_summary":"s","proposed_title":"t","proposed_scope":"s","severity":"low","rationale":"r","confidence":0.9}"#,
        )?;
        let errors = validate_classification_fields(&json);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("confidence"));
        Ok(())
    }

    #[test]
    fn multiple_cross_variant_fields_all_reported() -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{"classification":"fix-now","finding_summary":"s","severity":"high","affected_files":[],"mapped_to_bead_id":"x","confidence":0.5,"proposed_title":"y"}"#,
        )?;
        let errors = validate_classification_fields(&json);
        assert_eq!(errors.len(), 3);
        Ok(())
    }

    #[test]
    fn clean_fix_now_json_passes_field_validation() -> Result<(), Box<dyn std::error::Error>> {
        let c = fix_now_valid();
        let json = serde_json::to_value(&c)?;
        let errors = validate_classification_fields(&json);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn clean_planned_elsewhere_json_passes_field_validation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = planned_elsewhere_valid();
        let json = serde_json::to_value(&c)?;
        let errors = validate_classification_fields(&json);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn clean_propose_new_bead_json_passes_field_validation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = propose_new_bead_valid();
        let json = serde_json::to_value(&c)?;
        let errors = validate_classification_fields(&json);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn unknown_classification_variant_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value =
            serde_json::from_str(r#"{"classification":"unknown","some_field":"v"}"#)?;
        let errors = validate_classification_fields(&json);
        assert!(errors.iter().any(|e| e.contains("unknown classification")));
        Ok(())
    }

    #[test]
    fn non_object_json_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let json: serde_json::Value = serde_json::from_str(r#""just a string""#)?;
        let errors = validate_classification_fields(&json);
        assert!(errors.iter().any(|e| e.contains("must be a JSON object")));
        Ok(())
    }

    // ── Deserialization rejects cross-variant contamination ──────────

    #[test]
    fn deserialization_rejects_cross_variant_field_contamination(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"classification":"fix-now","finding_summary":"s","severity":"high","affected_files":[],"mapped_to_bead_id":"bead-2"}"#;
        let result = serde_json::from_str::<FindingClassification>(json);
        assert!(
            result.is_err(),
            "should reject cross-variant field during deserialization"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("mapped_to_bead_id"),
            "error should mention the cross-variant field: {err}"
        );
        Ok(())
    }

    #[test]
    fn deserialization_rejects_multiple_cross_variant_fields(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"classification":"fix-now","finding_summary":"s","severity":"high","affected_files":[],"mapped_to_bead_id":"x","confidence":0.5}"#;
        let result = serde_json::from_str::<FindingClassification>(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mapped_to_bead_id"), "error: {err}");
        assert!(err.contains("confidence"), "error: {err}");
        Ok(())
    }

    #[test]
    fn deserialization_accepts_clean_fix_now() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"classification":"fix-now","finding_summary":"s","severity":"high","affected_files":["src/a.rs"]}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::FixNow { .. }));
        Ok(())
    }

    #[test]
    fn deserialization_accepts_clean_planned_elsewhere() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"classification":"planned-elsewhere","finding_summary":"s","mapped_to_bead_id":"m1.b1","confidence":0.9,"rationale":"r"}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::PlannedElsewhere { .. }));
        Ok(())
    }

    #[test]
    fn deserialization_accepts_clean_propose_new_bead() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"classification":"propose-new-bead","finding_summary":"s","proposed_title":"t","proposed_scope":"s","severity":"low","rationale":"r"}"#;
        let c: FindingClassification = serde_json::from_str(json)?;
        assert!(matches!(c, FindingClassification::ProposeNewBead { .. }));
        Ok(())
    }

    // ── Bead ID: character allowlist ──────────────────────────────────

    #[test]
    fn planned_elsewhere_rejects_bead_id_starting_with_hyphen(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "--help".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("hyphen")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_colon() -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "scope:bead".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("invalid character")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_rejects_bead_id_with_question_mark(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "bead?x".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.iter().any(|e| e.contains("mapped_to_bead_id")));
        assert!(errors.iter().any(|e| e.contains("invalid character")));
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_bead_id_with_underscore() -> Result<(), Box<dyn std::error::Error>>
    {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "my_bead_id".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_accepts_bead_id_with_internal_hyphen(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let c = FindingClassification::PlannedElsewhere {
            finding_summary: "Valid".to_owned(),
            mapped_to_bead_id: "error-handling".to_owned(),
            confidence: 0.9,
            rationale: "Valid".to_owned(),
        };
        let errors = validate_classification(&c);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
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
