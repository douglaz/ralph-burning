#![forbid(unsafe_code)]

//! High-level read/query view models for milestone controller decisions.
//!
//! These types combine data from `br` (bead graph) and `bv` (bead viewer)
//! into enriched views that the milestone controller can consume directly,
//! without needing to correlate adapter-level structs at call sites.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use crate::adapters::br_models::{BeadPriority, BeadStatus, BeadType, DependencyRef};
use crate::contexts::milestone_record::model::TaskRunOutcome;
use crate::contexts::project_run_record::model::ProjectStatusSummary;
use crate::shared::domain::FlowPreset;

// ── BeadSummaryView ──────────────────────────────────────────────────────

/// Minimal bead view for listing and cross-referencing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadSummaryView {
    pub id: String,
    pub title: String,
    pub status: BeadStatus,
    pub priority: BeadPriority,
}

// ── BeadReadView ─────────────────────────────────────────────────────────

/// Enriched view of a single bead combining `br show` detail with
/// computed readiness and blocking state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadReadView {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: BeadStatus,
    pub priority: BeadPriority,
    pub bead_type: BeadType,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<DependencyRef>,
    #[serde(default)]
    pub dependents: Vec<DependencyRef>,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    /// Whether all blocking dependencies are resolved.
    pub is_ready: bool,
    /// Whether this bead is blocked by at least one unresolved dependency.
    pub is_blocked: bool,
    /// IDs of beads that block this one (empty when `is_blocked` is false).
    #[serde(default)]
    pub blocker_ids: Vec<String>,
}

// ── ReadyWorkView ────────────────────────────────────────────────────────

/// The set of beads currently available for execution, with aggregate counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadyWorkView {
    /// Beads that are unblocked and ready for work.
    pub ready_beads: Vec<BeadReadView>,
    /// Total number of open (non-terminal) beads in the milestone.
    pub total_open: u32,
    /// Total number of blocked beads in the milestone.
    pub total_blocked: u32,
}

// ── BeadRecommendation ───────────────────────────────────────────────────

/// A single bead recommendation with scoring and reasoning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadRecommendation {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub action: String,
    /// IDs of beads that block this recommendation.
    #[serde(default)]
    pub blocked_by: Vec<String>,
}

// ── Existing-Bead Matching ─────────────────────────────────────────────────

/// Jaccard similarity for reviewer-proposed bead text against candidate bead
/// ownership text. The parser intentionally uses only ASCII alphanumeric
/// tokens so the score is deterministic and easy to inspect in tests.
pub fn bead_ownership_text_similarity(left: &str, right: &str) -> f64 {
    let left_tokens = text_tokens(left);
    let right_tokens = text_tokens(right);
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }

    let intersection = left_tokens.intersection(&right_tokens).count();
    let union = left_tokens.union(&right_tokens).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn text_tokens(value: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let mut current = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.insert(current);
    }
    tokens
}

// ── TriageView ───────────────────────────────────────────────────────────

/// Actionable triage summary for the milestone controller, combining
/// `bv --robot-triage` and `bv --robot-next` outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriageView {
    /// The single best bead to work on next, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_pick: Option<BeadReadView>,
    /// Ordered list of recommended beads with reasoning.
    #[serde(default)]
    pub recommendations: Vec<BeadRecommendation>,
    /// Small, low-effort beads that can be knocked out quickly.
    #[serde(default)]
    pub quick_wins: Vec<BeadRecommendation>,
}

// ── PlannedElsewhereView ─────────────────────────────────────────────────

/// Beads that are related to the current milestone but belong to other
/// milestones or contexts. Useful for cross-milestone dependency awareness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlannedElsewhereView {
    /// Beads from other milestones that interact with the current one.
    #[serde(default)]
    pub related_beads: Vec<BeadSummaryView>,
}

// ── Lineage Queries ───────────────────────────────────────────────────────

/// Stable lineage metadata for a milestone bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadLineageView {
    pub milestone_id: String,
    pub milestone_name: String,
    pub bead_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_title: Option<String>,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
}

/// A single bead execution attempt with computed duration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRunAttemptView {
    pub milestone_id: String,
    pub bead_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    pub outcome: TaskRunOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_detail: Option<String>,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

/// Execution history for a bead, including retries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadExecutionHistoryView {
    pub lineage: BeadLineageView,
    #[serde(default)]
    pub runs: Vec<TaskRunAttemptView>,
}

/// A Ralph task linked to a milestone bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MilestoneTaskView {
    pub project_id: String,
    pub project_name: String,
    pub flow: FlowPreset,
    pub status_summary: ProjectStatusSummary,
    pub created_at: DateTime<Utc>,
    pub bead_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_title: Option<String>,
}

/// All Ralph tasks currently linked to a milestone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MilestoneTaskListView {
    pub milestone_id: String,
    pub milestone_name: String,
    #[serde(default)]
    pub tasks: Vec<MilestoneTaskView>,
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_models::DependencyKind;
    use crate::contexts::milestone_record::model::TaskRunOutcome;
    use crate::contexts::project_run_record::model::ProjectStatusSummary;
    use crate::shared::domain::FlowPreset;

    // ── helpers ──────────────────────────────────────────────────────

    fn sample_summary() -> BeadSummaryView {
        BeadSummaryView {
            id: "ms-1.bead-3".to_owned(),
            title: "Add logging".to_owned(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(2),
        }
    }

    fn sample_read_view(ready: bool, blocked: bool, blockers: Vec<String>) -> BeadReadView {
        BeadReadView {
            id: "ms-1.bead-1".to_owned(),
            title: "Create query models".to_owned(),
            description: Some("High-level views for milestone data".to_owned()),
            status: BeadStatus::Open,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Task,
            labels: vec!["core".to_owned(), "models".to_owned()],
            dependencies: vec![DependencyRef {
                id: "ms-1.bead-0".to_owned(),
                kind: DependencyKind::Blocks,
                title: Some("Setup project".to_owned()),
                status: None,
            }],
            dependents: vec![DependencyRef {
                id: "ms-1.bead-2".to_owned(),
                kind: DependencyKind::Blocks,
                title: None,
                status: None,
            }],
            acceptance_criteria: vec![
                "All types derive Debug, Clone, PartialEq".to_owned(),
                "Comprehensive serialization tests".to_owned(),
            ],
            is_ready: ready,
            is_blocked: blocked,
            blocker_ids: blockers,
        }
    }

    fn sample_recommendation() -> BeadRecommendation {
        BeadRecommendation {
            id: "ms-1.bead-4".to_owned(),
            title: "Implement caching layer".to_owned(),
            score: 8.5,
            reasons: vec![
                "unblocks 3 downstream beads".to_owned(),
                "well-scoped".to_owned(),
            ],
            action: "implement".to_owned(),
            blocked_by: vec![],
        }
    }

    #[test]
    fn bead_ownership_text_similarity_scores_token_overlap() {
        let score = bead_ownership_text_similarity(
            "Retry telemetry Instrument retry loops with counters",
            "Add retry telemetry. Instrument retry loops with counters and histograms.",
        );

        assert!(score >= 0.65, "expected strong overlap, got {score}");
        assert_eq!(0.0, bead_ownership_text_similarity("", "something"));
    }

    fn sample_bead_lineage() -> BeadLineageView {
        BeadLineageView {
            milestone_id: "ms-1".to_owned(),
            milestone_name: "Milestone 1".to_owned(),
            bead_id: "ms-1.bead-1".to_owned(),
            bead_title: Some("Create query models".to_owned()),
            acceptance_criteria: vec!["Queries return lineage".to_owned()],
        }
    }

    fn sample_task_run_attempt() -> TaskRunAttemptView {
        TaskRunAttemptView {
            milestone_id: "ms-1".to_owned(),
            bead_id: "ms-1.bead-1".to_owned(),
            project_id: "task-alpha".to_owned(),
            run_id: Some("run-1".to_owned()),
            plan_hash: Some("plan-hash".to_owned()),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("completed".to_owned()),
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            duration_ms: Some(250),
            task_id: Some("task-1".to_owned()),
        }
    }

    // ── BeadSummaryView ─────────────────────────────────────────────

    #[test]
    fn summary_view_construction() -> Result<(), Box<dyn std::error::Error>> {
        let view = sample_summary();
        assert_eq!(view.id, "ms-1.bead-3");
        assert_eq!(view.title, "Add logging");
        assert_eq!(view.status, BeadStatus::Open);
        assert_eq!(view.priority.value(), 2);
        Ok(())
    }

    #[test]
    fn summary_view_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let view = sample_summary();
        let json = serde_json::to_string(&view)?;
        let parsed: BeadSummaryView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    #[test]
    fn summary_view_from_json() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-7",
            "title": "Write docs",
            "status": "in_progress",
            "priority": 0
        }"#;
        let view: BeadSummaryView = serde_json::from_str(json)?;
        assert_eq!(view.id, "bead-7");
        assert_eq!(view.status, BeadStatus::InProgress);
        assert_eq!(view.priority.value(), 0);
        Ok(())
    }

    // ── BeadReadView ────────────────────────────────────────────────

    #[test]
    fn read_view_ready_construction() -> Result<(), Box<dyn std::error::Error>> {
        let view = sample_read_view(true, false, vec![]);
        assert!(view.is_ready);
        assert!(!view.is_blocked);
        assert!(view.blocker_ids.is_empty());
        assert_eq!(view.acceptance_criteria.len(), 2);
        assert_eq!(view.labels, vec!["core", "models"]);
        Ok(())
    }

    #[test]
    fn read_view_blocked_construction() -> Result<(), Box<dyn std::error::Error>> {
        let view = sample_read_view(
            false,
            true,
            vec!["ms-1.bead-0".to_owned(), "ms-1.bead-9".to_owned()],
        );
        assert!(!view.is_ready);
        assert!(view.is_blocked);
        assert_eq!(view.blocker_ids.len(), 2);
        assert_eq!(view.blocker_ids[0], "ms-1.bead-0");
        Ok(())
    }

    #[test]
    fn read_view_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let view = sample_read_view(true, false, vec![]);
        let json = serde_json::to_string_pretty(&view)?;
        let parsed: BeadReadView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    #[test]
    fn read_view_minimal_json() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-min",
            "title": "Minimal",
            "status": "open",
            "priority": 3,
            "bead_type": "chore",
            "is_ready": true,
            "is_blocked": false
        }"#;
        let view: BeadReadView = serde_json::from_str(json)?;
        assert_eq!(view.id, "bead-min");
        assert!(view.description.is_none());
        assert!(view.labels.is_empty());
        assert!(view.dependencies.is_empty());
        assert!(view.dependents.is_empty());
        assert!(view.acceptance_criteria.is_empty());
        assert!(view.blocker_ids.is_empty());
        Ok(())
    }

    #[test]
    fn read_view_description_omitted_when_none() -> Result<(), Box<dyn std::error::Error>> {
        let mut view = sample_read_view(true, false, vec![]);
        view.description = None;
        let json = serde_json::to_string(&view)?;
        assert!(!json.contains("description"));
        Ok(())
    }

    // ── ReadyWorkView ───────────────────────────────────────────────

    #[test]
    fn ready_work_view_construction() -> Result<(), Box<dyn std::error::Error>> {
        let view = ReadyWorkView {
            ready_beads: vec![
                sample_read_view(true, false, vec![]),
                sample_read_view(true, false, vec![]),
            ],
            total_open: 10,
            total_blocked: 3,
        };
        assert_eq!(view.ready_beads.len(), 2);
        assert_eq!(view.total_open, 10);
        assert_eq!(view.total_blocked, 3);
        Ok(())
    }

    #[test]
    fn ready_work_view_empty() -> Result<(), Box<dyn std::error::Error>> {
        let view = ReadyWorkView {
            ready_beads: vec![],
            total_open: 0,
            total_blocked: 0,
        };
        let json = serde_json::to_string(&view)?;
        let parsed: ReadyWorkView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        assert!(parsed.ready_beads.is_empty());
        Ok(())
    }

    #[test]
    fn ready_work_view_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let view = ReadyWorkView {
            ready_beads: vec![sample_read_view(true, false, vec![])],
            total_open: 5,
            total_blocked: 2,
        };
        let json = serde_json::to_string_pretty(&view)?;
        let parsed: ReadyWorkView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    // ── BeadRecommendation ──────────────────────────────────────────

    #[test]
    fn recommendation_construction() -> Result<(), Box<dyn std::error::Error>> {
        let rec = sample_recommendation();
        assert_eq!(rec.id, "ms-1.bead-4");
        assert!((rec.score - 8.5).abs() < f64::EPSILON);
        assert_eq!(rec.reasons.len(), 2);
        assert_eq!(rec.action, "implement");
        assert!(rec.blocked_by.is_empty());
        Ok(())
    }

    #[test]
    fn recommendation_with_blockers() -> Result<(), Box<dyn std::error::Error>> {
        let rec = BeadRecommendation {
            id: "bead-blocked".to_owned(),
            title: "Blocked feature".to_owned(),
            score: 3.0,
            reasons: vec!["important but blocked".to_owned()],
            action: "wait".to_owned(),
            blocked_by: vec!["bead-a".to_owned(), "bead-b".to_owned()],
        };
        assert_eq!(rec.blocked_by.len(), 2);
        Ok(())
    }

    #[test]
    fn recommendation_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let rec = sample_recommendation();
        let json = serde_json::to_string(&rec)?;
        let parsed: BeadRecommendation = serde_json::from_str(&json)?;
        assert_eq!(parsed, rec);
        Ok(())
    }

    #[test]
    fn recommendation_minimal_json() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"id":"r-1","title":"Quick task"}"#;
        let rec: BeadRecommendation = serde_json::from_str(json)?;
        assert_eq!(rec.id, "r-1");
        assert!((rec.score - 0.0).abs() < f64::EPSILON);
        assert!(rec.reasons.is_empty());
        assert!(rec.action.is_empty());
        assert!(rec.blocked_by.is_empty());
        Ok(())
    }

    // ── TriageView ──────────────────────────────────────────────────

    #[test]
    fn triage_view_with_top_pick() -> Result<(), Box<dyn std::error::Error>> {
        let view = TriageView {
            top_pick: Some(sample_read_view(true, false, vec![])),
            recommendations: vec![sample_recommendation()],
            quick_wins: vec![BeadRecommendation {
                id: "qw-1".to_owned(),
                title: "Fix typo".to_owned(),
                score: 2.0,
                reasons: vec!["trivial fix".to_owned()],
                action: "fix".to_owned(),
                blocked_by: vec![],
            }],
        };
        assert!(view.top_pick.is_some());
        assert_eq!(view.recommendations.len(), 1);
        assert_eq!(view.quick_wins.len(), 1);
        Ok(())
    }

    #[test]
    fn triage_view_no_top_pick() -> Result<(), Box<dyn std::error::Error>> {
        let view = TriageView {
            top_pick: None,
            recommendations: vec![],
            quick_wins: vec![],
        };
        assert!(view.top_pick.is_none());
        assert!(view.recommendations.is_empty());
        assert!(view.quick_wins.is_empty());
        Ok(())
    }

    #[test]
    fn triage_view_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let view = TriageView {
            top_pick: Some(sample_read_view(true, false, vec![])),
            recommendations: vec![sample_recommendation()],
            quick_wins: vec![],
        };
        let json = serde_json::to_string_pretty(&view)?;
        let parsed: TriageView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    #[test]
    fn triage_view_top_pick_omitted_when_none() -> Result<(), Box<dyn std::error::Error>> {
        let view = TriageView {
            top_pick: None,
            recommendations: vec![],
            quick_wins: vec![],
        };
        let json = serde_json::to_string(&view)?;
        assert!(!json.contains("top_pick"));
        Ok(())
    }

    // ── PlannedElsewhereView ────────────────────────────────────────

    #[test]
    fn planned_elsewhere_view_construction() -> Result<(), Box<dyn std::error::Error>> {
        let view = PlannedElsewhereView {
            related_beads: vec![
                sample_summary(),
                BeadSummaryView {
                    id: "other-ms.bead-1".to_owned(),
                    title: "External dependency".to_owned(),
                    status: BeadStatus::InProgress,
                    priority: BeadPriority::new(0),
                },
            ],
        };
        assert_eq!(view.related_beads.len(), 2);
        assert_eq!(view.related_beads[1].id, "other-ms.bead-1");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_view_empty() -> Result<(), Box<dyn std::error::Error>> {
        let view = PlannedElsewhereView {
            related_beads: vec![],
        };
        let json = serde_json::to_string(&view)?;
        let parsed: PlannedElsewhereView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        assert!(parsed.related_beads.is_empty());
        Ok(())
    }

    #[test]
    fn planned_elsewhere_view_json_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let view = PlannedElsewhereView {
            related_beads: vec![sample_summary()],
        };
        let json = serde_json::to_string_pretty(&view)?;
        let parsed: PlannedElsewhereView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    // ── Cross-type consistency ──────────────────────────────────────

    #[test]
    fn all_types_are_debug_clone() -> Result<(), Box<dyn std::error::Error>> {
        // Verify Debug
        let summary = sample_summary();
        let _ = format!("{summary:?}");

        let read = sample_read_view(true, false, vec![]);
        let _ = format!("{read:?}");

        let rec = sample_recommendation();
        let _ = format!("{rec:?}");

        let triage = TriageView {
            top_pick: None,
            recommendations: vec![],
            quick_wins: vec![],
        };
        let _ = format!("{triage:?}");

        let planned = PlannedElsewhereView {
            related_beads: vec![],
        };
        let _ = format!("{planned:?}");

        let ready = ReadyWorkView {
            ready_beads: vec![],
            total_open: 0,
            total_blocked: 0,
        };
        let _ = format!("{ready:?}");

        // Verify Clone
        let _summary_clone = summary.clone();
        let _read_clone = read.clone();
        let _rec_clone = rec.clone();
        let _triage_clone = triage.clone();
        let _planned_clone = planned.clone();
        let _ready_clone = ready.clone();

        Ok(())
    }

    #[test]
    fn all_types_are_partial_eq() -> Result<(), Box<dyn std::error::Error>> {
        let a = sample_summary();
        let b = sample_summary();
        assert_eq!(a, b);

        let mut c = sample_summary();
        c.id = "different".to_owned();
        assert_ne!(a, c);

        Ok(())
    }

    #[test]
    fn recommendation_score_precision() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"id":"r-1","title":"Precise","score":7.123456789}"#;
        let rec: BeadRecommendation = serde_json::from_str(json)?;
        assert!((rec.score - 7.123_456_789).abs() < f64::EPSILON);

        let serialized = serde_json::to_string(&rec)?;
        let reparsed: BeadRecommendation = serde_json::from_str(&serialized)?;
        assert!((reparsed.score - rec.score).abs() < f64::EPSILON);
        Ok(())
    }

    #[test]
    fn read_view_all_statuses() -> Result<(), Box<dyn std::error::Error>> {
        for status in [
            BeadStatus::Open,
            BeadStatus::InProgress,
            BeadStatus::Closed,
            BeadStatus::Deferred,
        ] {
            let view = BeadReadView {
                id: "test".to_owned(),
                title: "Test".to_owned(),
                description: None,
                status: status.clone(),
                priority: BeadPriority::new(2),
                bead_type: BeadType::Task,
                labels: vec![],
                dependencies: vec![],
                dependents: vec![],
                acceptance_criteria: vec![],
                is_ready: false,
                is_blocked: false,
                blocker_ids: vec![],
            };
            let json = serde_json::to_string(&view)?;
            let parsed: BeadReadView = serde_json::from_str(&json)?;
            assert_eq!(parsed.status, status);
        }
        Ok(())
    }

    #[test]
    fn read_view_known_bead_types() -> Result<(), Box<dyn std::error::Error>> {
        for bead_type in [
            BeadType::Task,
            BeadType::Bug,
            BeadType::Feature,
            BeadType::Epic,
            BeadType::Chore,
            BeadType::Docs,
            BeadType::Question,
            BeadType::Spike,
            BeadType::Meta,
        ] {
            let view = BeadReadView {
                id: "test".to_owned(),
                title: "Test".to_owned(),
                description: None,
                status: BeadStatus::Open,
                priority: BeadPriority::new(0),
                bead_type: bead_type.clone(),
                labels: vec![],
                dependencies: vec![],
                dependents: vec![],
                acceptance_criteria: vec![],
                is_ready: true,
                is_blocked: false,
                blocker_ids: vec![],
            };
            let json = serde_json::to_string(&view)?;
            let parsed: BeadReadView = serde_json::from_str(&json)?;
            assert_eq!(parsed.bead_type, bead_type);
        }
        Ok(())
    }

    #[test]
    fn read_view_other_bead_type_from_json() -> Result<(), Box<dyn std::error::Error>> {
        // BeadType::Other deserializes from a plain string (custom Deserialize impl)
        let json = r#"{
            "id": "test",
            "title": "Test",
            "status": "open",
            "priority": 0,
            "bead_type": "custom_type",
            "is_ready": true,
            "is_blocked": false
        }"#;
        let view: BeadReadView = serde_json::from_str(json)?;
        assert_eq!(view.bead_type, BeadType::Other("custom_type".to_owned()));
        Ok(())
    }

    #[test]
    fn bead_execution_history_view_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let view = BeadExecutionHistoryView {
            lineage: sample_bead_lineage(),
            runs: vec![sample_task_run_attempt()],
        };

        let json = serde_json::to_string(&view)?;
        let parsed: BeadExecutionHistoryView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }

    #[test]
    fn milestone_task_list_view_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let view = MilestoneTaskListView {
            milestone_id: "ms-1".to_owned(),
            milestone_name: "Milestone 1".to_owned(),
            tasks: vec![MilestoneTaskView {
                project_id: "task-alpha".to_owned(),
                project_name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                status_summary: ProjectStatusSummary::Active,
                created_at: chrono::Utc::now(),
                bead_id: "ms-1.bead-1".to_owned(),
                bead_title: Some("Create query models".to_owned()),
            }],
        };

        let json = serde_json::to_string(&view)?;
        let parsed: MilestoneTaskListView = serde_json::from_str(&json)?;
        assert_eq!(parsed, view);
        Ok(())
    }
}
