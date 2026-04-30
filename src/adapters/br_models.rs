#![forbid(unsafe_code)]

//! Typed Rust structs for parsing `br` (beads_rust) JSON output.
//!
//! Each struct maps to a specific `br` subcommand's JSON output format.
//! All types are forward-compatible: unknown fields are silently ignored
//! via `#[serde(default)]` on collection/optional fields.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize};

// ── BeadStatus ──────────────────────────────────────────────────────────────

/// Lifecycle status of a bead.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadStatus {
    Open,
    InProgress,
    Closed,
    Deferred,
}

impl fmt::Display for BeadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "open"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Closed => write!(f, "closed"),
            Self::Deferred => write!(f, "deferred"),
        }
    }
}

impl FromStr for BeadStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Self::Open),
            "in_progress" => Ok(Self::InProgress),
            "closed" => Ok(Self::Closed),
            "deferred" => Ok(Self::Deferred),
            other => Err(format!("unknown bead status: {other}")),
        }
    }
}

// ── BeadPriority ────────────────────────────────────────────────────────────

/// Priority level (0-4) where 0 is highest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct BeadPriority(u32);

impl BeadPriority {
    /// Create a new priority, clamping to the 0-4 range.
    pub fn new(value: u32) -> Self {
        Self(value.min(4))
    }

    /// Return the raw numeric value.
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for BeadPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "P{}", self.0)
    }
}

impl FromStr for BeadPriority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept "P2", "p2", or plain "2"
        let numeric = s
            .strip_prefix('P')
            .or_else(|| s.strip_prefix('p'))
            .unwrap_or(s);
        let value: u32 = numeric
            .parse()
            .map_err(|_| format!("invalid priority: {s}"))?;
        if value > 4 {
            return Err(format!("priority out of range (0-4): {value}"));
        }
        Ok(Self(value))
    }
}

impl<'de> Deserialize<'de> for BeadPriority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PriorityVisitor;

        impl<'de> de::Visitor<'de> for PriorityVisitor {
            type Value = BeadPriority;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an integer 0-4 or a string like \"P2\" or \"2\"")
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<BeadPriority, E> {
                if v > 4 {
                    return Err(E::custom(format!("priority out of range (0-4): {v}")));
                }
                Ok(BeadPriority(v as u32))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<BeadPriority, E> {
                if !(0..=4).contains(&v) {
                    return Err(E::custom(format!("priority out of range (0-4): {v}")));
                }
                Ok(BeadPriority(v as u32))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<BeadPriority, E> {
                BeadPriority::from_str(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(PriorityVisitor)
    }
}

// ── BeadType ────────────────────────────────────────────────────────────────

/// Categorization of a bead's purpose.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadType {
    Task,
    Bug,
    Feature,
    Epic,
    Chore,
    Docs,
    Question,
    Spike,
    Meta,
    /// Catch-all for forward compatibility with new bead types.
    Other(String),
}

impl fmt::Display for BeadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task => write!(f, "task"),
            Self::Bug => write!(f, "bug"),
            Self::Feature => write!(f, "feature"),
            Self::Epic => write!(f, "epic"),
            Self::Chore => write!(f, "chore"),
            Self::Docs => write!(f, "docs"),
            Self::Question => write!(f, "question"),
            Self::Spike => write!(f, "spike"),
            Self::Meta => write!(f, "meta"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

impl<'de> Deserialize<'de> for BeadType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "task" => Self::Task,
            "bug" => Self::Bug,
            "feature" => Self::Feature,
            "epic" => Self::Epic,
            "chore" => Self::Chore,
            "docs" => Self::Docs,
            "question" => Self::Question,
            "spike" => Self::Spike,
            "meta" => Self::Meta,
            _ => Self::Other(s),
        })
    }
}

// ── DependencyKind ──────────────────────────────────────────────────────────

/// How two beads are related.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    Blocks,
    ParentChild,
}

impl fmt::Display for DependencyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blocks => write!(f, "blocks"),
            Self::ParentChild => write!(f, "parent_child"),
        }
    }
}

impl<'de> Deserialize<'de> for DependencyKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.as_str() {
            "blocks" => Ok(Self::Blocks),
            "parent_child" | "parent-child" => Ok(Self::ParentChild),
            other => Err(de::Error::unknown_variant(
                other,
                &["blocks", "parent_child", "parent-child"],
            )),
        }
    }
}

// ── DependencyRef ───────────────────────────────────────────────────────────

/// A reference to a dependency or dependent bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyRef {
    pub id: String,
    #[serde(alias = "dependency_type")]
    pub kind: DependencyKind,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<BeadStatus>,
}

/// A comment attached to a bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadComment {
    pub id: u64,
    pub issue_id: String,
    pub author: String,
    pub text: String,
    #[serde(default)]
    pub created_at: Option<String>,
}

// ── BeadSummary ─────────────────────────────────────────────────────────────

/// Summary view of a bead, as returned by `br list --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadSummary {
    pub id: String,
    pub title: String,
    pub status: BeadStatus,
    pub priority: BeadPriority,
    #[serde(alias = "issue_type")]
    pub bead_type: BeadType,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<DependencyRef>,
    #[serde(default)]
    pub dependents: Vec<DependencyRef>,
}

// ── BeadDetail ──────────────────────────────────────────────────────────────

/// Full detail view of a bead, as returned by `br show <id> --json`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BeadDetail {
    pub id: String,
    pub title: String,
    pub status: BeadStatus,
    pub priority: BeadPriority,
    #[serde(alias = "issue_type")]
    pub bead_type: BeadType,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, deserialize_with = "deserialize_acceptance_criteria")]
    pub acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<DependencyRef>,
    #[serde(default)]
    pub dependents: Vec<DependencyRef>,
    #[serde(default)]
    pub comments: Vec<BeadComment>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RawBeadDetail {
    id: String,
    title: String,
    status: BeadStatus,
    priority: BeadPriority,
    #[serde(alias = "issue_type")]
    bead_type: BeadType,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, deserialize_with = "deserialize_acceptance_criteria")]
    acceptance_criteria: Vec<String>,
    #[serde(default)]
    dependencies: Vec<DependencyRef>,
    #[serde(default)]
    dependents: Vec<DependencyRef>,
    #[serde(default)]
    comments: Vec<BeadComment>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawBeadShowResponse {
    Single(Box<RawBeadDetail>),
    Many(Vec<RawBeadDetail>),
}

impl From<RawBeadDetail> for BeadDetail {
    fn from(value: RawBeadDetail) -> Self {
        Self {
            id: value.id,
            title: value.title,
            status: value.status,
            priority: value.priority,
            bead_type: value.bead_type,
            labels: value.labels,
            description: value.description,
            acceptance_criteria: value.acceptance_criteria,
            dependencies: value.dependencies,
            dependents: value.dependents,
            comments: value.comments,
            owner: value.owner,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

impl<'de> Deserialize<'de> for BeadDetail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match RawBeadShowResponse::deserialize(deserializer)? {
            RawBeadShowResponse::Single(detail) => Ok((*detail).into()),
            RawBeadShowResponse::Many(mut details) => match details.len() {
                1 => Ok(details
                    .pop()
                    .expect("len=1 guarantees a detail is present")
                    .into()),
                count => Err(de::Error::custom(format!(
                    "expected one bead detail, got {count}"
                ))),
            },
        }
    }
}

// ── ReadyBead ───────────────────────────────────────────────────────────────

/// A bead that is ready for work, as returned by `br ready --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadyBead {
    pub id: String,
    pub title: String,
    pub priority: BeadPriority,
    #[serde(alias = "issue_type")]
    pub bead_type: BeadType,
    #[serde(default)]
    pub labels: Vec<String>,
}

fn deserialize_acceptance_criteria<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawAcceptanceCriteria {
        List(Vec<String>),
        Text(String),
    }

    let raw = Option::<RawAcceptanceCriteria>::deserialize(deserializer)?;
    Ok(match raw {
        None => Vec::new(),
        Some(RawAcceptanceCriteria::List(items)) => items
            .into_iter()
            .flat_map(|item| split_acceptance_criteria_text(&item))
            .collect(),
        Some(RawAcceptanceCriteria::Text(item)) => split_acceptance_criteria_text(&item),
    })
}

fn split_acceptance_criteria_text(value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let lines = trimmed
        .lines()
        .map(clean_acceptance_line)
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();

    if lines.len() <= 1 && !trimmed.contains('\n') {
        vec![clean_acceptance_line(trimmed)]
    } else {
        lines
    }
}

fn clean_acceptance_line(line: &str) -> String {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let without_bullet = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .unwrap_or(trimmed);

    let numbered = without_bullet
        .split_once(". ")
        .filter(|(prefix, _)| !prefix.is_empty() && prefix.chars().all(|ch| ch.is_ascii_digit()))
        .map(|(_, remainder)| remainder)
        .unwrap_or(without_bullet);

    numbered.trim().to_owned()
}

// ── DepTreeNode ─────────────────────────────────────────────────────────────

/// A node in the dependency tree, as returned by `br dep tree <id> --json`.
///
/// The live `br` CLI emits a top-level flat array of nodes with `depth` and
/// `parent_id` metadata. The `children` field remains defaulted so older nested
/// fixtures continue to deserialize, but production callers should use the flat
/// metadata to reconstruct direct relationships.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DepTreeNode {
    pub id: String,
    pub title: String,
    pub status: String,
    #[serde(default)]
    pub depth: usize,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub children: Vec<DepTreeNode>,
}

// ── BlockedBead ─────────────────────────────────────────────────────────────

/// A bead that is blocked by other beads, as returned by `br blocked --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockedBead {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub blocked_by: Vec<String>,
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── BeadStatus ──────────────────────────────────────────────────────

    #[test]
    fn status_roundtrip_serde() -> Result<(), Box<dyn std::error::Error>> {
        for (json, expected) in [
            (r#""open""#, BeadStatus::Open),
            (r#""in_progress""#, BeadStatus::InProgress),
            (r#""closed""#, BeadStatus::Closed),
            (r#""deferred""#, BeadStatus::Deferred),
        ] {
            let parsed: BeadStatus = serde_json::from_str(json)?;
            assert_eq!(parsed, expected);
            let serialized = serde_json::to_string(&parsed)?;
            assert_eq!(serialized, json);
        }
        Ok(())
    }

    #[test]
    fn status_display_and_fromstr() -> Result<(), Box<dyn std::error::Error>> {
        for (text, expected) in [
            ("open", BeadStatus::Open),
            ("in_progress", BeadStatus::InProgress),
            ("closed", BeadStatus::Closed),
            ("deferred", BeadStatus::Deferred),
        ] {
            let parsed = BeadStatus::from_str(text)?;
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), text);
        }
        Ok(())
    }

    #[test]
    fn status_fromstr_unknown_returns_error() {
        assert!(BeadStatus::from_str("archived").is_err());
    }

    // ── BeadPriority ────────────────────────────────────────────────────

    #[test]
    fn priority_from_integer() -> Result<(), Box<dyn std::error::Error>> {
        let p: BeadPriority = serde_json::from_str("2")?;
        assert_eq!(p.value(), 2);
        assert_eq!(p.to_string(), "P2");
        Ok(())
    }

    #[test]
    fn priority_from_string_with_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let p: BeadPriority = serde_json::from_str(r#""P1""#)?;
        assert_eq!(p.value(), 1);
        assert_eq!(p.to_string(), "P1");
        Ok(())
    }

    #[test]
    fn priority_from_string_without_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let p: BeadPriority = serde_json::from_str(r#""3""#)?;
        assert_eq!(p.value(), 3);
        assert_eq!(p.to_string(), "P3");
        Ok(())
    }

    #[test]
    fn bead_detail_deserializes_from_show_array() -> Result<(), Box<dyn std::error::Error>> {
        let detail: BeadDetail = serde_json::from_str(
            r#"[{
                "id":"bead-1",
                "title":"Test",
                "status":"open",
                "priority":1,
                "bead_type":"task",
                "labels":["milestone:ms-alpha"]
            }]"#,
        )?;

        assert_eq!(detail.id, "bead-1");
        assert_eq!(detail.title, "Test");
        assert_eq!(detail.status, BeadStatus::Open);
        assert_eq!(detail.priority.value(), 1);
        assert_eq!(detail.bead_type, BeadType::Task);
        Ok(())
    }

    #[test]
    fn priority_from_lowercase_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let p: BeadPriority = serde_json::from_str(r#""p0""#)?;
        assert_eq!(p.value(), 0);
        assert_eq!(p.to_string(), "P0");
        Ok(())
    }

    #[test]
    fn priority_out_of_range_integer_errors() {
        let result: Result<BeadPriority, _> = serde_json::from_str("5");
        assert!(result.is_err());
    }

    #[test]
    fn priority_out_of_range_string_errors() {
        let result: Result<BeadPriority, _> = serde_json::from_str(r#""P7""#);
        assert!(result.is_err());
    }

    #[test]
    fn priority_negative_errors() {
        let result: Result<BeadPriority, _> = serde_json::from_str("-1");
        assert!(result.is_err());
    }

    #[test]
    fn priority_clamps_via_new() {
        let p = BeadPriority::new(10);
        assert_eq!(p.value(), 4);
    }

    #[test]
    fn priority_fromstr() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(BeadPriority::from_str("P2")?.value(), 2);
        assert_eq!(BeadPriority::from_str("p4")?.value(), 4);
        assert_eq!(BeadPriority::from_str("0")?.value(), 0);
        assert!(BeadPriority::from_str("P9").is_err());
        assert!(BeadPriority::from_str("xyz").is_err());
        Ok(())
    }

    // ── BeadType ────────────────────────────────────────────────────────

    #[test]
    fn bead_type_known_variants() -> Result<(), Box<dyn std::error::Error>> {
        for (json, expected) in [
            (r#""task""#, BeadType::Task),
            (r#""bug""#, BeadType::Bug),
            (r#""feature""#, BeadType::Feature),
            (r#""epic""#, BeadType::Epic),
            (r#""chore""#, BeadType::Chore),
            (r#""docs""#, BeadType::Docs),
            (r#""question""#, BeadType::Question),
            (r#""spike""#, BeadType::Spike),
            (r#""meta""#, BeadType::Meta),
        ] {
            let parsed: BeadType = serde_json::from_str(json)?;
            assert_eq!(parsed, expected);
        }
        Ok(())
    }

    #[test]
    fn bead_type_unknown_maps_to_other() -> Result<(), Box<dyn std::error::Error>> {
        let parsed: BeadType = serde_json::from_str(r#""research""#)?;
        assert_eq!(parsed, BeadType::Other("research".to_owned()));
        assert_eq!(parsed.to_string(), "research");
        Ok(())
    }

    #[test]
    fn bead_type_display() {
        assert_eq!(BeadType::Task.to_string(), "task");
        assert_eq!(BeadType::Bug.to_string(), "bug");
        assert_eq!(BeadType::Other("custom".to_owned()).to_string(), "custom");
    }

    // ── DependencyKind ──────────────────────────────────────────────────

    #[test]
    fn dependency_kind_serde() -> Result<(), Box<dyn std::error::Error>> {
        let parsed: DependencyKind = serde_json::from_str(r#""blocks""#)?;
        assert_eq!(parsed, DependencyKind::Blocks);

        let parsed: DependencyKind = serde_json::from_str(r#""parent_child""#)?;
        assert_eq!(parsed, DependencyKind::ParentChild);

        let parsed: DependencyKind = serde_json::from_str(r#""parent-child""#)?;
        assert_eq!(parsed, DependencyKind::ParentChild);

        assert_eq!(
            serde_json::to_string(&DependencyKind::Blocks)?,
            r#""blocks""#
        );
        assert_eq!(
            serde_json::to_string(&DependencyKind::ParentChild)?,
            r#""parent_child""#
        );
        Ok(())
    }

    // ── BeadSummary ─────────────────────────────────────────────────────

    #[test]
    fn bead_summary_from_br_list() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "ralph-burning-9ni.4.1",
            "title": "Implement auth middleware",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend", "security"]
        }"#;

        let summary: BeadSummary = serde_json::from_str(json)?;
        assert_eq!(summary.id, "ralph-burning-9ni.4.1");
        assert_eq!(summary.title, "Implement auth middleware");
        assert_eq!(summary.status, BeadStatus::Open);
        assert_eq!(summary.priority.value(), 2);
        assert_eq!(summary.bead_type, BeadType::Task);
        assert_eq!(summary.labels, vec!["backend", "security"]);
        Ok(())
    }

    #[test]
    fn bead_summary_missing_labels_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "Minimal bead",
            "status": "closed",
            "priority": "P0",
            "bead_type": "bug"
        }"#;

        let summary: BeadSummary = serde_json::from_str(json)?;
        assert!(summary.labels.is_empty());
        assert_eq!(summary.priority.value(), 0);
        Ok(())
    }

    #[test]
    fn bead_summary_with_unknown_fields_ignored() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "A bead",
            "status": "open",
            "priority": 1,
            "bead_type": "task",
            "labels": [],
            "some_future_field": "should be ignored",
            "another_field": 42
        }"#;

        let summary: BeadSummary = serde_json::from_str(json)?;
        assert_eq!(summary.id, "bead-1");
        Ok(())
    }

    #[test]
    fn bead_summary_list_response() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"[
            {
                "id": "bead-1",
                "title": "First",
                "status": "open",
                "priority": 1,
                "bead_type": "task",
                "labels": []
            },
            {
                "id": "bead-2",
                "title": "Second",
                "status": "in_progress",
                "priority": 0,
                "bead_type": "feature",
                "labels": ["urgent"]
            }
        ]"#;

        let beads: Vec<BeadSummary> = serde_json::from_str(json)?;
        assert_eq!(beads.len(), 2);
        assert_eq!(beads[0].status, BeadStatus::Open);
        assert_eq!(beads[1].status, BeadStatus::InProgress);
        assert_eq!(beads[1].priority.to_string(), "P0");
        Ok(())
    }

    // ── BeadDetail ──────────────────────────────────────────────────────

    #[test]
    fn bead_detail_full() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "ralph-burning-9ni.4.1.2",
            "title": "Create br_models types",
            "status": "in_progress",
            "priority": 1,
            "bead_type": "task",
            "labels": ["adapters", "models"],
            "description": "Define typed Rust structs for br JSON output.",
            "acceptance_criteria": [
                "All structs derive Debug, Clone, PartialEq",
                "Tests cover deserialization"
            ],
            "dependencies": [
                {
                    "id": "ralph-burning-9ni.4.1.1",
                    "kind": "blocks",
                    "title": "Create br process adapter"
                }
            ],
            "dependents": [
                {
                    "id": "ralph-burning-9ni.4.1.3",
                    "kind": "blocks"
                }
            ],
            "comments": [
                {
                    "id": 7,
                    "issue_id": "ralph-burning-9ni.4.1.2",
                    "author": "planner",
                    "text": "Ship it",
                    "created_at": "2026-03-27T14:31:00Z"
                }
            ],
            "owner": "claude",
            "created_at": "2026-03-25T10:00:00Z",
            "updated_at": "2026-03-27T14:30:00Z"
        }"#;

        let detail: BeadDetail = serde_json::from_str(json)?;
        assert_eq!(detail.id, "ralph-burning-9ni.4.1.2");
        assert_eq!(detail.status, BeadStatus::InProgress);
        assert_eq!(detail.priority.value(), 1);
        assert_eq!(detail.bead_type, BeadType::Task);
        assert_eq!(detail.labels.len(), 2);
        assert_eq!(
            detail.description.as_deref(),
            Some("Define typed Rust structs for br JSON output.")
        );
        assert_eq!(detail.acceptance_criteria.len(), 2);
        assert_eq!(detail.dependencies.len(), 1);
        assert_eq!(detail.dependencies[0].kind, DependencyKind::Blocks);
        assert_eq!(
            detail.dependencies[0].title.as_deref(),
            Some("Create br process adapter")
        );
        assert_eq!(detail.dependents.len(), 1);
        assert!(detail.dependents[0].title.is_none());
        assert_eq!(detail.comments.len(), 1);
        assert_eq!(detail.comments[0].text, "Ship it");
        assert_eq!(detail.owner.as_deref(), Some("claude"));
        assert!(detail.created_at.is_some());
        assert!(detail.updated_at.is_some());
        Ok(())
    }

    #[test]
    fn bead_detail_minimal() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "Minimal",
            "status": "open",
            "priority": 3,
            "bead_type": "chore"
        }"#;

        let detail: BeadDetail = serde_json::from_str(json)?;
        assert_eq!(detail.id, "bead-1");
        assert!(detail.labels.is_empty());
        assert!(detail.description.is_none());
        assert!(detail.acceptance_criteria.is_empty());
        assert!(detail.dependencies.is_empty());
        assert!(detail.dependents.is_empty());
        assert!(detail.comments.is_empty());
        assert!(detail.owner.is_none());
        assert!(detail.created_at.is_none());
        assert!(detail.updated_at.is_none());
        Ok(())
    }

    #[test]
    fn bead_detail_accepts_live_show_field_names_and_splits_acceptance_criteria(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "Live shape",
            "status": "open",
            "priority": "P1",
            "issue_type": "feature",
            "acceptance_criteria": "- Ship the task creation path\n- Keep the result inspectable",
            "dependencies": [
                {
                    "id": "epic-1",
                    "dependency_type": "parent-child",
                    "title": "Parent epic"
                }
            ]
        }"#;

        let detail: BeadDetail = serde_json::from_str(json)?;
        assert_eq!(detail.bead_type, BeadType::Feature);
        assert_eq!(
            detail.acceptance_criteria,
            vec![
                "Ship the task creation path".to_owned(),
                "Keep the result inspectable".to_owned()
            ]
        );
        assert_eq!(detail.dependencies.len(), 1);
        assert_eq!(detail.dependencies[0].kind, DependencyKind::ParentChild);
        Ok(())
    }

    // ── ReadyBead ───────────────────────────────────────────────────────

    #[test]
    fn ready_bead_from_br_ready() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"[
            {
                "id": "bead-5",
                "title": "Write tests",
                "priority": "P2",
                "bead_type": "task",
                "labels": ["testing"]
            },
            {
                "id": "bead-7",
                "title": "Fix login bug",
                "priority": 0,
                "bead_type": "bug",
                "labels": []
            }
        ]"#;

        let beads: Vec<ReadyBead> = serde_json::from_str(json)?;
        assert_eq!(beads.len(), 2);
        assert_eq!(beads[0].id, "bead-5");
        assert_eq!(beads[0].priority.to_string(), "P2");
        assert_eq!(beads[0].bead_type, BeadType::Task);
        assert_eq!(beads[1].priority.value(), 0);
        assert_eq!(beads[1].bead_type, BeadType::Bug);
        Ok(())
    }

    // ── DepTreeNode ─────────────────────────────────────────────────────

    #[test]
    fn dep_tree_nested() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "Root task",
            "status": "in_progress",
            "children": [
                {
                    "id": "bead-1.1",
                    "title": "Subtask A",
                    "status": "closed",
                    "children": []
                },
                {
                    "id": "bead-1.2",
                    "title": "Subtask B",
                    "status": "open",
                    "children": [
                        {
                            "id": "bead-1.2.1",
                            "title": "Leaf task",
                            "status": "open",
                            "children": []
                        }
                    ]
                }
            ]
        }"#;

        let tree: DepTreeNode = serde_json::from_str(json)?;
        assert_eq!(tree.id, "bead-1");
        assert_eq!(tree.children.len(), 2);
        assert_eq!(tree.children[0].status, "closed");
        assert_eq!(tree.children[1].children.len(), 1);
        assert_eq!(tree.children[1].children[0].id, "bead-1.2.1");
        Ok(())
    }

    #[test]
    fn dep_tree_flat_live_shape() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"[
            {
                "id": "bead-root",
                "title": "Root",
                "depth": 0,
                "parent_id": null,
                "priority": 1,
                "status": "open",
                "truncated": false
            },
            {
                "id": "bead-child",
                "title": "Child",
                "depth": 1,
                "parent_id": "bead-root",
                "priority": 2,
                "status": "closed",
                "truncated": false
            }
        ]"#;

        let tree: Vec<DepTreeNode> = serde_json::from_str(json)?;
        assert_eq!(tree.len(), 2);
        assert_eq!(tree[1].parent_id.as_deref(), Some("bead-root"));
        assert_eq!(tree[1].depth, 1);
        assert_eq!(tree[1].status, "closed");
        Ok(())
    }

    #[test]
    fn dep_tree_leaf_no_children() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-leaf",
            "title": "Leaf",
            "status": "open"
        }"#;

        let node: DepTreeNode = serde_json::from_str(json)?;
        assert!(node.children.is_empty());
        Ok(())
    }

    // ── BlockedBead ─────────────────────────────────────────────────────

    #[test]
    fn blocked_bead_from_br_blocked() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"[
            {
                "id": "bead-3",
                "title": "Deploy to staging",
                "blocked_by": ["bead-1", "bead-2"]
            },
            {
                "id": "bead-5",
                "title": "Run integration tests",
                "blocked_by": ["bead-4"]
            }
        ]"#;

        let blocked: Vec<BlockedBead> = serde_json::from_str(json)?;
        assert_eq!(blocked.len(), 2);
        assert_eq!(blocked[0].blocked_by, vec!["bead-1", "bead-2"]);
        assert_eq!(blocked[1].blocked_by, vec!["bead-4"]);
        Ok(())
    }

    #[test]
    fn blocked_bead_empty_blockers() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{
            "id": "bead-1",
            "title": "Free bead"
        }"#;

        let bead: BlockedBead = serde_json::from_str(json)?;
        assert!(bead.blocked_by.is_empty());
        Ok(())
    }

    // ── Serialization roundtrip ─────────────────────────────────────────

    #[test]
    fn summary_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let summary = BeadSummary {
            id: "bead-42".to_owned(),
            title: "Roundtrip test".to_owned(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(2),
            bead_type: BeadType::Feature,
            labels: vec!["test".to_owned()],
            description: None,
            dependencies: Vec::new(),
            dependents: Vec::new(),
        };

        let json = serde_json::to_string(&summary)?;
        let parsed: BeadSummary = serde_json::from_str(&json)?;
        assert_eq!(parsed, summary);
        Ok(())
    }

    #[test]
    fn dependency_ref_optional_title() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"id": "dep-1", "kind": "blocks"}"#;
        let dep: DependencyRef = serde_json::from_str(json)?;
        assert_eq!(dep.id, "dep-1");
        assert_eq!(dep.kind, DependencyKind::Blocks);
        assert!(dep.title.is_none());
        assert!(dep.status.is_none());

        let json =
            r#"{"id": "dep-2", "kind": "parent_child", "title": "Parent", "status": "closed"}"#;
        let dep: DependencyRef = serde_json::from_str(json)?;
        assert_eq!(dep.title.as_deref(), Some("Parent"));
        assert_eq!(dep.status, Some(BeadStatus::Closed));
        Ok(())
    }

    #[test]
    fn dependency_ref_omits_null_status_on_serialize() -> Result<(), Box<dyn std::error::Error>> {
        let dep = DependencyRef {
            id: "dep-1".to_owned(),
            kind: DependencyKind::Blocks,
            title: Some("Setup project".to_owned()),
            status: None,
        };

        let value = serde_json::to_value(&dep)?;
        assert_eq!(
            value,
            serde_json::json!({
                "id": "dep-1",
                "kind": "blocks",
                "title": "Setup project"
            })
        );
        Ok(())
    }

    #[test]
    fn bead_type_other_serializes_correctly() -> Result<(), Box<dyn std::error::Error>> {
        let bead_type = BeadType::Other("experiment".to_owned());
        let _json = serde_json::to_string(&bead_type)?;
        let parsed: BeadType = serde_json::from_str(r#""experiment""#)?;
        assert_eq!(parsed, bead_type);
        Ok(())
    }

    #[test]
    fn priority_serializes_as_integer() -> Result<(), Box<dyn std::error::Error>> {
        let p = BeadPriority::new(3);
        let json = serde_json::to_string(&p)?;
        assert_eq!(json, "3");
        Ok(())
    }
}
