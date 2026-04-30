#![forbid(unsafe_code)]

//! Deterministic project prompt rendering from bead metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::adapters::br_models::{
    BeadDetail, BeadStatus, DepTreeNode, DependencyKind, DependencyRef,
};
use crate::adapters::br_process::{BrAdapter, BrCommand, BrError, ProcessRunner};

pub const ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE: &str = "Files under `.ralph-burning/` are live orchestration state and MUST NOT be\nreviewed or flagged. Only review source code under `src/`, `tests/`,\n`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadGraphRef {
    pub id: String,
    pub title: String,
    pub status: Option<String>,
}

impl BeadGraphRef {
    fn from_dependency(
        value: &DependencyRef,
        dep_tree_refs: &BTreeMap<String, DepTreeRef>,
    ) -> Self {
        let tree_ref = dep_tree_refs.get(&value.id);
        Self {
            id: value.id.clone(),
            title: value
                .title
                .clone()
                .or_else(|| tree_ref.map(|reference| reference.title.clone()))
                .unwrap_or_else(|| "(untitled)".to_owned()),
            status: tree_ref
                .map(|reference| reference.status.clone())
                .or_else(|| value.status.as_ref().map(ToString::to_string)),
        }
    }
}

#[derive(Debug, Error)]
pub enum BeadPromptReadError {
    #[error("bead '{bead_id}' was not found")]
    NotFound { bead_id: String },

    #[error(transparent)]
    Br(#[from] BrError),
}

#[derive(Debug, Error)]
pub enum ProjectPromptRenderError {
    #[error("bead '{bead_id}' was not found")]
    BeadNotFound { bead_id: String },

    #[error("refusing to render prompt for closed bead '{bead_id}'")]
    ClosedBead { bead_id: String },

    #[error(
        "refusing to render prompt for bead '{bead_id}' because it has open blockers: {blockers}",
        blockers = format_blockers(.open_blockers)
    )]
    OpenBlockers {
        bead_id: String,
        open_blockers: Vec<BeadGraphRef>,
    },

    #[error("failed to read bead '{bead_id}' from br: {source}")]
    BrRead {
        bead_id: String,
        source: BeadPromptReadError,
    },
}

#[allow(async_fn_in_trait)]
pub trait BeadPromptBrPort {
    async fn bead_show(&self, bead_id: &str) -> Result<BeadDetail, BeadPromptReadError>;

    async fn bead_dep_tree(&self, bead_id: &str) -> Result<Vec<DepTreeNode>, BeadPromptReadError>;
}

impl<R: ProcessRunner> BeadPromptBrPort for BrAdapter<R> {
    async fn bead_show(&self, bead_id: &str) -> Result<BeadDetail, BeadPromptReadError> {
        self.exec_json::<BeadDetail>(&BrCommand::show(bead_id.to_owned()))
            .await
            .map_err(|error| map_br_read_error(bead_id, error))
    }

    async fn bead_dep_tree(&self, bead_id: &str) -> Result<Vec<DepTreeNode>, BeadPromptReadError> {
        self.exec_json::<Vec<DepTreeNode>>(&BrCommand::dep_tree(bead_id.to_owned()))
            .await
            .map_err(|error| map_br_read_error(bead_id, error))
    }
}

pub async fn render_project_prompt_from_bead<P: BeadPromptBrPort>(
    bead_id: &str,
    br: &P,
    repo_root: PathBuf,
) -> Result<String, ProjectPromptRenderError> {
    let bead = br
        .bead_show(bead_id)
        .await
        .map_err(|source| map_render_read_error(bead_id, source))?;

    if bead.status == BeadStatus::Closed {
        return Err(ProjectPromptRenderError::ClosedBead {
            bead_id: bead.id.clone(),
        });
    }

    let dep_tree = br
        .bead_dep_tree(bead_id)
        .await
        .map_err(|source| map_render_read_error(bead_id, source))?;

    let graph_context = graph_context_from_bead(&bead, &dep_tree)?;

    Ok(render_prompt(&bead, &graph_context, &repo_root))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NearbyGraphContext {
    parents: Vec<BeadGraphRef>,
    closed_blockers: Vec<BeadGraphRef>,
    direct_dependents: Vec<BeadGraphRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DepTreeRef {
    title: String,
    status: String,
}

fn graph_context_from_bead(
    bead: &BeadDetail,
    dep_tree: &[DepTreeNode],
) -> Result<NearbyGraphContext, ProjectPromptRenderError> {
    let dep_tree_refs = direct_dep_tree_refs(&bead.id, dep_tree);
    let mut parents = bead
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind == DependencyKind::ParentChild)
        .map(|dependency| BeadGraphRef::from_dependency(dependency, &dep_tree_refs))
        .collect::<Vec<_>>();
    sort_graph_refs(&mut parents);

    let blockers = bead
        .dependencies
        .iter()
        .filter(|dependency| dependency.kind == DependencyKind::Blocks)
        .map(|dependency| BeadGraphRef::from_dependency(dependency, &dep_tree_refs))
        .collect::<Vec<_>>();

    let (mut closed_blockers, mut open_blockers): (Vec<_>, Vec<_>) = blockers
        .into_iter()
        .partition(|blocker| blocker.status.as_deref() == Some("closed"));
    sort_graph_refs(&mut closed_blockers);
    sort_graph_refs(&mut open_blockers);

    if !open_blockers.is_empty() {
        return Err(ProjectPromptRenderError::OpenBlockers {
            bead_id: bead.id.clone(),
            open_blockers,
        });
    }

    let empty_tree_refs = BTreeMap::new();
    let mut direct_dependents = bead
        .dependents
        .iter()
        .map(|dependency| BeadGraphRef::from_dependency(dependency, &empty_tree_refs))
        .collect::<Vec<_>>();
    sort_graph_refs(&mut direct_dependents);

    Ok(NearbyGraphContext {
        parents,
        closed_blockers,
        direct_dependents,
    })
}

fn direct_dep_tree_refs(bead_id: &str, dep_tree: &[DepTreeNode]) -> BTreeMap<String, DepTreeRef> {
    dep_tree
        .iter()
        .filter(|node| node.depth == 1 && node.parent_id.as_deref() == Some(bead_id))
        .map(|child| {
            (
                child.id.clone(),
                DepTreeRef {
                    title: child.title.clone(),
                    status: child.status.clone(),
                },
            )
        })
        .collect()
}

fn render_prompt(
    bead: &BeadDetail,
    graph_context: &NearbyGraphContext,
    repo_root: &Path,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Work Item: {} - {}", bead.id, bead.title).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Goal").unwrap();
    writeln!(out).unwrap();
    let description = bead.description.as_deref().unwrap_or("");
    if description.trim().is_empty() {
        writeln!(out, "No description provided.").unwrap();
    } else {
        writeln!(out, "{description}").unwrap();
    }
    writeln!(out).unwrap();

    if !bead.acceptance_criteria.is_empty() {
        writeln!(out, "## Acceptance Criteria").unwrap();
        writeln!(out).unwrap();
        for criterion in &bead.acceptance_criteria {
            writeln!(out, "- {criterion}").unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(out, "## Nearby Graph Context").unwrap();
    writeln!(out).unwrap();
    write_ref_group(&mut out, "Parent", &graph_context.parents);
    write_ref_group(&mut out, "Closed blockers", &graph_context.closed_blockers);
    write_ref_group(
        &mut out,
        "Direct dependents",
        &graph_context.direct_dependents,
    );
    writeln!(out).unwrap();

    let path_pointers = explicit_path_pointers(bead.description.as_deref().unwrap_or(""));
    if !path_pointers.is_empty() {
        writeln!(out, "## Where to Look").unwrap();
        writeln!(out).unwrap();
        for pointer in path_pointers {
            writeln!(out, "- `{pointer}`").unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(
        out,
        "## IMPORTANT: Exclude orchestration state from review scope"
    )
    .unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE}").unwrap();

    let norm_refs = repo_norm_refs(repo_root);
    if !norm_refs.is_empty() {
        writeln!(out).unwrap();
        writeln!(out, "## Repository Norms").unwrap();
        writeln!(out).unwrap();
        for reference in norm_refs {
            writeln!(out, "- `{reference}`").unwrap();
        }
    }

    out
}

fn write_ref_group(out: &mut String, label: &str, refs: &[BeadGraphRef]) {
    writeln!(out, "- **{label}:**").unwrap();
    if refs.is_empty() {
        writeln!(out, "  - None.").unwrap();
        return;
    }

    for reference in refs {
        writeln!(out, "  - {} - {}", reference.id, reference.title).unwrap();
    }
}

fn repo_norm_refs(repo_root: &Path) -> Vec<&'static str> {
    ["AGENTS.md", "CLAUDE.md"]
        .into_iter()
        .filter(|file| repo_root.join(file).is_file())
        .collect()
}

fn explicit_path_pointers(description: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut pointers = Vec::new();
    let mut in_path_section = false;
    for line in description.lines() {
        collect_backtick_paths(line, &mut seen, &mut pointers);

        if starts_path_pointer_section(line) {
            in_path_section = true;
            collect_bare_paths(line, &mut seen, &mut pointers);
            continue;
        }

        if !in_path_section {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            in_path_section = false;
            continue;
        }

        collect_bare_paths(line, &mut seen, &mut pointers);
    }
    pointers
}

fn starts_path_pointer_section(line: &str) -> bool {
    let normalized = line
        .trim()
        .trim_start_matches('#')
        .trim()
        .to_ascii_lowercase();
    normalized.starts_with("where to look:")
        || normalized == "where to look"
        || normalized.starts_with("where to look -")
}

fn collect_backtick_paths(line: &str, seen: &mut BTreeSet<String>, pointers: &mut Vec<String>) {
    let mut rest = line;
    while let Some(start) = rest.find('`') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('`') else {
            break;
        };
        let candidate = &rest[..end];
        maybe_insert_path(candidate, seen, pointers);
        rest = &rest[end + 1..];
    }
}

fn collect_bare_paths(line: &str, seen: &mut BTreeSet<String>, pointers: &mut Vec<String>) {
    for token in line.split_whitespace() {
        maybe_insert_path(token, seen, pointers);
    }
}

fn maybe_insert_path(candidate: &str, seen: &mut BTreeSet<String>, pointers: &mut Vec<String>) {
    let cleaned = candidate
        .trim()
        .trim_matches(|ch: char| matches!(ch, ',' | '.' | ';' | ':' | ')' | '(' | '[' | ']'));
    if looks_like_explicit_path(cleaned) && seen.insert(cleaned.to_owned()) {
        pointers.push(cleaned.to_owned());
    }
}

fn looks_like_explicit_path(candidate: &str) -> bool {
    if candidate.is_empty() || candidate.contains(' ') {
        return false;
    }
    if candidate.starts_with(".ralph-burning/") {
        return false;
    }
    looks_like_repo_path(candidate)
        || matches!(
            candidate,
            "AGENTS.md" | "CLAUDE.md" | "Cargo.toml" | "flake.nix" | "flake.lock"
        )
        || [".rs", ".md", ".toml", ".nix", ".json", ".jsonl"]
            .iter()
            .any(|suffix| candidate.ends_with(suffix))
}

fn looks_like_repo_path(candidate: &str) -> bool {
    if !candidate.contains('/') || candidate == "/" || candidate.contains("//") {
        return false;
    }

    if let Some(body) = candidate
        .strip_prefix("./")
        .or_else(|| candidate.strip_prefix("../"))
    {
        return !body.is_empty();
    }

    let parts = candidate.split('/').collect::<Vec<_>>();
    let Some(first) = parts.first().copied() else {
        return false;
    };
    if first.is_empty() || parts.iter().any(|part| *part == "." || *part == "..") {
        return false;
    }

    matches!(
        first,
        "src"
            | "tests"
            | "docs"
            | "benches"
            | "examples"
            | "fixtures"
            | "scripts"
            | "crates"
            | ".beads"
            | ".github"
    ) || parts
        .iter()
        .rev()
        .find(|part| !part.is_empty())
        .is_some_and(has_path_extension)
}

fn has_path_extension(segment: &&str) -> bool {
    [
        ".rs", ".md", ".toml", ".nix", ".json", ".jsonl", ".yaml", ".yml", ".sh",
    ]
    .iter()
    .any(|suffix| segment.ends_with(suffix))
}

fn sort_graph_refs(refs: &mut [BeadGraphRef]) {
    refs.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn map_render_read_error(bead_id: &str, source: BeadPromptReadError) -> ProjectPromptRenderError {
    match source {
        BeadPromptReadError::NotFound { .. } => ProjectPromptRenderError::BeadNotFound {
            bead_id: bead_id.to_owned(),
        },
        source => ProjectPromptRenderError::BrRead {
            bead_id: bead_id.to_owned(),
            source,
        },
    }
}

fn map_br_read_error(bead_id: &str, error: BrError) -> BeadPromptReadError {
    match &error {
        BrError::BrExitError { stdout, stderr, .. }
            if br_output_indicates_missing(stdout, stderr) =>
        {
            BeadPromptReadError::NotFound {
                bead_id: bead_id.to_owned(),
            }
        }
        _ => BeadPromptReadError::Br(error),
    }
}

fn br_output_indicates_missing(stdout: &str, stderr: &str) -> bool {
    let combined = format!("{stdout}\n{stderr}").to_ascii_lowercase();
    combined.contains("not found")
        || combined.contains("no such")
        || combined.contains("unknown")
        || combined.contains("does not exist")
}

fn format_blockers(blockers: &[BeadGraphRef]) -> String {
    blockers
        .iter()
        .map(|blocker| match &blocker.status {
            Some(status) => format!("{} ({}, {})", blocker.id, blocker.title, status),
            None => format!("{} ({}, status unknown)", blocker.id, blocker.title),
        })
        .collect::<Vec<_>>()
        .join(", ")
}
