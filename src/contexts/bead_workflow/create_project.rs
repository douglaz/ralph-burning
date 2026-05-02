#![forbid(unsafe_code)]

//! Create a Ralph project directly from a bead prompt.

use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::adapters::br_models::BeadDetail;
use crate::adapters::br_process::{BrAdapter, BrMutationAdapter, OsProcessRunner};
use crate::adapters::fs::FileSystem;
use crate::contexts::project_run_record::model::ProjectRecord;
use crate::contexts::project_run_record::service::{
    self as project_service, CreateProjectInput, JournalStorePort, ProjectStorePort,
};
use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::AppError;

use super::project_prompt::{
    render_project_prompt_from_bead, BeadPromptBrPort, BeadPromptReadError,
    ProjectPromptRenderError,
};

#[derive(Debug, Clone)]
pub struct CreateProjectFromBeadInput {
    pub bead_id: String,
    pub flow: FlowPreset,
    pub branch: Option<String>,
    pub created_at: DateTime<Utc>,
    /// Optional verbatim log from a prior failed attempt at this bead.
    /// When set, a "Previous attempt failure" section is appended to the
    /// rendered prompt so the implementer can see the exact failures
    /// it needs to avoid this round. Used by drain's `RetryBeadFresh`
    /// recovery path for cleanable verification failures.
    pub prior_failure_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateProjectFromBeadOutput {
    pub project: ProjectRecord,
    pub branch_name: Option<String>,
}

#[derive(Debug, Error)]
pub enum BeadProjectCreationError {
    #[error(transparent)]
    Prompt(#[from] ProjectPromptRenderError),

    #[error("project '{project_id}' already exists; use `ralph-burning project select {project_id}` and `ralph-burning run resume` instead")]
    ProjectExists { project_id: String },

    #[error("failed to create feature branch '{branch_name}': {details}")]
    BranchCreate {
        branch_name: String,
        details: String,
    },

    #[error("project '{project_id}' was created but bead status update failed; run `br update {bead_id} --status=in_progress` manually: {details}")]
    BeadStatusUpdatePartial {
        project_id: String,
        bead_id: String,
        details: String,
    },

    #[error(transparent)]
    App(#[from] AppError),
}

#[allow(async_fn_in_trait)]
pub trait BeadProjectBrPort: BeadPromptBrPort {
    async fn mark_bead_in_progress(&self, bead_id: &str) -> Result<(), String>;
}

pub trait FeatureBranchPort {
    fn create_branch(&self, base_dir: &Path, branch_name: &str) -> Result<(), String>;
}

pub struct NoopFeatureBranchPort;

impl FeatureBranchPort for NoopFeatureBranchPort {
    fn create_branch(&self, _base_dir: &Path, _branch_name: &str) -> Result<(), String> {
        Ok(())
    }
}

pub struct GitFeatureBranchPort;

impl FeatureBranchPort for GitFeatureBranchPort {
    fn create_branch(&self, base_dir: &Path, branch_name: &str) -> Result<(), String> {
        let output = Command::new("git")
            .arg("switch")
            .arg("-c")
            .arg(branch_name)
            .current_dir(base_dir)
            .output()
            .map_err(|error| error.to_string())?;
        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if stderr.is_empty() {
            Err(format!(
                "git switch -c exited with status {}",
                output.status
            ))
        } else {
            Err(stderr)
        }
    }
}

pub struct ProcessBeadProjectBrPort {
    base_dir: PathBuf,
    read: BrAdapter<OsProcessRunner>,
    mutation: BrMutationAdapter<OsProcessRunner>,
}

impl ProcessBeadProjectBrPort {
    pub fn new(base_dir: PathBuf) -> Self {
        let read = BrAdapter::new().with_working_dir(base_dir.clone());
        let mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::new().with_working_dir(base_dir.clone()),
            "bead-create-project".to_owned(),
        );
        Self {
            base_dir,
            read,
            mutation,
        }
    }

    pub fn with_br_binary(base_dir: PathBuf, br_binary: PathBuf) -> Self {
        let read =
            BrAdapter::with_binary_path(br_binary.clone()).with_working_dir(base_dir.clone());
        let mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_binary_path(br_binary).with_working_dir(base_dir.clone()),
            "bead-create-project".to_owned(),
        );
        Self {
            base_dir,
            read,
            mutation,
        }
    }
}

impl BeadPromptBrPort for ProcessBeadProjectBrPort {
    async fn bead_show(&self, bead_id: &str) -> Result<BeadDetail, BeadPromptReadError> {
        self.read.bead_show(bead_id).await
    }

    async fn bead_dep_tree(
        &self,
        bead_id: &str,
    ) -> Result<Vec<crate::adapters::br_models::DepTreeNode>, BeadPromptReadError> {
        self.read.bead_dep_tree(bead_id).await
    }
}

impl BeadProjectBrPort for ProcessBeadProjectBrPort {
    async fn mark_bead_in_progress(&self, bead_id: &str) -> Result<(), String> {
        self.mutation
            .update_bead_status(bead_id, "in_progress")
            .await
            .map_err(|error| error.to_string())?;
        self.mutation
            .sync_own_dirty_if_beads_healthy(&self.base_dir)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

pub async fn create_project_from_bead(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    br: &impl BeadProjectBrPort,
    branch_port: &dyn FeatureBranchPort,
    base_dir: &Path,
    input: CreateProjectFromBeadInput,
) -> Result<CreateProjectFromBeadOutput, BeadProjectCreationError> {
    // Resolve the bead and check for project-id collisions BEFORE rendering
    // the prompt. Otherwise, in rerun/recovery scenarios where the project
    // already exists but the bead has since been closed or has new open
    // blockers, the renderer would error first and the user would never
    // see the friendly ProjectExists message pointing at `ralph run resume`.
    let bead = br
        .bead_show(&input.bead_id)
        .await
        .map_err(|source| map_prompt_read_error(&input.bead_id, source))?;
    let project_id = derive_project_id_from_bead_id(&bead.id)?;

    if store.project_exists(base_dir, &project_id)? {
        return Err(BeadProjectCreationError::ProjectExists {
            project_id: project_id.to_string(),
        });
    }

    let prompt = {
        let base_prompt =
            render_project_prompt_from_bead(&input.bead_id, br, base_dir.to_path_buf()).await?;
        match &input.prior_failure_context {
            Some(failure) if !failure.trim().is_empty() => format!(
                "{base_prompt}\n\n## Previous attempt failure (auto-injected by drain)\n\nThe previous attempt at this bead produced these verification failures.\nTreat them as explicit signals to fix in this round — they are the\nreason the prior implementation was discarded.\n\n```\n{}\n```\n",
                failure.trim()
            ),
            _ => base_prompt,
        }
    };

    let branch_name = input
        .branch
        .as_deref()
        .map(|branch| effective_branch_name(branch, &bead));
    if let Some(branch_name) = &branch_name {
        branch_port
            .create_branch(base_dir, branch_name)
            .map_err(|details| BeadProjectCreationError::BranchCreate {
                branch_name: branch_name.clone(),
                details,
            })?;
    }

    let record = project_service::create_project(
        store,
        journal_store,
        base_dir,
        CreateProjectInput {
            id: project_id.clone(),
            name: format!("{}: {}", bead.id, bead.title),
            flow: input.flow,
            prompt_path: "generated bead project prompt".to_owned(),
            prompt_hash: FileSystem::prompt_hash(&prompt),
            prompt_contents: prompt,
            created_at: input.created_at,
            task_source: None,
        },
    )?;

    if let Err(details) = br.mark_bead_in_progress(&bead.id).await {
        return Err(BeadProjectCreationError::BeadStatusUpdatePartial {
            project_id: record.id.to_string(),
            bead_id: bead.id,
            details,
        });
    }

    Ok(CreateProjectFromBeadOutput {
        project: record,
        branch_name,
    })
}

pub fn derive_project_id_from_bead_id(bead_id: &str) -> Result<ProjectId, AppError> {
    let suffix = bead_id
        .rsplit('-')
        .next()
        .filter(|part| !part.trim().is_empty())
        .unwrap_or(bead_id);
    ProjectId::new(slugify_identifier(suffix))
}

pub fn derive_feature_branch_name(bead: &BeadDetail) -> String {
    let slug = short_slug(&bead.title);
    if slug.is_empty() {
        format!("feat/{}", bead.id)
    } else {
        format!("feat/{}-{slug}", bead.id)
    }
}

fn effective_branch_name(raw: &str, bead: &BeadDetail) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        derive_feature_branch_name(bead)
    } else {
        trimmed.to_owned()
    }
}

fn short_slug(title: &str) -> String {
    slugify_identifier(title)
        .split('-')
        .take(6)
        .collect::<Vec<_>>()
        .join("-")
}

fn slugify_identifier(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if !previous_was_separator && !slug.is_empty() {
            slug.push('-');
            previous_was_separator = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.is_empty() {
        "bead".to_owned()
    } else {
        slug
    }
}

fn map_prompt_read_error(bead_id: &str, source: BeadPromptReadError) -> BeadProjectCreationError {
    match source {
        BeadPromptReadError::NotFound { .. } => ProjectPromptRenderError::BeadNotFound {
            bead_id: bead_id.to_owned(),
        }
        .into(),
        source => ProjectPromptRenderError::BrRead {
            bead_id: bead_id.to_owned(),
            source,
        }
        .into(),
    }
}
