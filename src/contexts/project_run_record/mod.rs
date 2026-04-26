pub(crate) mod fence_util;
pub mod journal;
pub mod model;
pub mod queries;
pub mod service;
pub mod task_prompt_contract;

pub const CONTEXT_NAME: &str = "project_run_record";

// Re-export ports for adapter implementation
pub use service::{
    ActiveProjectPort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    ProjectStorePort, RepositoryResetPort, RollbackPointStorePort, RunSnapshotPort,
    RunSnapshotWritePort, RuntimeLogStorePort, RuntimeLogWritePort,
};

// Re-export service use cases
pub use service::{
    create_project, create_project_from_bead_context, delete_project, get_rollback_point_for_stage,
    list_projects, list_rollback_points, perform_rollback, render_bead_task_prompt, run_history,
    run_status, run_status_json_reconciled, run_status_reconciled, run_tail, show_project,
    BeadProjectContext, CreateProjectFromBeadContextInput, CreateProjectInput,
};
