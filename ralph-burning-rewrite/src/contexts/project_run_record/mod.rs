pub mod journal;
pub mod model;
pub mod queries;
pub mod service;

pub const CONTEXT_NAME: &str = "project_run_record";

// Re-export ports for adapter implementation
pub use service::{
    ActiveProjectPort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    ProjectStorePort, RunSnapshotPort, RunSnapshotWritePort, RuntimeLogStorePort,
    RuntimeLogWritePort,
};

// Re-export service use cases
pub use service::{
    create_project, delete_project, list_projects, run_history, run_status, run_tail, show_project,
    CreateProjectInput,
};
