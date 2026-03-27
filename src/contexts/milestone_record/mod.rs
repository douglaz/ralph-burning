pub mod bundle;
pub mod model;
pub mod queries;
pub mod service;

pub const CONTEXT_NAME: &str = "milestone_record";

// Re-export ports for adapter implementation
pub use service::{
    MilestoneJournalPort, MilestonePlanPort, MilestoneSnapshotPort, MilestoneStorePort,
    TaskRunLineagePort,
};
