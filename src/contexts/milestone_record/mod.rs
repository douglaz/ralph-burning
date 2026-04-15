pub mod bead_refs;
pub mod bundle;
pub mod controller;
pub mod model;
pub mod queries;
pub mod service;

pub const CONTEXT_NAME: &str = "milestone_record";

// Re-export ports for adapter implementation
pub use service::{
    MilestoneJournalPort, MilestonePlanPort, MilestoneSnapshotPort, MilestoneStorePort,
    PlannedElsewhereMappingPort, TaskRunLineagePort,
};
