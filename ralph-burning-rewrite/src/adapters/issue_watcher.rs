use std::path::Path;

use crate::contexts::automation_runtime::model::WatchedIssueMeta;
use crate::contexts::automation_runtime::watcher::IssueWatcherPort;
use crate::shared::error::AppResult;

/// File-based issue watcher that reads candidate issues from
/// `.ralph-burning/daemon/watched/*.json`.
///
/// Each JSON file represents a watched issue candidate with fields matching
/// `WatchedIssueMeta`. The daemon polls this directory each cycle.
pub struct FileIssueWatcher;

impl IssueWatcherPort for FileIssueWatcher {
    fn poll(&self, base_dir: &Path) -> AppResult<Vec<WatchedIssueMeta>> {
        let watched_dir = base_dir
            .join(".ralph-burning")
            .join("daemon")
            .join("watched");

        if !watched_dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut candidates = Vec::new();
        for entry in std::fs::read_dir(&watched_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.extension().is_some_and(|ext| ext == "json") {
                continue;
            }
            let contents = std::fs::read_to_string(&path)?;
            let meta: WatchedIssueMeta = serde_json::from_str(&contents)?;
            candidates.push(meta);
        }

        Ok(candidates)
    }
}

/// In-memory watcher for testing. Returns a fixed set of candidates.
pub struct InMemoryIssueWatcher {
    pub candidates: Vec<WatchedIssueMeta>,
}

impl InMemoryIssueWatcher {
    pub fn new(candidates: Vec<WatchedIssueMeta>) -> Self {
        Self { candidates }
    }

    pub fn empty() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }
}

impl IssueWatcherPort for InMemoryIssueWatcher {
    fn poll(&self, _base_dir: &Path) -> AppResult<Vec<WatchedIssueMeta>> {
        Ok(self.candidates.clone())
    }
}
