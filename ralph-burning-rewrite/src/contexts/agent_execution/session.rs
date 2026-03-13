use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::domain::{BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy};
use crate::shared::error::AppResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub role: BackendRole,
    pub backend_family: BackendFamily,
    pub model_id: String,
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: DateTime<Utc>,
    pub invocation_count: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedSessions {
    pub sessions: Vec<SessionMetadata>,
}

impl PersistedSessions {
    pub fn empty() -> Self {
        Self {
            sessions: Vec::new(),
        }
    }
}

pub trait SessionStorePort {
    fn load_sessions(&self, project_root: &Path) -> AppResult<PersistedSessions>;
    fn save_sessions(&self, project_root: &Path, sessions: &PersistedSessions) -> AppResult<()>;
}

pub struct SessionManager<S> {
    store: S,
}

impl<S> SessionManager<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<S> SessionManager<S>
where
    S: SessionStorePort,
{
    pub fn load_reusable_session(
        &self,
        project_root: &Path,
        role: BackendRole,
        target: &ResolvedBackendTarget,
        session_policy: SessionPolicy,
    ) -> AppResult<Option<SessionMetadata>> {
        if !matches!(session_policy, SessionPolicy::ReuseIfAllowed)
            || !role.allows_session_reuse()
            || !target.supports_session_reuse()
        {
            return Ok(None);
        }

        let sessions = self.store.load_sessions(project_root)?;
        Ok(sessions
            .sessions
            .into_iter()
            .find(|session| session_matches(session, role, target)))
    }

    pub fn record_session(
        &self,
        project_root: &Path,
        role: BackendRole,
        target: &ResolvedBackendTarget,
        session_id: Option<&str>,
        timestamp: DateTime<Utc>,
    ) -> AppResult<()> {
        if !role.allows_session_reuse() || !target.supports_session_reuse() {
            return Ok(());
        }

        let Some(session_id) = session_id else {
            return Ok(());
        };

        let mut sessions = self.store.load_sessions(project_root)?;
        if let Some(existing) = sessions
            .sessions
            .iter_mut()
            .find(|session| session_matches(session, role, target))
        {
            existing.session_id = session_id.to_owned();
            existing.last_used_at = timestamp;
            existing.invocation_count += 1;
        } else {
            sessions.sessions.push(SessionMetadata {
                role,
                backend_family: target.backend.family,
                model_id: target.model.model_id.clone(),
                session_id: session_id.to_owned(),
                created_at: timestamp,
                last_used_at: timestamp,
                invocation_count: 1,
            });
        }

        self.store.save_sessions(project_root, &sessions)
    }
}

fn session_matches(
    session: &SessionMetadata,
    role: BackendRole,
    target: &ResolvedBackendTarget,
) -> bool {
    session.role == role
        && session.backend_family == target.backend.family
        && session.model_id == target.model.model_id
}
