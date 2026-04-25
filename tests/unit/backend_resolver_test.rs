use ralph_burning::contexts::agent_execution::service::{BackendResolver, BackendSelectionConfig};
use ralph_burning::shared::domain::{BackendFamily, BackendRole};

#[test]
fn backend_resolver_preserves_role_specific_default_model_when_backend_family_changes() {
    let resolver = BackendResolver::new();
    let workspace = BackendSelectionConfig {
        backend_family: Some(BackendFamily::Claude),
        model_id: None,
    };
    let project = BackendSelectionConfig {
        backend_family: Some(BackendFamily::Codex),
        model_id: None,
    };

    let resolved = resolver
        .resolve(
            BackendRole::Implementer,
            None,
            Some(&project),
            Some(&workspace),
        )
        .expect("resolve target");

    assert_eq!(resolved.backend.family, BackendFamily::Codex);
    assert_eq!(resolved.model.backend_family, BackendFamily::Codex);
    assert_eq!(resolved.model.model_id, "gpt-5.5-high");
}
