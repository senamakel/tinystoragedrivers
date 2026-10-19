//! Native scope purge authority and replay behavior.
use super::*;
use crate::{Capabilities, ErrorKind, Fence, MemoryStorage, ScopedStorage, StorageBackend};
use std::sync::Arc;

#[test]
fn purge_identities_are_explicit_and_validated() {
    for id in ["", "space id", "slash/id", &"x".repeat(129)] {
        assert_eq!(
            PurgeRequest {
                operation_id: id.into(),
                mode: PurgeMode::Atomic
            }
            .validate()
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
    }
    for id in ["reset_1", "RESET-2"] {
        let request = PurgeRequest {
            operation_id: id.into(),
            mode: PurgeMode::Quiescent,
        };
        request.validate().unwrap();
        let encoded = serde_json::to_string(&request).unwrap();
        assert_eq!(
            serde_json::from_str::<PurgeRequest>(&encoded).unwrap(),
            request
        );
    }
}

#[derive(Debug)]
struct Legacy;
impl StorageBackend for Legacy {
    fn driver(&self) -> &'static str {
        "legacy"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        MemoryStorage::new().for_scope(scope)
    }
}
#[test]
fn unknown_adapters_only_accept_legacy_generation() {
    let scope = Scope::local();
    assert!(
        Legacy
            .for_scope_generation(&scope, ScopeGeneration::default())
            .is_ok()
    );
    assert_eq!(
        Legacy
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    let fence = Fence::epoch(Scope::new("guard").unwrap(), "leases", "l", "epoch", 1);
    assert_eq!(
        Legacy
            .for_scope_fenced_generation(&scope, ScopeGeneration::default(), &fence)
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        Legacy
            .for_scope_fenced_generation(&scope, ScopeGeneration(1), &fence)
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[derive(Debug)]
struct StatusUnsupported;
#[async_trait]
impl ScopePurge for StatusUnsupported {
    async fn purge(&self, _: &PurgeRequest) -> Result<PurgeReceipt> {
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "unsupported",
        ))
    }
}

#[tokio::test]
async fn optional_status_defaults_to_typed_unsupported() {
    assert_eq!(
        StatusUnsupported.status("reset").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    let bound = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .with_purge(Arc::new(StatusUnsupported));
    assert_eq!(
        bound.purge_status("reset").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        bound
            .purge(&PurgeRequest {
                operation_id: "reset".into(),
                mode: PurgeMode::Atomic
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[tokio::test]
async fn legacy_purge_ports_do_not_claim_generation_or_fence_validation() {
    let purge = StatusUnsupported;
    assert!(!purge.supports_generation_validation());
    assert!(!purge.supports_binding_validation());
    assert_eq!(
        purge.validate_generation().await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        purge.validate_binding().await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[test]
fn unsupported_generation_refusal_is_typed() {
    let error = unsupported_generation::<()>(&Scope::local(), ScopeGeneration(1)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::ScopePurge));
}
