//! Composite scope, database and fencing contracts.
use super::*;
use crate::MemoryStorage;

#[tokio::test]
async fn independent_ports_pass_conformance_with_safe_fencing() {
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let backend = CompositeStorage::new(records, blobs);
    crate::conformance::run(&backend, false).await;
}

#[tokio::test]
async fn fenced_composite_blobs_allow_reads_but_refuse_every_mutation() {
    let backend = MemoryStorage::new();
    let scope = backend.for_scope(&Scope::local()).unwrap();
    scope
        .blobs()
        .put("existing", vec![1, 2, 3], None)
        .await
        .unwrap();
    let blobs = FencedBlobs(scope.blobs().clone());
    assert_eq!(blobs.max_key_len(), scope.blobs().max_key_len());
    assert!(!blobs.capabilities().contains(Capability::PresignedUrls));
    assert!(!blobs.capabilities().contains(Capability::BlobSweep));
    assert_eq!(
        blobs.get("existing").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
    assert!(blobs.get_stream("existing").await.unwrap().is_some());
    assert_eq!(
        blobs.get_range("existing", 1..3).await.unwrap().unwrap(),
        [2, 3]
    );
    assert!(blobs.head("existing").await.unwrap().is_some());
    assert_eq!(blobs.list("").await.unwrap().len(), 1);
    assert!(
        blobs
            .presign_get("existing", Duration::from_secs(60))
            .await
            .is_err()
    );
    for error in [
        blobs.put("existing", vec![9], None).await.unwrap_err(),
        blobs
            .put_stream("existing", Box::pin(std::io::Cursor::new(vec![9])), 1, None)
            .await
            .unwrap_err(),
        blobs.delete("existing").await.unwrap_err(),
        blobs
            .presign_put("existing", Duration::from_secs(60))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            error.kind(),
            crate::ErrorKind::Unsupported(Capability::Fencing)
        );
    }
    assert_eq!(
        scope.blobs().get("existing").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
}

#[derive(Debug)]
struct OverclaimingBlobs(BackendBlobs);
impl BlobBackend for OverclaimingBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.0.for_scope(scope)
    }
}

#[derive(Debug)]
struct MaskedRecords(MemoryStorage);
impl StorageBackend for MaskedRecords {
    fn driver(&self) -> &'static str {
        "masked-memory"
    }
    fn document_driver(&self) -> &'static str {
        self.0.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        self.0.for_scope(scope)
    }
}

#[test]
fn blob_backend_cannot_invent_document_watch_or_transaction_capabilities() {
    let records = Arc::new(MaskedRecords(MemoryStorage::new()));
    let blobs = Arc::new(OverclaimingBlobs(BackendBlobs::new(Arc::new(
        MemoryStorage::new(),
    ))));
    let composite = CompositeStorage::new(records.clone(), blobs);
    for cap in [
        Capability::Watch,
        Capability::FullText,
        Capability::ValidatedBatch,
        Capability::Fencing,
    ] {
        assert_eq!(
            composite.capabilities().contains(cap),
            records.capabilities().contains(cap)
        );
    }
    assert!(
        composite
            .capabilities()
            .contains(Capability::StreamingBlobs)
    );
    assert!(composite.capabilities().contains(Capability::PresignedUrls));
}

#[tokio::test]
async fn composite_refuses_purge_and_generation_before_erasing_either_port() {
    use crate::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(OverclaimingBlobs(BackendBlobs::new(blobs.clone()))),
    );
    let scope = Scope::local();
    let bound = backend.for_scope(&scope).unwrap();
    bound
        .streams()
        .append("retained", serde_json::json!(1))
        .await
        .unwrap();
    bound.blobs().put("retained", vec![1], None).await.unwrap();
    for cap in [Capability::ScopePurge, Capability::AtomicScopePurge] {
        assert!(!backend.capabilities().contains(cap));
    }
    let error = bound
        .purge(&PurgeRequest {
            operation_id: "reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::ScopePurge));
    assert_eq!(
        backend
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        records
            .for_scope(&scope)
            .unwrap()
            .streams()
            .len("retained")
            .await
            .unwrap(),
        1
    );
    assert!(
        blobs
            .for_scope(&scope)
            .unwrap()
            .blobs()
            .get("retained")
            .await
            .unwrap()
            .is_some()
    );
}

#[test]
fn composite_document_driver_preserves_record_identity_instead_of_blob_or_wrapper_name() {
    let records = Arc::new(MaskedRecords(MemoryStorage::new()));
    let blobs = Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let backend = CompositeStorage::new(records, blobs);
    assert_eq!(
        backend.driver(),
        "composite",
        "diagnostic wrapper label remains compatible"
    );
    assert_eq!(
        backend.document_driver(),
        "memory",
        "record coordination must not use composite or blob identity"
    );
}

#[test]
fn native_default_and_erased_nested_composite_database_keep_document_driver() {
    let native: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    assert_eq!(native.document_driver(), "memory");
    let inner: Arc<dyn StorageBackend> = Arc::new(CompositeStorage::new(
        native,
        Arc::new(BackendBlobs::new(Arc::new(MaskedRecords(
            MemoryStorage::new(),
        )))),
    ));
    let outer: Arc<dyn StorageBackend> = Arc::new(CompositeStorage::new(
        inner,
        Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
    ));
    assert_eq!(outer.driver(), "composite");
    assert_eq!(outer.document_driver(), "memory");
    assert_eq!(outer.document_driver(), "memory");
}
