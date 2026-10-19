//! Composite scope, database and fencing contracts.
use super::*;
use crate::{
    Fence, MemoryStorage, PurgeMode, PurgeRequest, PurgeStatus, ScopeGeneration, StorageError,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Debug)]
struct FailPurgeOnce {
    inner: MemoryGenerationBlobs,
    fail: AtomicBool,
}

#[derive(Debug)]
struct PauseFirstPurge {
    inner: MemoryGenerationBlobs,
    first: AtomicBool,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone)]
struct MemoryGenerationBlobs(Arc<MemoryStorage>);

#[derive(Debug)]
struct LegacyBlobBackend;

#[async_trait]
impl BlobBackend for LegacyBlobBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        Ok(MemoryStorage::new().for_scope(scope)?.blobs().clone())
    }
}

#[tokio::test]
async fn independent_ports_pass_conformance_with_safe_fencing() {
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let backend = CompositeStorage::new(records, blobs);
    crate::conformance::run_with_process_capabilities(&backend, false, false, false).await;
}

#[tokio::test]
async fn legacy_blob_backend_defaults_refuse_generation_purge_and_enumeration() {
    let backend = LegacyBlobBackend;
    let scope = Scope::local();
    assert_eq!(
        backend
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend
            .purge_scope_generation(&scope, ScopeGeneration(1))
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend.purge_scope(&scope).await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend.scopes("").await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ScopeEnumeration)
    );
}

impl MemoryGenerationBlobs {
    fn physical_scope(scope: &Scope, generation: ScopeGeneration) -> Scope {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(scope.as_str().len() * 2);
        for byte in scope.as_str().bytes() {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        Scope::new(format!("u:test-generation:{encoded}:{}", generation.0)).unwrap()
    }
}

#[async_trait]
impl BlobBackend for MemoryGenerationBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::ScopePurge)
            .with(Capability::ScopeEnumeration)
            .with(Capability::StreamingBlobs)
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.for_scope_generation(scope, ScopeGeneration(0))
    }
    fn supports_generation_purge(&self) -> bool {
        true
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        Ok(self
            .0
            .for_scope(&Self::physical_scope(scope, generation))?
            .blobs()
            .clone())
    }
    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        let blobs = self.for_scope_generation(scope, generation)?;
        for entry in blobs.list("").await? {
            blobs.delete(&entry.key).await?;
        }
        Ok(())
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        for generation in 0..=2 {
            self.purge_scope_generation(scope, ScopeGeneration(generation))
                .await?;
        }
        Ok(())
    }
    async fn scopes(&self, _prefix: &str) -> Result<Vec<Scope>> {
        Ok(Vec::new())
    }
}

#[derive(Debug)]
struct NoPurgeBlobs(BackendBlobs);
impl BlobBackend for NoPurgeBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.0.for_scope(scope)
    }
}

#[derive(Debug)]
struct NoPurgeRecords(MemoryStorage);

impl StorageBackend for NoPurgeRecords {
    fn driver(&self) -> &'static str {
        self.0.driver()
    }
    fn document_driver(&self) -> &'static str {
        self.0.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities().without(Capability::ScopePurge)
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        self.0.for_scope(scope)
    }
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        self.0.for_scope_fenced(scope, fence)
    }
}

#[derive(Debug)]
struct LegacyPurgeRecords(MemoryStorage);

#[derive(Debug)]
struct BindingOnlyPurge;

#[derive(Debug)]
struct BindingOnlyRecords(MemoryStorage);

#[derive(Debug)]
struct LegacyNativePurge;

#[async_trait]
impl crate::ScopePurge for LegacyNativePurge {
    async fn purge(&self, request: &PurgeRequest) -> Result<crate::PurgeReceipt> {
        Ok(crate::PurgeReceipt {
            operation_id: request.operation_id.clone(),
            generation: ScopeGeneration(1),
            status: PurgeStatus::Complete,
            stage: crate::PurgeStage::Complete,
        })
    }
}

#[async_trait]
impl crate::ScopePurge for BindingOnlyPurge {
    fn supports_binding_validation(&self) -> bool {
        true
    }
    async fn validate_binding(&self) -> Result<()> {
        Ok(())
    }
    async fn purge(&self, request: &PurgeRequest) -> Result<crate::PurgeReceipt> {
        LegacyNativePurge.purge(request).await
    }
}

impl StorageBackend for LegacyPurgeRecords {
    fn driver(&self) -> &'static str {
        self.0.driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self
            .0
            .for_scope(scope)?
            .with_purge(Arc::new(LegacyNativePurge)))
    }
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        Ok(self
            .0
            .for_scope_fenced(scope, fence)?
            .with_purge(Arc::new(LegacyNativePurge)))
    }
}

impl StorageBackend for BindingOnlyRecords {
    fn driver(&self) -> &'static str {
        self.0.driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }
    fn supports_scope_purge_binding_validation(&self) -> bool {
        true
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self
            .0
            .for_scope(scope)?
            .with_purge(Arc::new(BindingOnlyPurge)))
    }
}

#[tokio::test]
async fn composite_does_not_advertise_purge_for_legacy_native_purge_ports() {
    let composite = CompositeStorage::new(
        Arc::new(LegacyPurgeRecords(MemoryStorage::new())),
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    let scope = Scope::new("u:legacy-purge-validation").unwrap();
    assert!(!composite.capabilities().contains(Capability::ScopePurge));
    let scoped = composite.for_scope(&scope).unwrap();
    assert_eq!(
        scoped
            .purge(&PurgeRequest {
                operation_id: "legacy-validator".into(),
                mode: PurgeMode::Quiescent,
            })
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[tokio::test]
async fn composite_with_binding_only_records_does_not_advertise_generation_purge() {
    let composite = CompositeStorage::new(
        Arc::new(BindingOnlyRecords(MemoryStorage::new())),
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    assert!(!composite.capabilities().contains(Capability::ScopePurge));
    assert_eq!(
        composite
            .for_scope_generation(
                &Scope::new("u:binding-only-records").unwrap(),
                ScopeGeneration(1),
            )
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(composite.driver(), "composite");
    assert_eq!(composite.document_driver(), "memory");
    assert_eq!(composite.composite_depth(), Some(1));
    assert!(!composite.supports_scope_purge_binding_validation());
    assert!(!composite.supports_scope_purge_generation_validation());
    assert_eq!(
        composite
            .for_composite_internal_scope(&Scope::new("u:ordinary").unwrap())
            .unwrap_err()
            .kind(),
        crate::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn native_records_purge_revokes_external_blob_handles_without_composite_purge() {
    let records = Arc::new(MemoryStorage::new());
    let external_blobs = Arc::new(MemoryStorage::new());
    let composite = CompositeStorage::new(
        records.clone(),
        Arc::new(NoPurgeBlobs(BackendBlobs::new(external_blobs))),
    );
    let scope = Scope::new("u:native-purge-external-blobs").unwrap();
    let old = composite.for_scope(&scope).unwrap();
    old.blobs().put("object", vec![7], None).await.unwrap();

    records
        .for_scope(&scope)
        .unwrap()
        .purge(&PurgeRequest {
            operation_id: "native-record-purge".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();

    assert_eq!(
        old.blobs().get("object").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
}

#[tokio::test]
async fn blob_backends_without_optional_scope_ports_refuse_them() {
    let backend = NoPurgeBlobs(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let scope = Scope::new("u:no-optional-blob-ports").unwrap();
    assert_eq!(
        backend.purge_scope(&scope).await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend.scopes("").await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ScopeEnumeration)
    );

    let composite = CompositeStorage::new(
        Arc::new(MaskedRecords(MemoryStorage::new())),
        Arc::new(backend),
    );
    let scoped = composite.for_scope(&scope).unwrap();
    assert_eq!(
        scoped
            .purge(&PurgeRequest {
                operation_id: "unsupported".into(),
                mode: PurgeMode::Quiescent
            })
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        composite
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
    let fence = Fence::epoch(Scope::local(), "leases", "lease", "epoch", 1);
    assert_eq!(
        composite
            .for_scope_fenced_generation(&scope, ScopeGeneration(1), &fence)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[async_trait]
impl BlobBackend for FailPurgeOnce {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.inner.for_scope(scope)
    }
    fn supports_generation_purge(&self) -> bool {
        true
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        self.inner.for_scope_generation(scope, generation)
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StorageError::backend("injected blob purge failure"));
        }
        self.inner.purge_scope(scope).await
    }
    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StorageError::backend("injected blob purge failure"));
        }
        self.inner.purge_scope_generation(scope, generation).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        self.inner.scopes(prefix).await
    }
}

#[async_trait]
impl BlobBackend for PauseFirstPurge {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.inner.for_scope(scope)
    }
    fn supports_generation_purge(&self) -> bool {
        true
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        self.inner.for_scope_generation(scope, generation)
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        if self.first.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.purge_scope(scope).await
    }
    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        if self.first.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.purge_scope_generation(scope, generation).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        self.inner.scopes(prefix).await
    }
}

#[path = "backend_blobs_tests.rs"]
mod backend_blobs_tests;

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
        blobs
            .put_expiring("existing", vec![9], None, Duration::from_secs(60))
            .await
            .unwrap_err(),
        blobs
            .put_stream_expiring(
                "existing",
                Box::pin(std::io::Cursor::new(vec![9])),
                1,
                None,
                Duration::from_secs(60),
            )
            .await
            .unwrap_err(),
        blobs.delete("existing").await.unwrap_err(),
        blobs.sweep_expired().await.unwrap_err(),
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

#[tokio::test]
async fn generation_admitted_blobs_forward_every_blob_operation() {
    let backend = CompositeStorage::new(
        Arc::new(MemoryStorage::new()),
        Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
    );
    let scope = Scope::new("u:generation-blob-forwarding").unwrap();
    let blobs = backend.for_scope(&scope).unwrap();
    let store = blobs.blobs();
    store.put("plain", vec![1, 2, 3], None).await.unwrap();
    store
        .put_stream(
            "stream",
            Box::pin(std::io::Cursor::new(vec![4, 5])),
            2,
            None,
        )
        .await
        .unwrap();
    let _ = store
        .put_expiring("expiring", vec![6], None, Duration::from_secs(5))
        .await;
    let _ = store
        .put_stream_expiring(
            "stream-expiring",
            Box::pin(std::io::Cursor::new(vec![7])),
            1,
            None,
            Duration::from_secs(5),
        )
        .await;
    assert!(store.get("plain").await.unwrap().is_some());
    assert!(store.get_stream("plain").await.unwrap().is_some());
    assert_eq!(store.get_range("plain", 0..1).await.unwrap().unwrap(), [1]);
    assert!(store.head("plain").await.unwrap().is_some());
    assert_ne!(store.list("").await.unwrap(), []);
    let _ = store.presign_get("plain", Duration::from_secs(5)).await;
    let _ = store.presign_put("presigned", Duration::from_secs(5)).await;
    let _ = store.sweep_expired().await;
    store
        .put_tree(
            "tree",
            vec![crate::TreeFile {
                path: "child".into(),
                bytes: vec![8],
                content_type: None,
            }],
        )
        .await
        .unwrap();
    assert_ne!(store.list_dir("tree").await.unwrap(), []);
    assert_eq!(store.delete_tree("tree").await.unwrap(), 1);
    assert!(store.delete("plain").await.unwrap());
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

#[path = "composite_purge_tests.rs"]
mod purge_tests;

#[path = "composite_scope_tests.rs"]
mod scope_tests;

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
