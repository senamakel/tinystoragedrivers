//! Generation-isolated `BackendBlobs` contracts.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn scope_digest(scope: &Scope) -> String {
    use std::fmt::Write as _;

    let mut digest = String::with_capacity(64);
    for byte in Sha256::digest(scope.as_str().as_bytes()) {
        write!(&mut digest, "{byte:02x}").unwrap();
    }
    digest
}

fn memory_with_clock() -> (Arc<MemoryStorage>, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000));
    let reading = Arc::clone(&now);
    (
        Arc::new(MemoryStorage::with_clock(Arc::new(move || {
            reading.load(Ordering::SeqCst)
        }))),
        now,
    )
}

async fn put_expired_blob(store: &dyn BlobStore, value: u8) -> crate::Result<()> {
    store
        .put_expiring("expired", vec![value], None, Duration::from_millis(1))
        .await?;
    Ok(())
}

fn physical_generation(
    storage: &MemoryStorage,
    scope: &Scope,
    generation: u64,
) -> crate::Result<Arc<dyn BlobStore>> {
    let physical = Scope::new(format!(
        "__tsd_composite_blob_generation/{}/{}",
        scope_digest(scope),
        generation
    ))?;
    Ok(storage.for_scope(&physical)?.blobs().clone())
}

#[tokio::test]
async fn backend_blobs_reuses_predeclared_mapping_schema() {
    let setup = MemoryStorage::new();
    setup
        .for_scope(&Scope::new("__tsd_composite_blob_scope_map").unwrap())
        .unwrap()
        .documents()
        .ensure_collection(&crate::CollectionSpec::new("scopes"))
        .await
        .unwrap();
    let backend = BackendBlobs::new(setup.existing_schema().unwrap());
    let scope = Scope::new("u:existing-schema-blob-map").unwrap();
    let generation = backend
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap();

    generation.put("object", vec![4], None).await.unwrap();
    assert_eq!(generation.get("object").await.unwrap().unwrap().bytes, [4]);
    backend
        .purge_scope_generation(&scope, ScopeGeneration(1))
        .await
        .unwrap();
    assert!(generation.get("object").await.unwrap().is_none());
}

#[tokio::test]
async fn backend_blobs_requires_a_bootstrapped_mapping_schema() {
    let storage = MemoryStorage::new();
    let backend = BackendBlobs::new(storage.existing_schema().unwrap());
    let scope = Scope::new("u:missing-existing-schema-blob-map").unwrap();
    let error = backend
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap()
        .put("object", vec![4], None)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), crate::ErrorKind::NotFound);
    let physical = Scope::new(format!(
        "__tsd_composite_blob_generation/{}/1",
        scope_digest(&scope)
    ))
    .unwrap();
    assert_eq!(
        storage
            .for_scope(&physical)
            .unwrap()
            .blobs()
            .list("")
            .await
            .unwrap(),
        []
    );
}

#[tokio::test]
async fn backend_blobs_purges_expired_bytes_in_legacy_mapped_and_unmapped_namespaces()
-> crate::Result<()> {
    let (storage, now) = memory_with_clock();
    let backend = BackendBlobs::new(storage.clone());

    let legacy_scope = Scope::new("u:expired-legacy-purge").unwrap();
    let legacy = storage.for_scope(&legacy_scope).unwrap().blobs().clone();
    put_expired_blob(legacy.as_ref(), 1).await?;

    let generation_scope = Scope::new("u:expired-generation-purge").unwrap();
    let generation = backend
        .for_scope_generation(&generation_scope, ScopeGeneration(1))
        .unwrap();
    put_expired_blob(generation.as_ref(), 2).await?;
    let generation_bytes = physical_generation(&storage, &generation_scope, 1)?;
    let later_generation = backend
        .for_scope_generation(&generation_scope, ScopeGeneration(2))
        .unwrap();
    later_generation.put("replacement", vec![22], None).await?;

    let sibling_scope = Scope::new("u:expired-purge-sibling").unwrap();
    let sibling = backend
        .for_scope_generation(&sibling_scope, ScopeGeneration(1))
        .unwrap();
    sibling.put("sibling", vec![23], None).await?;

    let whole_scope = Scope::new("u:expired-whole-scope-purge").unwrap();
    let whole_legacy = storage.for_scope(&whole_scope).unwrap().blobs().clone();
    put_expired_blob(whole_legacy.as_ref(), 3).await?;
    let whole_generation = backend
        .for_scope_generation(&whole_scope, ScopeGeneration(1))
        .unwrap();
    put_expired_blob(whole_generation.as_ref(), 4).await?;
    let whole_generation_bytes = physical_generation(&storage, &whole_scope, 1)?;

    let unmapped_scope = Scope::new("u:expired-unmapped-generation").unwrap();
    let unmapped_bytes = physical_generation(&storage, &unmapped_scope, 7)?;
    put_expired_blob(unmapped_bytes.as_ref(), 5).await?;

    now.fetch_add(2, Ordering::SeqCst);
    backend
        .purge_scope_generation(&legacy_scope, ScopeGeneration(0))
        .await
        .unwrap();
    backend
        .purge_scope_generation(&generation_scope, ScopeGeneration(1))
        .await
        .unwrap();
    backend.purge_scope(&whole_scope).await.unwrap();
    backend
        .purge_scope_generation(&unmapped_scope, ScopeGeneration(7))
        .await
        .unwrap();

    let remaining = legacy.sweep_expired().await?
        + generation_bytes.sweep_expired().await?
        + whole_legacy.sweep_expired().await?
        + whole_generation_bytes.sweep_expired().await?
        + unmapped_bytes.sweep_expired().await?;
    assert_eq!(remaining, 0, "purge must reclaim expired physical bytes");
    assert_eq!(
        later_generation.get("replacement").await?.unwrap().bytes,
        [22],
        "generation purge must retain later-generation data",
    );
    assert_eq!(sibling.get("sibling").await?.unwrap().bytes, [23]);
    Ok(())
}

#[tokio::test]
async fn backend_blobs_isolates_independent_record_and_blob_generations() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage.clone()));
    let backend = CompositeStorage::new(records, blobs.clone());
    let scope = Scope::new("u:independent-purge-supported").unwrap();
    let scoped = backend.for_scope(&scope).unwrap();
    scoped.blobs().put("keep", vec![1], None).await.unwrap();
    assert!(backend.capabilities().contains(Capability::ScopePurge));
    let receipt = scoped
        .purge(&PurgeRequest {
            operation_id: "independent-generation-purge".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(receipt.status, PurgeStatus::Complete);
    let reopened = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    reopened.blobs().put("keep", vec![2], None).await.unwrap();
    assert_eq!(
        reopened.blobs().get("keep").await.unwrap().unwrap().bytes,
        [2]
    );
    assert_eq!(
        blobs.scopes("u:independent").await.unwrap(),
        vec![scope.clone()]
    );
    blobs
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap();
    assert_eq!(
        reopened.blobs().get("keep").await.unwrap().unwrap().bytes,
        [2]
    );
    assert_eq!(
        blob_storage
            .for_scope(&scope)
            .unwrap()
            .blobs()
            .get("keep")
            .await
            .unwrap(),
        None,
        "generation zero is the unchanged legacy namespace and was erased",
    );
    let max_scope = Scope::new("x".repeat(crate::MAX_SCOPE_LEN)).unwrap();
    let max_gen1 = blobs
        .for_scope_generation(&max_scope, ScopeGeneration(1))
        .unwrap();
    max_gen1.put("long-scope", vec![3], None).await.unwrap();
    assert_eq!(blobs.scopes(&"x".repeat(8)).await.unwrap(), vec![max_scope]);
    assert_eq!(
        reopened.blobs().get("keep").await.unwrap().unwrap().bytes,
        [2]
    );
    crate::conformance::run_ports(&reopened).await;
}

#[tokio::test]
async fn backend_blobs_refuses_reserved_scopes_and_legacy_namespace_collisions() {
    let storage = Arc::new(MemoryStorage::new());
    let blobs = BackendBlobs::new(storage.clone());
    for reserved in [
        "__tsd_composite_blob_scope_map",
        "__tsd_composite_blob_scope_map/private",
        "__tsd_composite_blob_generation",
        "__tsd_composite_blob_generation/legacy-user-data",
        "__tsd_composite_purge_coordinator",
        "__tsd_composite_purge_coordinator/private",
    ] {
        let scope = Scope::new(reserved).unwrap();
        assert!(blobs.for_scope(&scope).is_err());
        assert!(
            blobs
                .for_scope_generation(&scope, ScopeGeneration(1))
                .is_err()
        );
        assert!(blobs.purge_scope(&scope).await.is_err());
        assert!(
            blobs
                .purge_scope_generation(&scope, ScopeGeneration(1))
                .await
                .is_err()
        );
    }

    let logical = Scope::new("u:preexisting-collision").unwrap();
    let digest = scope_digest(&logical);
    let physical = Scope::new(format!("__tsd_composite_blob_generation/{digest}/1")).unwrap();
    storage
        .for_scope(&physical)
        .unwrap()
        .blobs()
        .put("legacy-key", vec![7], None)
        .await
        .unwrap();

    let gen1 = blobs
        .for_scope_generation(&logical, ScopeGeneration(1))
        .unwrap();
    let read_rejected = gen1.get("legacy-key").await.is_err();
    let delete_rejected = gen1.delete("legacy-key").await.is_err();
    let legacy_survives = storage
        .for_scope(&physical)
        .unwrap()
        .blobs()
        .get("legacy-key")
        .await
        .unwrap()
        .is_some();
    assert!(read_rejected && delete_rejected && legacy_survives);
    assert!(gen1.put("new-key", vec![8], None).await.is_err());
    assert_eq!(
        storage
            .for_scope(&physical)
            .unwrap()
            .blobs()
            .get("legacy-key")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [7]
    );
}

#[tokio::test]
async fn whole_purge_validates_generation_index_before_erasing_legacy_blobs() {
    let storage = Arc::new(MemoryStorage::new());
    let logical = Scope::new("u:corrupt-index-preserves-legacy").unwrap();
    storage
        .for_scope(&logical)
        .unwrap()
        .blobs()
        .put("legacy", vec![9], None)
        .await
        .unwrap();
    let digest = scope_digest(&logical);
    let map = storage
        .for_scope(&Scope::new("__tsd_composite_blob_scope_map").unwrap())
        .unwrap();
    map.documents()
        .ensure_collection(&crate::CollectionSpec::new("scopes"))
        .await
        .unwrap();
    map.documents()
        .put(
            "scopes",
            &digest,
            serde_json::json!({"schema": 1, "scope": logical.as_str(), "generations": "corrupt"}),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();

    assert!(
        BackendBlobs::new(storage.clone())
            .purge_scope(&logical)
            .await
            .is_err()
    );
    assert_eq!(
        storage
            .for_scope(&logical)
            .unwrap()
            .blobs()
            .get("legacy")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [9]
    );
}

#[tokio::test]
async fn corrupt_generation_mapping_schema_and_order_fail_closed_before_purge() {
    for mapping in [
        serde_json::json!({"schema": 2, "scope": "u:corrupt-map", "generations": [1]}),
        serde_json::json!({"schema": 1, "scope": "u:corrupt-map", "generations": [2, 1]}),
    ] {
        let storage = Arc::new(MemoryStorage::new());
        let logical = Scope::new("u:corrupt-map").unwrap();
        storage
            .for_scope(&logical)
            .unwrap()
            .blobs()
            .put("legacy", vec![6], None)
            .await
            .unwrap();
        let map = storage
            .for_scope(&Scope::new("__tsd_composite_blob_scope_map").unwrap())
            .unwrap();
        map.documents()
            .ensure_collection(&crate::CollectionSpec::new("scopes"))
            .await
            .unwrap();
        map.documents()
            .put(
                "scopes",
                &scope_digest(&logical),
                mapping,
                crate::Precondition::Absent,
            )
            .await
            .unwrap();

        assert!(
            BackendBlobs::new(storage.clone())
                .purge_scope(&logical)
                .await
                .is_err()
        );
        assert_eq!(
            storage
                .for_scope(&logical)
                .unwrap()
                .blobs()
                .get("legacy")
                .await
                .unwrap()
                .unwrap()
                .bytes,
            [6],
            "invalid mapping metadata must not erase legacy data"
        );
    }
}

#[tokio::test]
async fn enumeration_rejects_a_generation_mapping_to_a_reserved_scope() {
    let storage = Arc::new(MemoryStorage::new());
    let logical = Scope::new("__tsd_composite_blob_scope_map").unwrap();
    let digest = scope_digest(&logical);
    let physical = Scope::new(format!("__tsd_composite_blob_generation/{digest}/1")).unwrap();
    storage
        .for_scope(&physical)
        .unwrap()
        .blobs()
        .put("object", vec![1], None)
        .await
        .unwrap();
    let map = storage
        .for_scope(&Scope::new("__tsd_composite_blob_scope_map").unwrap())
        .unwrap();
    map.documents()
        .ensure_collection(&crate::CollectionSpec::new("scopes"))
        .await
        .unwrap();
    map.documents()
        .put(
            "scopes",
            &digest,
            serde_json::json!({"schema": 1, "scope": logical.as_str(), "generations": [1]}),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();

    assert!(BackendBlobs::new(storage).scopes("").await.is_err());
}

#[tokio::test]
async fn backend_blobs_purge_uses_its_generation_index_without_scope_enumeration() {
    let storage = Arc::new(MemoryStorage::new());
    let backend = BackendBlobs::new(Arc::new(NoScopeEnumeration(storage.clone())));
    let scope = Scope::new("u:no-enum-generation-index").unwrap();
    let generation = backend
        .for_scope_generation(&scope, ScopeGeneration(3))
        .unwrap();
    generation.put("object", vec![4], None).await.unwrap();
    assert!(
        !backend
            .capabilities()
            .contains(Capability::ScopeEnumeration)
    );
    assert!(backend.capabilities().contains(Capability::ScopePurge));
    backend.purge_scope(&scope).await.unwrap();
    assert!(generation.get("object").await.unwrap().is_none());
}

#[tokio::test]
async fn fresh_generation_handles_read_as_empty_without_claiming_a_namespace() {
    let storage = Arc::new(MemoryStorage::new());
    let blobs = BackendBlobs::new(storage.clone());
    let scope = Scope::new("u:fresh-empty-generation").unwrap();
    let fresh = blobs
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap();

    assert!(fresh.get("missing").await.unwrap().is_none());
    assert!(fresh.head("missing").await.unwrap().is_none());
    assert!(fresh.get_range("missing", 0..2).await.unwrap().is_none());
    assert!(fresh.get_stream("missing").await.unwrap().is_none());
    assert_eq!(fresh.list("").await.unwrap(), []);
    assert!(!fresh.delete("missing").await.unwrap());
    assert_eq!(fresh.sweep_expired().await.unwrap(), 0);

    let digest = scope_digest(&scope);
    let physical = Scope::new(format!("__tsd_composite_blob_generation/{digest}/1")).unwrap();
    assert_eq!(
        storage
            .for_scope(&physical)
            .unwrap()
            .blobs()
            .list("")
            .await
            .unwrap(),
        []
    );
    assert!(
        storage
            .for_scope(&Scope::new("__tsd_composite_blob_scope_map").unwrap())
            .unwrap()
            .documents()
            .get("scopes", &digest)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn enumeration_rejects_unmapped_malformed_generation_namespaces() {
    let storage = Arc::new(MemoryStorage::new());
    let malformed = Scope::new("__tsd_composite_blob_generation/not-a-digest/1").unwrap();
    storage
        .for_scope(&malformed)
        .unwrap()
        .blobs()
        .put("foreign", vec![1], None)
        .await
        .unwrap();

    assert!(BackendBlobs::new(storage).scopes("").await.is_err());
}

#[derive(Debug, Clone)]
struct NoScopeEnumeration(Arc<MemoryStorage>);

impl StorageBackend for NoScopeEnumeration {
    fn driver(&self) -> &'static str {
        self.0.driver()
    }
    fn document_driver(&self) -> &'static str {
        self.0.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities().without(Capability::ScopeEnumeration)
    }
    fn for_scope(&self, scope: &Scope) -> Result<crate::ScopedStorage> {
        self.0.for_scope(scope)
    }
}
