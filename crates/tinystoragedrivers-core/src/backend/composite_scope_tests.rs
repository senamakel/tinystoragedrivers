//! Composite scope binding, schema admission, and nested composition contracts.

use super::*;

async fn physical_blob_exists(
    storage: &Arc<MemoryStorage>,
    scope: &Scope,
    generation: ScopeGeneration,
    key: &str,
) -> bool {
    let physical_scope = MemoryGenerationBlobs::physical_scope(scope, generation);
    storage
        .for_scope(&physical_scope)
        .unwrap()
        .blobs()
        .get(key)
        .await
        .unwrap()
        .is_some()
}

async fn legacy_composite() -> (CompositeStorage, Scope, Fence) {
    let records = Arc::new(NoPurgeRecords(MemoryStorage::new()));
    let blobs = Arc::new(NoPurgeBlobs(BackendBlobs::new(Arc::new(
        MemoryStorage::new(),
    ))));
    let scope = Scope::new("u:legacy-generation-zero").unwrap();
    let guard_scope = Scope::new("u:legacy-generation-zero-guard").unwrap();
    records
        .0
        .for_scope(&guard_scope)
        .unwrap()
        .documents()
        .put(
            "leases",
            "scope",
            serde_json::json!({"epoch": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    let fence = Fence::epoch(guard_scope, "leases", "scope", "epoch", 1);
    (CompositeStorage::new(records, blobs), scope, fence)
}

#[tokio::test]
async fn generation_zero_composition_uses_legacy_ports_without_purge_capabilities() {
    let (composite, scope, _) = legacy_composite().await;
    let scoped = composite
        .for_scope_generation(&scope, ScopeGeneration::default())
        .unwrap();
    scoped
        .documents()
        .put(
            "notes",
            "one",
            serde_json::json!({"value": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    scoped.blobs().put("one", vec![1], None).await.unwrap();
}

#[tokio::test]
async fn fenced_generation_zero_composition_uses_legacy_ports_without_purge_capabilities() {
    let (composite, scope, fence) = legacy_composite().await;
    let fenced = composite
        .for_scope_fenced_generation(&scope, ScopeGeneration::default(), &fence)
        .unwrap();
    fenced
        .documents()
        .put(
            "notes",
            "two",
            serde_json::json!({"value": 2}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn composite_purge_works_with_predeclared_existing_schema() {
    let records = MemoryStorage::new();
    records.prepare_schema().await.unwrap();
    records
        .for_scope(&Scope::new("__tsd_composite_purge_coordinator").unwrap())
        .unwrap()
        .documents()
        .ensure_collection(&crate::CollectionSpec::new("__tsd_composite_purges"))
        .await
        .unwrap();
    records
        .for_scope(&Scope::new("u:existing-schema-purge").unwrap())
        .unwrap()
        .documents()
        .ensure_collection(&crate::CollectionSpec::new("__tsd_admission"))
        .await
        .unwrap();

    let restricted = records.existing_schema().unwrap();
    let composite = CompositeStorage::new(
        restricted,
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    let scope = Scope::new("u:existing-schema-purge").unwrap();
    let scoped = composite.for_scope(&scope).unwrap();
    scoped.blobs().put("before", vec![1], None).await.unwrap();

    let receipt = scoped
        .purge(&PurgeRequest {
            operation_id: "existing-schema-purge".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(receipt.status, PurgeStatus::Complete);
}

#[tokio::test]
async fn unrestricted_composite_purge_prepares_its_coordinator_schema() {
    let records = Arc::new(MemoryStorage::new());
    let composite = CompositeStorage::new(
        records.clone(),
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    let scoped = composite
        .for_scope(&Scope::new("u:fresh-composite-purge").unwrap())
        .unwrap();

    let receipt = scoped
        .purge(&PurgeRequest {
            operation_id: "fresh-coordinator-schema".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();

    assert_eq!(receipt.status, PurgeStatus::Complete);
    records
        .for_scope(&Scope::new("__tsd_composite_purge_coordinator").unwrap())
        .unwrap()
        .documents()
        .validate_collection_spec(&crate::CollectionSpec::new("__tsd_composite_purges"))
        .await
        .unwrap();
}

#[tokio::test]
async fn existing_schema_blob_admission_does_not_require_an_undeclared_probe() {
    let records = MemoryStorage::new();
    records.prepare_schema().await.unwrap();
    records
        .for_scope(&Scope::new("__tsd_composite_purge_coordinator").unwrap())
        .unwrap()
        .documents()
        .ensure_collection(&crate::CollectionSpec::new("__tsd_composite_purges"))
        .await
        .unwrap();

    let composite = CompositeStorage::new(
        records.existing_schema().unwrap(),
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    let scoped = composite
        .for_scope(&Scope::new("u:existing-schema-no-probe").unwrap())
        .unwrap();
    scoped.blobs().put("ordinary", vec![1], None).await.unwrap();
}

#[tokio::test]
async fn nested_composite_records_keep_public_scope_access() {
    use crate::{PurgeMode, PurgeRequest};

    let inner_blob_data = Arc::new(MemoryStorage::new());
    let outer_blob_data = Arc::new(MemoryStorage::new());
    let inner_records = Arc::new(MemoryStorage::new());
    let inner: Arc<dyn StorageBackend> = Arc::new(CompositeStorage::new(
        inner_records,
        Arc::new(MemoryGenerationBlobs(Arc::clone(&inner_blob_data))),
    ));
    let outer = CompositeStorage::new(
        Arc::clone(&inner),
        Arc::new(MemoryGenerationBlobs(Arc::clone(&outer_blob_data))),
    );
    let scope = Scope::new("u:nested-public-scope").unwrap();

    let inner_scope = inner.for_scope(&scope).unwrap();
    inner_scope
        .blobs()
        .put("inner-before", vec![1], None)
        .await
        .unwrap();
    let scoped = outer.for_scope(&scope).unwrap();
    scoped
        .documents()
        .put(
            "notes",
            "one",
            serde_json::json!({"value": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    scoped.blobs().put("one", vec![1], None).await.unwrap();

    let request = PurgeRequest {
        operation_id: "nested-purge".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = scoped.purge(&request).await.unwrap();
    assert!(
        !physical_blob_exists(&inner_blob_data, &scope, ScopeGeneration(0), "inner-before").await
    );
    assert!(!physical_blob_exists(&outer_blob_data, &scope, ScopeGeneration(0), "one").await);

    let current_inner = inner
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current_inner
        .blobs()
        .put("inner-after", vec![2], None)
        .await
        .unwrap();
    let current_outer = outer
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    current_outer
        .blobs()
        .put("outer-after", vec![3], None)
        .await
        .unwrap();
    assert_eq!(scoped.purge(&request).await.unwrap(), receipt);
    assert!(
        current_inner
            .blobs()
            .get("inner-after")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        current_outer
            .blobs()
            .get("outer-after")
            .await
            .unwrap()
            .is_some()
    );

    let reopened = CompositeStorage::new(
        Arc::clone(&inner),
        Arc::new(MemoryGenerationBlobs(Arc::clone(&outer_blob_data))),
    );
    let reopened_generation = reopened
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    assert!(
        reopened_generation
            .blobs()
            .get("outer-after")
            .await
            .unwrap()
            .is_some()
    );
}
