use super::*;
use crate::{BackendBlobs, BlobBackend, CompositeStorage, MemoryStorage, StorageBackend};

#[derive(Debug)]
struct InvalidZeroGenerationReceipt;

#[async_trait]
impl ScopePurge for InvalidZeroGenerationReceipt {
    async fn validate_binding(&self) -> crate::Result<()> {
        Ok(())
    }

    async fn purge(&self, _request: &PurgeRequest) -> Result<PurgeReceipt> {
        Err(StorageError::backend("unexpected native purge call"))
    }

    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        Ok(Some(PurgeReceipt {
            operation_id: operation_id.to_owned(),
            generation: ScopeGeneration(0),
            status: PurgeStatus::Complete,
            stage: crate::PurgeStage::Complete,
        }))
    }
}

#[derive(Debug)]
struct MismatchedOperationReceipt;

#[async_trait]
impl ScopePurge for MismatchedOperationReceipt {
    async fn validate_binding(&self) -> crate::Result<()> {
        Ok(())
    }

    async fn purge(&self, _request: &PurgeRequest) -> Result<PurgeReceipt> {
        Err(StorageError::backend("unexpected native purge call"))
    }

    async fn status(&self, _operation_id: &str) -> Result<Option<PurgeReceipt>> {
        Ok(Some(PurgeReceipt {
            operation_id: "another-operation".into(),
            generation: ScopeGeneration(1),
            status: PurgeStatus::Complete,
            stage: crate::PurgeStage::Complete,
        }))
    }
}

#[tokio::test]
async fn malformed_coordinator_record_fails_closed_before_scope_erasure() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let target = Scope::new("u:corrupt-coordinator").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    coordinator
        .documents()
        .put(
            COMPOSITE_PURGE_COLLECTION,
            &coordinator_id(&target).unwrap(),
            serde_json::json!({"not_a_receipt": true}),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();
    let scoped = records.for_scope(&target).unwrap();
    scoped
        .streams()
        .append("preserve", serde_json::json!(1))
        .await
        .unwrap();
    let purge = CompositePurge {
        records: scoped,
        blobs: Arc::new(BackendBlobs::new(blob_storage)),
        scope: target,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let error = purge
        .purge(&PurgeRequest {
            operation_id: "corrupt".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert_eq!(
        purge.records.streams().len("preserve").await.unwrap(),
        1,
        "invalid coordination state must not erase tenant records"
    );
}

#[test]
fn composite_nesting_rejects_a_layer_beyond_the_supported_depth() {
    assert_eq!(
        coordinator_scope_for_layer(33).unwrap_err().kind(),
        crate::ErrorKind::InvalidInput
    );
    assert!(coordinator_scope_for_layer(32).is_ok());
}

#[tokio::test]
async fn status_for_an_unseen_operation_returns_none() {
    let records = Arc::new(MemoryStorage::new());
    let scope = Scope::new("u:unseen-composite-operation").unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs: Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
        scope,
        generation: ScopeGeneration(0),
        coordinator: records.for_scope(&coordinator_scope().unwrap()).unwrap(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    assert_eq!(composite.status("never-started").await.unwrap(), None);
}

#[tokio::test]
async fn conflict_recovery_without_a_current_receipt_is_not_completion() {
    let records = Arc::new(MemoryStorage::new());
    let scope = Scope::new("u:disappeared-purge-coordinator").unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs: Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
        scope,
        generation: ScopeGeneration(0),
        coordinator: records.for_scope(&coordinator_scope().unwrap()).unwrap(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    assert_eq!(
        composite
            .completed_after_conflict("concurrent-operation")
            .await
            .unwrap(),
        None,
        "a missing coordinator receipt cannot confirm a concurrent completion"
    );
    let current = composite
        .coordinator
        .documents()
        .get(
            COMPOSITE_PURGE_COLLECTION,
            &coordinator_id(&composite.scope).unwrap(),
        )
        .await
        .unwrap();
    let history = composite
        .coordinator
        .documents()
        .get(
            COMPOSITE_PURGE_COLLECTION,
            &coordinator_operation_id(&composite.scope, "concurrent-operation").unwrap(),
        )
        .await
        .unwrap();
    assert!(current.is_none());
    assert!(history.is_none());
}

#[tokio::test]
async fn revoked_coordinator_handle_fails_closed_before_blob_erasure() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:revoked-purge-coordinator").unwrap();
    let coordinator_scope = coordinator_scope().unwrap();
    let coordinator = records.for_scope(&coordinator_scope).unwrap();
    let tenant_blobs = blobs.for_scope(&scope).unwrap();
    tenant_blobs.put("preserve", vec![9], None).await.unwrap();

    records
        .for_scope(&coordinator_scope)
        .unwrap()
        .purge(&PurgeRequest {
            operation_id: "reset-coordinator-state".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs,
        scope,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let error = composite
        .purge(&PurgeRequest {
            operation_id: "must-not-delete".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::ScopeGenerationRejected);
    assert_eq!(
        tenant_blobs.get("preserve").await.unwrap().unwrap().bytes,
        [9]
    );
}

#[tokio::test]
async fn maximum_scope_and_operation_ids_have_bounded_coordinator_keys() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let scope = Scope::new("x".repeat(crate::MAX_SCOPE_LEN)).unwrap();
    let scoped = records.for_scope(&scope).unwrap();
    let purge = CompositePurge {
        records: scoped,
        blobs: Arc::new(BackendBlobs::new(blob_storage)),
        scope,
        generation: ScopeGeneration(0),
        coordinator: records.for_scope(&coordinator_scope().unwrap()).unwrap(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let operation_id = "o".repeat(128);
    let receipt = purge
        .purge(&PurgeRequest {
            operation_id: operation_id.clone(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(
        purge.status(&operation_id).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        purge
            .purge(&PurgeRequest {
                operation_id,
                mode: PurgeMode::Quiescent,
            })
            .await
            .unwrap(),
        receipt
    );
}

#[tokio::test]
async fn completed_history_replay_after_a_later_generation_preserves_replacement_data() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:composite-history-replay").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();

    let first = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs: blobs.clone(),
        scope: scope.clone(),
        generation: ScopeGeneration(0),
        coordinator: coordinator.clone(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let first_request = PurgeRequest {
        operation_id: "history-replay-1".into(),
        mode: PurgeMode::Quiescent,
    };
    let first_receipt = first.purge(&first_request).await.unwrap();

    let second = CompositePurge {
        records: records
            .for_scope_generation(&scope, first_receipt.generation)
            .unwrap(),
        blobs: blobs.clone(),
        scope: scope.clone(),
        generation: first_receipt.generation,
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let second_receipt = second
        .purge(&PurgeRequest {
            operation_id: "history-replay-2".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    blobs
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap()
        .put("replacement", vec![9], None)
        .await
        .unwrap();

    assert_eq!(
        first.status("history-replay-1").await.unwrap(),
        Some(first_receipt.clone())
    );
    assert_eq!(first.purge(&first_request).await.unwrap(), first_receipt);
    assert_eq!(
        blobs
            .for_scope_generation(&scope, second_receipt.generation)
            .unwrap()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [9],
        "replaying historical completion must not erase later-generation data"
    );
}

#[tokio::test]
async fn current_completion_replays_when_its_history_index_is_missing() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:current-purge-replay").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    let scoped = records.for_scope(&scope).unwrap();
    let composite = CompositePurge {
        records: scoped,
        blobs: blobs.clone(),
        scope: scope.clone(),
        generation: ScopeGeneration(0),
        coordinator: coordinator.clone(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let request = PurgeRequest {
        operation_id: "current-completion-replay".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = composite.purge(&request).await.unwrap();
    let replacement = blobs
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    replacement.put("replacement", vec![8], None).await.unwrap();

    let history_id = coordinator_operation_id(&scope, &request.operation_id).unwrap();
    let history = coordinator
        .documents()
        .get(COMPOSITE_PURGE_COLLECTION, &history_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        coordinator
            .documents()
            .delete(COMPOSITE_PURGE_COLLECTION, &history_id, history.unchanged(),)
            .await
            .unwrap()
    );

    assert_eq!(composite.purge(&request).await.unwrap(), receipt);
    assert_eq!(
        replacement.get("replacement").await.unwrap().unwrap().bytes,
        [8],
        "the current complete receipt remains authoritative if its history row is absent"
    );
}

#[tokio::test]
async fn mismatched_native_status_cannot_authorize_blob_deletion() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage.clone()));
    let scope = Scope::new("u:mismatched-native-receipt").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    let legacy = blobs.for_scope(&scope).unwrap();
    legacy.put("preserve", vec![3], None).await.unwrap();
    let scoped = records
        .for_scope(&scope)
        .unwrap()
        .with_purge(Arc::new(MismatchedOperationReceipt));
    let composite = CompositePurge {
        records: scoped,
        blobs,
        scope,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let error = composite
        .purge(&PurgeRequest {
            operation_id: "requested-operation".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert_eq!(legacy.get("preserve").await.unwrap().unwrap().bytes, [3]);
}

#[tokio::test]
async fn exhausted_generation_space_refuses_purge_without_mutation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:exhausted-generation").unwrap();
    let generation = ScopeGeneration(u64::MAX);
    let scoped = records.for_scope_generation(&scope, generation).unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    let current_blobs = blobs.for_scope_generation(&scope, generation).unwrap();
    current_blobs.put("preserve", vec![6], None).await.unwrap();
    assert_eq!(
        CompositePurge::generations(generation, "generation-overflow", None)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Backend
    );
    let composite = CompositePurge {
        records: scoped,
        blobs,
        scope,
        generation,
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let error = composite
        .purge(&PurgeRequest {
            operation_id: "generation-overflow".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::ScopeGenerationRejected);
    assert_eq!(
        current_blobs.get("preserve").await.unwrap().unwrap().bytes,
        [6]
    );
}

#[tokio::test]
async fn composite_purge_reconciles_native_receipt_without_erasing_new_generation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:composite-native-receipt-recovery").unwrap();
    let operation_id = "native-receipt-recovery";
    let request = PurgeRequest {
        operation_id: operation_id.into(),
        mode: PurgeMode::Quiescent,
    };
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    let legacy_blobs = blobs.for_scope(&scope).unwrap();
    legacy_blobs
        .put("pending-delete", vec![4], None)
        .await
        .unwrap();

    let native_receipt = records
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    let next_generation_blobs = blobs
        .for_scope_generation(&scope, native_receipt.generation)
        .unwrap();
    next_generation_blobs
        .put("replacement", vec![5], None)
        .await
        .unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs,
        scope: scope.clone(),
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let receipt = composite.purge(&request).await.unwrap();
    assert_eq!(receipt, native_receipt);
    assert_eq!(receipt.status, crate::PurgeStatus::Complete);
    assert!(legacy_blobs.get("pending-delete").await.unwrap().is_none());
    assert_eq!(
        next_generation_blobs
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [5]
    );
    assert_eq!(composite.status(operation_id).await.unwrap(), Some(receipt));
}

#[tokio::test]
async fn public_composite_reconciliation_preserves_writes_after_native_purge() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs: Arc<dyn BlobBackend> = Arc::new(BackendBlobs::new(blob_storage));
    let backend = CompositeStorage::new(records.clone(), blobs);
    let scope = Scope::new("u:public-native-reconciliation").unwrap();
    let request = PurgeRequest {
        operation_id: "native-reset".into(),
        mode: PurgeMode::Quiescent,
    };
    backend
        .for_scope(&scope)
        .unwrap()
        .blobs()
        .put("legacy", vec![1], None)
        .await
        .unwrap();

    let native_receipt = records
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    let current = backend
        .for_scope_generation(&scope, native_receipt.generation)
        .unwrap();
    current
        .blobs()
        .put("replacement", vec![5], None)
        .await
        .unwrap();

    let receipt = current.purge(&request).await.unwrap();
    assert_eq!(receipt, native_receipt);
    assert_eq!(
        current
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [5],
        "reconciling an older native receipt must not erase generation-one writes"
    );
}

#[tokio::test]
async fn invalid_native_receipt_is_rejected_before_blob_deletion() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage.clone()));
    let scope = Scope::new("u:invalid-native-receipt").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    let scoped_records = records
        .for_scope(&scope)
        .unwrap()
        .with_purge(Arc::new(InvalidZeroGenerationReceipt));
    let legacy_blobs = blobs.for_scope(&scope).unwrap();
    legacy_blobs.put("preserve", vec![7], None).await.unwrap();
    let composite = CompositePurge {
        records: scoped_records,
        blobs,
        scope,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let error = composite
        .purge(&PurgeRequest {
            operation_id: "invalid-receipt".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert_eq!(
        legacy_blobs.get("preserve").await.unwrap().unwrap().bytes,
        [7]
    );
}

#[tokio::test]
async fn corrupt_pending_target_generation_fails_closed_before_blob_deletion() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage.clone()));
    let scope = Scope::new("u:corrupt-target-generation").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    coordinator
        .documents()
        .put(
            COMPOSITE_PURGE_COLLECTION,
            &coordinator_id(&scope).unwrap(),
            serde_json::json!({
                "receipt": PurgeReceipt {
                    operation_id: "corrupt-target".into(),
                    generation: ScopeGeneration(2),
                    status: PurgeStatus::Pending,
                    stage: crate::PurgeStage::Blobs,
                },
                (TARGET_GENERATION_FIELD): 0
            }),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();
    let legacy = blobs.for_scope(&scope).unwrap();
    legacy.put("legacy", vec![1], None).await.unwrap();
    let next = blobs
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap();
    next.put("next", vec![2], None).await.unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs,
        scope,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let error = composite
        .purge(&PurgeRequest {
            operation_id: "corrupt-target".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert_eq!(legacy.get("legacy").await.unwrap().unwrap().bytes, [1]);
    assert_eq!(next.get("next").await.unwrap().unwrap().bytes, [2]);
}
