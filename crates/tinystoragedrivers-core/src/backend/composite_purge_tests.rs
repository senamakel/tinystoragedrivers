//! Composite purge lifecycle and generation-safety regressions.
use super::*;

#[tokio::test]
async fn composite_without_blob_generation_isolation_refuses_purge() {
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
    assert!(!backend.capabilities().contains(Capability::ScopePurge));
    assert!(
        !backend
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    let error = bound
        .purge(&PurgeRequest {
            operation_id: "reset".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::ScopePurge));
    assert_eq!(
        backend
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::ScopePurge)
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
fn every_composite_scope_constructor_rejects_internal_scopes() {
    let backend = CompositeStorage::new(
        Arc::new(MemoryStorage::new()),
        Arc::new(MemoryGenerationBlobs(Arc::new(MemoryStorage::new()))),
    );
    let fence = Fence::epoch(Scope::local(), "leases", "lease", "epoch", 1);
    for name in [
        purge::COMPOSITE_PURGE_SCOPE,
        "__tsd_composite_purge_coordinator/1",
        "__tsd_composite_blob_scope_map",
        "__tsd_composite_blob_scope_map/private",
        "__tsd_composite_blob_generation",
        "__tsd_composite_blob_generation/deadbeef/1",
    ] {
        let reserved = Scope::new(name).unwrap();
        assert_eq!(
            purge::ensure_public_scope(&reserved).unwrap_err().kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            backend.for_scope(&reserved).unwrap_err().kind(),
            crate::ErrorKind::InvalidInput
        );
        assert_eq!(
            backend
                .for_scope_fenced(&reserved, &fence)
                .unwrap_err()
                .kind(),
            crate::ErrorKind::InvalidInput
        );
        assert!(
            backend
                .for_scope_generation(&reserved, ScopeGeneration(0))
                .is_err()
        );
        assert!(
            backend
                .for_scope_fenced_generation(&reserved, ScopeGeneration(0), &fence)
                .is_err()
        );
    }
}

#[tokio::test]
async fn composite_scope_purge_erases_both_ports_and_preserves_siblings() {
    use crate::{PurgeMode, PurgeRequest, PurgeStatus};
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(MemoryGenerationBlobs(blob_storage.clone())),
    );
    assert!(backend.capabilities().contains(Capability::ScopePurge));
    assert!(
        !backend
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    let target = Scope::new("u:purge-target").unwrap();
    let sibling = Scope::new("u:purge-sibling").unwrap();
    for (store, scope) in [(&records, &target), (&records, &sibling)] {
        store
            .for_scope(scope)
            .unwrap()
            .streams()
            .append("s", serde_json::json!(1))
            .await
            .unwrap();
    }
    for scope in [&target, &sibling] {
        MemoryGenerationBlobs(blob_storage.clone())
            .for_scope(scope)
            .unwrap()
            .put("b", vec![1], None)
            .await
            .unwrap();
    }
    let bound = backend.for_scope(&target).unwrap();
    let receipt = bound
        .purge(&PurgeRequest {
            operation_id: "reset-1".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(receipt.status, PurgeStatus::Complete);
    assert_eq!(
        bound.blobs().get("b").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        backend
            .for_scope_generation(&target, receipt.generation)
            .unwrap()
            .streams()
            .len("s")
            .await
            .unwrap(),
        0
    );
    assert!(
        MemoryGenerationBlobs(blob_storage.clone())
            .for_scope(&target)
            .unwrap()
            .get("b")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        records
            .for_scope(&sibling)
            .unwrap()
            .streams()
            .len("s")
            .await
            .unwrap(),
        1
    );
    assert!(
        MemoryGenerationBlobs(blob_storage.clone())
            .for_scope(&sibling)
            .unwrap()
            .get("b")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn successive_composite_purges_preserve_each_completed_history_receipt() {
    use crate::{PurgeMode, PurgeRequest, ScopeGeneration};
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(records, Arc::new(MemoryGenerationBlobs(blob_storage)));
    let scope = Scope::new("u:successive-resets").unwrap();
    let first_request = PurgeRequest {
        operation_id: "successive-first".into(),
        mode: PurgeMode::Quiescent,
    };
    let first = backend
        .for_scope(&scope)
        .unwrap()
        .purge(&first_request)
        .await
        .unwrap();

    let second_scope = backend
        .for_scope_generation(&scope, first.generation)
        .unwrap();
    second_scope
        .blobs()
        .put("generation-one", vec![1], None)
        .await
        .unwrap();
    let second_request = PurgeRequest {
        operation_id: "successive-second".into(),
        mode: PurgeMode::Quiescent,
    };
    let second = second_scope.purge(&second_request).await.unwrap();
    assert_eq!(second.generation, ScopeGeneration(2));
    let current = backend
        .for_scope_generation(&scope, second.generation)
        .unwrap();
    current
        .blobs()
        .put("generation-two", vec![2], None)
        .await
        .unwrap();
    let sibling = Scope::new("u:successive-reset-sibling").unwrap();
    let sibling_store = backend.for_scope(&sibling).unwrap();
    sibling_store
        .blobs()
        .put("sibling", vec![3], None)
        .await
        .unwrap();
    assert_eq!(
        current
            .purge_status(&first_request.operation_id)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(current.purge(&first_request).await.unwrap(), first);
    assert_eq!(
        current
            .blobs()
            .get("generation-two")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [2]
    );
    assert_eq!(
        sibling_store
            .blobs()
            .get("sibling")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [3]
    );
}

#[tokio::test]
async fn composite_purge_records_pending_external_stage_and_replay_preserves_new_generation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(FailPurgeOnce {
        inner: MemoryGenerationBlobs(blob_storage.clone()),
        fail: AtomicBool::new(true),
    });
    let backend = CompositeStorage::new(records, blobs);
    let scope = Scope::new("u:retry-composite").unwrap();
    let old = backend.for_scope(&scope).unwrap();
    old.blobs().put("before", vec![1], None).await.unwrap();
    let request = PurgeRequest {
        operation_id: "retry-composite".into(),
        mode: PurgeMode::Quiescent,
    };
    assert!(old.purge(&request).await.is_err());
    let pending = old.purge_status("retry-composite").await.unwrap().unwrap();
    assert_eq!(pending.status, PurgeStatus::Pending);
    assert_eq!(pending.stage, crate::PurgeStage::Blobs);
    assert_eq!(
        old.purge(&PurgeRequest {
            operation_id: "overlapping-reset".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err()
        .kind(),
        crate::ErrorKind::Conflict
    );
    assert_eq!(
        old.purge_status("retry-composite").await.unwrap(),
        Some(pending.clone()),
        "a different operation cannot replace the durable pending stage"
    );
    let reopened = backend
        .for_scope_generation(&scope, ScopeGeneration(0))
        .unwrap();
    assert_eq!(
        old.validate_purge_generation().await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        reopened
            .validate_purge_generation()
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        old.validate_purge_binding().await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        reopened.validate_purge_binding().await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    for error in [
        old.documents().get("items", "x").await.unwrap_err(),
        reopened.documents().get("items", "x").await.unwrap_err(),
        old.streams().len("journal").await.unwrap_err(),
        reopened.streams().len("journal").await.unwrap_err(),
        old.blobs().get("before").await.unwrap_err(),
        reopened.blobs().get("before").await.unwrap_err(),
    ] {
        assert_eq!(error.kind(), crate::ErrorKind::ScopeGenerationRejected);
    }
    let completed = old.purge(&request).await.unwrap();
    assert_eq!(completed.status, PurgeStatus::Complete);
    let current = backend
        .for_scope_generation(&scope, completed.generation)
        .unwrap();
    current.blobs().put("after", vec![2], None).await.unwrap();
    assert_eq!(
        current.blobs().get("after").await.unwrap().unwrap().bytes,
        [2]
    );
    assert_eq!(
        old.blobs().get("after").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(current.purge(&request).await.unwrap(), completed);
    assert_eq!(
        current.blobs().get("after").await.unwrap().unwrap().bytes,
        [2]
    );
}

#[tokio::test]
async fn stale_composite_generation_cannot_delete_replacement_blobs() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records,
        Arc::new(MemoryGenerationBlobs(blob_storage.clone())),
    );
    let scope = Scope::new("u:stale-composite-purge").unwrap();
    let old = backend.for_scope(&scope).unwrap();
    old.purge(&PurgeRequest {
        operation_id: "first-reset".into(),
        mode: PurgeMode::Quiescent,
    })
    .await
    .unwrap();
    let fresh = backend
        .for_scope_generation(&scope, ScopeGeneration(1))
        .unwrap();
    fresh
        .blobs()
        .put("replacement", vec![7], None)
        .await
        .unwrap();

    assert_eq!(
        old.purge_status("new-reset").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );

    assert_eq!(
        old.purge(&PurgeRequest {
            operation_id: "stale-reset".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err()
        .kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        fresh
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [7]
    );
}

#[tokio::test]
async fn overlapping_instances_cannot_delete_the_next_blob_generation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let first_backend = Arc::new(CompositeStorage::new(
        records.clone(),
        Arc::new(PauseFirstPurge {
            inner: MemoryGenerationBlobs(blob_storage.clone()),
            first: AtomicBool::new(true),
            entered: entered.clone(),
            release: release.clone(),
        }),
    ));
    let second_backend =
        CompositeStorage::new(records, Arc::new(MemoryGenerationBlobs(blob_storage)));
    let scope = Scope::new("u:concurrent-composite-purge").unwrap();
    let request = PurgeRequest {
        operation_id: "same-reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let first_runner = first_backend.clone();
    let first_scope = scope.clone();
    let first_request = request.clone();
    let first = tokio::spawn(async move {
        first_runner
            .for_scope(&first_scope)
            .unwrap()
            .purge(&first_request)
            .await
    });
    entered.notified().await;

    let second_scope = scope.clone();
    let second_request = request.clone();
    let second = async move {
        second_backend
            .for_scope(&second_scope)
            .unwrap()
            .purge(&second_request)
            .await
    };
    let second_receipt = second.await.unwrap();
    assert_eq!(second_receipt.status, crate::PurgeStatus::Complete);
    let fresh = first_backend
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap();
    fresh
        .blobs()
        .put("replacement", vec![5], None)
        .await
        .unwrap();
    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), second_receipt);
    let newest = first_backend
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap();
    assert_eq!(
        newest
            .blobs()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [5]
    );
}

#[tokio::test]
async fn stale_composite_fence_cannot_delete_blobs() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(MemoryGenerationBlobs(blob_storage.clone())),
    );
    let target = Scope::new("u:stale-composite-fence").unwrap();
    let lease_scope = records.for_scope(&Scope::local()).unwrap();
    lease_scope
        .documents()
        .ensure_collection(&crate::CollectionSpec::new("leases"))
        .await
        .unwrap();
    lease_scope
        .documents()
        .put(
            "leases",
            "lease",
            serde_json::json!({"epoch": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    let fence = Fence::epoch(Scope::local(), "leases", "lease", "epoch", 1);
    let stale = backend.for_scope_fenced(&target, &fence).unwrap();
    MemoryGenerationBlobs(blob_storage.clone())
        .for_scope(&target)
        .unwrap()
        .put("preserve", vec![3], None)
        .await
        .unwrap();
    lease_scope
        .documents()
        .put(
            "leases",
            "lease",
            serde_json::json!({"epoch": 2}),
            crate::Precondition::None,
        )
        .await
        .unwrap();

    assert!(stale.purge_status("stale-fenced-reset").await.is_err());
    assert!(
        stale
            .purge(&PurgeRequest {
                operation_id: "stale-fenced-reset".into(),
                mode: PurgeMode::Quiescent,
            })
            .await
            .is_err()
    );
    assert_eq!(
        MemoryGenerationBlobs(blob_storage.clone())
            .for_scope(&target)
            .unwrap()
            .get("preserve")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [3]
    );
}

#[tokio::test]
async fn corrupt_history_fails_closed_before_replacement_data_is_erased() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(MemoryGenerationBlobs(blob_storage.clone())),
    );
    let scope = Scope::new("u:corrupt-history").unwrap();
    let first = backend.for_scope(&scope).unwrap();
    let first_receipt = first
        .purge(&PurgeRequest {
            operation_id: "history-1".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let second = backend
        .for_scope_generation(&scope, first_receipt.generation)
        .unwrap();
    let second_receipt = second
        .purge(&PurgeRequest {
            operation_id: "history-2".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    let current = backend
        .for_scope_generation(&scope, second_receipt.generation)
        .unwrap();
    current.blobs().put("keep", vec![9], None).await.unwrap();

    let coordinator = records
        .for_scope(&super::super::purge::coordinator_scope().unwrap())
        .unwrap();
    let history_id = super::super::purge::coordinator_operation_id(&scope, "history-1").unwrap();
    let previous = coordinator
        .documents()
        .get("__tsd_composite_purges", &history_id)
        .await
        .unwrap()
        .unwrap();
    coordinator
        .documents()
        .put(
            "__tsd_composite_purges",
            &history_id,
            serde_json::json!({"receipt": crate::PurgeReceipt {
                operation_id: "different-operation".into(),
                ..first_receipt.clone()
            }}),
            crate::Precondition::Version(previous.version),
        )
        .await
        .unwrap();

    assert_eq!(
        first.purge_status("history-1").await.unwrap_err().kind(),
        crate::ErrorKind::Backend
    );
    assert_eq!(
        first
            .purge(&PurgeRequest {
                operation_id: "history-1".into(),
                mode: PurgeMode::Quiescent,
            })
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Backend
    );
    assert_eq!(
        current.blobs().get("keep").await.unwrap().unwrap().bytes,
        [9]
    );
}
