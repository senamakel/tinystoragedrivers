use serde_json::json;
use tinystoragedrivers_core::{
    ErrorKind, Fence, Filter, MemoryStorage, Precondition, Scope, StorageBackend,
};

use super::for_scope_fenced;

#[tokio::test]
async fn native_fence_guards_document_stream_and_blob_writes_after_takeover() {
    let backend = MemoryStorage::new();
    let guard_scope = Scope::new("cluster").unwrap();
    let data_scope = Scope::new("profile").unwrap();
    let guard = backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put(
            "leases",
            "alice",
            json!({"holder":"a", "epoch":1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let stale = for_scope_fenced(
        &backend,
        &data_scope,
        &Fence::new(
            guard_scope.clone(),
            "leases",
            "alice",
            Filter::eq("holder", "a").and(Filter::eq("epoch", 1)),
        ),
    )
    .unwrap();
    stale
        .documents()
        .put("notes", "n1", json!({"text":"first"}), Precondition::None)
        .await
        .unwrap();
    stale
        .streams()
        .append("events", json!({"n":1}))
        .await
        .unwrap();
    stale.blobs().put("files/a", vec![1], None).await.unwrap();

    guard
        .documents()
        .put(
            "leases",
            "alice",
            json!({"holder":"b", "epoch":2}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        stale
            .documents()
            .put("notes", "n2", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        stale
            .streams()
            .append("events", json!({"n":2}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        stale
            .blobs()
            .put("files/b", vec![2], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
}
