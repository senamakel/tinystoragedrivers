use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, MemoryStorage, Precondition, Scope, StorageBackend};

use crate::{DocumentLeases, LeaseError, LeaseStore, for_scope_fenced};

#[tokio::test]
async fn document_store_tracks_reentrancy_and_fences_after_expired_takeover() {
    let backend = MemoryStorage::new();
    let guard_scope = Scope::new("cluster").unwrap();
    let cluster = backend.for_scope(&guard_scope).unwrap();
    let first = DocumentLeases::new(
        cluster.documents(),
        guard_scope.clone(),
        "profile",
        "node-a",
        100,
    )
    .unwrap()
    .with_endpoint(Some("https://node-a.example".to_owned()));
    let second = DocumentLeases::new(
        cluster.documents(),
        guard_scope.clone(),
        "profile",
        "node-b",
        100,
    )
    .unwrap()
    .with_endpoint(Some("https://node-b.example".to_owned()));

    let initial = first.acquire("alice", 1_000).await.unwrap();
    let reentrant = first.acquire("alice", 1_025).await.unwrap();
    assert_eq!(initial.epoch, reentrant.epoch);
    assert_eq!(reentrant.version.0, initial.version.0 + 1);
    assert!(matches!(
        first.renew(&initial, 1_030).await,
        Err(LeaseError::Lost)
    ));
    let held = second.acquire("alice", 1_050).await.unwrap_err();
    let LeaseError::Held(record) = held else {
        panic!("expected lease held")
    };
    assert_eq!(record.endpoint.as_deref(), Some("https://node-a.example"));

    let data_scope = Scope::new("profile-data").unwrap();
    let stale = for_scope_fenced(
        &backend,
        &data_scope,
        &first
            .driver_fence("alice", "node-a", reentrant.epoch)
            .unwrap(),
    )
    .unwrap();
    stale
        .documents()
        .put("notes", "n1", json!({"ok":true}), Precondition::None)
        .await
        .unwrap();
    let takeover = second.acquire("alice", 1_126).await.unwrap();
    assert_eq!(takeover.epoch, reentrant.epoch + 1);
    assert!(takeover.previous_unclean);
    assert_eq!(
        second
            .inspect("alice")
            .await
            .unwrap()
            .unwrap()
            .endpoint
            .as_deref(),
        Some("https://node-b.example")
    );
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
        first.inspect("alice").await.unwrap().unwrap().epoch,
        takeover.epoch
    );
}
