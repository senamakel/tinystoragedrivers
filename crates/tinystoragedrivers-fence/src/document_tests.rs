use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, MemoryStorage, Precondition, Scope, StorageBackend};

use crate::{DocumentLeases, LeaseError, LeaseStore, for_scope_fenced};

fn memory_documents() -> (Arc<dyn tinystoragedrivers_core::DocumentStore>, Scope) {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let scoped = backend.for_scope(&scope).unwrap();
    let documents = scoped.documents().clone();
    (documents, scope)
}

#[test]
fn document_store_rejects_invalid_owner_holder_and_ttl() {
    let (documents, scope) = memory_documents();
    for (owner, holder, ttl_ms) in [
        ("", "node", 1),
        ("profile", "", 1),
        ("profile", "node", 0),
        ("profile", "node", -1),
    ] {
        assert_eq!(
            DocumentLeases::new(&documents, scope.clone(), owner, holder, ttl_ms)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[test]
fn document_store_debug_shows_configuration_without_internal_state() {
    let (documents, scope) = memory_documents();
    let store = DocumentLeases::new(&documents, scope, "profile", "node-a", 100)
        .unwrap()
        .with_endpoint(Some("https://node-a.example".to_owned()));
    let debug = format!("{store:?}");
    assert!(debug.contains("guard_scope"));
    assert!(debug.contains("profile"));
    assert!(debug.contains("node-a"));
    assert!(debug.contains("https://node-a.example"));
    assert!(debug.contains(".."));
    assert!(!debug.contains("now_ms"));
    assert!(!debug.contains("grants"));
}

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

#[tokio::test]
async fn document_store_release_retires_only_the_latest_grant() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();
    let first = DocumentLeases::new(
        documents.documents(),
        scope.clone(),
        "profile",
        "node-a",
        100,
    )
    .unwrap();
    let next = DocumentLeases::new(documents.documents(), scope, "profile", "node-b", 100).unwrap();

    let original = first.acquire("alice", 1_000).await.unwrap();
    let latest = first.renew(&original, 1_010).await.unwrap();
    assert!(matches!(
        first.release(original).await,
        Err(LeaseError::Lost)
    ));
    assert!(!first.inspect("alice").await.unwrap().unwrap().released);
    first.release(latest.clone()).await.unwrap();
    assert!(first.inspect("alice").await.unwrap().unwrap().released);
    assert!(matches!(
        first.release(latest.clone()).await,
        Err(LeaseError::Lost)
    ));

    let replacement = next.acquire("alice", 1_001).await.unwrap();
    assert_eq!(replacement.epoch, 2);
    assert!(matches!(first.release(latest).await, Err(LeaseError::Lost)));
    let current = next.inspect("alice").await.unwrap().unwrap();
    assert_eq!(current.holder, "node-b");
    assert_eq!(current.epoch, replacement.epoch);
    assert!(!current.released);
}

#[tokio::test]
async fn document_store_default_holder_delegates_to_inspect() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();
    let store =
        DocumentLeases::new(documents.documents(), scope, "profile", "node-a", 100).unwrap();

    assert!(store.holder("alice").await.unwrap().is_none());
    let grant = store.acquire("alice", 1_000).await.unwrap();
    assert_eq!(
        store.holder("alice").await.unwrap().unwrap().epoch,
        grant.epoch
    );
}
