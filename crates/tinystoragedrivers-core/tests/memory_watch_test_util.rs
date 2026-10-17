//! Public API coverage for the test-only native memory watch.

#![cfg(feature = "test-util")]

use std::time::Duration;

use serde_json::json;
use tinystoragedrivers_core::{
    Capability, CollectionSpec, DocumentWatch, ErrorKind, Fence, Filter, MemoryStorage,
    Precondition, PurgeMode, PurgeRequest, Scope, StorageBackend, Version, WatchNotice,
    WatchOptions, WatchReset, WriteOp,
};

fn options() -> WatchOptions {
    WatchOptions {
        interval: Duration::from_secs(60),
        ..WatchOptions::default()
    }
}

async fn assert_prompt_invalidation(watch: &mut dyn DocumentWatch) {
    let result = tokio::time::timeout(Duration::from_millis(1), watch.next()).await;
    assert!(
        matches!(
            result,
            Ok(Ok(Some(WatchNotice::Invalidated { cursor: None })))
        ),
        "mutation invalidation arrives before the quiet interval"
    );
}

async fn assert_no_prompt_invalidation(watch: &mut dyn DocumentWatch) {
    assert!(
        tokio::time::timeout(Duration::from_millis(1), watch.next())
            .await
            .is_err(),
        "a failed operation must not invalidate before the quiet interval"
    );
}

#[tokio::test]
async fn native_memory_watch_is_opt_in() {
    let storage = MemoryStorage::new();
    assert!(!storage.capabilities().contains(Capability::Watch));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert!(!docs.capabilities().contains(Capability::Watch));
    assert_eq!(
        docs.watch("notes", &Filter::All, &options())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Watch)
    );

    let opted_in = MemoryStorage::with_native_watch();
    assert!(opted_in.capabilities().contains(Capability::Watch));
    let opted_in_docs = opted_in
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert!(opted_in_docs.capabilities().contains(Capability::Watch));
}

#[tokio::test(start_paused = true)]
async fn watch_invalidates_for_document_mutations_even_when_filter_stays_false() {
    let storage = MemoryStorage::with_native_watch();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let mut watch: Box<dyn DocumentWatch> = docs
        .watch("notes", &Filter::eq("state", "open"), &options())
        .await
        .unwrap();

    assert!(docs.get("notes", "one").await.unwrap().is_none());
    docs.put(
        "notes",
        "one",
        json!({"state":"closed"}),
        Precondition::Absent,
    )
    .await
    .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
    docs.put("notes", "one", json!({"state":"open"}), Precondition::None)
        .await
        .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
    docs.delete("notes", "one", Precondition::None)
        .await
        .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;

    for id in ["two", "three"] {
        docs.put("notes", id, json!({"state":"open"}), Precondition::Absent)
            .await
            .unwrap();
        assert_prompt_invalidation(watch.as_mut()).await;
    }
    assert_eq!(docs.delete_where("notes", &Filter::All).await.unwrap(), 2);
    assert_prompt_invalidation(watch.as_mut()).await;

    docs.put(
        "notes",
        "claim",
        json!({"state":"open"}),
        Precondition::Absent,
    )
    .await
    .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
    docs.claim(
        "notes",
        &Filter::eq("state", "open"),
        &[],
        &json!({"state":"closed"}),
    )
    .await
    .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;

    docs.atomic_batch(vec![WriteOp::Put {
        collection: "notes".into(),
        id: "batch".into(),
        doc: json!({"state":"open"}),
        precondition: Precondition::Absent,
    }])
    .await
    .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
    docs.drop_collection("notes").await.unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
}

#[tokio::test(start_paused = true)]
async fn watch_is_bound_to_scope_and_collection() {
    let storage = MemoryStorage::with_native_watch();
    let alice = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    let bob = storage
        .for_scope(&Scope::new("bob").unwrap())
        .unwrap()
        .documents()
        .clone();
    let mut watch = alice
        .watch("notes", &Filter::All, &options())
        .await
        .unwrap();
    let mut pending = tokio::spawn(async move { watch.next().await });
    tokio::task::yield_now().await;

    bob.put("notes", "private", json!({}), Precondition::Absent)
        .await
        .unwrap();
    alice
        .put("other", "unrelated", json!({}), Precondition::Absent)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());

    alice
        .put("notes", "visible", json!({}), Precondition::Absent)
        .await
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_millis(1), &mut pending)
        .await
        .expect("same scope and collection invalidation arrives promptly")
        .unwrap()
        .unwrap();
    assert!(matches!(
        notice,
        Some(WatchNotice::Invalidated { cursor: None })
    ));
}

#[tokio::test(start_paused = true)]
async fn rejected_atomic_batch_rolls_back_without_invalidating() {
    let docs = MemoryStorage::with_native_watch()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let mut watch = docs.watch("notes", &Filter::All, &options()).await.unwrap();

    let error = docs
        .atomic_batch(vec![
            WriteOp::Put {
                collection: "notes".into(),
                id: "rolled_back".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            },
            WriteOp::Delete {
                collection: "notes".into(),
                id: "missing".into(),
                precondition: Precondition::Version(Version::FIRST),
            },
        ])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert!(docs.get("notes", "rolled_back").await.unwrap().is_none());
    assert_no_prompt_invalidation(watch.as_mut()).await;

    docs.put("notes", "committed", json!({}), Precondition::Absent)
        .await
        .unwrap();
    assert_prompt_invalidation(watch.as_mut()).await;
}

#[tokio::test(start_paused = true)]
async fn atomic_batch_delete_invalidates_each_collection_once() {
    let docs = MemoryStorage::with_native_watch()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    for (collection, id) in [("notes", "one"), ("notes", "two"), ("tasks", "three")] {
        docs.put(collection, id, json!({}), Precondition::Absent)
            .await
            .unwrap();
    }
    let mut notes = docs.watch("notes", &Filter::All, &options()).await.unwrap();
    let mut tasks = docs.watch("tasks", &Filter::All, &options()).await.unwrap();

    docs.atomic_batch(vec![
        WriteOp::Delete {
            collection: "notes".into(),
            id: "one".into(),
            precondition: Precondition::None,
        },
        WriteOp::Delete {
            collection: "notes".into(),
            id: "two".into(),
            precondition: Precondition::None,
        },
        WriteOp::Delete {
            collection: "tasks".into(),
            id: "three".into(),
            precondition: Precondition::None,
        },
    ])
    .await
    .unwrap();

    assert_prompt_invalidation(notes.as_mut()).await;
    assert_no_prompt_invalidation(notes.as_mut()).await;
    assert_prompt_invalidation(tasks.as_mut()).await;
    assert_no_prompt_invalidation(tasks.as_mut()).await;
}

#[tokio::test]
async fn retired_scope_watch_resets_and_stops() {
    let storage = MemoryStorage::with_native_watch();
    let scope = Scope::new("reset_watch").unwrap();
    let ports = storage.for_scope(&scope).unwrap();
    let mut watch = ports
        .documents()
        .watch("notes", &Filter::All, &options())
        .await
        .unwrap();

    ports
        .purge(&PurgeRequest {
            operation_id: "reset_watch_once".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    assert_eq!(
        ports
            .documents()
            .watch("notes", &Filter::All, &options())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::ResetRequired {
            reason: WatchReset::GenerationChanged,
            ..
        })
    ));
    assert!(watch.next().await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn watch_delivery_rechecks_its_captured_fence() {
    let storage = MemoryStorage::with_native_watch();
    let owner = Scope::new("watch_owner").unwrap();
    let guard_scope = Scope::new("watch_guard").unwrap();
    let guard = storage.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "active", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::epoch(guard_scope, "leases", "active", "epoch", 1);
    let fenced = storage.for_scope_fenced(&owner, &fence).unwrap();
    let mut watch = fenced
        .documents()
        .watch("notes", &Filter::All, &options())
        .await
        .unwrap();
    let mut pending = tokio::spawn(async move { watch.next().await });
    tokio::task::yield_now().await;

    guard
        .documents()
        .put("leases", "active", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    storage
        .for_scope(&owner)
        .unwrap()
        .documents()
        .put("notes", "trigger", json!({}), Precondition::None)
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_millis(1), &mut pending)
        .await
        .expect("stale fence is rejected at notification delivery")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[tokio::test(start_paused = true)]
async fn quiet_watch_interval_invalidates_expired_documents_and_survives_cancellation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
        .enable_native_watch();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until"))
        .await
        .unwrap();
    docs.put(
        "leases",
        "short",
        json!({"until":1_100}),
        Precondition::Absent,
    )
    .await
    .unwrap();
    let mut watch = docs
        .watch(
            "leases",
            &Filter::All,
            &WatchOptions {
                interval: Duration::from_millis(10),
                ..WatchOptions::default()
            },
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(1), watch.next())
            .await
            .is_err()
    );
    let pending = tokio::spawn(async move { watch.next().await });
    tokio::task::yield_now().await;
    now.store(1_100, Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(matches!(
        pending.await.unwrap().unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
    assert!(docs.get("leases", "short").await.unwrap().is_none());
}

#[tokio::test]
async fn memory_watch_rejects_resume_cursors() {
    let docs = MemoryStorage::with_native_watch()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let error = docs
        .watch(
            "notes",
            &Filter::All,
            &WatchOptions {
                after: Some(tinystoragedrivers_core::WatchCursor::new(vec![1]).unwrap()),
                ..options()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}
