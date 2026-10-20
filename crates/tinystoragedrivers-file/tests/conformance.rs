//! The file driver against the shared conformance suite, and across reopens.

use serde_json::json;
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, Precondition, Query, Scope, StorageBackend, WriteOp,
    conformance,
};
use tinystoragedrivers_file::FileStorage;

#[tokio::test]
async fn passes_the_conformance_suite() {
    let dir = tempfile::tempdir().unwrap();
    conformance::run(&FileStorage::open(dir.path()).unwrap(), false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn passes_the_conformance_suite_on_a_current_thread_runtime() {
    let dir = tempfile::tempdir().unwrap();
    conformance::run(&FileStorage::open(dir.path()).unwrap(), false).await;
}

#[tokio::test]
async fn quiescent_purge_revokes_old_handles_and_replays_without_erasing_replacements() {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest, ScopeGeneration};
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let target = Scope::new("purge-target").unwrap();
    let sibling = Scope::new("purge-sibling").unwrap();
    let old = storage.for_scope(&target).unwrap();
    let sibling_handle = storage.for_scope(&sibling).unwrap();
    old.blobs().put("old", b"old".to_vec(), None).await.unwrap();
    sibling_handle
        .blobs()
        .put("keep", b"keep".to_vec(), None)
        .await
        .unwrap();
    let request = PurgeRequest {
        operation_id: "reset-1".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = old.purge(&request).await.unwrap();
    assert_eq!(receipt.generation, ScopeGeneration(1));
    assert_eq!(
        old.purge_status("reset-1").await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        old.blobs().get("old").await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        old.streams().len("old").await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
    let current = storage
        .for_scope_generation(&target, receipt.generation)
        .unwrap();
    assert!(current.blobs().get("old").await.unwrap().is_none());
    current
        .blobs()
        .put("new", b"new".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(old.purge(&request).await.unwrap(), receipt);
    assert_eq!(
        current.blobs().get("new").await.unwrap().unwrap().bytes,
        b"new"
    );
    assert_eq!(
        FileStorage::open(dir.path())
            .unwrap()
            .for_scope(&target)
            .unwrap()
            .purge_status("reset-1")
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        sibling_handle
            .blobs()
            .get("keep")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"keep"
    );
}

#[test]
fn works_without_a_tokio_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    // A minimal executor: the futures never suspend outside a runtime.
    let version = block_on(docs.put("c", "a", json!({}), Precondition::None)).unwrap();
    assert_eq!(version.0, 1);
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

/// Write one of everything through a store that is dropped afterwards.
async fn seed(dir: &std::path::Path, alice: &Scope) -> tinystoragedrivers_core::Result<()> {
    let storage = FileStorage::open(dir)?;
    storage
        .for_scope(alice)?
        .documents()
        .ensure_collection(&CollectionSpec::new("Notes").searchable(["text"]))
        .await?;
    let scoped = storage.for_scope(alice)?;
    scoped
        .documents()
        .put(
            "Notes",
            "n/1",
            json!({"text": "hello world"}),
            Precondition::Absent,
        )
        .await?;
    scoped
        .streams()
        .append_batch("thread:1", vec![json!(1), json!(2), json!(3)])
        .await?;
    scoped.streams().truncate_before("thread:1", 1).await?;
    scoped
        .blobs()
        .put("files/a.txt", b"bytes".to_vec(), Some("text/plain"))
        .await?;
    Ok(())
}

#[tokio::test]
async fn data_survives_reopening_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let alice = Scope::new("tenant:alice/1").unwrap();
    seed(dir.path(), &alice).await.unwrap();

    let storage = FileStorage::open(dir.path()).unwrap();
    let scoped = storage.for_scope(&alice).unwrap();
    let doc = scoped
        .documents()
        .get("Notes", "n/1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(doc.version.0, 1);
    assert_eq!(doc.doc["text"], "hello world");
    let hits = scoped
        .documents()
        .search("Notes", "world", 5)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1, "the collection declaration persisted");
    let v2 = scoped
        .documents()
        .put(
            "Notes",
            "n/1",
            json!({"text": "again"}),
            Precondition::Version(doc.version),
        )
        .await
        .unwrap();
    assert_eq!(v2.0, 2, "versions continue after a reopen");

    assert_eq!(scoped.streams().len("thread:1").await.unwrap(), 3);
    let window = scoped
        .streams()
        .read_window("thread:1", 0, 10)
        .await
        .unwrap();
    assert_eq!(window.iter().map(|e| e.offset).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(
        scoped.streams().append("thread:1", json!(4)).await.unwrap(),
        3
    );
    assert_eq!(scoped.streams().streams("").await.unwrap(), ["thread:1"]);

    let blob = scoped.blobs().get("files/a.txt").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"bytes");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));
}

#[tokio::test]
async fn cursors_survive_reopening_the_directory() {
    let dir = tempfile::tempdir().unwrap();
    let first = FileStorage::open(dir.path()).unwrap();
    let docs = first
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    for id in ["a", "b", "c"] {
        docs.put("c", id, json!({}), Precondition::None)
            .await
            .unwrap();
    }
    let cursor = docs
        .query("c", &Query::all().limit(1))
        .await
        .unwrap()
        .next
        .unwrap();

    let again = FileStorage::open(dir.path()).unwrap();
    let docs = again
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let page = docs
        .query("c", &Query::all().limit(1).after(cursor))
        .await
        .unwrap();
    assert_eq!(page.items[0].id, "b");
}

#[tokio::test]
async fn reports_its_driver_and_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(storage.driver(), "file");
    let caps = storage.capabilities();
    assert!(caps.contains(Capability::Ttl));
    assert!(caps.contains(Capability::FullText));
    assert!(!caps.contains(Capability::Transactions));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(docs.capabilities(), caps);
    let error = docs
        .atomic_batch(vec![WriteOp::Delete {
            collection: "c".into(),
            id: "a".into(),
            precondition: Precondition::None,
        }])
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    let error = docs
        .atomic_batch_with(
            vec![WriteOp::Put {
                collection: "c".into(),
                id: "a".into(),
                doc: json!({"value":1}),
                precondition: Precondition::None,
            }],
            tinystoragedrivers_core::WriteOptions::new().schema(2),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::AtomicWriteOptions)
    );
    assert!(docs.get("c", "a").await.unwrap().is_none());
    assert_eq!(storage.dir(), dir.path().canonicalize().unwrap());
    assert!(format!("{storage:?}").starts_with("FileStorage"));
}

#[tokio::test]
async fn reopening_the_directory_keeps_what_was_written() {
    // The file driver does not claim `SharedAcrossProcesses`, so this runs
    // the reopen-durability part of the shared-handle checks alone.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    assert!(
        !FileStorage::open(&path)
            .unwrap()
            .capabilities()
            .contains(Capability::SharedAcrossProcesses)
    );
    conformance::run_shared(|| {
        let storage = FileStorage::open(&path).unwrap();
        async move { storage }
    })
    .await;
}
