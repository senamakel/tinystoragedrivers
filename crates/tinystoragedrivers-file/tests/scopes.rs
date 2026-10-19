//! Scope listing reads the directory tree, including names the encoding

#![allow(
    clippy::unwrap_used,
    reason = "test helpers report a failed call by panicking"
)]
//! splits across nested directories.

use serde_json::json;
use tinystoragedrivers_core::{
    Capability, Checkpoint, ErrorKind, MaintenanceBudget, Precondition, Scope, StorageBackend,
};
use tinystoragedrivers_file::FileStorage;

fn scope(text: &str) -> Scope {
    Scope::new(text).unwrap()
}

#[tokio::test]
async fn lists_scopes_by_kind_with_names_that_need_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert!(
        storage
            .capabilities()
            .contains(Capability::ScopeEnumeration)
    );
    assert!(storage.capabilities().contains(Capability::ScopePurge));

    let doc = scope("u:ann");
    let stream = scope("u:ann/agent:Z");
    let blob = scope("u:ann/module:caf\u{e9}");
    // A scope named like the directories that hold data must not confuse the walk.
    let reserved = scope("docs");
    for scope in [&doc, &reserved] {
        storage
            .for_scope(scope)
            .unwrap()
            .documents()
            .put("c", "d", json!({}), Precondition::None)
            .await
            .unwrap();
    }
    storage
        .for_scope(&stream)
        .unwrap()
        .streams()
        .append("s", json!(1))
        .await
        .unwrap();
    storage
        .for_scope(&blob)
        .unwrap()
        .blobs()
        .put("k", vec![1], None)
        .await
        .unwrap();

    assert_eq!(
        storage.scopes("u:").await.unwrap(),
        [doc.clone(), stream, blob]
    );
    assert_eq!(storage.scopes("do").await.unwrap(), [reserved]);
    assert_eq!(storage.scopes("u:ann/agent:").await.unwrap().len(), 1);
    // Reopening reads the same tree.
    let again = FileStorage::open(dir.path()).unwrap();
    assert_eq!(again.scopes("u:ann").await.unwrap().len(), 3);
}

#[tokio::test]
async fn lists_a_scope_whose_encoding_spans_nested_directories() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let long = scope(&format!("u:{}", "a".repeat(240)));
    let sibling = scope(&format!("u:{}b", "a".repeat(240)));
    for scope in [&long, &sibling] {
        storage
            .for_scope(scope)
            .unwrap()
            .documents()
            .put("c", "d", json!({}), Precondition::None)
            .await
            .unwrap();
    }
    assert_eq!(storage.scopes("u:").await.unwrap(), [long, sibling]);
}

#[tokio::test]
async fn a_scope_whose_records_are_all_gone_is_not_listed() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let gone = scope("u:gone");
    let handles = storage.for_scope(&gone).unwrap();
    handles.blobs().put("k", vec![1], None).await.unwrap();
    handles.blobs().delete("k").await.unwrap();
    // Opened, never written.
    storage.for_scope(&scope("u:never")).unwrap();
    // A stray entry nothing in this driver wrote.
    std::fs::create_dir_all(dir.path().join("scopes").join("NotEncoded")).unwrap();
    assert_eq!(storage.scopes("").await.unwrap().len(), 0);
}

#[tokio::test]
async fn an_empty_store_lists_nothing_and_maintenance_is_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(storage.scopes("").await.unwrap().len(), 0);
    assert!(!storage.capabilities().contains(Capability::Maintenance));
    let expected = ErrorKind::Unsupported(Capability::Maintenance);
    assert_eq!(
        storage
            .maintain(MaintenanceBudget::unlimited())
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        storage
            .checkpoint(Checkpoint::Truncate)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
}
