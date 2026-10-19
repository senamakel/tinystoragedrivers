//! Existing SQLite databases can be inspected without mutation.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, Precondition, PurgeMode, PurgeRequest, Scope,
    ScopedStorage, StorageBackend,
};

async fn assert_read_only_purge_is_unavailable(storage: &SqliteStorage, ports: &ScopedStorage) {
    assert!(!storage.capabilities().contains(Capability::ScopePurge));
    assert!(
        !storage
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    assert!(
        !ports
            .documents()
            .capabilities()
            .contains(Capability::ScopePurge)
    );
    assert!(
        !ports
            .documents()
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    assert!(!storage.supports_scope_purge_binding_validation());
    assert!(!storage.supports_scope_purge_generation_validation());
    assert_eq!(
        ports
            .purge(&PurgeRequest {
                operation_id: "readonly-purge".into(),
                mode: PurgeMode::Atomic,
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
}

#[tokio::test]
async fn existing_read_only_storage_reads_ports_and_refuses_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("storage");
    let database = path.join(ROOT_FILE);
    let storage = SqliteStorage::open(&path).unwrap();
    let ports = storage.for_scope(&Scope::local()).unwrap();
    ports
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    ports
        .documents()
        .put("notes", "one", json!({"value": 1}), Precondition::Absent)
        .await
        .unwrap();
    ports
        .streams()
        .append("journal", json!({"event": "kept"}))
        .await
        .unwrap();
    drop(ports);
    drop(storage);
    let before = std::fs::read(&database).unwrap();

    let read_only = SqliteStorage::open_existing_read_only(&path).unwrap();
    let ports = read_only.for_scope(&Scope::local()).unwrap();
    assert_read_only_purge_is_unavailable(&read_only, &ports).await;
    assert_eq!(
        ports
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"value": 1})
    );
    assert_eq!(
        ports.streams().read_window("journal", 0, 1).await.unwrap()[0].value,
        json!({"event": "kept"})
    );
    assert_eq!(
        ports
            .blobs()
            .put("new-blob", b"forbidden".to_vec(), Some("text/plain"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        ports
            .documents()
            .put("notes", "two", json!({}), Precondition::Absent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        ports
            .streams()
            .append("journal", json!({"event": "refused"}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(
        read_only
            .native()
            .with_connection(|connection| connection
                .execute("CREATE TABLE forbidden (id INTEGER)", [])
                .map(|_| ()))
            .await
            .is_err()
    );
    drop(ports);
    drop(read_only);
    assert_eq!(std::fs::read(&database).unwrap(), before);
}

#[test]
fn existing_read_only_open_does_not_create_a_missing_path() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("not-created");
    assert!(SqliteStorage::open_existing_read_only(&missing).is_err());
    assert!(!missing.exists());
}

#[test]
fn existing_read_only_open_does_not_initialize_an_old_schema() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("old.db");
    {
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute_batch("CREATE TABLE legacy_sessions (id TEXT PRIMARY KEY)")
            .unwrap();
    }
    let before = std::fs::read(&database).unwrap();
    let error = SqliteStorage::open_existing_read_only(&database).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert_eq!(std::fs::read(&database).unwrap(), before);
}
