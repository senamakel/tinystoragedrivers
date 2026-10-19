//! The scoped handle bundle.

use super::*;
use crate::MemoryStorage;

#[test]
fn scoped_storage_exposes_its_parts() {
    let scope = Scope::new("t1").unwrap();
    let storage = MemoryStorage::new().for_scope(&scope).unwrap();
    assert_eq!(storage.scope(), &scope);
    assert_eq!(storage.driver(), "memory");
    assert!(
        storage
            .documents()
            .capabilities()
            .contains(crate::Capability::Ttl)
    );
    let _ = (storage.streams(), storage.blobs());
    let rendered = format!("{storage:?}");
    assert!(
        rendered.contains("t1") && rendered.contains("memory"),
        "{rendered}"
    );
}

#[tokio::test]
async fn unknown_adapters_refuse_fencing() {
    #[derive(Debug)]
    struct Adapter;
    impl StorageBackend for Adapter {
        fn driver(&self) -> &'static str {
            "unknown"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::none()
        }
        fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
            MemoryStorage::new().for_scope(scope)
        }
    }
    assert_eq!(
        Adapter.prepare_schema().await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    assert!(matches!(
        Adapter.existing_schema().unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ExistingSchema)
    ));
    let fence = Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    assert_eq!(
        Adapter
            .for_scope_fenced(&Scope::local(), &fence)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::Fencing)
    );
}

#[tokio::test]
async fn the_queue_port_defaults_to_a_document_queue_and_can_be_replaced() {
    use crate::queue::{DocumentQueue, NewMessage, QueueStore};

    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let queues = storage.queues();
    assert!(format!("{queues:?}").contains("DocumentQueue"));
    assert!(queues.features().groups);
    queues
        .enqueue("q", &[NewMessage::new(serde_json::json!(1))])
        .await
        .unwrap();
    // The default queue lives in the scope's documents.
    let again = DocumentQueue::new(storage.documents());
    assert_eq!(again.stats("q").await.unwrap().ready, 1);
    assert!(Arc::ptr_eq(storage.queues(), storage.clone().queues()));

    // A replacement is served instead, on the same scope.
    let other = backend.for_scope(&Scope::new("other").unwrap()).unwrap();
    let replaced = storage.with_queues(Arc::clone(other.queues()));
    assert!(Arc::ptr_eq(replaced.queues(), other.queues()));
    assert_eq!(replaced.queues().stats("q").await.unwrap().ready, 0);
}
