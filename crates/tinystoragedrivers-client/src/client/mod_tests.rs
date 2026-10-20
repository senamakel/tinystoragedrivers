//! Client behavior that conformance does not pin: binding and error mapping.

use super::*;

use std::sync::{Arc, Mutex};

use serde_json::json;
use tinystoragedrivers_bus::{BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageKind};
use tinystoragedrivers_core::{
    Capability, ErrorKind, MemoryStorage, Precondition, Scope, StorageBackend, WriteOp,
    WriteOptions,
};

use crate::{Admission, Loopback, StorageServer};

#[derive(Debug)]
struct OlderHost {
    calls: Mutex<Vec<String>>,
    signals: tokio::sync::broadcast::Sender<tinystoragedrivers_bus::WatchChanged>,
}

#[async_trait::async_trait]
impl StorageTransport for OlderHost {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        _request: serde_json::Value,
    ) -> std::result::Result<serde_json::Value, WireError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("{}.{selector}", object.as_str()));
        match (object, selector) {
            (Object::Documents, "Capabilities") => Ok(json!({"capabilities":[
                "transactions", "validated_batch", "record_metadata"
            ]})),
            (Object::Blobs, "Capabilities") => Ok(json!({
                "presigned_urls": false,
                "ttl": false,
                "max_key_len": 0
            })),
            (Object::Queues, "Features") => Ok(json!({
                "dedupe": false,
                "groups": false,
                "priorities": false,
                "delay": false
            })),
            _ => Err(WireError::Storage(tinystoragedrivers_bus::StorageFailure {
                kind: StorageKind::InvalidInput,
            })),
        }
    }

    async fn put_blob_stream(
        &self,
        _head: PutBlobStreamRequest,
        _body: crate::BlobBody,
    ) -> std::result::Result<serde_json::Value, WireError> {
        unreachable!("this test transport never transfers blobs")
    }

    async fn get_blob_stream(
        &self,
        _request: BlobKeyRequest,
    ) -> std::result::Result<Option<(BlobStreamHead, crate::BlobBody)>, WireError> {
        unreachable!("this test transport never transfers blobs")
    }

    fn signals(&self) -> tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
        self.signals.subscribe()
    }
}

async fn bus() -> BusStorage {
    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap());
    BusStorage::connect(
        Arc::new(Loopback::new(Arc::new(server), Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_bus_caller_is_bound_to_its_scope_and_cannot_pick_fences() {
    let bus = bus().await;
    assert_eq!(bus.driver(), "bus");
    assert!(bus.for_scope(&Scope::local()).is_ok());
    let other = Scope::new("bob").unwrap();
    assert_eq!(
        bus.for_scope(&other).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for host_only in [
        Capability::Fencing,
        Capability::ScopePurge,
        Capability::ExistingSchema,
    ] {
        assert!(!bus.capabilities().contains(host_only));
    }
    let fence = tinystoragedrivers_core::Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    assert!(bus.for_scope_fenced(&Scope::local(), &fence).is_err());
}

#[tokio::test]
async fn unsupported_refusals_name_the_capability_the_call_needs() {
    let bus = bus().await;
    let storage = bus.for_scope(&Scope::local()).unwrap();
    let expected = Capability::PresignedUrls;
    let error = storage
        .blobs()
        .presign_get("k", std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(expected));
}

#[tokio::test]
async fn an_old_host_without_atomic_write_options_refuses_before_any_new_selector() {
    let (signals, _) = tokio::sync::broadcast::channel(8);
    let old_host = Arc::new(OlderHost {
        calls: Mutex::new(Vec::new()),
        signals,
    });
    let bus = BusStorage::connect(old_host.clone(), Scope::local())
        .await
        .unwrap();
    let docs = bus.for_scope(&Scope::local()).unwrap().documents().clone();
    assert!(!docs.capabilities().contains(Capability::AtomicWriteOptions));

    let put = WriteOp::Put {
        collection: "records".into(),
        id: "a".into(),
        doc: json!({"value":1}),
        precondition: Precondition::Absent,
    };
    for error in [
        docs.atomic_batch_with(vec![put.clone()], WriteOptions::new().schema(2))
            .await
            .unwrap_err(),
        docs.atomic_batch_checked_with(Vec::new(), vec![put], WriteOptions::default())
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            error.kind(),
            ErrorKind::Unsupported(Capability::AtomicWriteOptions)
        );
    }
    let calls = old_host.calls.lock().unwrap();
    assert!(!calls.iter().any(|call| {
        matches!(
            call.as_str(),
            "Documents.AtomicBatchWith" | "Documents.AtomicBatchCheckedWith" | "Documents.Put"
        )
    }));
}

/// Blobs that forward everything but cannot expire, like the `MongoDB` driver.
#[derive(Debug)]
struct NoTtl(Arc<dyn tinystoragedrivers_core::BlobStore>);

#[tinystoragedrivers_core::async_trait]
impl tinystoragedrivers_core::BlobStore for NoTtl {
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::BlobMeta> {
        self.0.put(key, bytes, content_type).await
    }
    async fn get(
        &self,
        key: &str,
    ) -> tinystoragedrivers_core::Result<Option<tinystoragedrivers_core::Blob>> {
        self.0.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> tinystoragedrivers_core::Result<Option<Vec<u8>>> {
        self.0.get_range(key, range).await
    }
    async fn head(
        &self,
        key: &str,
    ) -> tinystoragedrivers_core::Result<Option<tinystoragedrivers_core::BlobMeta>> {
        self.0.head(key).await
    }
    async fn delete(&self, key: &str) -> tinystoragedrivers_core::Result<bool> {
        self.0.delete(key).await
    }
    async fn list(
        &self,
        prefix: &str,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::BlobMeta>> {
        self.0.list(prefix).await
    }
}

#[tokio::test]
async fn a_host_without_blob_expiry_is_refused_before_any_io() {
    use std::time::Duration;
    use tinystoragedrivers_core::ScopedStorage;

    let backend = MemoryStorage::new();
    let base = backend.for_scope(&Scope::local()).unwrap();
    let storage = ScopedStorage::new(
        Scope::local(),
        "no-ttl",
        base.documents().clone(),
        base.streams().clone(),
        Arc::new(NoTtl(base.blobs().clone())),
    );
    let server = Arc::new(StorageServer::new(storage));
    let bus = BusStorage::connect(
        Arc::new(Loopback::new(server, Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap();
    let blobs = bus.for_scope(&Scope::local()).unwrap().blobs().clone();
    assert!(!blobs.capabilities().contains(Capability::BlobTtl));
    let expected = ErrorKind::Unsupported(Capability::BlobTtl);
    let ttl = Duration::from_secs(60);
    assert_eq!(
        blobs
            .put_expiring("k", vec![1], None, ttl)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        blobs
            .put_stream_expiring("k", Box::pin(std::io::Cursor::new(vec![1])), 1, None, ttl)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    // The refusal came before any write.
    assert!(base.blobs().get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn queue_batches_split_at_the_op_cap_and_payloads_are_bounded() {
    use serde_json::json;
    use tinystoragedrivers_core::queue::NewMessage;

    let bus = bus().await;
    let queues = bus.for_scope(&Scope::local()).unwrap().queues().clone();
    let many: Vec<_> = (0..=tinystoragedrivers_bus::MAX_BATCH_OPS)
        .map(|i| NewMessage::new(json!(i)))
        .collect();
    let enqueued = queues.enqueue("many", &many).await.unwrap();
    assert_eq!(enqueued.len(), many.len());
    assert_eq!(queues.stats("many").await.unwrap().ready, many.len() as u64);

    let huge = NewMessage::new(json!(
        "x".repeat(tinystoragedrivers_bus::MAX_QUEUE_MESSAGE_BYTES)
    ));
    assert_eq!(
        queues.enqueue("many", &[huge]).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}
