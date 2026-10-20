//! The shared conformance suite and the size, stream and watch contracts,
//! run through a client wired to a server by [`Loopback`].
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tinystoragedrivers_bus::{BlobKeyRequest, MAX_FRAME_BYTES, MAX_INLINE_BLOB_BYTES};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, ErrorKind, ExpectedDocument, Filter, MemoryStorage, Precondition,
    ReadCheck, Scope, ScopedStorage, StorageBackend, WatchNotice, WatchOptions, WriteOp,
    WriteOptions, WriteResult, conformance,
};
use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};
use tokio::io::AsyncReadExt;

use super::*;
use crate::client::BusStorage;

struct Rig {
    storage: ScopedStorage,
    loopback: Arc<Loopback>,
    bus: BusStorage,
}

async fn rig(admission: Admission) -> Rig {
    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
        .with_secrets(Arc::new(MemorySecrets::new()));
    let loopback = Arc::new(Loopback::new(Arc::new(server), admission));
    let bus = BusStorage::connect(loopback.clone(), Scope::local())
        .await
        .unwrap();
    let storage = bus.for_scope(&Scope::local()).unwrap();
    Rig {
        storage,
        loopback,
        bus,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn port_conformance_passes_through_the_bus_client() {
    let rig = rig(Admission::Private).await;
    conformance::run_ports(&rig.storage).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_over_the_frame_cap_moves_over_a_stream() {
    let rig = rig(Admission::Private).await;
    let blobs = rig.storage.blobs();
    let len = MAX_FRAME_BYTES + 1024 * 1024;
    let bytes: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();

    let meta = blobs
        .put(
            "big/object",
            bytes.clone(),
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(meta.len, len as u64);
    assert!(rig.loopback.streamed_bytes() >= len as u64);
    assert!(rig.loopback.largest_frame() <= MAX_FRAME_BYTES);

    let back = blobs.get("big/object").await.unwrap().unwrap();
    assert_eq!(back.bytes, bytes);

    let mut stream = blobs.get_stream("big/object").await.unwrap().unwrap();
    let mut streamed = Vec::new();
    stream.reader.read_to_end(&mut streamed).await.unwrap();
    assert_eq!(streamed.len(), len);

    let start = (MAX_INLINE_BLOB_BYTES - 10) as u64;
    let end = (MAX_INLINE_BLOB_BYTES * 2 + 10) as u64;
    let range = blobs
        .get_range("big/object", start..end)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        range,
        bytes[usize::try_from(start).unwrap()..usize::try_from(end).unwrap()]
    );
    assert!(rig.loopback.largest_frame() <= MAX_FRAME_BYTES);
}

#[tokio::test]
async fn absolute_expiry_reads_preserve_absence_across_inline_and_stream_paths() {
    let rig = rig(Admission::Private).await;
    let request = BlobKeyRequest {
        key: "missing".into(),
    };

    assert!(
        rig.loopback
            .get_blob_with_expiry(request.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        rig.loopback
            .get_blob_stream_with_expiry(request)
            .await
            .unwrap()
            .is_none()
    );
    assert!(rig.loopback.largest_frame() <= MAX_FRAME_BYTES);
}

struct PendingThenBytes {
    pending: bool,
    sent_bytes: bool,
}

impl AsyncRead for PendingThenBytes {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.pending {
            self.pending = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if !self.sent_bytes {
            self.sent_bytes = true;
            buf.put_slice(b"chunk");
        }
        Poll::Ready(Ok(()))
    }
}

struct FailedRead;

impl AsyncRead for FailedRead {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::other("source read failed")))
    }
}

#[tokio::test]
async fn chunked_reader_preserves_pending_and_error_results_without_corrupting_bytes_or_counts() {
    let moved = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut reader = Box::pin(Chunked {
        inner: Box::pin(PendingThenBytes {
            pending: false,
            sent_bytes: false,
        }),
        moved: Arc::clone(&moved),
    });
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"chunk");
    assert_eq!(moved.load(std::sync::atomic::Ordering::Relaxed), 5);

    let moved = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut reader = Box::pin(Chunked {
        inner: Box::pin(FailedRead),
        moved: Arc::clone(&moved),
    });
    let mut bytes = Vec::new();
    let error = reader.read_to_end(&mut bytes).await.unwrap_err();
    assert_eq!(error.to_string(), "source read failed");
    assert_eq!(bytes, [] as [u8; 0]);
    assert_eq!(moved.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[tokio::test]
async fn a_stream_body_of_the_wrong_length_is_refused_before_it_is_published() {
    let rig = rig(Admission::Private).await;
    let blobs = rig.storage.blobs();
    let short = blobs
        .put_stream(
            "len/short",
            Box::pin(std::io::Cursor::new(vec![1u8; 10])),
            20,
            None,
        )
        .await;
    assert!(short.is_err());
    let long = blobs
        .put_stream(
            "len/long",
            Box::pin(std::io::Cursor::new(vec![1u8; 30])),
            20,
            None,
        )
        .await;
    assert!(long.is_err());
    assert!(blobs.head("len/short").await.unwrap().is_none());
    assert!(blobs.head("len/long").await.unwrap().is_none());
}

#[tokio::test]
async fn a_frame_over_the_cap_fails_in_the_transport() {
    let rig = rig(Admission::Private).await;
    let docs = rig.storage.documents();
    let huge = "x".repeat(MAX_FRAME_BYTES);
    let error = docs
        .put("frames", "big", json!({"v": huge}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::Unavailable
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_memory_driver_refuses_watch_through_the_bus() {
    let rig = rig(Admission::Private).await;
    let docs = rig.storage.documents();
    let options = WatchOptions {
        interval: Duration::from_millis(5),
        ..WatchOptions::default()
    };
    let collection = "watched";
    docs.ensure_collection(&CollectionSpec::new(collection))
        .await
        .unwrap();
    // The memory driver has no native watch, and says so.
    assert!(
        !docs
            .capabilities()
            .contains(tinystoragedrivers_core::Capability::Watch)
    );
    let refused = docs
        .watch(collection, &Filter::All, &options)
        .await
        .unwrap_err();
    assert_eq!(
        refused.kind(),
        tinystoragedrivers_core::ErrorKind::Unsupported(tinystoragedrivers_core::Capability::Watch)
    );
}

/// A memory store whose documents report a native watch fed by a channel.
mod watching {
    use std::sync::{Arc, Mutex};

    use serde_json::Value;
    use tinystoragedrivers_core::*;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    pub(super) struct Source(pub Mutex<Vec<mpsc::UnboundedSender<Option<WatchNotice>>>>);

    #[derive(Debug)]
    pub(super) struct Docs {
        pub inner: Arc<dyn DocumentStore>,
        pub source: Arc<Source>,
    }

    #[derive(Debug)]
    struct Feed(Option<mpsc::UnboundedReceiver<Option<WatchNotice>>>);

    #[async_trait]
    impl DocumentWatch for Feed {
        async fn next(&mut self) -> Result<Option<WatchNotice>> {
            match self.0.as_mut() {
                Some(feed) => Ok(feed.recv().await.flatten()),
                None => Err(tinystoragedrivers_core::StorageError::backend(
                    "feed failed",
                )),
            }
        }
    }

    #[async_trait]
    impl DocumentStore for Docs {
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities().with(Capability::Watch)
        }
        async fn watch(
            &self,
            collection: &str,
            _filter: &Filter,
            _options: &WatchOptions,
        ) -> Result<Box<dyn DocumentWatch>> {
            if collection == "failing" {
                return Ok(Box::new(Feed(None)));
            }
            let (tx, rx) = mpsc::unbounded_channel();
            self.source.0.lock().unwrap().push(tx);
            Ok(Box::new(Feed(Some(rx))))
        }
        async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
            self.inner.ensure_collection(spec).await
        }
        async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
            self.inner.get(c, id).await
        }
        async fn put(&self, c: &str, id: &str, doc: Value, p: Precondition) -> Result<Version> {
            self.inner.put(c, id, doc, p).await
        }
        async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
            self.inner.delete(c, id, p).await
        }
        async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
            self.inner.query(c, q).await
        }
        async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
            self.inner.count(c, f).await
        }
        async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
            self.inner.delete_where(c, f).await
        }
        async fn claim(
            &self,
            c: &str,
            f: &Filter,
            s: &[Sort],
            p: &Value,
        ) -> Result<Option<Versioned<Value>>> {
            self.inner.claim(c, f, s, p).await
        }
        async fn drop_collection(&self, c: &str) -> Result<()> {
            self.inner.drop_collection(c).await
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_native_watch_becomes_signals_and_closes_with_the_client() {
    use tinystoragedrivers_core::{WatchCursor, WatchReset};
    let backend = MemoryStorage::new();
    let base = backend.for_scope(&Scope::local()).unwrap();
    let source = Arc::new(watching::Source(std::sync::Mutex::new(Vec::new())));
    let docs = watching::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
    };
    let storage = ScopedStorage::new(
        Scope::local(),
        "watching",
        Arc::new(docs),
        base.streams().clone(),
        base.blobs().clone(),
    );
    let server = Arc::new(StorageServer::new(storage));
    let loopback = Arc::new(Loopback::new(server.clone(), Admission::Private));
    let bus = BusStorage::connect(loopback, Scope::local()).await.unwrap();
    let docs = bus.for_scope(&Scope::local()).unwrap().documents().clone();
    assert!(
        docs.capabilities()
            .contains(tinystoragedrivers_core::Capability::Watch)
    );

    let options = WatchOptions {
        interval: Duration::from_millis(5),
        ..WatchOptions::default()
    };
    let mut watch = docs.watch("things", &Filter::All, &options).await.unwrap();
    assert_eq!(server.open_watches(), 1);
    let tx = source.0.lock().unwrap()[0].clone();

    tx.send(Some(WatchNotice::Invalidated {
        cursor: Some(WatchCursor::new(vec![7, 8]).unwrap()),
    }))
    .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("signal arrives")
        .unwrap()
        .unwrap();
    match notice {
        WatchNotice::Invalidated { cursor } => {
            assert_eq!(cursor.unwrap().as_bytes(), [7, 8]);
        }
        other @ WatchNotice::ResetRequired { .. } => panic!("unexpected {other:?}"),
    }

    tx.send(Some(WatchNotice::ResetRequired {
        reason: WatchReset::ResumeLost,
        source: None,
    }))
    .unwrap();
    let reset = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("reset arrives")
        .unwrap()
        .unwrap();
    assert!(matches!(
        reset,
        WatchNotice::ResetRequired {
            reason: WatchReset::ResumeLost,
            ..
        }
    ));
    assert!(watch.next().await.unwrap().is_none());

    drop(watch);
    for _ in 0..100 {
        if server.open_watches() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test]
async fn secrets_need_the_confidential_route() {
    let private = rig(Admission::Private).await;
    let error = private.bus.secrets().set("k", b"v").await.unwrap_err();
    assert!(matches!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::Backend
            | tinystoragedrivers_core::ErrorKind::Unsupported(_)
    ));
    assert!(private.bus.secrets().get("k").await.is_err());

    let confidential = rig(Admission::Confidential).await;
    let secrets = confidential.bus.secrets();
    secrets.set("api/key", b"hunter2").await.unwrap();
    let value = secrets.get("api/key").await.unwrap().unwrap();
    assert_eq!(value.as_slice(), b"hunter2");
    assert_eq!(
        secrets.list("api/").await.unwrap(),
        vec!["api/key".to_owned()]
    );
    assert!(secrets.delete("api/key").await.unwrap());
    assert!(secrets.get("api/key").await.unwrap().is_none());
    assert!(!format!("{secrets:?}").contains("hunter2"));
}

fn watching_server() -> (Arc<StorageServer>, Arc<watching::Source>) {
    let backend = MemoryStorage::new();
    let base = backend.for_scope(&Scope::local()).unwrap();
    let source = Arc::new(watching::Source(std::sync::Mutex::new(Vec::new())));
    let docs = watching::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
    };
    let storage = ScopedStorage::new(
        Scope::local(),
        "watching",
        Arc::new(docs),
        base.streams().clone(),
        base.blobs().clone(),
    );
    (Arc::new(StorageServer::new(storage)), source)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_reset_reason_maps_to_a_wire_category_and_back() {
    use tinystoragedrivers_core::WatchReset;
    let (server, source) = watching_server();
    let loopback = Arc::new(Loopback::new(server.clone(), Admission::Private));
    let bus = BusStorage::connect(loopback, Scope::local()).await.unwrap();
    let docs = bus.for_scope(&Scope::local()).unwrap().documents().clone();
    let options = WatchOptions {
        interval: Duration::from_millis(5),
        ..WatchOptions::default()
    };
    for (native, expected) in [
        (WatchReset::GenerationChanged, WatchReset::GenerationChanged),
        (WatchReset::Overflow, WatchReset::Invalidated),
        (WatchReset::Invalidated, WatchReset::Invalidated),
    ] {
        let mut watch = docs.watch("c", &Filter::All, &options).await.unwrap();
        let tx = source.0.lock().unwrap().pop().unwrap();
        tx.send(Some(WatchNotice::ResetRequired {
            reason: native,
            source: None,
        }))
        .unwrap();
        let reset = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match reset {
            WatchNotice::ResetRequired { reason, .. } => assert_eq!(reason, expected),
            WatchNotice::Invalidated { .. } => panic!("expected a reset"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_open_validates_options_and_caps_subscriptions() {
    let (server, _source) = watching_server();
    let open = |after: serde_json::Value, interval: u64| {
        let server = server.clone();
        async move {
            server
                .handle(
                    Admission::Private,
                    "Documents",
                    "WatchOpen",
                    json!({"collection":"c","filter":{"op":"all"},"after":after,"interval_ms":interval}),
                )
                .await
        }
    };
    assert!(open(json!([]), 5).await.is_err());
    assert!(open(json!(null), 0).await.is_err());
    assert!(open(json!(null), 60_001).await.is_err());
    for _ in 0..64 {
        open(json!(null), 5).await.unwrap();
    }
    assert_eq!(server.open_watches(), 64);
    assert!(open(json!(null), 5).await.is_err());
    assert!(format!("{server:?}").contains("StorageServer"));
    drop(server);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_opens_cannot_exceed_the_cap_and_ended_watches_free_their_slot() {
    use tinystoragedrivers_core::WatchReset;
    let (server, source) = watching_server();
    let request = json!({"collection":"c","filter":{"op":"all"},"after":null,"interval_ms":5});
    let attempts: Vec<_> = (0..100)
        .map(|_| {
            let server = server.clone();
            let request = request.clone();
            tokio::spawn(async move {
                server
                    .handle(Admission::Private, "Documents", "WatchOpen", request)
                    .await
            })
        })
        .collect();
    let mut opened = 0;
    for attempt in attempts {
        if attempt.await.unwrap().is_ok() {
            opened += 1;
        }
    }
    assert_eq!(opened, 64, "exactly the cap is admitted");
    assert_eq!(server.open_watches(), 64);

    // A watch that ends on its own gives its slot back without a WatchClose.
    let tx = source.0.lock().unwrap()[0].clone();
    tx.send(Some(WatchNotice::ResetRequired {
        reason: WatchReset::ResumeLost,
        source: None,
    }))
    .unwrap();
    for _ in 0..100 {
        if server.open_watches() < 64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.open_watches(), 63);
    server
        .handle(Admission::Private, "Documents", "WatchOpen", request)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn secret_handles_never_print_and_report_their_backend() {
    let rig = rig(Admission::Confidential).await;
    let secrets = rig.bus.secrets();
    assert_eq!(secrets.backend_name(), "bus");
    assert!(secrets.get("").await.is_err());
    assert!(secrets.delete("").await.is_err());
    assert!(secrets.list("a\u{0}").await.is_err());
    assert!(secrets.set("", b"x").await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_that_fails_ends_with_a_backend_reset_and_frees_its_slot() {
    let (server, _source) = watching_server();
    let mut signals = server.signals();
    server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"failing","filter":{"op":"all"},"after":null,"interval_ms":5}),
        )
        .await
        .unwrap();
    let signal = tokio::time::timeout(Duration::from_secs(5), signals.recv())
        .await
        .expect("signal arrives")
        .unwrap();
    assert!(matches!(
        signal.notice,
        tinystoragedrivers_bus::WatchNotice::ResetRequired {
            reason: tinystoragedrivers_bus::WatchReset::Backend
        }
    ));
    for _ in 0..100 {
        if server.open_watches() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exhausted_watch_and_an_explicit_close_both_release_the_slot() {
    let (server, source) = watching_server();
    let request = json!({"collection":"c","filter":{"op":"all"},"after":null,"interval_ms":5});
    let opened = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            request.clone(),
        )
        .await
        .unwrap();
    let id = opened["watch_id"].as_str().unwrap().to_owned();
    server
        .handle(Admission::Private, "Documents", "WatchOpen", request)
        .await
        .unwrap();
    assert_eq!(server.open_watches(), 2);

    // The source ending (no more notices) drops the first watch.
    let tx = source.0.lock().unwrap()[1].clone();
    tx.send(None).unwrap();
    for _ in 0..100 {
        if server.open_watches() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(server.open_watches(), 1);

    // Closing the other by id succeeds once, then it is gone.
    server
        .handle(
            Admission::Private,
            "Documents",
            "WatchClose",
            json!({"watch_id": id}),
        )
        .await
        .unwrap();
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_poisoned_watch_registry_is_a_backend_failure() {
    let (server, _source) = watching_server();
    let registry = server.watches.clone();
    let _ = std::thread::spawn(move || {
        let _guard = registry.lock().unwrap();
        panic!("poison the registry");
    })
    .join();
    let error = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"c","filter":{"op":"all"},"after":null,"interval_ms":5}),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, tinystoragedrivers_bus::StorageKind::Backend);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_writes_options_reads_metadata_and_cannot_set_created_ms() {
    let rig = rig(Admission::Private).await;
    let docs = rig.storage.documents();
    let options = WriteOptions::new().schema(7);
    docs.put_with("meta", "a", json!({"x": 1}), Precondition::None, options)
        .await
        .unwrap();
    let found = docs.get("meta", "a").await.unwrap().unwrap();
    assert_eq!(found.meta.schema, 7);
    assert!(found.meta.created_ms > 0);
    let refused = docs
        .put_with(
            "meta",
            "b",
            json!({}),
            Precondition::None,
            WriteOptions::new().created_ms(5),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidInput);
    assert!(docs.get("meta", "b").await.unwrap().is_none());
    // Host-only capabilities stay hidden from the module.
    let caps = rig.storage.documents().capabilities();
    assert!(caps.contains(Capability::RecordMetadata));
    assert!(caps.contains(Capability::AtomicWriteOptions));
    assert!(caps.contains(Capability::StreamSeal));
    assert!(!caps.contains(Capability::Maintenance));
    assert!(!caps.contains(Capability::ScopeEnumeration));
}

#[tokio::test(flavor = "multi_thread")]
async fn atomic_write_options_roundtrip_and_failed_chunk_rolls_back() {
    let rig = rig(Admission::Private).await;
    let docs = rig.storage.documents();
    let results = docs
        .atomic_batch_with(
            vec![
                WriteOp::Put {
                    collection: "meta".into(),
                    id: "a".into(),
                    doc: json!({"value":1}),
                    precondition: Precondition::Absent,
                },
                WriteOp::Put {
                    collection: "meta".into(),
                    id: "b".into(),
                    doc: json!({"value":2}),
                    precondition: Precondition::Absent,
                },
            ],
            WriteOptions::new().schema(2).expires_ms(9_000_000_000_000),
        )
        .await
        .unwrap();
    assert_eq!(
        results,
        [
            WriteResult::Put {
                version: tinystoragedrivers_core::Version::FIRST,
            },
            WriteResult::Put {
                version: tinystoragedrivers_core::Version::FIRST,
            },
        ]
    );
    for id in ["a", "b"] {
        assert_eq!(docs.get("meta", id).await.unwrap().unwrap().meta.schema, 2);
        assert_eq!(
            docs.get("meta", id).await.unwrap().unwrap().meta.expires_ms,
            Some(9_000_000_000_000)
        );
    }
    let refused = docs
        .atomic_batch_with(
            vec![WriteOp::Put {
                collection: "meta".into(),
                id: "host-time".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            }],
            WriteOptions::new().created_ms(5),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidInput);
    assert!(docs.get("meta", "host-time").await.unwrap().is_none());
    let before = docs.get("meta", "a").await.unwrap().unwrap();
    let error = docs
        .atomic_batch_checked_with(
            vec![ReadCheck {
                collection: "meta".into(),
                id: "a".into(),
                expected: ExpectedDocument::Version(before.version),
            }],
            vec![
                WriteOp::Put {
                    collection: "meta".into(),
                    id: "a".into(),
                    doc: json!({"value":3}),
                    precondition: Precondition::Version(before.version),
                },
                WriteOp::Put {
                    collection: "meta".into(),
                    id: "c".into(),
                    doc: json!({"value":4}),
                    precondition: Precondition::Version(tinystoragedrivers_core::Version(99)),
                },
            ],
            WriteOptions::new().schema(7).expires_ms(20_000),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(docs.get("meta", "a").await.unwrap().unwrap(), before);
    assert!(docs.get("meta", "c").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_seals_a_stream_and_reads_it_back() {
    let rig = rig(Admission::Private).await;
    let streams = rig.storage.streams();
    streams
        .append_batch("log", vec![json!(1), json!(2)])
        .await
        .unwrap();
    let outcome = streams.seal("log").await.unwrap();
    assert_eq!((outcome.base, outcome.len), (0, 2));
    assert_eq!(streams.read_window("log", 0, 10).await.unwrap().len(), 2);
    assert_eq!(
        streams.append("log", json!(3)).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_cannot_run_the_hosts_conditional_retention_delete() {
    let rig = rig(Admission::Private).await;
    let streams = rig.storage.streams();
    streams.append("log", json!(1)).await.unwrap();
    let sealed_ms = streams
        .seal("log")
        .await
        .map(|_| ())
        .and(streams.info("log").await);
    let sealed_ms = sealed_ms.unwrap().and_then(|info| info.sealed_ms).unwrap();
    assert_eq!(
        streams
            .delete_sealed_stream("log", sealed_ms)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::StreamSeal)
    );
    assert_eq!(streams.len("log").await.unwrap(), 1, "nothing was deleted");
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_checks_run_across_the_bus_on_the_host_clock() {
    let rig = rig(Admission::Private).await;
    let queues = rig.storage.queues();
    assert!(format!("{queues:?}").contains("BusQueues"));
    let features = queues.features();
    assert!(features.dedupe && features.groups && features.priorities && features.delay);
    conformance::run_queue(queues.as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_manifest_declares_the_queues_a_module_reaches() {
    let backend = MemoryStorage::new();
    let manifest = tinystoragedrivers_core::queue::QueueManifest::from_json(
        r#"{"version": 1, "queues": [{"name": "inbound"}]}"#,
    )
    .unwrap();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
        .with_queue_manifest(Arc::new(manifest.clone()));
    let bus = BusStorage::connect(
        Arc::new(Loopback::new(Arc::new(server), Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap();
    let queues = bus.for_scope(&Scope::local()).unwrap().queues().clone();
    // A module applying the same manifest agrees with the host.
    manifest.apply(queues.as_ref()).await.unwrap();
    let message = tinystoragedrivers_core::queue::NewMessage::new(json!({"t": 1}));
    queues
        .enqueue("inbound", std::slice::from_ref(&message))
        .await
        .unwrap();
    assert_eq!(
        queues
            .enqueue("elsewhere", &[message])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}
