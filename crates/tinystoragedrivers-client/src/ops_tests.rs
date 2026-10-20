//! Every port operation, once, through the client and a loopback server.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::json;
use tinystoragedrivers_bus::{BlobKeyRequest, MAX_INLINE_BLOB_BYTES, StorageFailure, StorageKind};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, Cursor, ErrorKind, ExpectedDocument, Filter, MemoryStorage,
    Precondition, Query, ReadCheck, Scope, ScopedStorage, Sort, StorageBackend, WriteOp,
    WriteResult,
};
use tokio::io::AsyncReadExt;

use crate::convert::{
    capabilities_of, capability_names, error_of, failure_of, versioned_from_wire,
};
use crate::{Admission, BusStorage, Loopback, Object, StorageServer, StorageTransport, WireError};

async fn connect() -> (ScopedStorage, Arc<StorageServer>, BusStorage) {
    let backend = MemoryStorage::new();
    let server = Arc::new(StorageServer::new(
        backend.for_scope(&Scope::local()).unwrap(),
    ));
    let transport = Arc::new(Loopback::new(server.clone(), Admission::Private));
    let bus = BusStorage::connect(transport, Scope::local())
        .await
        .unwrap();
    (bus.for_scope(&Scope::local()).unwrap(), server, bus)
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one walk through every Documents operation"
)]
async fn documents_cover_every_operation() {
    let (storage, _, _) = connect().await;
    let docs = storage.documents();
    let spec = CollectionSpec::new("jobs").searchable(["title"]);
    docs.ensure_collection(&spec).await.unwrap();
    docs.validate_collection_spec(&spec).await.ok();
    for i in 0..5 {
        docs.put(
            "jobs",
            &format!("j{i}"),
            json!({"state": "queued", "n": i, "title": format!("alpha job {i}")}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    }
    assert_eq!(docs.count("jobs", &Filter::All).await.unwrap(), 5);

    let query = Query::filter(Filter::eq("state", "queued"))
        .sort(Sort::desc("n"))
        .limit(2);
    let first = docs.query("jobs", &query).await.unwrap();
    assert_eq!(first.items.len(), 2);
    let cursor: Cursor = first.next.clone().expect("another page");
    let second = docs
        .query("jobs", &query.clone().after(cursor))
        .await
        .unwrap();
    assert_eq!(second.items.len(), 2);

    let claimed = docs
        .claim(
            "jobs",
            &Filter::eq("state", "queued"),
            &[Sort::asc("n")],
            &json!({"state": "running"}),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, "j0");
    assert_eq!(claimed.doc["state"], "running");

    let results = docs
        .atomic_batch(vec![
            WriteOp::Put {
                collection: "jobs".into(),
                id: "j9".into(),
                doc: json!({"state": "queued", "n": 9}),
                precondition: Precondition::Absent,
            },
            WriteOp::Delete {
                collection: "jobs".into(),
                id: "j1".into(),
                precondition: Precondition::None,
            },
        ])
        .await
        .unwrap();
    assert!(matches!(results[0], WriteResult::Put { .. }));
    assert_eq!(results[1], WriteResult::Delete { removed: true });

    let checked = docs
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "jobs".into(),
                id: "gone".into(),
                expected: ExpectedDocument::Absent,
            }],
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(checked.len(), 0);
    let stale = docs
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "jobs".into(),
                id: "j2".into(),
                expected: ExpectedDocument::Absent,
            }],
            vec![],
        )
        .await
        .unwrap_err();
    assert_eq!(stale.kind(), ErrorKind::Conflict);

    let hits = docs.search("jobs", "alpha", 3).await.unwrap();
    assert!(hits.len() <= 3);
    assert_eq!(
        docs.collections("job").await.unwrap(),
        vec!["jobs".to_owned()]
    );
    assert!(
        docs.delete_where("jobs", &Filter::eq("state", "queued"))
            .await
            .unwrap()
            > 0
    );
    assert!(docs.get("jobs", "nope").await.unwrap().is_none());
    assert!(
        !docs
            .delete("jobs", "nope", Precondition::None)
            .await
            .unwrap()
    );
    docs.drop_collection("jobs").await.unwrap();

    let too_many: Vec<WriteOp> = (0..1001)
        .map(|i| WriteOp::Delete {
            collection: "jobs".into(),
            id: format!("d{i}"),
            precondition: Precondition::None,
        })
        .collect();
    assert_eq!(
        docs.atomic_batch(too_many).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn a_query_page_over_the_byte_cap_is_cut_and_continues() {
    let (storage, _, _) = connect().await;
    let docs = storage.documents();
    docs.ensure_collection(&CollectionSpec::new("fat"))
        .await
        .unwrap();
    let body = "x".repeat(1024 * 1024);
    for i in 0..10 {
        docs.put(
            "fat",
            &format!("f{i}"),
            json!({"body": body}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    }
    let mut seen = 0;
    let mut query = Query::all().limit(10);
    loop {
        let page = docs.query("fat", &query).await.unwrap();
        assert!(page.items.len() < 10, "page is cut below the 8 MiB cap");
        seen += page.items.len();
        match page.next {
            Some(cursor) => query = query.after(cursor),
            None => break,
        }
    }
    assert_eq!(seen, 10);
}

#[tokio::test]
async fn streams_cover_every_operation() {
    let (storage, _, _) = connect().await;
    let streams = storage.streams();
    assert_eq!(streams.append("log", json!(1)).await.unwrap(), 0);
    assert_eq!(
        streams
            .append_batch("log", vec![json!(2), json!(3)])
            .await
            .unwrap(),
        1
    );
    let window = streams.read_window("log", 0, 10).await.unwrap();
    assert_eq!(window.len(), 3);
    assert_eq!(streams.len("log").await.unwrap(), 3);
    let conditional = streams
        .append_batch_if_offset("log", 3, vec![json!(4)])
        .await;
    match conditional {
        Ok(offset) => assert_eq!(offset, 3),
        Err(error) => assert_eq!(
            error.kind(),
            ErrorKind::Unsupported(Capability::ConditionalStreamAppend)
        ),
    }
    streams.truncate_before("log", 1).await.unwrap();
    assert_eq!(streams.streams("l").await.unwrap(), vec!["log".to_owned()]);
    assert!(streams.delete_stream("log").await.unwrap());
    let many: Vec<_> = (0..1001).map(|i| json!(i)).collect();
    assert_eq!(
        streams.append_batch("log", many).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn read_window_over_the_page_cap_is_assembled_from_pages() {
    let (storage, _, _) = connect().await;
    let streams = storage.streams();
    for chunk in 0..3 {
        let values: Vec<_> = (0..1000).map(|i| json!(chunk * 1000 + i)).collect();
        streams.append_batch("long", values).await.unwrap();
    }
    let window = streams.read_window("long", 0, 2500).await.unwrap();
    assert_eq!(window.len(), 2500);
    assert_eq!(window[2499].offset, 2499);
}

#[tokio::test]
async fn read_window_continues_after_a_byte_capped_page() {
    let (storage, _, _) = connect().await;
    let streams = storage.streams();
    // Three records of 3 MiB cannot share one 8 MiB reply, so the server cuts
    // each page short of the requested limit; the client must keep reading.
    let big = "x".repeat(3 * 1024 * 1024);
    for i in 0..5 {
        streams
            .append("fat", json!({"i": i, "pad": big}))
            .await
            .unwrap();
    }
    let window = streams.read_window("fat", 0, 5).await.unwrap();
    assert_eq!(window.len(), 5, "a short page is not the end of the stream");
    assert_eq!(window[4].offset, 4);
    let tail = streams.read_window("fat", 3, 10).await.unwrap();
    assert_eq!(tail.len(), 2);
}

#[tokio::test]
async fn an_unreadable_stream_record_is_refused_before_native_append() {
    let (storage, _, _) = connect().await;
    let streams = storage.streams();
    let huge = json!("y".repeat(8 * 1024 * 1024));
    assert_eq!(
        streams
            .append("big", huge.clone())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        streams
            .append_batch("big", vec![json!(1), huge.clone()])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        streams
            .append_batch_if_offset("big", 0, vec![huge])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(streams.len("big").await.unwrap(), 0, "nothing was written");
}

#[tokio::test]
async fn blobs_cover_every_operation() {
    let (storage, _, bus) = connect().await;
    let blobs = storage.blobs();
    assert_eq!(blobs.max_key_len(), 1024);
    assert!(blobs.capabilities().contains(Capability::StreamingBlobs));
    assert!(!blobs.capabilities().contains(Capability::PresignedUrls));
    let meta = blobs
        .put("a/b", b"hello world".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(meta.len, 11);
    assert_eq!(
        blobs
            .head("a/b")
            .await
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        blobs.get("a/b").await.unwrap().unwrap().bytes,
        b"hello world"
    );
    assert_eq!(
        blobs.get_range("a/b", 6..100).await.unwrap().unwrap(),
        b"world"
    );
    assert_eq!(blobs.list("a/").await.unwrap().len(), 1);
    assert!(blobs.get("none").await.unwrap().is_none());
    assert!(blobs.get_range("none", 0..1).await.unwrap().is_none());
    assert!(blobs.get_stream("none").await.unwrap().is_none());
    let mut stream = blobs.get_stream("a/b").await.unwrap().unwrap();
    let mut text = String::new();
    stream.reader.read_to_string(&mut text).await.unwrap();
    assert_eq!(text, "hello world");
    assert!(blobs.delete("a/b").await.unwrap());
    let refused = blobs
        .presign_put("a/b", Duration::from_secs(10))
        .await
        .unwrap_err();
    assert_eq!(
        refused.kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    let bad_ttl = blobs
        .presign_get("a/b", Duration::from_secs(0))
        .await
        .unwrap_err();
    assert_eq!(bad_ttl.kind(), ErrorKind::InvalidInput);
    assert_ne!(format!("{bus:?}"), "");
}

#[tokio::test]
async fn bus_blobs_preserve_absolute_expiry_over_inline_and_stream_transfers() {
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let backend = MemoryStorage::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    scoped
        .blobs()
        .put_expiring(
            "small",
            b"bytes".to_vec(),
            Some("text/plain"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let server = Arc::new(StorageServer::new(scoped));
    let transport = Arc::new(Loopback::new(server, Admission::Private));
    let bus = BusStorage::connect(transport.clone(), Scope::local())
        .await
        .unwrap();
    let blobs = bus.for_scope(&Scope::local()).unwrap().blobs().clone();

    let source = blobs.get_with_expiry("small").await.unwrap().unwrap();
    assert_eq!(source.blob.bytes, b"bytes");
    assert_eq!(source.expires_at_ms, Some(6_000));
    let inline = transport
        .get_blob_with_expiry(BlobKeyRequest {
            key: "small".into(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inline.expires_at_ms, Some(6_000));
    blobs
        .put_until("inline-write", b"exact".to_vec(), None, Some(6_000))
        .await
        .unwrap();
    assert_eq!(
        blobs
            .get_with_expiry("inline-write")
            .await
            .unwrap()
            .unwrap()
            .expires_at_ms,
        Some(6_000)
    );

    let large_bytes = vec![9; MAX_INLINE_BLOB_BYTES + 1];
    blobs
        .put_until(
            "large",
            large_bytes.clone(),
            Some("application/octet-stream"),
            Some(6_000),
        )
        .await
        .unwrap();
    let large = blobs.get_with_expiry("large").await.unwrap().unwrap();
    assert_eq!(large.blob.bytes, large_bytes);
    assert_eq!(large.expires_at_ms, Some(6_000));
    assert!(transport.streamed_bytes() > 0);

    now.store(6_000, Ordering::SeqCst);
    assert!(blobs.get_with_expiry("small").await.unwrap().is_none());
    assert!(blobs.get_with_expiry("large").await.unwrap().is_none());
}

#[test]
fn errors_round_trip_through_their_wire_category() {
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::Conflict,
        ErrorKind::AlreadyExists,
        ErrorKind::InvalidInput,
        ErrorKind::Serialization,
        ErrorKind::Crypto,
        ErrorKind::Unavailable,
        ErrorKind::Backend,
        ErrorKind::Fenced,
        ErrorKind::ScopeGenerationRejected,
    ] {
        let native = tinystoragedrivers_core::StorageError::new(kind, "boom");
        let failure = failure_of(&native);
        assert_eq!(error_of(failure, None).kind(), kind);
    }
    let unsupported =
        tinystoragedrivers_core::StorageError::unsupported(Capability::Watch, "backend-detail");
    let failure = failure_of(&unsupported);
    assert_eq!(failure.kind, StorageKind::Unsupported);
    assert_eq!(
        error_of(failure, Some(Capability::Watch)).kind(),
        ErrorKind::Unsupported(Capability::Watch)
    );
    assert_eq!(error_of(failure, None).kind(), ErrorKind::Backend);
    let message = error_of(failure, None).to_string();
    assert!(!message.contains("backend-detail"));
}

#[test]
fn capability_names_round_trip_without_host_only_entries() {
    let all = tinystoragedrivers_core::Capabilities::all();
    let names = capability_names(all);
    assert!(!names.contains(&"fencing".to_owned()));
    // Maintenance and scope enumeration are host-only: a module must not
    // trigger vacuum or list other tenants. The metadata and seal calls it may use.
    assert!(!names.contains(&"maintenance".to_owned()));
    assert!(!names.contains(&"scope_enumeration".to_owned()));
    assert!(!names.contains(&"shared_across_processes".to_owned()));
    assert!(!names.contains(&"cross_process_cas".to_owned()));
    assert!(names.contains(&"record_metadata".to_owned()));
    assert!(names.contains(&"atomic_write_options".to_owned()));
    assert!(names.contains(&"stream_seal".to_owned()));
    let parsed = capabilities_of(&names);
    assert!(parsed.contains(Capability::FullText));
    assert!(!parsed.contains(Capability::Fencing));
    let forged = capabilities_of(&["fencing".to_owned(), "from_the_future".to_owned()]);
    assert_eq!(forged, tinystoragedrivers_core::Capabilities::none());
}

#[test]
fn objects_parse_by_wire_name_only() {
    for object in [
        Object::Documents,
        Object::Streams,
        Object::Blobs,
        Object::Secrets,
    ] {
        assert_eq!(Object::parse(object.as_str()), Some(object));
    }
    assert_eq!(Object::parse("documents"), None);
    let storage = WireError::Storage(StorageFailure {
        kind: StorageKind::Conflict,
    });
    assert!(storage.to_string().contains("Conflict"));
    assert!(
        WireError::Transport("closed".into())
            .to_string()
            .contains("closed")
    );
}

#[tokio::test]
async fn a_closed_transport_surfaces_as_unavailable() {
    #[derive(Debug)]
    struct Closed(tokio::sync::broadcast::Sender<tinystoragedrivers_bus::WatchChanged>);
    #[async_trait::async_trait]
    impl StorageTransport for Closed {
        async fn call(
            &self,
            _: Object,
            _: &str,
            _: serde_json::Value,
        ) -> Result<serde_json::Value, WireError> {
            Err(WireError::Transport("bus closed".into()))
        }
        async fn put_blob_stream(
            &self,
            _: tinystoragedrivers_bus::PutBlobStreamRequest,
            _: crate::BlobBody,
        ) -> Result<serde_json::Value, WireError> {
            Err(WireError::Transport("bus closed".into()))
        }
        async fn get_blob_stream(
            &self,
            _: tinystoragedrivers_bus::BlobKeyRequest,
        ) -> Result<Option<(tinystoragedrivers_bus::BlobStreamHead, crate::BlobBody)>, WireError>
        {
            Err(WireError::Transport("bus closed".into()))
        }
        fn signals(
            &self,
        ) -> tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
            self.0.subscribe()
        }
    }
    let (tx, _) = tokio::sync::broadcast::channel(1);
    let error = BusStorage::connect(Arc::new(Closed(tx)), Scope::local())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_watch_asks_for_a_reset_and_other_ids_are_ignored() {
    use tinystoragedrivers_bus::{WatchChanged, WatchNotice as WireNotice};
    use tinystoragedrivers_core::{WatchNotice, WatchReset};
    let (tx, rx) = tokio::sync::broadcast::channel(2);
    // Drive the client watch type directly through a fabricated receiver.
    let mut watch = crate::client::testing::watch(rx);
    for i in 0..5 {
        tx.send(WatchChanged {
            watch_id: format!("other{i}"),
            notice: WireNotice::Invalidated { cursor: None },
        })
        .unwrap();
    }
    let lagged = watch.next().await.unwrap().unwrap();
    assert!(matches!(
        lagged,
        WatchNotice::ResetRequired {
            reason: WatchReset::Overflow,
            ..
        }
    ));
    assert!(watch.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_reset_categories_map_to_native_reasons_and_watches_close_off_runtime() {
    use tinystoragedrivers_bus::{
        WatchChanged, WatchNotice as WireNotice, WatchReset as WireReset,
    };
    use tinystoragedrivers_core::{WatchNotice, WatchReset};
    for (wire, native) in [
        (WireReset::CursorExpired, WatchReset::ResumeLost),
        (WireReset::Fenced, WatchReset::GenerationChanged),
        (WireReset::Backend, WatchReset::Invalidated),
    ] {
        let (tx, rx) = tokio::sync::broadcast::channel(8);
        let mut watch = crate::client::testing::watch(rx);
        assert!(format!("{watch:?}").contains("BusWatch"));
        tx.send(WatchChanged {
            watch_id: "mine".into(),
            notice: WireNotice::ResetRequired { reason: wire },
        })
        .unwrap();
        match watch.next().await.unwrap().unwrap() {
            WatchNotice::ResetRequired { reason, .. } => assert_eq!(reason, native),
            WatchNotice::Invalidated { .. } => panic!("expected a reset"),
        }
    }
    let (tx, rx) = tokio::sync::broadcast::channel(8);
    let watch = crate::client::testing::watch(rx);
    drop(tx);
    // Dropping with no runtime on the thread must not panic.
    std::thread::spawn(move || drop(watch)).join().unwrap();
}

#[test]
fn a_reply_from_a_pre_v4_host_reads_as_unknown_metadata() {
    let reply: tinystoragedrivers_bus::Versioned =
        serde_json::from_value(json!({"document_id":"a","version":2,"document_json":"{}"}))
            .unwrap();
    let native = versioned_from_wire(reply);
    assert_eq!(native.meta, tinystoragedrivers_core::Meta::unknown());
}
