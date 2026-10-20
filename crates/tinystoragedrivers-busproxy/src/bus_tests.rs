//! The storage bus over a real in-process `TinyBus` broker: no network, one
//! host connection serving the objects and module connections calling them.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tinybus::{BusName, Connection, broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_client::{BusStorage, StorageServer};
use tinystoragedrivers_core::{
    CollectionSpec, ErrorKind, Filter, MemoryStorage, Precondition, Scope, ScopedStorage,
    StorageBackend, WatchNotice, WatchOptions, WriteOptions,
};
use tinystoragedrivers_secrets::SecretStore;

use super::*;
use crate::watching_fake::{Docs, Source};

type Bindings = Arc<Mutex<HashMap<BusName, ScopedStorage>>>;

struct Rig {
    bus: MemoryBus,
    _host: Connection,
    served: ServedStorage,
    bindings: Bindings,
    backend: MemoryStorage,
}

async fn rig() -> Rig {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bindings: Bindings = Arc::default();
    let lookup = Arc::clone(&bindings);
    let endpoint = PerCallerEndpoint::new(move |sender| {
        let storage = lookup.lock().unwrap().get(sender).cloned()?;
        Some(StorageServer::new(storage))
    });
    let served = serve(&host, Arc::new(endpoint)).await.unwrap();
    Rig {
        bus,
        _host: host,
        served,
        bindings,
        backend: MemoryStorage::new(),
    }
}

impl Rig {
    /// A module connection, bound by the host to `scope` on the shared backend.
    async fn module(&self, scope: &str) -> (Connection, ScopedStorage) {
        let storage = self.backend.for_scope(&Scope::new(scope).unwrap()).unwrap();
        self.module_with(storage).await
    }

    async fn module_with(&self, storage: ScopedStorage) -> (Connection, ScopedStorage) {
        let connection = Connection::connect(self.bus.connect().await.unwrap())
            .await
            .unwrap();
        self.bindings
            .lock()
            .unwrap()
            .insert(connection.unique_name().unwrap(), storage);
        let bus = connect_storage(connection.clone()).await.unwrap();
        let scoped = bus.for_scope(&module_scope_label()).unwrap();
        (connection, scoped)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_conformance_suite_passes_over_a_real_bus() {
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    tinystoragedrivers_core::conformance::run_ports(&storage).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_over_the_frame_cap_streams_both_ways_in_bounded_chunks() {
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    let blobs = storage.blobs();
    // 20 MiB cannot fit one 16 MiB frame, so only streams can carry it.
    let len: usize = 20 * 1024 * 1024 + 123;
    let body: Vec<u8> = (0..len).map(|at| u8::try_from(at % 251).unwrap()).collect();

    let meta = blobs
        .put_stream(
            "big/blob.bin",
            Box::pin(std::io::Cursor::new(body.clone())),
            len as u64,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(meta.len, len as u64);

    // The host's backend holds exactly what was sent.
    let direct = rig
        .backend
        .for_scope(&Scope::new("alpha").unwrap())
        .unwrap();
    let stored = direct.blobs().get("big/blob.bin").await.unwrap().unwrap();
    assert!(stored.bytes == body, "the upload arrived intact");

    let mut stream = blobs.get_stream("big/blob.bin").await.unwrap().unwrap();
    assert_eq!(stream.meta.len, len as u64);
    let mut read = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream.reader, &mut read)
        .await
        .unwrap();
    assert!(read == body, "the download arrived intact");

    assert!(blobs.get_stream("big/missing").await.unwrap().is_none());
    let whole = blobs.get("big/blob.bin").await.unwrap().unwrap();
    assert!(
        whole.bytes == body,
        "get falls back to a stream for a large blob"
    );
    assert_eq!(
        blobs
            .get_range("big/blob.bin", 1_000..1_004)
            .await
            .unwrap()
            .unwrap(),
        &body[1_000..1_004]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_blob_and_a_short_body_over_a_stream() {
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    let blobs = storage.blobs();
    blobs
        .put_stream("empty", Box::pin(std::io::Cursor::new(Vec::new())), 0, None)
        .await
        .unwrap();
    let mut stream = blobs.get_stream("empty").await.unwrap().unwrap();
    let mut read = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream.reader, &mut read)
        .await
        .unwrap();
    assert_eq!(read.len(), 0);

    // A body shorter than it declared is refused and publishes nothing.
    let error = blobs
        .put_stream(
            "short",
            Box::pin(std::io::Cursor::new(vec![1; 10])),
            4096,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(blobs.head("short").await.unwrap().is_none());

    // The absolute-expiry stream path shares the same sender/receiver race.
    let error = blobs
        .put_stream_until(
            "short-until",
            Box::pin(std::io::Cursor::new(vec![1; 10])),
            4096,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(blobs.head("short-until").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn blob_lifetimes_cross_the_bus() {
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    let blobs = storage.blobs();
    assert!(
        blobs
            .capabilities()
            .contains(tinystoragedrivers_core::Capability::BlobTtl)
    );
    blobs
        .put_expiring("gone", vec![1], None, Duration::ZERO)
        .await
        .unwrap();
    assert!(blobs.head("gone").await.unwrap().is_none());
    blobs
        .put_expiring("kept", vec![1], None, Duration::from_secs(3600))
        .await
        .unwrap();
    assert!(blobs.head("kept").await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn record_metadata_and_stream_sealing_cross_the_bus() {
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    let docs = storage.documents();
    docs.put_with(
        "meta",
        "a",
        json!({"x": 1}),
        Precondition::None,
        WriteOptions::new().schema(4),
    )
    .await
    .unwrap();
    let found = docs.get("meta", "a").await.unwrap().unwrap();
    assert_eq!(found.meta.schema, 4);
    assert!(found.meta.created_ms > 0);
    let refused = docs
        .put_with(
            "meta",
            "b",
            json!({}),
            Precondition::None,
            WriteOptions::new().created_ms(1),
        )
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::InvalidInput);

    let streams = storage.streams();
    streams.append("log", json!(1)).await.unwrap();
    let sealed = streams.seal("log").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 1));
    assert_eq!(
        streams.append("log", json!(2)).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_sees_only_the_scope_the_host_bound_it_to() {
    let rig = rig().await;
    let (_a, alpha) = rig.module("alpha").await;
    let (_b, beta) = rig.module("beta").await;
    for storage in [&alpha, &beta] {
        storage
            .documents()
            .ensure_collection(&CollectionSpec::new("notes"))
            .await
            .unwrap();
    }
    alpha
        .documents()
        .put(
            "notes",
            "one",
            json!({"owner": "alpha"}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        beta.documents()
            .get("notes", "one")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        alpha
            .documents()
            .count("notes", &Filter::All)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        beta.documents().count("notes", &Filter::All).await.unwrap(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_the_host_does_not_know_is_denied() {
    let rig = rig().await;
    let stranger = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let error = connect_storage(stranger).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_ne!(error.to_string(), "");
}

#[tokio::test(flavor = "multi_thread")]
async fn connecting_without_a_host_fails_cleanly() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let lonely = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let error = connect_storage(lonely).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn secrets_need_a_host_the_broker_has_attested() {
    let rig = rig().await;
    let connection = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    rig.bindings.lock().unwrap().insert(
        connection.unique_name().unwrap(),
        rig.backend
            .for_scope(&Scope::new("alpha").unwrap())
            .unwrap(),
    );
    let bus: BusStorage = connect_storage(connection).await.unwrap();
    // The in-process host is not an attested artifact, so the confidential
    // route is refused by the broker rather than delivering a secret to it.
    let error = bus.secrets().get("anything").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_host_name_can_be_taken_once_and_withdrawn() {
    let rig = rig().await;
    let rival = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let endpoint = PerCallerEndpoint::new(|_| None);
    let taken = serve(&rival, Arc::new(endpoint)).await;
    assert!(taken.is_err(), "the name already has an owner");

    rig.served.shutdown().await.unwrap();
    let connection = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(connect_storage(connection).await.is_err());
}

/// A module whose documents report a native watch the test drives.
async fn watching_module(rig: &Rig, scope: &str) -> (Connection, ScopedStorage, Arc<Source>) {
    let base = rig.backend.for_scope(&Scope::new(scope).unwrap()).unwrap();
    let source = Arc::new(Source::default());
    let docs = Docs {
        inner: base.documents().clone(),
        source: Arc::clone(&source),
    };
    let storage = ScopedStorage::new(
        base.scope().clone(),
        "watching",
        Arc::new(docs),
        base.streams().clone(),
        base.blobs().clone(),
    );
    let (connection, scoped) = rig.module_with(storage).await;
    (connection, scoped, source)
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_signals_reach_only_the_watching_module() {
    let rig = rig().await;
    let (_a, alpha, alpha_source) = watching_module(&rig, "alpha").await;
    let (_b, beta, beta_source) = watching_module(&rig, "beta").await;
    let options = WatchOptions {
        interval: Duration::from_millis(5),
        ..WatchOptions::default()
    };
    // Both callers' first watch is `w1`: signals must not cross.
    let mut alpha_watch = alpha
        .documents()
        .watch("things", &Filter::All, &options)
        .await
        .unwrap();
    let mut beta_watch = beta
        .documents()
        .watch("things", &Filter::All, &options)
        .await
        .unwrap();

    alpha_source.0.lock().unwrap()[0]
        .send(Some(WatchNotice::Invalidated { cursor: None }))
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), alpha_watch.next())
        .await
        .expect("the signal arrives")
        .unwrap()
        .unwrap();
    assert!(matches!(notice, WatchNotice::Invalidated { .. }));
    let crossed = tokio::time::timeout(Duration::from_millis(300), beta_watch.next()).await;
    assert!(crossed.is_err(), "beta did not see alpha's invalidation");

    beta_source.0.lock().unwrap()[0]
        .send(Some(WatchNotice::Invalidated { cursor: None }))
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), beta_watch.next())
        .await
        .expect("the signal arrives")
        .unwrap()
        .unwrap();
    assert!(matches!(notice, WatchNotice::Invalidated { .. }));

    // Withdrawing the objects also stops the relays that watching started.
    assert!(format!("{:?}", rig.served).contains("ServedStorage"));
    rig.served.shutdown().await.unwrap();
}

#[test]
fn watch_paths_are_injective_per_unique_name() {
    let path = |name: &str| watch_path(&BusName::new(name).unwrap()).unwrap();
    assert_eq!(
        path(":1.17").as_str(),
        "/ai/tinyhumans/tinystorage/Watch/_1_17"
    );
    assert_ne!(path(":1.17"), path(":11.7"));
    assert!(watch_path(&BusName::new(HOST_NAME).unwrap()).is_err());
}

#[test]
fn failures_become_named_bus_errors_and_back() {
    let denied: tinybus::Error = HostFailure::Denied("unknown-caller").into();
    assert!(
        matches!(&denied, tinybus::Error::MethodFailed { name, message }
            if name == ERROR_DENIED && message == "unknown-caller")
    );
    let storage: tinybus::Error = HostFailure::Storage(tinystoragedrivers_bus::StorageFailure {
        kind: tinystoragedrivers_bus::StorageKind::Conflict,
    })
    .into();
    assert!(
        matches!(&storage, tinybus::Error::MethodFailed { name, message }
            if name == ERROR_STORAGE && message == "conflict")
    );
}

/// A transport for a peer the host has no binding for.
async fn stranger_transport(rig: &Rig) -> BusTransport {
    let connection = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    BusTransport::connect(connection)
        .await
        .unwrap()
        .with_timeout(Duration::from_secs(5))
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_peer_is_denied_on_calls_and_both_streamed_transfers() {
    use tinystoragedrivers_client::{Object, StorageTransport, WireError};
    let rig = rig().await;
    let transport = stranger_transport(&rig).await;
    assert!(format!("{transport:?}").contains("BusTransport"));

    let denied = |error: WireError| matches!(&error, WireError::Transport(reason) if reason.contains("denied the call: unknown-caller"));
    let error = transport
        .call(Object::Documents, "Capabilities", serde_json::Value::Null)
        .await
        .unwrap_err();
    assert!(denied(error));

    let head = tinystoragedrivers_bus::PutBlobStreamRequest {
        key: "k".into(),
        len: 4,
        content_type: Some("text/plain".into()),
        ttl_ms: None,
    };
    let body: tinystoragedrivers_core::BlobReader =
        Box::pin(std::io::Cursor::new(vec![1, 2, 3, 4]));
    let error = transport.put_blob_stream(head, body).await.unwrap_err();
    assert!(denied(error));

    let request = tinystoragedrivers_bus::BlobKeyRequest { key: "k".into() };
    let error = transport.get_blob_stream(request).await.err().unwrap();
    assert!(denied(error));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_blob_source_fails_the_upload_without_publishing() {
    struct Broken;
    impl tokio::io::AsyncRead for Broken {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("disk gone")))
        }
    }
    let rig = rig().await;
    let (_module, storage) = rig.module("alpha").await;
    let error = storage
        .blobs()
        .put_stream("broken", Box::pin(Broken), 10, None)
        .await
        .unwrap_err();
    // Which side notices first (the module's source fault or the host's
    // aborted read) decides the category, so only the outcome is pinned.
    assert_ne!(error.to_string(), "");
    assert!(storage.blobs().head("broken").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_native_watch_fake_delegates_every_document_operation() {
    use tinystoragedrivers_core::{DocumentStore, Query, Sort};
    let rig = rig().await;
    let base = rig.backend.for_scope(&Scope::new("fake").unwrap()).unwrap();
    let docs = Docs {
        inner: base.documents().clone(),
        source: Arc::default(),
    };
    docs.ensure_collection(&CollectionSpec::new("c"))
        .await
        .unwrap();
    docs.put("c", "a", json!({"n": 1}), Precondition::None)
        .await
        .unwrap();
    assert!(docs.get("c", "a").await.unwrap().is_some());
    assert_eq!(docs.count("c", &Filter::All).await.unwrap(), 1);
    assert_eq!(
        docs.query("c", &Query::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    let sort: Vec<Sort> = Vec::new();
    assert!(
        docs.claim("c", &Filter::All, &sort, &json!({"n": 2}))
            .await
            .unwrap()
            .is_some()
    );
    assert!(docs.delete("c", "a", Precondition::None).await.unwrap());
    docs.put("c", "b", json!({}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.delete_where("c", &Filter::All).await.unwrap(), 1);
    docs.drop_collection("c").await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_peer_cannot_consume_another_peers_upload_stream() {
    use crate::names::{host_name, interface};
    use tinybus::StreamDescriptor;
    let rig = rig().await;
    let (victim, _) = rig.module("victim").await;
    let (intruder, _) = rig.module("intruder").await;

    // The victim opens an upload stream on the host but has not called yet.
    let mut writer = victim
        .open_stream(&host_name(), StreamDescriptor::with_len(4))
        .await
        .unwrap();
    let stream = writer.stream_ref();
    let head = json!({"key": "k", "len": 4, "content_type": null, "ttl_ms": null});

    // Another bound peer replays the victim's stream handle in its own call.
    let proxy = |connection: &Connection| {
        connection
            .proxy(
                host_name().as_str(),
                object_path(tinystoragedrivers_client::Object::Blobs).as_str(),
                interface().as_str(),
            )
            .unwrap()
            .with_timeout(Duration::from_secs(5))
    };
    let stolen = proxy(&intruder)
        .call::<serde_json::Value>("PutStream", (head.clone(), stream.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(&stolen, tinybus::Error::MethodFailed { message, .. } if message == "invalid_input"),
        "the replayed handle is refused like an unknown stream: {stolen:?}"
    );

    // The refusal left the stream with its owner, who can still use it.
    let victim_proxy = proxy(&victim);
    let calling = victim_proxy.call::<serde_json::Value>("PutStream", (head, stream));
    let sending = async {
        writer.write(b"mine").await.unwrap();
        writer.finish().await.unwrap();
    };
    let (reply, ()) = tokio::join!(calling, sending);
    reply.expect("the owner's own upload still completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_matching_all_watch_signals_does_not_receive_another_callers() {
    use crate::names::{host_name, interface, member};
    let rig = rig().await;
    let (_a, alpha, alpha_source) = watching_module(&rig, "alpha").await;
    let spy = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let owner = spy.name_owner(HOST_NAME).await.unwrap().unwrap();
    assert_eq!(host_name().as_str(), HOST_NAME);
    // No path filter: broad enough to catch any caller's signal if broadcast.
    let mut eavesdrop = spy
        .add_match(
            tinybus::MatchRule::new()
                .signals()
                .sender(owner)
                .interface(interface())
                .member(member(WATCH_MEMBER)),
        )
        .await
        .unwrap();
    let options = WatchOptions {
        interval: Duration::from_millis(5),
        ..WatchOptions::default()
    };
    let mut watch = alpha
        .documents()
        .watch("things", &Filter::All, &options)
        .await
        .unwrap();
    alpha_source.0.lock().unwrap()[0]
        .send(Some(WatchNotice::Invalidated { cursor: None }))
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("the subscriber still gets its signal")
        .unwrap()
        .unwrap();
    assert!(matches!(notice, WatchNotice::Invalidated { .. }));
    let overheard = tokio::time::timeout(Duration::from_millis(300), eavesdrop.recv()).await;
    assert!(overheard.is_err(), "the eavesdropper saw a unicast signal");
}
