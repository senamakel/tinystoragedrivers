//! End-to-end behavior through an in-process `TinyBus` broker.
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use tinybus::{
    BusName, Connection, Interface, InterfaceName, MemberName, ObjectPath, broker::Broker,
    transport::memory::MemoryBus,
};
use tinystoragedrivers_api::{Storage, StorageBackend};
use tinystoragedrivers_testkit::{MemoryBackend, conformance};
use tinystoragedrivers_types::{Result, Scope};

use super::*;

struct Rig {
    bus: MemoryBus,
    _host: Connection,
    _served: ServedStorage,
    bindings: Arc<Mutex<HashMap<BusName, Storage>>>,
    backend: MemoryBackend,
}

async fn rig() -> Rig {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bindings = Arc::new(Mutex::new(HashMap::<BusName, Storage>::new()));
    let lookup = Arc::clone(&bindings);
    let endpoint = PerCallerEndpoint::new(move |sender| {
        lookup
            .lock()
            .unwrap()
            .get(sender)
            .cloned()
            .map(StorageServer::new)
    });
    let served = serve(&host, Arc::new(endpoint)).await.unwrap();
    Rig {
        bus,
        _host: host,
        _served: served,
        bindings,
        backend: MemoryBackend::new(),
    }
}

impl Rig {
    async fn module(&self, scope: &str) -> (Connection, Storage) {
        let connection = Connection::connect(self.bus.connect().await.unwrap())
            .await
            .unwrap();
        let scope = Scope::new(scope).unwrap();
        self.bindings.lock().unwrap().insert(
            connection.unique_name().unwrap(),
            self.backend.for_scope(&scope).unwrap(),
        );
        let storage = connect_storage(connection.clone()).await.unwrap();
        assert_eq!(
            storage.secrets().get("secret").await.unwrap_err().kind(),
            tinystoragedrivers_types::ErrorKind::Unsupported(
                tinystoragedrivers_types::Capability::Secrets
            )
        );
        (connection, storage)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn all_six_ports_pass_the_shared_conformance_suites_over_loopback() {
    let server = StorageServer::new(tinystoragedrivers_testkit::memory_storage(
        &Scope::new("transport-conformance").unwrap(),
    ));
    let storage = BusStorage::connect(Arc::new(Loopback::new(
        Arc::new(server),
        Admission::Confidential,
    )))
    .await
    .unwrap();
    conformance::documents::run(storage.documents().as_ref()).await;
    conformance::streams::run(storage.streams().as_ref()).await;
    conformance::kv::run(storage.kv().as_ref()).await;
    conformance::blobs::run(storage.blobs().as_ref()).await;
    conformance::queue::run(storage.queues().as_ref()).await;
    conformance::secrets::run(storage.secrets().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_callers_receive_independent_storage_bindings() {
    let rig = rig().await;
    let (_first, first) = rig.module("caller-a").await;
    let (_second, second) = rig.module("caller-b").await;
    first
        .kv()
        .put("private", serde_json::json!("a"))
        .await
        .unwrap();
    assert!(second.kv().get("private").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn invocation_addresses_isolate_parallel_endpoints_on_one_attested_connection() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host_name = host.unique_name().unwrap();
    let address = |id: &str| {
        StorageAddress::try_new(
            host_name.clone(),
            ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/{id}")).unwrap(),
        )
        .unwrap()
    };

    let make_endpoint = |scope: &str| {
        let storage = MemoryBackend::new()
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap();
        Arc::new(PerCallerEndpoint::new(move |_| {
            Some(StorageServer::new(storage.clone()))
        }))
    };
    // The long-lived well-known service shares the same connection and remains
    // registered while both request-scoped endpoints come and go.
    let fixed = serve(&host, make_endpoint("fixed")).await.unwrap();
    let first_address = address("invocation_a");
    let second_address = address("invocation_b");
    let wrong_host_address = StorageAddress::try_new(
        BusName::new(":1.999").unwrap(),
        ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/invocation_a")).unwrap(),
    )
    .unwrap();
    assert!(
        serve_at_address(&host, wrong_host_address, make_endpoint("wrong-host"))
            .await
            .is_err(),
        "an address cannot redirect registration to a different unique host"
    );
    let first_guard = serve_at_address(&host, first_address.clone(), make_endpoint("first"))
        .await
        .unwrap();
    let second_guard = serve_at_address(&host, second_address.clone(), make_endpoint("second"))
        .await
        .unwrap();

    let client_a = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client_b = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let storage_a = connect_storage_at_address(client_a, first_address)
        .await
        .unwrap();
    let storage_b = connect_storage_at_address(client_b, second_address)
        .await
        .unwrap();
    storage_a
        .kv()
        .put("same", serde_json::json!("first"))
        .await
        .unwrap();
    storage_b
        .kv()
        .put("same", serde_json::json!("second"))
        .await
        .unwrap();
    assert_eq!(
        storage_a.kv().get("same").await.unwrap().unwrap().doc,
        serde_json::json!("first")
    );
    assert_eq!(
        storage_b.kv().get("same").await.unwrap().unwrap().doc,
        serde_json::json!("second")
    );

    first_guard.shutdown().await.unwrap();
    assert!(storage_a.kv().get("same").await.is_err());
    assert_eq!(
        storage_b.kv().get("same").await.unwrap().unwrap().doc,
        serde_json::json!("second")
    );
    // The ordinary destination is still intact and independent.
    let ordinary_client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let ordinary = connect_storage(ordinary_client).await.unwrap();
    assert!(ordinary.kv().get("same").await.unwrap().is_none());

    drop(second_guard);
    assert!(storage_b.kv().get("same").await.is_err());
    fixed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_invocation_address_cannot_replace_the_live_endpoint() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let address = StorageAddress::try_new(
        host.unique_name().unwrap(),
        ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/duplicate")).unwrap(),
    )
    .unwrap();
    let make_endpoint = |scope: &str| {
        let storage = MemoryBackend::new()
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap();
        Arc::new(PerCallerEndpoint::new(move |_| {
            Some(StorageServer::new(storage.clone()))
        }))
    };

    let first = serve_at_address(&host, address.clone(), make_endpoint("first"))
        .await
        .unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let storage = connect_storage_at_address(module, address.clone())
        .await
        .unwrap();
    storage
        .kv()
        .put("owner", serde_json::json!("first"))
        .await
        .unwrap();

    let duplicate = serve_at_address(&host, address, make_endpoint("second")).await;
    assert!(
        duplicate.is_err(),
        "a live endpoint address cannot be replaced"
    );
    assert_eq!(
        storage.kv().get("owner").await.unwrap().unwrap().doc,
        serde_json::json!("first"),
        "refusing a duplicate leaves the first endpoint active"
    );
    first.shutdown().await.unwrap();
    assert!(storage.kv().get("owner").await.is_err());
}

struct ExistingInvocationPort;

#[async_trait::async_trait]
impl Interface for ExistingInvocationPort {
    fn name(&self) -> InterfaceName {
        InterfaceName::new("ai.tinyhumans.tinystorage.Preexisting").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![MemberName::new("Probe").unwrap()]
    }

    async fn call(
        &self,
        _member: &MemberName,
        _args: serde_json::Value,
    ) -> tinybus::Result<serde_json::Value> {
        Ok(serde_json::json!("preexisting"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_invocation_registration_rolls_back_only_its_own_paths() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let address = StorageAddress::try_new(
        host.unique_name().unwrap(),
        ObjectPath::new(format!("{INVOCATION_OBJECT_ROOT}/partial")).unwrap(),
    )
    .unwrap();
    let preexisting_path = address.port_path(Object::Streams).clone();
    host.serve_at(preexisting_path.clone(), ExistingInvocationPort)
        .await
        .unwrap();
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("partial-registration").unwrap())
        .unwrap();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(storage.clone()))
    }));

    assert!(
        serve_at_address(&host, address.clone(), endpoint)
            .await
            .is_err()
    );

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let probe = client
        .proxy(
            address.host().clone(),
            preexisting_path,
            "ai.tinyhumans.tinystorage.Preexisting",
        )
        .unwrap();
    let response: String = probe.call("Probe", ()).await.unwrap();
    assert_eq!(response, "preexisting");

    let first_port_client = client
        .proxy(
            address.host().clone(),
            address.port_path(Object::Documents).clone(),
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )
        .unwrap();
    assert!(
        first_port_client
            .call::<serde_json::Value>("Get", ("x",))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ordinary_broker_connection_works_without_secret_admission() {
    let rig = rig().await;
    let (_module, storage) = rig.module("ordinary-secret-denial").await;
    assert!(!storage.secrets().enumerable());
    let error = storage.secrets().get("private").await.unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(
            tinystoragedrivers_types::Capability::Secrets
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_forwards_every_non_confidential_primitive_through_the_bound_host() {
    let rig = rig().await;
    let (_module, storage) = rig.module("broker-conformance").await;
    conformance::documents::run(storage.documents().as_ref()).await;
    conformance::streams::run(storage.streams().as_ref()).await;
    conformance::kv::run(storage.kv().as_ref()).await;
    conformance::blobs::run(storage.blobs().as_ref()).await;
    conformance::queue::run(storage.queues().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
    assert!(
        storage
            .blobs()
            .get_stream("absent")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage.secrets().get("private").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(
            tinystoragedrivers_types::Capability::Secrets
        )
    );
}

struct OneChunk(Option<Bytes>);

impl Stream for OneChunk {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

async fn collect(mut stream: tinystoragedrivers_api::BlobReader) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        body.extend_from_slice(&chunk?);
    }
    Ok(body)
}

#[tokio::test(flavor = "multi_thread")]
async fn blobs_larger_than_a_frame_stream_in_bounded_chunks_without_byte_loss() {
    let rig = rig().await;
    let (_module, storage) = rig.module("large-blob").await;
    let bytes: Vec<u8> = (0..(20 * 1024 * 1024 + 17))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    storage
        .blobs()
        .put_stream(
            "large.bin",
            Box::pin(OneChunk(Some(Bytes::copy_from_slice(&bytes)))),
            bytes.len() as u64,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    let streamed = storage
        .blobs()
        .get_stream("large.bin")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed.meta.len, bytes.len() as u64);
    assert_eq!(collect(streamed.reader).await.unwrap(), bytes);
}
