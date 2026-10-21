use super::*;
use crate::INVOCATION_OBJECT_ROOT;
use crate::PerCallerEndpoint;
use crate::StorageServer;
use std::sync::atomic::{AtomicU64, Ordering};
use tinybus::module::ModuleHost;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

struct SignalEndpoint {
    signals: Mutex<Option<tokio::sync::broadcast::Sender<WatchChanged>>>,
    has_signals: bool,
    epoch: AtomicU64,
}

struct ReplyEndpoint(Value);

struct CountingEndpoint(Arc<AtomicU64>);

#[async_trait::async_trait]
impl StorageEndpoint for ReplyEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Ok(self.0.clone())
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: tinystoragedrivers_api::BlobReader,
    ) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_api::BlobReader)>, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    fn watch_signals(
        &self,
        _sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        None
    }
}

#[async_trait::async_trait]
impl StorageEndpoint for CountingEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!("called"))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: tinystoragedrivers_api::BlobReader,
    ) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_api::BlobReader)>, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    fn watch_signals(
        &self,
        _sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        None
    }
}

struct MarkerInterface(&'static str);

#[async_trait::async_trait]
impl tinybus::Interface for MarkerInterface {
    fn name(&self) -> tinybus::InterfaceName {
        tinybus::InterfaceName::new("org.example.RegistrationMarker").unwrap()
    }

    fn members(&self) -> Vec<MemberName> {
        vec![member("Probe")]
    }

    async fn call(&self, _member: &MemberName, _args: Value) -> tinybus::Result<Value> {
        Ok(json!(self.0))
    }
}

fn addressed(host: &Connection, component: &str) -> StorageAddress {
    StorageAddress::try_new(
        host.unique_name().unwrap(),
        format!("{INVOCATION_OBJECT_ROOT}/{component}")
            .parse()
            .unwrap(),
    )
    .unwrap()
}

async fn call_addressed(
    connection: &Connection,
    address: &StorageAddress,
    object: Object,
) -> tinybus::Result<Value> {
    connection
        .proxy(
            address.host().as_str(),
            address.port_path(object).as_str(),
            interface().as_str(),
        )
        .unwrap()
        .call("Capabilities", ())
        .await
}

async fn call_fixed(connection: &Connection) -> tinybus::Result<Value> {
    connection
        .proxy(
            HOST_NAME,
            object_path(Object::Documents).as_str(),
            interface().as_str(),
        )
        .unwrap()
        .call("Capabilities", ())
        .await
}

async fn call_marker_interface(
    connection: &Connection,
    host: &Connection,
    path: &tinybus::ObjectPath,
) -> tinybus::Result<Value> {
    connection
        .proxy(
            host.unique_name().unwrap().as_str(),
            path.as_str(),
            "org.example.RegistrationMarker",
        )
        .unwrap()
        .call("Probe", ())
        .await
}

#[async_trait::async_trait]
impl StorageEndpoint for SignalEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: tinystoragedrivers_api::BlobReader,
    ) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_api::BlobReader)>, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    fn watch_signals(
        &self,
        _sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        if self.has_signals {
            self.signals
                .lock()
                .ok()?
                .as_ref()
                .map(tokio::sync::broadcast::Sender::subscribe)
        } else {
            None
        }
    }

    fn watch_epoch(&self, _sender: &BusName) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
}

fn signal_endpoint(
    has_signals: bool,
) -> (SignalEndpoint, tokio::sync::broadcast::Sender<WatchChanged>) {
    let (signals, _) = tokio::sync::broadcast::channel(8);
    (
        SignalEndpoint {
            signals: Mutex::new(Some(signals.clone())),
            has_signals,
            epoch: AtomicU64::new(0),
        },
        signals,
    )
}

async fn service(object: Object) -> (ObjectService, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryBackend::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::new("local").unwrap()).unwrap(),
        ))
    });
    let shared = Arc::new(Shared {
        transport: HostTransport::Connection(connection.downgrade()),
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    });
    (ObjectService { object, shared }, connection)
}

fn member(name: &str) -> MemberName {
    MemberName::new(name).unwrap()
}

#[test]
fn request_arguments_are_zero_or_one() {
    assert_eq!(request_of(Value::Null).unwrap(), Value::Null);
    assert_eq!(request_of(json!([])).unwrap(), Value::Null);
    assert_eq!(request_of(json!([{"a": 1}])).unwrap(), json!({"a": 1}));
    assert_eq!(request_of(json!([1, 2])).unwrap_err(), invalid());
    assert_eq!(request_of(json!({"a": 1})).unwrap_err(), invalid());
    assert_eq!(
        HostFailure::from(StorageFailure {
            kind: StorageKind::Fenced
        }),
        HostFailure::Storage(StorageFailure {
            kind: StorageKind::Fenced
        })
    );
}

#[test]
fn host_failures_keep_only_the_denial_label_or_storage_kind_on_the_bus() {
    let denied: tinybus::Error = HostFailure::Denied("scope-not-authorized").into();
    assert!(matches!(
        denied,
        tinybus::Error::MethodFailed { name, message }
            if name == ERROR_DENIED && message == "scope-not-authorized"
    ));

    let storage: tinybus::Error = HostFailure::Storage(StorageFailure {
        kind: StorageKind::Unavailable,
    })
    .into();
    assert!(matches!(
        storage,
        tinybus::Error::MethodFailed { name, message }
            if name == ERROR_STORAGE && message == "unavailable"
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn each_object_advertises_its_selectors_and_only_secrets_are_confidential() {
    for (object, count) in [
        (
            Object::Documents,
            tinystoragedrivers_bus::DOCUMENTS_SELECTORS.len(),
        ),
        (
            Object::Streams,
            tinystoragedrivers_bus::STREAMS_SELECTORS.len(),
        ),
        (Object::Blobs, tinystoragedrivers_bus::BLOBS_SELECTORS.len()),
        (
            Object::Secrets,
            tinystoragedrivers_bus::SECRETS_SELECTORS.len(),
        ),
    ] {
        let (service, _connection) = service(object).await;
        assert_eq!(service.name().as_str(), STORAGE_INTERFACE);
        assert_eq!(service.members().len(), count);
        assert_eq!(
            service.requires_confidential(&member("Get")),
            object == Object::Secrets
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn serve_claims_the_name_and_shutdown_withdraws_every_object() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let endpoint = PerCallerEndpoint::new(|_| None);
    let served = serve(&host, Arc::new(endpoint)).await.unwrap();
    assert!(format!("{served:?}").contains("ServedStorage"));
    assert!(host.name_owner(HOST_NAME).await.unwrap().is_some());
    served.shutdown().await.unwrap();
    assert!(host.name_owner(HOST_NAME).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_taken_host_name_rolls_back_the_second_registration() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let first_host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let second_host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let first = serve(&first_host, Arc::new(PerCallerEndpoint::new(|_| None)))
        .await
        .unwrap();
    let second = serve(&second_host, Arc::new(PerCallerEndpoint::new(|_| None))).await;
    assert!(second.is_err());
    assert_eq!(
        first_host.name_owner(HOST_NAME).await.unwrap(),
        Some(first_host.unique_name().unwrap())
    );
    first.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_object_registration_does_not_claim_the_host_name() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.close().await.unwrap();

    let result = serve(&host, Arc::new(PerCallerEndpoint::new(|_| None))).await;

    assert!(result.is_err());
    assert_eq!(observer.name_owner(HOST_NAME).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_address_registration_refuses_without_replacing_the_live_endpoint() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixed = serve(&host, Arc::new(ReplyEndpoint(json!("fixed-service"))))
        .await
        .unwrap();
    let address = addressed(&host, "duplicate_address");
    let first = serve_at_address(
        &host,
        address.clone(),
        Arc::new(ReplyEndpoint(json!("original-endpoint"))),
    )
    .await
    .unwrap();

    let duplicate = serve_at_address(
        &host,
        address.clone(),
        Arc::new(ReplyEndpoint(json!("replacement-endpoint"))),
    )
    .await;

    assert!(duplicate.is_err(), "an occupied address must be refused");
    assert_eq!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .unwrap(),
        json!("original-endpoint"),
        "refusing a duplicate must leave the original endpoint serving"
    );
    first.shutdown().await.unwrap();
    assert!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .is_err()
    );
    assert_eq!(
        call_fixed(&caller).await.unwrap(),
        json!("fixed-service"),
        "address cleanup must not withdraw the fixed service"
    );
    fixed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn address_registration_refuses_an_occupied_different_interface_untouched() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixed = serve(&host, Arc::new(ReplyEndpoint(json!("fixed-service"))))
        .await
        .unwrap();
    let address = addressed(&host, "different_interface");
    let documents_path = address.port_path(Object::Documents).clone();
    host.serve_at(
        documents_path.clone(),
        MarkerInterface("unrelated-interface"),
    )
    .await
    .unwrap();

    let result = serve_at_address(
        &host,
        address.clone(),
        Arc::new(ReplyEndpoint(json!("storage-endpoint"))),
    )
    .await;

    assert!(
        result.is_err(),
        "a different interface already owns the path"
    );
    assert_eq!(
        call_marker_interface(&caller, &host, &documents_path)
            .await
            .unwrap(),
        json!("unrelated-interface"),
        "refusal must preserve the existing interface at the path"
    );
    assert!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .is_err()
    );
    assert_eq!(call_fixed(&caller).await.unwrap(), json!("fixed-service"));
    fixed.shutdown().await.unwrap();
    host.unserve(&documents_path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn partial_address_registration_rolls_back_only_new_paths_and_preserves_siblings() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixed = serve(&host, Arc::new(ReplyEndpoint(json!("fixed-service"))))
        .await
        .unwrap();
    let address = addressed(&host, "partial_registration");
    let documents_path = address.port_path(Object::Documents).clone();
    let streams_path = address.port_path(Object::Streams).clone();
    let blobs_path = address.port_path(Object::Blobs).clone();
    host.serve_at(
        blobs_path.clone(),
        MarkerInterface("late-unrelated-interface"),
    )
    .await
    .unwrap();

    let result = serve_at_address(
        &host,
        address.clone(),
        Arc::new(ReplyEndpoint(json!("partial-storage-endpoint"))),
    )
    .await;

    assert!(
        result.is_err(),
        "registration must stop at the occupied blob path"
    );
    assert!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .is_err()
    );
    assert!(
        call_addressed(&caller, &address, Object::Streams)
            .await
            .is_err()
    );
    assert_eq!(
        call_marker_interface(&caller, &host, &blobs_path)
            .await
            .unwrap(),
        json!("late-unrelated-interface"),
        "rollback must not unregister the preexisting blob-path interface"
    );
    assert_eq!(call_fixed(&caller).await.unwrap(), json!("fixed-service"));
    fixed.shutdown().await.unwrap();
    host.unserve(&blobs_path).await;
    host.unserve(&documents_path).await;
    host.unserve(&streams_path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partially_registered_address_does_not_dispatch_before_all_paths_are_claimed() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixed = serve(&host, Arc::new(ReplyEndpoint(json!("fixed-service"))))
        .await
        .unwrap();
    let address = addressed(&host, "registration_window");
    let blobs_path = address.port_path(Object::Blobs).clone();
    host.serve_at(
        blobs_path.clone(),
        MarkerInterface("late-unrelated-interface"),
    )
    .await
    .unwrap();

    let calls = Arc::new(AtomicU64::new(0));
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
    let mut registered_tx = Some(registered_tx);
    let release = Arc::new(tokio::sync::Notify::new());
    let release_registration = Arc::clone(&release);
    let registration_host = host.clone();
    let registration_address = address.clone();
    let endpoint = Arc::new(CountingEndpoint(Arc::clone(&calls)));
    let result = tokio::spawn(async move {
        serve_at_address_with_registration_hook(
            &registration_host,
            registration_address,
            endpoint,
            move |index| {
                let release = Arc::clone(&release_registration);
                let registered = (index == 0).then(|| registered_tx.take()).flatten();
                async move {
                    if let Some(registered) = registered {
                        registered
                            .send(())
                            .expect("registration observer should still be listening");
                        release.notified().await;
                    }
                }
            },
        )
        .await
    });

    registered_rx.await.unwrap();
    assert!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .is_err(),
        "the first claimed path stays inactive until later paths are claimed"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    release.notify_one();
    assert!(result.await.unwrap().is_err());
    assert!(
        call_addressed(&caller, &address, Object::Documents)
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(call_fixed(&caller).await.unwrap(), json!("fixed-service"));

    fixed.shutdown().await.unwrap();
    host.unserve(&blobs_path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn addressed_shutdown_preserves_sibling_invocations_and_fixed_service() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let fixed = serve(&host, Arc::new(ReplyEndpoint(json!("fixed-service"))))
        .await
        .unwrap();
    let removed_address = addressed(&host, "removed_invocation");
    let sibling_address = addressed(&host, "sibling_invocation");
    let removed = serve_at_address(
        &host,
        removed_address.clone(),
        Arc::new(ReplyEndpoint(json!("removed-endpoint"))),
    )
    .await
    .unwrap();
    let sibling = serve_at_address(
        &host,
        sibling_address.clone(),
        Arc::new(ReplyEndpoint(json!("sibling-endpoint"))),
    )
    .await
    .unwrap();

    removed.shutdown().await.unwrap();

    assert!(
        call_addressed(&caller, &removed_address, Object::Documents)
            .await
            .is_err()
    );
    assert_eq!(
        call_addressed(&caller, &sibling_address, Object::Documents)
            .await
            .unwrap(),
        json!("sibling-endpoint"),
        "cleaning one invocation must preserve its sibling's endpoint"
    );
    assert_eq!(call_fixed(&caller).await.unwrap(), json!("fixed-service"));
    sibling.shutdown().await.unwrap();
    fixed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_an_addressed_guard_preserves_sibling_invocations() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let removed_address = addressed(&host, "dropped_invocation");
    let sibling_address = addressed(&host, "surviving_invocation");
    let removed = serve_at_address(
        &host,
        removed_address.clone(),
        Arc::new(ReplyEndpoint(json!("removed-endpoint"))),
    )
    .await
    .unwrap();
    let sibling = serve_at_address(
        &host,
        sibling_address.clone(),
        Arc::new(ReplyEndpoint(json!("sibling-endpoint"))),
    )
    .await
    .unwrap();

    drop(removed);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if call_addressed(&caller, &removed_address, Object::Documents)
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping a guard asynchronously unregisters its invocation");
    assert_eq!(
        call_addressed(&caller, &sibling_address, Object::Documents)
            .await
            .unwrap(),
        json!("sibling-endpoint")
    );
    sibling.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_without_a_proven_sender_is_refused_on_every_path() {
    let (documents, _c1) = service(Object::Documents).await;
    let error = documents
        .call(&member("Capabilities"), Value::Null)
        .await
        .unwrap_err();
    assert!(matches!(&error, tinybus::Error::MethodFailed { name, .. } if name == ERROR_DENIED));
    let context = CallContext::default();
    let error = documents
        .call_with_context(&member("Capabilities"), Value::Null, &context)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );

    let (blobs, _c2) = service(Object::Blobs).await;
    // GetStream has nowhere safe to send a body without a sender.
    let error = blobs
        .call_with_context(&member("GetStream"), json!([{"key": "k"}]), &context)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );
    // Malformed streamed-call arguments are invalid input, not a panic.
    for args in [
        Value::Null,
        json!([{"key": "k", "len": 1}]),
        json!([{"key": "k", "len": 1}, {"id": "s9"}, 1]),
        json!([{"bad": 1}, {"id": "s9"}]),
        json!([{"key": "k", "len": 1}, {"bad": 1}]),
        json!([{"key": "k", "len": 1}, {"id": "s9", "len": 2}]),
    ] {
        let error = blobs
            .call_with_context(&member("PutStream"), args, &context)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, tinybus::Error::MethodFailed { name, message }
                if name == ERROR_STORAGE && message == "invalid_input"),
            "{error:?}"
        );
    }
    // A well-formed upload without a proven sender is refused before any
    // stream is accepted: the stream must be bound to its opener.
    let error = blobs
        .call_with_context(
            &member("PutStream"),
            json!([{"key": "k", "len": 1}, {"id": "no-such-stream", "len": 1}]),
            &context,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );
    let error = blobs
        .call_with_context(&member("GetStream"), json!({"not": "an array"}), &context)
        .await
        .unwrap_err();
    assert!(matches!(&error, tinybus::Error::MethodFailed { name, .. } if name == ERROR_STORAGE));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_connection_makes_the_host_unavailable_not_a_panic() {
    let (blobs, connection) = service(Object::Blobs).await;
    drop(connection);
    let error = blobs.connection().err();
    assert_eq!(
        error,
        Some(HostFailure::Storage(StorageFailure {
            kind: StorageKind::Unavailable
        }))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_and_rebound_caller_gets_a_fresh_signal_forwarder() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryBackend::new();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::new("local").unwrap()).unwrap(),
        ))
    }));
    let shared = Shared {
        transport: HostTransport::Connection(connection.downgrade()),
        endpoint: Arc::clone(&endpoint) as Arc<dyn StorageEndpoint>,
        forwarders: Mutex::new(HashMap::new()),
    };
    let caller = BusName::new(":1.7").unwrap();
    let epoch_of = || {
        shared
            .forwarders
            .lock()
            .unwrap()
            .get(&caller)
            .map(|(e, _)| *e)
    };

    shared.forward_watch_signals(&caller);
    let first = epoch_of().expect("a forwarder is started");
    shared.forward_watch_signals(&caller);
    assert_eq!(
        epoch_of(),
        Some(first),
        "the same binding keeps its forwarder"
    );

    assert!(endpoint.release(&caller));
    shared.forward_watch_signals(&caller);
    let second = epoch_of().expect("the rebound caller is forwarded");
    assert_ne!(first, second, "the stale forwarder is replaced on rebind");
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_old_watch_forwarder_is_aborted_when_its_epoch_changes() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller_name = caller.unique_name().unwrap();
    let mut received = caller
        .add_match(tinybus::MatchRule {
            kind: Some(tinybus::MessageKind::Signal),
            sender: None,
            interface: Some(interface()),
            member: Some(member(WATCH_MEMBER)),
            path: Some(watch_path(&caller_name).unwrap()),
            path_namespace: None,
        })
        .await
        .unwrap();
    let (endpoint, sender) = signal_endpoint(true);
    let endpoint = Arc::new(endpoint);
    let shared = Shared {
        transport: HostTransport::Connection(host.downgrade()),
        endpoint: Arc::clone(&endpoint) as Arc<dyn StorageEndpoint>,
        forwarders: Mutex::new(HashMap::new()),
    };

    shared.forward_watch_signals(&caller_name);
    let old_task = shared
        .forwarders
        .lock()
        .unwrap()
        .get(&caller_name)
        .expect("first watch starts a forwarder")
        .1
        .abort_handle();
    assert!(!old_task.is_finished(), "the old forwarder is still live");

    endpoint.epoch.fetch_add(1, Ordering::SeqCst);
    shared.forward_watch_signals(&caller_name);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !old_task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacing the epoch aborts the previous forwarder");
    assert_eq!(
        shared
            .forwarders
            .lock()
            .unwrap()
            .get(&caller_name)
            .unwrap()
            .0,
        1
    );

    sender
        .send(WatchChanged {
            watch_id: "new-epoch".into(),
            notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
        })
        .unwrap();
    let message = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let message = received.recv().await.unwrap();
            if message.body[0]["watch_id"] == "new-epoch" {
                break message;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(message.body[0]["watch_id"], "new-epoch");
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_stream_ref_opened_by_another_peer_is_rejected() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attacker = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryBackend::new();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend
                .for_scope(&Scope::new("stream-owner").unwrap())
                .unwrap(),
        ))
    }));
    let shared = Arc::new(Shared {
        transport: HostTransport::Connection(host.downgrade()),
        endpoint,
        forwarders: Mutex::new(HashMap::new()),
    });
    let path = object_path(Object::Blobs);
    host.serve_at(
        path.clone(),
        ObjectService {
            object: Object::Blobs,
            shared,
        },
    )
    .await
    .unwrap();

    let destination = host.unique_name().unwrap();
    let forged = attacker
        .open_stream(&destination, StreamDescriptor::with_len(1))
        .await
        .unwrap();
    let proxy = caller
        .proxy(destination.as_str(), path.as_str(), interface().as_str())
        .unwrap();
    let error = proxy
        .call::<Value>(
            "PutStream",
            (
                PutBlobStreamRequest {
                    key: "forged-body".into(),
                    len: 1,
                    content_type: None,
                    ttl_ms: None,
                },
                forged.stream_ref(),
            ),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        tinybus::Error::MethodFailed { name, message }
            if name == ERROR_STORAGE && message == "invalid_input"
    ));
    drop(forged);
}

#[tokio::test]
async fn watch_signals_are_forwarded_only_to_the_bound_caller() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller_name = caller.unique_name().unwrap();
    let path = watch_path(&caller_name).unwrap();
    let rule = tinybus::MatchRule {
        kind: Some(tinybus::MessageKind::Signal),
        sender: None,
        interface: Some(interface()),
        member: Some(member(WATCH_MEMBER)),
        path: Some(path.clone()),
        path_namespace: None,
    };
    let mut caller_signals = caller.add_match(rule.clone()).await.unwrap();
    let mut observer_signals = observer.add_match(rule).await.unwrap();
    let (endpoint, sender) = signal_endpoint(true);
    assert_eq!(endpoint.watch_epoch(&caller_name), 0);
    let shared = Shared {
        transport: HostTransport::Connection(host.downgrade()),
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    };

    shared.forward_watch_signals(&caller_name);
    for index in 0..16 {
        sender
            .send(WatchChanged {
                watch_id: format!("watch-{index}"),
                notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
            })
            .unwrap();
    }

    let message = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let message = caller_signals.recv().await.unwrap();
            if message.body[0]["watch_id"] == "watch-15" {
                break message;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(message.header.path, Some(path));
    assert_eq!(
        message.header.member.as_ref().map(MemberName::as_str),
        Some(WATCH_MEMBER)
    );
    assert_eq!(message.body[0]["watch_id"], "watch-15");
    observer.name_owner(HOST_NAME).await.unwrap();
    assert!(observer_signals.try_recv().is_err());
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn addressed_watch_signals_use_the_invocation_watch_path() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller_name = caller.unique_name().unwrap();
    let address = addressed(&host, "watch_invocation");
    let mut received = caller
        .add_match(tinybus::MatchRule {
            kind: Some(tinybus::MessageKind::Signal),
            sender: None,
            interface: Some(interface()),
            member: Some(member(WATCH_MEMBER)),
            path: Some(address.watch_path().clone()),
            path_namespace: None,
        })
        .await
        .unwrap();
    let (endpoint, sender) = signal_endpoint(true);
    let shared = Shared {
        transport: HostTransport::Addressed {
            connection: host.downgrade(),
            watch_path: address.watch_path().clone(),
            active: Arc::new(AtomicBool::new(true)),
        },
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    };

    shared.forward_watch_signals(&caller_name);
    sender
        .send(WatchChanged {
            watch_id: "addressed-watch".into(),
            notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
        })
        .unwrap();

    let message = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let message = received.recv().await.unwrap();
            if message.body[0]["watch_id"] == "addressed-watch" {
                break message;
            }
        }
    })
    .await
    .expect("addressed watch signals reach their invocation path");
    assert_eq!(message.header.path, Some(address.watch_path().clone()));
    assert_eq!(message.body[0]["watch_id"], "addressed-watch");
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_or_closed_watch_source_does_not_leave_a_live_forwarder() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = BusName::new(":1.7").unwrap();
    let (missing, _) = signal_endpoint(false);
    let shared = Shared {
        transport: HostTransport::Connection(connection.downgrade()),
        endpoint: Arc::new(missing),
        forwarders: Mutex::new(HashMap::new()),
    };
    shared.forward_watch_signals(&caller);
    assert!(shared.forwarders.lock().unwrap().is_empty());

    let (closed, sender) = signal_endpoint(true);
    let closed = Arc::new(closed);
    let shared = Shared {
        transport: HostTransport::Connection(connection.downgrade()),
        endpoint: Arc::clone(&closed) as Arc<dyn StorageEndpoint>,
        forwarders: Mutex::new(HashMap::new()),
    };
    shared.forward_watch_signals(&caller);
    drop(sender);
    closed.signals.lock().unwrap().take();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            shared.forward_watch_signals(&caller);
            if shared.forwarders.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a closed watch source is removed from the forwarding table");
}

/// Executed by the full coverage gate after scripts/test-storage-callback.sh
/// builds the real module artifact. Keeping it ignored means normal unit tests
/// do not depend on an external compiled module or MongoDB service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run by the native full-workspace coverage gate with the compiled callback module"]
async fn admitted_module_exercises_callback_host_streams_and_secrets() {
    let artifact = std::path::PathBuf::from(
        std::env::var_os("TSD_STORAGE_CALLBACK_MODULE")
            .expect("coverage gate must provide TSD_STORAGE_CALLBACK_MODULE"),
    );
    let hash = tinybus::module::sha256_file(&artifact).unwrap();
    let artifact_dir = artifact.parent().unwrap().join(format!(
        "storage-callback-native-coverage-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&artifact_dir).unwrap();
    let staged = artifact_dir.join(artifact.file_name().unwrap());
    std::fs::copy(&artifact, &staged).unwrap();
    std::fs::write(
        artifact_dir.join("modules.toml"),
        format!(
            "{:?} = {hash:?}\n",
            staged.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let binding = prepare_storage_host_callbacks(&host).await.unwrap();
    let weak_binding = Arc::downgrade(&binding);
    let backend = Arc::new(MemoryBackend::new());
    let scope = Scope::new("callback-coverage").unwrap();
    let storage = backend.for_scope(&scope).unwrap();
    storage
        .blobs()
        .put("host-download", b"host callback body".to_vec(), None)
        .await
        .unwrap();
    let endpoint_storage = storage.clone();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(endpoint_storage.clone()))
    }));
    let served = serve_with_host_callbacks(Arc::clone(&binding), endpoint)
        .await
        .unwrap();
    host.load_file_with_host_callbacks(staged, json!({"host": binding.unique_name()}), &binding)
        .await
        .unwrap();

    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attacker = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let attacker_proxy = attacker
        .proxy(
            binding.unique_name().as_str(),
            object_path(Object::Documents).as_str(),
            STORAGE_INTERFACE,
        )
        .unwrap();
    assert!(
        attacker_proxy
            .call::<Value>("Capabilities", ())
            .await
            .is_err()
    );

    let module = client
        .proxy(
            "org.example.StorageCallbackProof",
            "/storage_proof",
            "org.example.StorageCallbackProof",
        )
        .unwrap();
    let result: Value = module.call("Exercise", ()).await.unwrap();
    assert_eq!(result["secret_updated"], true);
    assert_eq!(result["upload"], "ordinary module stream");
    assert_eq!(result["download"], "host callback body");

    served.shutdown().await.unwrap();
    drop(binding);
    assert!(weak_binding.upgrade().is_none());
    drop(host);
    drop(broker_task);
    std::fs::remove_dir_all(artifact_dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn callback_watch_forwarder_uses_the_bound_signal_route() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus);
    let host = ModuleHost::new(broker);
    let binding = prepare_storage_host_callbacks(&host).await.unwrap();
    let weak_binding = Arc::downgrade(&binding);
    let (endpoint, sender) = signal_endpoint(true);
    let shared = Shared {
        transport: HostTransport::Callback(Arc::downgrade(&binding)),
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    };
    let recipient = binding.unique_name().clone();
    shared.forward_watch_signals(&recipient);
    sender
        .send(WatchChanged {
            watch_id: "callback-watch".into(),
            notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
        })
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if shared
                .forwarders
                .lock()
                .unwrap()
                .get(&recipient)
                .is_some_and(|(_, task)| task.is_finished())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an inactive binding refuses the attempted callback signal");
    drop(shared);
    drop(binding);
    assert!(weak_binding.upgrade().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn callback_serve_failure_revokes_the_binding_and_shutdown_closes_it() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.spawn(bus);
    let host = ModuleHost::new(broker);
    let binding = prepare_storage_host_callbacks(&host).await.unwrap();
    let weak_binding = Arc::downgrade(&binding);
    let first = serve_with_host_callbacks(
        Arc::clone(&binding),
        Arc::new(PerCallerEndpoint::new(|_| None)),
    )
    .await
    .unwrap();

    let duplicate = serve_with_host_callbacks(
        Arc::clone(&binding),
        Arc::new(PerCallerEndpoint::new(|_| None)),
    )
    .await;
    assert!(duplicate.is_err());
    assert!(first.shutdown().await.is_ok());
    drop(binding);
    assert!(weak_binding.upgrade().is_none());
}
