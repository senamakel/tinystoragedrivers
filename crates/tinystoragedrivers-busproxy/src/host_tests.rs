use super::*;
use crate::PerCallerEndpoint;
use std::time::Duration;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

async fn service(object: Object) -> (ObjectService, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    });
    let shared = Arc::new(Shared {
        connection: connection.downgrade(),
        endpoint: Arc::new(endpoint),
        watch_root: OBJECT_ROOT.to_owned(),
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
async fn partial_lease_registration_rolls_back_only_its_paths() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let root = "/ai/tinyhumans/tinystorage/Leases/73";
    let documents_path = tinybus::ObjectPath::new(format!("{root}/Documents")).unwrap();
    let service_peer = connection.unique_name().unwrap();
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    });
    let proxy =
        tinybus::Proxy::new(client, service_peer, documents_path.clone(), interface()).unwrap();
    let failed = serve_lease_transaction(
        &connection,
        root,
        Arc::new(endpoint),
        tokio::runtime::Handle::current(),
        |object| {
            let proxy = proxy.clone();
            async move {
                if object == Object::Documents {
                    proxy.call::<Value>("Capabilities", (Value::Null,)).await?;
                    Err(tinybus::Error::failed("injected registration failure"))
                } else {
                    Ok(())
                }
            }
        },
    )
    .await;
    assert!(failed.is_err());

    // The injected failure happens only after a real call succeeded on the
    // first path. Once rollback finishes, that old path no longer dispatches.
    let mut withdrawn = false;
    for _ in 0..100 {
        if proxy
            .call::<Value>("Capabilities", (Value::Null,))
            .await
            .is_err()
        {
            withdrawn = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(withdrawn, "partial lease path was not rolled back");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_partial_lease_registration_uses_the_retained_cleanup_runtime() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let root = "/ai/tinyhumans/tinystorage/Leases/74";
    let documents_path = tinybus::ObjectPath::new(format!("{root}/Documents")).unwrap();
    let service_peer = connection.unique_name().unwrap();
    let proxy = tinybus::Proxy::new(client, service_peer, documents_path, interface()).unwrap();
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    });
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let mut ready_tx = Some(ready_tx);
    let mut continue_rx = Some(continue_rx);
    let hook = move |object| {
        let pause = if object == Object::Documents {
            Some((ready_tx.take().unwrap(), continue_rx.take().unwrap()))
        } else {
            None
        };
        async move {
            if let Some((ready, resume)) = pause {
                let _ = ready.send(());
                resume
                    .await
                    .map_err(|_| tinybus::Error::failed("test transaction cancelled"))?;
            }
            Ok(())
        }
    };
    let cleanup_runtime = tokio::runtime::Handle::current();
    let setup_connection = connection.clone();
    let task = tokio::spawn(async move {
        serve_lease_transaction(
            &setup_connection,
            root,
            Arc::new(endpoint),
            cleanup_runtime,
            hook,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("first path registration timed out")
        .unwrap();
    assert!(
        proxy
            .call::<Value>("Capabilities", (Value::Null,))
            .await
            .is_ok()
    );

    task.abort();
    let _ = task.await;
    let mut withdrawn = false;
    for _ in 0..100 {
        if proxy
            .call::<Value>("Capabilities", (Value::Null,))
            .await
            .is_err()
        {
            withdrawn = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(withdrawn, "cancelled lease path was not withdrawn");
    drop(continue_tx);
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
    let backend = MemoryStorage::new();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    }));
    let shared = Shared {
        connection: connection.downgrade(),
        endpoint: Arc::clone(&endpoint) as Arc<dyn StorageEndpoint>,
        watch_root: OBJECT_ROOT.to_owned(),
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

    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Capabilities",
            request: Value::Null,
            sender: Some(&caller),
            admission: tinystoragedrivers_client::Admission::Private,
        })
        .await
        .unwrap();
    shared.forward_watch_signals(&caller);
    let first = epoch_of().expect("a forwarder is started");
    shared.forward_watch_signals(&caller);
    assert_eq!(
        epoch_of(),
        Some(first),
        "the same binding keeps its forwarder"
    );

    assert!(endpoint.release(&caller));
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Capabilities",
            request: Value::Null,
            sender: Some(&caller),
            admission: tinystoragedrivers_client::Admission::Private,
        })
        .await
        .unwrap();
    shared.forward_watch_signals(&caller);
    let second = epoch_of().expect("the rebound caller is forwarded");
    assert_ne!(first, second, "the stale forwarder is replaced on rebind");
}
