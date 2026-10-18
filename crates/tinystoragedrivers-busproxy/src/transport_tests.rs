use super::*;
use crate::host::serve_lease;
use crate::{HostFailure, StorageCall, StorageEndpoint, serve};
use async_trait::async_trait;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{BlobInfo, BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest};
use tinystoragedrivers_core::BlobReader;
use tokio::io::AsyncReadExt;
use tokio::sync::broadcast;

struct MarkerEndpoint(&'static str);

#[async_trait]
impl StorageEndpoint for MarkerEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Ok(json!({"service": self.0}))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: BlobReader,
    ) -> Result<Value, HostFailure> {
        Ok(json!({"service": self.0}))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        Ok(Some((
            BlobStreamHead {
                meta: BlobInfo {
                    key: self.0.to_owned(),
                    len: 0,
                    content_type: None,
                },
            },
            Box::pin(std::io::Cursor::new(Vec::new())),
        )))
    }

    fn watch_signals(&self, _sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        None
    }
}

#[test]
fn stream_replies_are_decoded_strictly() {
    assert!(decode_stream_reply(Value::Null).unwrap().is_none());
    let head = json!({"meta": {"key": "k", "len": 3}});
    let stream = json!({"id": "s1", "len": 3});
    let (decoded, reference) = decode_stream_reply(json!([head, stream])).unwrap().unwrap();
    assert_eq!(decoded.meta.len, 3);
    assert_eq!(reference.id, "s1");
    for malformed in [
        json!("nope"),
        json!([]),
        json!([head]),
        json!([head, stream, 1]),
        json!([{"bad": 1}, stream]),
        json!([head, {"bad": 1}]),
    ] {
        let error = decode_stream_reply(malformed).unwrap_err();
        assert_eq!(
            error,
            WireError::Transport("malformed GetStream reply".into())
        );
    }
}

#[test]
fn remote_failures_keep_their_category_and_other_errors_become_transport() {
    let storage = tinybus::Error::MethodFailed {
        name: ERROR_STORAGE.into(),
        message: "conflict".into(),
    };
    assert_eq!(
        wire_error(storage),
        WireError::Storage(StorageFailure {
            kind: StorageKind::Conflict
        })
    );
    let unknown_kind = tinybus::Error::MethodFailed {
        name: ERROR_STORAGE.into(),
        message: "from-the-future".into(),
    };
    assert_eq!(
        wire_error(unknown_kind),
        WireError::Storage(StorageFailure {
            kind: StorageKind::Backend
        })
    );
    let denied = tinybus::Error::MethodFailed {
        name: ERROR_DENIED.into(),
        message: "no-grant-declared".into(),
    };
    assert_eq!(
        wire_error(denied),
        WireError::Transport("the storage host denied the call: no-grant-declared".into())
    );
    assert!(matches!(
        wire_error(tinybus::Error::ConnectionClosed),
        WireError::Transport(_)
    ));
}

#[test]
fn watch_signals_must_be_a_single_well_formed_notice() {
    assert!(watch_path_at(OBJECT_ROOT, &BusName::new(HOST_NAME).unwrap()).is_err());
    let expected_path = tinybus::ObjectPath::new("/a").unwrap();
    let expected_sender = BusName::new(":1.1").unwrap();
    let signal = |body: Value| {
        let mut message = Message::signal(
            expected_path.clone(),
            interface(),
            member(WATCH_MEMBER),
            body,
        );
        message.header.sender = Some(expected_sender.clone());
        message
    };
    let notice = json!({"watch_id": "w1", "notice": {"kind": "invalidated"}});
    let decoded = decode_signal(
        &signal(json!([notice.clone()])),
        &expected_path,
        &expected_sender,
    )
    .unwrap();
    assert_eq!(decoded.watch_id, "w1");
    assert!(decode_signal(&signal(notice.clone()), &expected_path, &expected_sender).is_none());
    assert!(
        decode_signal(
            &signal(json!([notice.clone(), notice])),
            &expected_path,
            &expected_sender
        )
        .is_none()
    );
    assert!(
        decode_signal(
            &signal(json!([{"bad": 1}])),
            &expected_path,
            &expected_sender
        )
        .is_none()
    );
    let other_path = tinybus::ObjectPath::new("/b").unwrap();
    assert!(
        decode_signal(
            &signal(json!([
                json!({"watch_id":"w1","notice":{"kind":"invalidated"}})
            ])),
            &other_path,
            &expected_sender
        )
        .is_none()
    );
    let other_sender = BusName::new(":1.2").unwrap();
    assert!(
        decode_signal(
            &signal(json!([
                json!({"watch_id":"w1","notice":{"kind":"invalidated"}})
            ])),
            &expected_path,
            &other_sender
        )
        .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bound_transport_rejects_alias_malformed_and_stale_service_routes() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let live_peer = service.unique_name().unwrap().as_str().to_owned();
    let root = format!("{OBJECT_ROOT}/Leases/11");

    let alias = StorageDescriptor {
        service_peer: HOST_NAME.to_owned(),
        registration_id: 11,
        object_root: root.clone(),
    };
    assert!(
        BusTransport::connect_bound(caller.clone(), alias)
            .await
            .is_err(),
        "well-known aliases cannot be used as bound service identities"
    );

    let malformed_root = StorageDescriptor {
        service_peer: live_peer.clone(),
        registration_id: 11,
        object_root: format!("{OBJECT_ROOT}/Leases/not-a-number"),
    };
    assert!(
        BusTransport::connect_bound(caller.clone(), malformed_root)
            .await
            .is_err(),
        "a descriptor root must be one exact numeric lease path"
    );

    let mismatched_root = StorageDescriptor {
        service_peer: live_peer,
        registration_id: 11,
        object_root: format!("{OBJECT_ROOT}/Leases/12"),
    };
    assert!(
        BusTransport::connect_bound(caller.clone(), mismatched_root)
            .await
            .is_err(),
        "the registration id must agree with the serialized root"
    );

    let disconnected = StorageDescriptor {
        service_peer: ":123.456".to_owned(),
        registration_id: 11,
        object_root: root,
    };
    assert!(
        BusTransport::connect_bound(caller, disconnected)
            .await
            .is_err(),
        "a unique but disconnected service peer cannot be resolved again by alias"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn transports_on_one_connection_filter_watch_signals_by_lease_path() {
    let bus = MemoryBus::new();
    let _broker_task = Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let other_service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let peer = client.unique_name().unwrap();
    let service_peer = service.unique_name().unwrap().as_str().to_owned();
    let root_a = format!("{OBJECT_ROOT}/Leases/11");
    let root_b = format!("{OBJECT_ROOT}/Leases/12");
    let route = |registration_id, root: String| StorageDescriptor {
        service_peer: service_peer.clone(),
        registration_id,
        object_root: root,
    };
    let transport_a = BusTransport::connect_bound(client.clone(), route(11, root_a.clone()))
        .await
        .unwrap();
    let transport_b = BusTransport::connect_bound(client.clone(), route(12, root_b.clone()))
        .await
        .unwrap();
    let forged_same_path = BusTransport::connect_bound(
        client.clone(),
        StorageDescriptor {
            service_peer: other_service.unique_name().unwrap().as_str().to_owned(),
            registration_id: 11,
            object_root: root_a.clone(),
        },
    )
    .await
    .unwrap();
    let mut incoming_a = transport_a.signals();
    let mut incoming_b = transport_b.signals();
    let changed = |watch_id: &str| WatchChanged {
        watch_id: watch_id.to_owned(),
        notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
    };
    other_service
        .emit_to(
            peer.clone(),
            watch_path_at(&root_a, &peer).unwrap(),
            interface(),
            member(WATCH_MEMBER),
            (changed("other-service-same-path"),),
        )
        .await
        .unwrap();
    service
        .emit_to(
            peer.clone(),
            watch_path_at(&root_a, &peer).unwrap(),
            interface(),
            member(WATCH_MEMBER),
            (changed("lease-a-watch"),),
        )
        .await
        .unwrap();
    service
        .emit_to(
            peer,
            watch_path_at(&root_b, &client.unique_name().unwrap()).unwrap(),
            interface(),
            member(WATCH_MEMBER),
            (changed("lease-b-watch"),),
        )
        .await
        .unwrap();

    let watch_update_a = tokio::time::timeout(Duration::from_secs(2), incoming_a.recv())
        .await
        .unwrap()
        .unwrap();
    let watch_update_b = tokio::time::timeout(Duration::from_secs(2), incoming_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watch_update_a.watch_id, "lease-a-watch");
    assert_eq!(watch_update_b.watch_id, "lease-b-watch");
    drop(forged_same_path);
}

/// A host-less broker with one module connection attached to a transport.
#[tokio::test(flavor = "multi_thread")]
async fn a_transport_without_a_host_cannot_be_built_and_debug_hides_the_wire() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let lonely = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let error = BusTransport::connect(lonely).await.unwrap_err();
    assert!(matches!(error, tinybus::Error::NameHasNoOwner(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn bound_calls_stay_on_the_issuing_peer_after_public_alias_handover() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service_a = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_b = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let peer_a = service_a.unique_name().unwrap();
    service_a.request_name(HOST_NAME).await.unwrap();
    let served_a = serve_lease(
        &service_a,
        "/ai/tinyhumans/tinystorage/Leases/19",
        Arc::new(MarkerEndpoint("A")),
    )
    .await
    .unwrap();

    // The public alias is handed to B while A's unique peer and lease objects
    // remain alive. Bound routing must keep addressing A directly.
    service_a.release_name(HOST_NAME).await.unwrap();
    let _served_b = serve(&service_b, Arc::new(MarkerEndpoint("B")))
        .await
        .unwrap();
    let descriptor = StorageDescriptor {
        service_peer: peer_a.as_str().to_owned(),
        registration_id: 19,
        object_root: "/ai/tinyhumans/tinystorage/Leases/19".to_owned(),
    };
    let bound = BusTransport::connect_bound(client.clone(), descriptor)
        .await
        .unwrap();
    let legacy = BusTransport::connect(client.clone()).await.unwrap();
    assert_eq!(
        bound
            .call(Object::Documents, "Capabilities", Value::Null)
            .await
            .unwrap(),
        json!({"service":"A"})
    );
    assert_eq!(
        legacy
            .call(Object::Documents, "Capabilities", Value::Null)
            .await
            .unwrap(),
        json!({"service":"B"})
    );
    let uploaded = bound
        .put_blob_stream(
            PutBlobStreamRequest {
                key: "handover/empty".to_owned(),
                len: 0,
                content_type: None,
                ttl_ms: None,
            },
            Box::pin(std::io::Cursor::new(Vec::new())),
        )
        .await
        .unwrap();
    assert_eq!(uploaded, json!({"service":"A"}));
    let (head, mut body) = bound
        .get_blob_stream(BlobKeyRequest {
            key: "handover/empty".to_owned(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.meta.key, "A");
    let mut downloaded = Vec::new();
    body.read_to_end(&mut downloaded).await.unwrap();
    assert!(downloaded.is_empty());

    service_a.close().await.unwrap();
    assert!(
        bound
            .call(Object::Documents, "Capabilities", Value::Null)
            .await
            .is_err()
    );
    assert_eq!(
        legacy
            .call(Object::Documents, "Capabilities", Value::Null)
            .await
            .unwrap(),
        json!({"service":"B"})
    );
    drop(served_a);
}
