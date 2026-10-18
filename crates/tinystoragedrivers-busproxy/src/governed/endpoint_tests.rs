use super::*;
use crate::{BusTransport, GrantPolicy};
use serde_json::json;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tinybus::{broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{
    CollectionSpec as WireCollectionSpec, Filter, Precondition, StorageNeeds,
};
use tinystoragedrivers_client::{Admission, Object, StorageTransport};
use tinystoragedrivers_core::{CollectionSpec, MemoryStorage, Scope, StorageBackend};
use tokio::sync::{Semaphore, mpsc};

mod relay_test_caller {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.test.GovernedRelayRaceCaller"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[derive(Clone)]
struct RelayContext;
impl HostContext for RelayContext {}

struct RelayEnvironment {
    epoch: AtomicU64,
    collection: WireCollectionSpec,
    servers: std::collections::HashMap<u64, Arc<StorageServer>>,
}

#[async_trait::async_trait]
impl Environment<RelayContext> for RelayEnvironment {
    async fn resolve(
        &self,
        _context: &RelayContext,
        _module: &ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        let binding_epoch = self.epoch.load(Ordering::Acquire);
        let needs = StorageNeeds {
            collections: std::slice::from_ref(&self.collection),
            ..StorageNeeds::default()
        };
        let grant = StorageGrant::from_needs(
            needs,
            &GrantPolicy {
                writable: true,
                ..GrantPolicy::default()
            },
        )
        .map_err(|_| HostFailure::Denied("test-grant-invalid"))?;
        Ok(ResolvedStorage {
            backend_identity: format!("relay-test-{binding_epoch}"),
            binding_epoch,
            canonical_scope: format!("relay-profile-{binding_epoch}"),
            grant,
            server: Arc::clone(
                self.servers
                    .get(&binding_epoch)
                    .ok_or(HostFailure::Denied("test-backend-unavailable"))?,
            ),
        })
    }

    async fn admit_write(
        &self,
        _context: &RelayContext,
        _module: &ModuleId,
    ) -> Result<(), HostFailure> {
        Ok(())
    }
}

struct RelayCallerDirectory {
    peer: BusName,
    module: ModuleId,
}

impl CallerDirectory for RelayCallerDirectory {
    fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        (sender == &self.peer).then(|| self.module.clone())
    }
}

struct RelayAudit;

impl AuditSink for RelayAudit {
    fn record(&self, _event: AuditEvent) {}
}

struct DropCountingLegacyEndpoint(Arc<AtomicU64>);

impl Drop for DropCountingLegacyEndpoint {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[async_trait::async_trait]
impl StorageEndpoint for DropCountingLegacyEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<serde_json::Value, HostFailure> {
        Ok(json!({"legacy":true}))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: tinystoragedrivers_bus::PutBlobStreamRequest,
        _body: tinystoragedrivers_core::BlobReader,
    ) -> Result<serde_json::Value, HostFailure> {
        Ok(serde_json::Value::Null)
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> Result<
        Option<(
            tinystoragedrivers_bus::BlobStreamHead,
            tinystoragedrivers_core::BlobReader,
        )>,
        HostFailure,
    > {
        Ok(None)
    }

    fn watch_signals(
        &self,
        _sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        None
    }
}

fn document_get<'a>(sender: &'a BusName, document_id: &str) -> StorageCall<'a> {
    StorageCall {
        object: Object::Documents,
        selector: "Get",
        request: json!({"collection":"records","document_id":document_id}),
        sender: Some(sender),
        admission: Admission::Private,
    }
}

fn assert_watch_invalidation(signal: WatchChanged, expected_watch: &str) -> Option<Vec<u8>> {
    assert_eq!(signal.watch_id, expected_watch);
    match signal.notice {
        WatchNotice::Invalidated { cursor } => cursor,
        WatchNotice::ResetRequired { reason } => {
            panic!("expected a fresh invalidation canary, got reset {reason:?}")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This fixture gates the relay and proves lag recovery with real backend watch traffic."
)]
async fn a_lagged_backend_signal_relay_resets_only_its_live_watch() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("lagged relay caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();
    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let storage = MemoryStorage::new().enable_native_watch();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();
    let server = Arc::new(StorageServer::new(scoped));
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers: [(1, Arc::clone(&server))].into_iter().collect(),
    });
    let resolved = environment.resolve(&RelayContext, &module).await.unwrap();
    let rendered_resolved = format!("{resolved:?}");
    assert!(!rendered_resolved.contains("relay-profile-1"));
    assert!(rendered_resolved.contains("[REDACTED]"));
    let policy = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(RelayAudit),
        GovernedEndpointLimits::default(),
    );
    assert!(format!("{policy:?}").contains("GovernedStorage"));
    assert!(format!("{:?}", policy.clone()).contains("GovernedStorage"));
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment,
        audit: Arc::new(RelayAudit),
        callers: Arc::new(RelayCallerDirectory {
            peer: peer.clone(),
            module: module.clone(),
        }),
        context: HostStorageContext::new(RelayContext),
        lease: LeaseClaims {
            registration_id: 1,
            epoch: 1,
            caller_peer: peer.clone(),
            module,
            revoked: Arc::new(AtomicBool::new(false)),
            published: Arc::new(AtomicBool::new(true)),
        },
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(2)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let (paused_tx, mut paused_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    *endpoint.relay_recv_test_gate.lock().unwrap() = Some(RelayRecvTestGate {
        reached: paused_tx,
        release: Arc::clone(&release),
    });
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    let mut backend_signals = server.signals();
    endpoint
        .call(document_get(&peer, "bind-before-open"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), paused_rx.recv())
        .await
        .expect("signal relay did not stop at its receive gate")
        .unwrap();
    let opened = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();

    for index in 0..300 {
        endpoint
            .call(StorageCall {
                object: Object::Documents,
                selector: "Put",
                request: serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                    collection: "records".to_owned(),
                    id: format!("lag-canary-{index}"),
                    doc: json!({"index":index}),
                    precondition: Precondition::Absent,
                    schema: 0,
                    expires_ms: None,
                })
                .unwrap(),
                sender: Some(&peer),
                admission: Admission::Private,
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), backend_signals.recv())
            .await
            .expect("backend did not publish each native watch invalidation")
            .unwrap();
    }
    assert_eq!(server.open_watches(), 1);
    release.add_permits(1);
    let reset = tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("lagged relay did not recover")
        .unwrap();
    assert_eq!(reset.watch_id, watch_id);
    assert_eq!(
        reset.notice,
        WatchNotice::ResetRequired {
            reason: WatchReset::Backend
        }
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchClose",
            request: json!({"watch_id":watch_id}),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test observes lease, legacy-route, native-watch, and observer cleanup together."
)]
async fn dropping_a_live_registration_cleans_leases_and_legacy_route_on_retained_server() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("drop cleanup caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();
    let service = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let storage = MemoryStorage::new().enable_native_watch();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .ensure_collection(&CollectionSpec::new("records"))
        .await
        .unwrap();
    let server = Arc::new(StorageServer::new(scoped));
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers: [(1, Arc::clone(&server))].into_iter().collect(),
    });
    let cleanup_count = Arc::new(AtomicU64::new(0));
    let policy = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(RelayAudit),
        GovernedEndpointLimits::default(),
    )
    .with_legacy_endpoint(Arc::new(DropCountingLegacyEndpoint(Arc::clone(
        &cleanup_count,
    ))));
    let registration = policy.register(
        service,
        Arc::new(RelayCallerDirectory {
            peer: peer.clone(),
            module,
        }),
        tokio::runtime::Handle::current(),
    );
    registration.install_legacy().await.unwrap();
    let (lease, descriptor) = registration
        .register_lease(HostStorageContext::new(RelayContext), peer.clone())
        .await
        .unwrap();
    let bound = BusTransport::connect_bound(caller.clone(), descriptor)
        .await
        .unwrap();
    let legacy = BusTransport::connect(caller.clone()).await.unwrap();
    assert_eq!(
        legacy
            .call(Object::Documents, "Capabilities", serde_json::Value::Null)
            .await
            .unwrap(),
        json!({"legacy":true})
    );
    bound
        .call(
            Object::Documents,
            "WatchOpen",
            json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
        )
        .await
        .unwrap();
    assert_eq!(server.open_watches(), 1);

    drop(policy);
    drop(registration);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if server.open_watches() == 0 && cleanup_count.load(Ordering::Acquire) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping registration must clean its watch and legacy endpoint");
    assert!(
        bound
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"records","document_id":"after-drop"}),
            )
            .await
            .is_err(),
        "dropping the owner must withdraw the lease route"
    );
    assert!(
        legacy
            .call(Object::Documents, "Capabilities", serde_json::Value::Null)
            .await
            .is_err(),
        "dropping the owner must withdraw the legacy alias route"
    );
    assert_eq!(server.open_watches(), 0);
    assert_eq!(cleanup_count.load(Ordering::Acquire), 1);
    drop(lease);
    assert!(caller.name_owner(tinybus::BUS_NAME).await.is_ok());
    host.shutdown(Duration::from_secs(1)).await;
}

#[test]
fn a_newer_async_binding_resolution_fences_a_late_completion() {
    let gate = ResolutionGate::new();
    let slow_old_resolution = gate.begin();
    let fast_new_resolution = gate.begin();

    assert!(!gate.is_latest(slow_old_resolution));
    assert!(gate.is_latest(fast_new_resolution));
    // Finishing the old future later has no operation that can roll the gate
    // back or make its server eligible for publication.
    assert!(!gate.is_latest(slow_old_resolution));
}

#[tokio::test]
async fn a_backend_panic_becomes_a_content_free_failure() {
    let result = guarded(async { panic!("private backend detail") }).await;
    assert_eq!(
        result.unwrap_err(),
        HostFailure::Storage(StorageFailure {
            kind: StorageKind::Backend,
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test orders two real bindings and verifies both externally visible canaries."
)]
async fn a_late_epoch_cannot_replace_the_latest_watch_relay_after_publication() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("relay race caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();

    let wire_collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let mut servers = std::collections::HashMap::new();
    for epoch in 1..=3 {
        let storage = MemoryStorage::new().enable_native_watch();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .ensure_collection(&CollectionSpec::new("records"))
            .await
            .unwrap();
        servers.insert(epoch, Arc::new(StorageServer::new(scoped)));
    }
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection: wire_collection,
        servers,
    });
    let callers: Arc<dyn CallerDirectory> = Arc::new(RelayCallerDirectory {
        peer: peer.clone(),
        module: module.clone(),
    });
    let lease = LeaseClaims {
        registration_id: 1,
        epoch: 1,
        caller_peer: peer.clone(),
        module,
        revoked: Arc::new(AtomicBool::new(false)),
        published: Arc::new(AtomicBool::new(true)),
    };
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers,
        context: HostStorageContext::new(RelayContext),
        lease,
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));

    let (relay_events_tx, mut relay_events) = mpsc::unbounded_channel();
    *endpoint.relay_test_events.lock().unwrap() = Some(relay_events_tx);
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    endpoint
        .call(document_get(&peer, "epoch-one"))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-one relay did not install"),
        Some(RelayTestEvent::Installed(1))
    );

    let (epoch_two_reached, mut epoch_two_published) = mpsc::unbounded_channel();
    let epoch_two_release = Arc::new(Semaphore::new(0));
    *endpoint.relay_install_test_gate.lock().unwrap() = Some(RelayInstallTestGate {
        epoch: 2,
        pause: RelayPublicationPause::BeforeCommit,
        reached: epoch_two_reached,
        release: Arc::clone(&epoch_two_release),
    });
    environment.epoch.store(2, Ordering::Release);
    let epoch_two_endpoint = Arc::clone(&endpoint);
    let epoch_two_peer = peer.clone();
    let epoch_two_call = tokio::spawn(async move {
        epoch_two_endpoint
            .call(document_get(&epoch_two_peer, "epoch-two-paused"))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), epoch_two_published.recv())
        .await
        .expect("epoch two did not pause before binding commit")
        .unwrap();

    environment.epoch.store(3, Ordering::Release);
    let opened = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-three relay did not install"),
        Some(RelayTestEvent::Installed(3))
    );
    assert_eq!(environment.servers[&3].open_watches(), 1);
    assert_eq!(environment.servers[&3].open_watches(), 1);

    let put = |id: &'static str| {
        serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
            collection: "records".to_owned(),
            id: id.to_owned(),
            doc: json!({"canary":id}),
            precondition: Precondition::Absent,
            schema: 0,
            expires_ms: None,
        })
        .unwrap()
    };
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("before-stale-relay"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let first_cursor = assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("watch output missed its initial canary")
            .unwrap(),
        &watch_id,
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-three relay missed its initial canary"),
        Some(RelayTestEvent::Forwarded {
            epoch: 3,
            watch_id: watch_id.clone(),
        })
    );

    epoch_two_release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), epoch_two_call)
            .await
            .expect("stale epoch-two operation did not settle")
            .unwrap()
            .unwrap_err(),
        HostFailure::Denied("stale-binding-resolution")
    );
    assert_eq!(
        endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .binding_epoch,
        3
    );
    // Defer checking these diagnostic transitions until after the externally
    // visible fresh canary has had the chance to prove the relay still works.
    let late_relay_transitions =
        std::iter::from_fn(|| relay_events.try_recv().ok()).collect::<Vec<_>>();

    endpoint
        .call(document_get(&peer, "same-epoch-no-repair"))
        .await
        .unwrap();
    let same_epoch_relay_transitions =
        std::iter::from_fn(|| relay_events.try_recv().ok()).collect::<Vec<_>>();
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("after-stale-relay"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let second_cursor = assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("the current epoch-three watch must receive a fresh canary")
            .unwrap(),
        &watch_id,
    );
    if let (Some(first), Some(second)) = (&first_cursor, &second_cursor) {
        assert_ne!(first, second, "fresh invalidations must have fresh cursors");
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("the current epoch-three relay must report its later canary"),
        Some(RelayTestEvent::Forwarded {
            epoch: 3,
            watch_id: watch_id.clone(),
        })
    );
    assert!(
        late_relay_transitions
            .iter()
            .all(|event| !matches!(event, RelayTestEvent::Installed(2))),
        "the stale epoch-two completion cannot replace the epoch-three relay"
    );
    assert!(
        same_epoch_relay_transitions.is_empty(),
        "a same-epoch call must not be the mechanism that repairs a lost relay"
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchClose",
            request: json!({"watch_id":watch_id}),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test holds a real watch across a controlled stale-publication cleanup race."
)]
async fn a_stale_publication_cannot_reset_a_newer_binding_watch() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("watch reset caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();

    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let mut servers = std::collections::HashMap::new();
    for epoch in 1..=3 {
        let storage = MemoryStorage::new().enable_native_watch();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .ensure_collection(&CollectionSpec::new("records"))
            .await
            .unwrap();
        servers.insert(epoch, Arc::new(StorageServer::new(scoped)));
    }
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers,
    });
    let callers: Arc<dyn CallerDirectory> = Arc::new(RelayCallerDirectory {
        peer: peer.clone(),
        module: module.clone(),
    });
    let lease = LeaseClaims {
        registration_id: 1,
        epoch: 1,
        caller_peer: peer.clone(),
        module,
        revoked: Arc::new(AtomicBool::new(false)),
        published: Arc::new(AtomicBool::new(true)),
    };
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers,
        context: HostStorageContext::new(RelayContext),
        lease,
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let (relay_events_tx, mut relay_events) = mpsc::unbounded_channel();
    *endpoint.relay_test_events.lock().unwrap() = Some(relay_events_tx);
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    endpoint
        .call(document_get(&peer, "epoch-one"))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-one relay did not install"),
        Some(RelayTestEvent::Installed(1))
    );

    let (epoch_two_reached, mut epoch_two_published) = mpsc::unbounded_channel();
    let epoch_two_release = Arc::new(Semaphore::new(0));
    *endpoint.relay_install_test_gate.lock().unwrap() = Some(RelayInstallTestGate {
        epoch: 2,
        pause: RelayPublicationPause::BeforeCommit,
        reached: epoch_two_reached,
        release: Arc::clone(&epoch_two_release),
    });
    environment.epoch.store(2, Ordering::Release);
    let epoch_two_endpoint = Arc::clone(&endpoint);
    let epoch_two_peer = peer.clone();
    let epoch_two_call = tokio::spawn(async move {
        epoch_two_endpoint
            .call(document_get(
                &epoch_two_peer,
                "epoch-two-paused-before-reset",
            ))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), epoch_two_published.recv())
        .await
        .expect("epoch two did not pause before binding commit")
        .unwrap();

    environment.epoch.store(3, Ordering::Release);
    let opened = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-three relay did not install"),
        Some(RelayTestEvent::Installed(3))
    );
    let put = |id: &'static str| {
        serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
            collection: "records".to_owned(),
            id: id.to_owned(),
            doc: json!({"canary":id}),
            precondition: Precondition::Absent,
            schema: 0,
            expires_ms: None,
        })
        .unwrap()
    };
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("before-stale-reset"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let first_signal = tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("epoch-three watch missed its pre-reset canary")
        .unwrap();
    let first_cursor = assert_watch_invalidation(first_signal, &watch_id);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-three relay missed its pre-reset canary"),
        Some(RelayTestEvent::Forwarded {
            epoch: 3,
            watch_id: watch_id.clone(),
        })
    );

    epoch_two_release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), epoch_two_call)
            .await
            .expect("stale epoch-two publication did not settle")
            .unwrap()
            .unwrap_err(),
        HostFailure::Denied("stale-binding-resolution")
    );
    assert_eq!(
        endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .binding_epoch,
        3
    );

    endpoint
        .call(document_get(&peer, "same-epoch-no-watch-repair"))
        .await
        .unwrap();
    let same_epoch_events = std::iter::from_fn(|| relay_events.try_recv().ok()).collect::<Vec<_>>();
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("after-stale-reset"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let second_cursor = assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("stale epoch-two reset must not remove the epoch-three watch")
            .unwrap(),
        &watch_id,
    );
    if let (Some(first), Some(second)) = (&first_cursor, &second_cursor) {
        assert_ne!(first, second, "fresh invalidations must have fresh cursors");
    }
    assert!(same_epoch_events.is_empty());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-three relay missed its post-reset canary"),
        Some(RelayTestEvent::Forwarded {
            epoch: 3,
            watch_id: watch_id.clone(),
        })
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchClose",
            request: json!({"watch_id":watch_id}),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test proves same-epoch concurrent publication preserves a live relay."
)]
async fn a_same_epoch_newer_ticket_repairs_a_paused_publication_relay() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("same epoch caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();

    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let mut servers = std::collections::HashMap::new();
    for epoch in 1..=2 {
        let storage = MemoryStorage::new().enable_native_watch();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .ensure_collection(&CollectionSpec::new("records"))
            .await
            .unwrap();
        servers.insert(epoch, Arc::new(StorageServer::new(scoped)));
    }
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers,
    });
    let callers: Arc<dyn CallerDirectory> = Arc::new(RelayCallerDirectory {
        peer: peer.clone(),
        module,
    });
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers,
        context: HostStorageContext::new(RelayContext),
        lease: LeaseClaims {
            registration_id: 1,
            epoch: 1,
            caller_peer: peer.clone(),
            module: host.sender_lookup().module_for_sender(&peer).unwrap(),
            revoked: Arc::new(AtomicBool::new(false)),
            published: Arc::new(AtomicBool::new(true)),
        },
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let (relay_events_tx, mut relay_events) = mpsc::unbounded_channel();
    *endpoint.relay_test_events.lock().unwrap() = Some(relay_events_tx);
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    endpoint
        .call(document_get(&peer, "epoch-one"))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-one relay did not install"),
        Some(RelayTestEvent::Installed(1))
    );

    let (paused_tx, mut paused_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    *endpoint.relay_install_test_gate.lock().unwrap() = Some(RelayInstallTestGate {
        epoch: 2,
        pause: RelayPublicationPause::BeforeCommit,
        reached: paused_tx,
        release: Arc::clone(&release),
    });
    environment.epoch.store(2, Ordering::Release);
    let paused_endpoint = Arc::clone(&endpoint);
    let paused_peer = peer.clone();
    let paused_call = tokio::spawn(async move {
        paused_endpoint
            .call(document_get(&paused_peer, "epoch-two-paused"))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), paused_rx.recv())
        .await
        .expect("epoch-two publication did not pause")
        .unwrap();

    let opened = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .expect("newer same-epoch WatchOpen must complete while older resolution is paused");
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();
    assert_eq!(
        endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .binding_epoch,
        2
    );
    assert_eq!(
        endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .server
            .open_watches(),
        1,
        "the same-epoch operation must establish a real backend watch"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("the newer same-epoch publication did not commit its relay"),
        Some(RelayTestEvent::Installed(2)),
        "the same-epoch operation must repair the relay before returning"
    );

    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), paused_call)
            .await
            .expect("stale epoch-two call did not settle")
            .unwrap()
            .unwrap_err(),
        HostFailure::Denied("stale-binding-resolution")
    );

    endpoint
        .call(document_get(&peer, "same-epoch-follow-up"))
        .await
        .unwrap();
    assert!(relay_events.try_recv().is_err());
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "records".to_owned(),
                id: "same-epoch-fresh-canary".to_owned(),
                doc: json!({"canary":"same-epoch-fresh-canary"}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let signal = tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("the real epoch-two invalidation canary must be forwarded after the newer same-epoch operation")
        .unwrap();
    assert_watch_invalidation(signal, &watch_id);
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchClose",
            request: json!({"watch_id":watch_id}),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test catches stale reset notices crossing reused backend watch IDs."
)]
async fn reset_notices_and_signals_keep_lease_unique_watch_handles_after_rebind() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("reset notice caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();
    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let mut servers = std::collections::HashMap::new();
    for epoch in 1..=2 {
        let storage = MemoryStorage::new().enable_native_watch();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .ensure_collection(&CollectionSpec::new("records"))
            .await
            .unwrap();
        servers.insert(epoch, Arc::new(StorageServer::new(scoped)));
    }
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers,
    });
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers: Arc::new(RelayCallerDirectory {
            peer: peer.clone(),
            module: module.clone(),
        }),
        context: HostStorageContext::new(RelayContext),
        lease: LeaseClaims {
            registration_id: 1,
            epoch: 1,
            caller_peer: peer.clone(),
            module,
            revoked: Arc::new(AtomicBool::new(false)),
            published: Arc::new(AtomicBool::new(true)),
        },
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let (relay_events_tx, mut relay_events) = mpsc::unbounded_channel();
    *endpoint.relay_test_events.lock().unwrap() = Some(relay_events_tx);
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    endpoint
        .call(document_get(&peer, "warm-epoch-one"))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-one relay did not install"),
        Some(RelayTestEvent::Installed(1))
    );
    let first_open = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let old_watch_id = first_open["watch_id"].as_str().unwrap().to_owned();
    let put = |id: &'static str| {
        serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
            collection: "records".to_owned(),
            id: id.to_owned(),
            doc: json!({"canary":id}),
            precondition: Precondition::Absent,
            schema: 0,
            expires_ms: None,
        })
        .unwrap()
    };
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("epoch-one-canary"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("epoch-one watch missed its canary")
            .unwrap(),
        &old_watch_id,
    );
    let old_backend_watch_id = endpoint
        .watch_ids
        .lock()
        .unwrap()
        .get(&old_watch_id)
        .unwrap()
        .backend_watch_id
        .clone();
    let _ = relay_events.try_recv();

    environment.epoch.store(2, Ordering::Release);
    endpoint
        .call(document_get(&peer, "rebind-epoch-two"))
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("old watch did not receive its binding reset")
        .unwrap()
    {
        WatchChanged {
            watch_id,
            notice:
                WatchNotice::ResetRequired {
                    reason: WatchReset::Backend,
                },
        } if watch_id == old_watch_id => {}
        signal => panic!("expected reset for the old outward handle, got {signal:?}"),
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("epoch-two relay did not install"),
        Some(RelayTestEvent::Installed(2))
    );

    let second_open = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let new_watch_id = second_open["watch_id"].as_str().unwrap().to_owned();
    let new_backend_watch_id = endpoint
        .watch_ids
        .lock()
        .unwrap()
        .get(&new_watch_id)
        .unwrap()
        .backend_watch_id
        .clone();
    assert_ne!(
        old_watch_id, new_watch_id,
        "outward handles must not be reused"
    );
    assert_eq!(
        old_backend_watch_id, new_backend_watch_id,
        "the fixture must exercise native watch-ID reuse across backends"
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("new-backend-before-old-reset"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let first_new_cursor = assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("new backend watch missed its positive canary")
            .unwrap(),
        &new_watch_id,
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), relay_events.recv())
            .await
            .expect("new backend relay missed its canary"),
        Some(RelayTestEvent::Forwarded {
            epoch: 2,
            watch_id: new_watch_id.clone(),
        })
    );

    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: put("new-backend-second-canary"),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let second_new_cursor = assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("old reset notice contaminated the new watch")
            .unwrap(),
        &new_watch_id,
    );
    if let (Some(first), Some(second)) = (&first_new_cursor, &second_new_cursor) {
        assert_ne!(
            first, second,
            "new backend invalidations need fresh cursors"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test models a delayed old-client WatchClose after backend rebinding."
)]
async fn an_old_watch_close_cannot_close_a_new_backend_watch_with_the_same_native_id() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("watch close caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let peer = caller.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();
    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let mut servers = std::collections::HashMap::new();
    for epoch in 1..=2 {
        let storage = MemoryStorage::new().enable_native_watch();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .ensure_collection(&CollectionSpec::new("records"))
            .await
            .unwrap();
        servers.insert(epoch, Arc::new(StorageServer::new(scoped)));
    }
    let environment = Arc::new(RelayEnvironment {
        epoch: AtomicU64::new(1),
        collection,
        servers,
    });
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers: Arc::new(RelayCallerDirectory {
            peer: peer.clone(),
            module: module.clone(),
        }),
        context: HostStorageContext::new(RelayContext),
        lease: LeaseClaims {
            registration_id: 1,
            epoch: 1,
            caller_peer: peer.clone(),
            module,
            revoked: Arc::new(AtomicBool::new(false)),
            published: Arc::new(AtomicBool::new(true)),
        },
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let mut signals = endpoint.watch_signals(&peer).unwrap();
    endpoint
        .call(document_get(&peer, "warm-epoch-one"))
        .await
        .unwrap();
    let first_open = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let stale_handle = first_open["watch_id"].as_str().unwrap().to_owned();
    let stale_backend_watch_id = endpoint
        .watch_ids
        .lock()
        .unwrap()
        .get(&stale_handle)
        .unwrap()
        .backend_watch_id
        .clone();
    assert_eq!(
        endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .server
            .open_watches(),
        1
    );

    environment.epoch.store(2, Ordering::Release);
    endpoint
        .call(document_get(&peer, "rebind-epoch-two"))
        .await
        .unwrap();
    match tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("the previous binding should receive its reset notice")
        .unwrap()
    {
        WatchChanged {
            watch_id,
            notice:
                WatchNotice::ResetRequired {
                    reason: WatchReset::Backend,
                },
        } if watch_id == stale_handle => {}
        signal => panic!("expected previous watch reset, got {signal:?}"),
    }
    let second_open = endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "WatchOpen",
            request: json!({
                "collection":"records",
                "filter":serde_json::to_value(Filter::All).unwrap(),
                "interval_ms":1000
            }),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    let current_handle = second_open["watch_id"].as_str().unwrap().to_owned();
    let current_backend_watch_id = endpoint
        .watch_ids
        .lock()
        .unwrap()
        .get(&current_handle)
        .unwrap()
        .backend_watch_id
        .clone();
    assert_ne!(
        stale_handle, current_handle,
        "outward handles must not be reused across binding epochs"
    );
    assert_eq!(
        stale_backend_watch_id, current_backend_watch_id,
        "the fixture requires the backend to reuse its native watch ID"
    );
    let current_server = Arc::clone(
        &endpoint
            .bound_server
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .server,
    );
    assert_eq!(current_server.open_watches(), 1);

    assert_eq!(
        endpoint
            .call(StorageCall {
                object: Object::Documents,
                selector: "WatchClose",
                request: json!({"watch_id":stale_handle}),
                sender: Some(&peer),
                admission: Admission::Private,
            })
            .await
            .unwrap_err(),
        HostFailure::Denied("watch-handle-not-owned")
    );
    assert_eq!(
        current_server.open_watches(),
        1,
        "a delayed close for the prior backend must leave the current watch open"
    );
    endpoint
        .call(StorageCall {
            object: Object::Documents,
            selector: "Put",
            request: serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "records".to_owned(),
                id: "post-close-canary".to_owned(),
                doc: json!({"canary":"post-close-canary"}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
            sender: Some(&peer),
            admission: Admission::Private,
        })
        .await
        .unwrap();
    assert_watch_invalidation(
        tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("current backend watch was closed by the delayed old handle")
            .unwrap(),
        &current_handle,
    );
}
