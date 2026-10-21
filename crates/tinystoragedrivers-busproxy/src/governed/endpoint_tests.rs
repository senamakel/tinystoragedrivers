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

fn fixed_lease_claims(
    registration_id: u64,
    caller_peer: BusName,
    module: &ModuleId,
) -> LeaseClaims {
    let peer_pin = Arc::new(std::sync::OnceLock::new());
    peer_pin.set(caller_peer.clone()).unwrap();
    LeaseClaims {
        registration_id,
        epoch: registration_id,
        caller_peer: Some(caller_peer),
        module: module.as_str().to_owned(),
        peer_pin,
        revoked: Arc::new(AtomicBool::new(false)),
        published: Arc::new(AtomicBool::new(true)),
    }
}

#[derive(Clone, Debug)]
struct CapturedCallAudit {
    event: AuditEvent,
    has_module: bool,
    canonical_scope: Option<String>,
    selector: Option<String>,
    target: Option<String>,
}

#[derive(Default)]
struct RecordingCallAudit(std::sync::Mutex<Vec<CapturedCallAudit>>);

impl RecordingCallAudit {
    fn events(&self) -> Vec<CapturedCallAudit> {
        self.0.lock().unwrap().clone()
    }
}

impl AuditSink for RecordingCallAudit {
    fn record(&self, event: AuditEvent) {
        self.0.lock().unwrap().push(CapturedCallAudit {
            event,
            has_module: false,
            canonical_scope: None,
            selector: None,
            target: None,
        });
    }

    fn record_call(&self, event: AuditEvent, metadata: CallAuditMetadata<'_>) {
        self.0.lock().unwrap().push(CapturedCallAudit {
            event,
            has_module: metadata.module.is_some(),
            canonical_scope: metadata.canonical_scope.map(str::to_owned),
            selector: Some(metadata.selector.to_owned()),
            target: metadata.target.map(str::to_owned),
        });
    }
}

#[derive(Default)]
struct RecordOnlyAudit(std::sync::Mutex<Vec<AuditEvent>>);

impl RecordOnlyAudit {
    fn events(&self) -> Vec<AuditEvent> {
        self.0.lock().unwrap().clone()
    }
}

impl AuditSink for RecordOnlyAudit {
    fn record(&self, event: AuditEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct ResolvePause {
    entered: tokio::sync::Notify,
    release: Semaphore,
}

struct CallAuditEnvironment {
    grant: crate::StorageGrant,
    server: Arc<StorageServer>,
    resolve_calls: std::sync::atomic::AtomicUsize,
    fail_resolve: std::sync::atomic::AtomicBool,
    panic_resolve: std::sync::atomic::AtomicBool,
    fail_write: std::sync::atomic::AtomicBool,
    resolve_pause: std::sync::Mutex<Option<Arc<ResolvePause>>>,
}

struct CallAuditCallerDirectory {
    peer: BusName,
    module: ModuleId,
    hide_peer: std::sync::atomic::AtomicBool,
}

struct ExpectedModuleCallerDirectory(
    std::sync::Mutex<std::collections::HashMap<BusName, ModuleId>>,
);

impl CallerDirectory for ExpectedModuleCallerDirectory {
    fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        self.0.lock().unwrap().get(sender).cloned()
    }
}

impl CallerDirectory for CallAuditCallerDirectory {
    fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        if self.hide_peer.load(Ordering::Acquire) || sender != &self.peer {
            None
        } else {
            Some(self.module.clone())
        }
    }
}

#[async_trait::async_trait]
impl Environment<RelayContext> for CallAuditEnvironment {
    async fn resolve(
        &self,
        _context: &RelayContext,
        _module: &ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        self.resolve_calls.fetch_add(1, Ordering::AcqRel);
        let pause = self.resolve_pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.entered.notify_one();
            pause
                .release
                .acquire()
                .await
                .map_err(|_| HostFailure::Denied("test-pause-closed"))?
                .forget();
        }
        assert!(
            !self.panic_resolve.load(Ordering::Acquire),
            "audit fixture resolution panicked"
        );
        if self.fail_resolve.load(Ordering::Acquire) {
            return Err(HostFailure::Denied("test-resolution-refused"));
        }
        Ok(ResolvedStorage {
            backend_identity: "audit-fixture-backend".to_string(),
            binding_epoch: 1,
            canonical_scope: "audit-fixture-private-scope".to_string(),
            grant: self.grant.clone(),
            server: Arc::clone(&self.server),
        })
    }

    async fn admit_write(
        &self,
        _context: &RelayContext,
        _module: &ModuleId,
    ) -> Result<(), HostFailure> {
        if self.fail_write.load(Ordering::Acquire) {
            Err(HostFailure::Storage(StorageFailure {
                kind: StorageKind::Fenced,
            }))
        } else {
            Ok(())
        }
    }
}

struct CallAuditFixture<A: AuditSink> {
    endpoint: Arc<GovernedEndpoint<CallAuditEnvironment, A, RelayContext>>,
    environment: Arc<CallAuditEnvironment>,
    callers: Arc<CallAuditCallerDirectory>,
    sender: BusName,
    _host: ModuleHost,
    _broker_task: tokio::task::JoinHandle<Result<(), tinybus::Error>>,
}

async fn call_audit_module_peer() -> (
    ModuleHost,
    tokio::task::JoinHandle<Result<(), tinybus::Error>>,
    BusName,
    ModuleId,
) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("audit fixture receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("audit fixture module setup completes")
        .unwrap();
    let sender = caller.unique_name().unwrap();
    let module = host
        .sender_lookup()
        .module_for_sender(&sender)
        .expect("linked module has an admitted id");
    (host, broker_task, sender, module)
}

async fn call_audit_environment() -> Arc<CallAuditEnvironment> {
    let storage = MemoryStorage::new();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let queues = Arc::new(
        tinystoragedrivers_core::queue::QueueManifest::from_json(
            r#"{"version":1,"queues":[{"name":"inbound"}]}"#,
        )
        .unwrap(),
    );
    let server = Arc::new(StorageServer::new(scoped).with_queue_manifest(Arc::clone(&queues)));
    let collection = [WireCollectionSpec {
        name: "notes".to_string(),
        indexes: Vec::new(),
        ttl_field: None,
        search: None,
    }];
    let blob_prefixes = ["attachments/"];
    let secret_prefixes = ["private/"];
    let grant = crate::StorageGrant::from_needs(
        StorageNeeds {
            collections: &collection,
            blob_prefixes: &blob_prefixes,
            secret_prefixes: &secret_prefixes,
            ..StorageNeeds::default()
        },
        &GrantPolicy {
            writable: true,
            allow_secrets: true,
            ..GrantPolicy::default()
        },
    )
    .unwrap()
    .with_queue_manifest(queues)
    .unwrap();

    Arc::new(CallAuditEnvironment {
        grant,
        server,
        resolve_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_resolve: std::sync::atomic::AtomicBool::new(false),
        panic_resolve: std::sync::atomic::AtomicBool::new(false),
        fail_write: std::sync::atomic::AtomicBool::new(false),
        resolve_pause: std::sync::Mutex::new(None),
    })
}

mod expected_peer_export {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.test.ExpectedStoragePeer"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

async fn attach_expected_peer(
    host: &ModuleHost,
    module_id: &str,
    bus_name: &str,
    object_path: &str,
) -> (Connection, ModuleId) {
    let mut export = expected_peer_export::linked_module().unwrap();
    export.descriptor.module_name = [0; 64];
    export.descriptor.module_name[..module_id.len()].copy_from_slice(module_id.as_bytes());
    export.manifest.module.name = module_id.to_owned();
    export.manifest.bus_name = bus_name.parse().unwrap();
    export.manifest.object_path = object_path.parse().unwrap();
    let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(export, json!({}), move |connection| async move {
        connection_tx
            .send(connection.clone())
            .map_err(|_| tinybus::Error::failed("expected peer receiver closed"))
    })
    .await
    .unwrap();
    let connection = connection_rx.await.unwrap();
    let peer = connection.unique_name().unwrap();
    let module = host.sender_lookup().module_for_sender(&peer).unwrap();
    (connection, module)
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This broker-level test verifies pending publication, caller rejection, atomic peer claim, and revoke as one transaction."
)]
async fn expected_module_lease_defers_resolution_and_pins_the_first_matching_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (expected_peer, expected_module) = attach_expected_peer(
        &host,
        "expected-storage-peer",
        "ai.tinyhumans.test.expectedstoragepeer",
        "/ai/tinyhumans/test/expected_storage_peer",
    )
    .await;
    let (wrong_peer, wrong_module) = attach_expected_peer(
        &host,
        "wrong-storage-peer",
        "ai.tinyhumans.test.wrongstoragepeer",
        "/ai/tinyhumans/test/wrong_storage_peer",
    )
    .await;
    let second_expected_peer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let unknown_peer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let first_expected_name = expected_peer.unique_name().unwrap();
    let second_expected_name = second_expected_peer.unique_name().unwrap();
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let directory = Arc::new(ExpectedModuleCallerDirectory(std::sync::Mutex::new(
        [
            (
                expected_peer.unique_name().unwrap(),
                expected_module.clone(),
            ),
            (
                second_expected_peer.unique_name().unwrap(),
                expected_module.clone(),
            ),
            (wrong_peer.unique_name().unwrap(), wrong_module),
        ]
        .into_iter()
        .collect(),
    )));
    let environment = call_audit_environment().await;
    let registration = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(RecordOnlyAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .register(service, directory, tokio::runtime::Handle::current());

    assert_eq!(
        registration
            .register_expected_module_lease(HostStorageContext::new(RelayContext), "")
            .await
            .err(),
        Some(HostFailure::Denied("invalid-module-id")),
        "an empty host registry id cannot receive a lease"
    );
    let oversized_module_id = "x".repeat(65);
    assert_eq!(
        registration
            .register_expected_module_lease(
                HostStorageContext::new(RelayContext),
                &oversized_module_id,
            )
            .await
            .err(),
        Some(HostFailure::Denied("invalid-module-id")),
        "module ids cannot exceed TinyBus's fixed descriptor bound"
    );

    let (lease, descriptor) = registration
        .register_expected_module_lease(
            HostStorageContext::new(RelayContext),
            expected_module.as_str(),
        )
        .await
        .expect("host can publish an expected-module route before the module starts");
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        0,
        "a pending route must not resolve storage before its authenticated peer calls"
    );

    let unknown = BusTransport::connect_bound(unknown_peer, descriptor.clone())
        .await
        .unwrap();
    assert!(
        unknown
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"}),
            )
            .await
            .is_err(),
        "an unknown peer cannot claim the route"
    );
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        0,
        "unknown peer refusal happens before Environment::resolve"
    );

    let wrong = BusTransport::connect_bound(wrong_peer, descriptor.clone())
        .await
        .unwrap();
    assert!(
        wrong
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"})
            )
            .await
            .is_err(),
        "an admitted peer for another module cannot claim the route"
    );
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        0,
        "foreign module refusal happens before Environment::resolve"
    );

    let first = BusTransport::connect_bound(expected_peer, descriptor.clone())
        .await
        .unwrap();
    let second = BusTransport::connect_bound(second_expected_peer, descriptor)
        .await
        .unwrap();
    let first_call = first.call(
        Object::Documents,
        "Get",
        json!({"collection":"notes","document_id":"x"}),
    );
    let second_call = second.call(
        Object::Documents,
        "Get",
        json!({"collection":"notes","document_id":"x"}),
    );
    let (first_result, second_result) = tokio::join!(first_call, second_call);
    assert_ne!(
        first_result.is_ok(),
        second_result.is_ok(),
        "exactly one concurrently authenticating peer pins the lease"
    );
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        2,
        "only the winning peer reaches initial and post-backend resolution"
    );

    let (winner, loser, winner_peer) = if first_result.is_ok() {
        (&first, &second, first_expected_name)
    } else {
        (&second, &first, second_expected_name)
    };
    assert!(
        loser
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"}),
            )
            .await
            .is_err(),
        "the losing same-module peer cannot take over after the first claim"
    );
    assert_eq!(environment.resolve_calls.load(Ordering::Acquire), 2);
    assert!(
        winner
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"}),
            )
            .await
            .is_ok(),
        "the pinned peer remains authorized"
    );
    assert_eq!(environment.resolve_calls.load(Ordering::Acquire), 4);

    assert_eq!(
        registration.revoke_peer(&winner_peer).await,
        1,
        "peer detach revokes the pinned pre-load lease"
    );
    assert!(
        winner
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"})
            )
            .await
            .is_err(),
        "revocation permanently closes the published route"
    );
    assert!(
        !registration.revoke(&lease).await,
        "peer revocation cannot unpin and reassign the same lease handle"
    );
    registration.shutdown().await.unwrap();
    host.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn expected_module_lease_retries_failed_first_resolution_only_for_its_pinned_peer() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (expected_peer, expected_module) = attach_expected_peer(
        &host,
        "expected-storage-peer",
        "ai.tinyhumans.test.expectedstoragepeer",
        "/ai/tinyhumans/test/expected_storage_peer",
    )
    .await;
    let second_expected_peer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let directory = Arc::new(ExpectedModuleCallerDirectory(std::sync::Mutex::new(
        [
            (
                expected_peer.unique_name().unwrap(),
                expected_module.clone(),
            ),
            (
                second_expected_peer.unique_name().unwrap(),
                expected_module.clone(),
            ),
        ]
        .into_iter()
        .collect(),
    )));
    let environment = call_audit_environment().await;
    let registration = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(RecordOnlyAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .register(service, directory, tokio::runtime::Handle::current());
    let (lease, descriptor) = registration
        .register_expected_module_lease(
            HostStorageContext::new(RelayContext),
            expected_module.as_str(),
        )
        .await
        .unwrap();
    let first = BusTransport::connect_bound(expected_peer, descriptor.clone())
        .await
        .unwrap();
    environment.fail_resolve.store(true, Ordering::Release);
    assert!(
        first
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"})
            )
            .await
            .is_err(),
        "a failed first resolution is refused"
    );
    assert_eq!(environment.resolve_calls.load(Ordering::Acquire), 1);

    let second = BusTransport::connect_bound(second_expected_peer, descriptor)
        .await
        .unwrap();
    assert!(
        second
            .call(
                Object::Documents,
                "Get",
                json!({"collection":"notes","document_id":"x"})
            )
            .await
            .is_err(),
        "a failed warm does not let another peer claim the lease"
    );
    assert_eq!(environment.resolve_calls.load(Ordering::Acquire), 1);

    environment.fail_resolve.store(false, Ordering::Release);
    first
        .call(
            Object::Documents,
            "Get",
            json!({"collection":"notes","document_id":"x"}),
        )
        .await
        .unwrap();
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        3,
        "the pinned peer can retry the failed first resolution"
    );
    registration.revoke(&lease).await;
    registration.shutdown().await.unwrap();
    host.shutdown(Duration::from_secs(1)).await;
}

async fn call_audit_fixture<A: AuditSink>(
    audit: Arc<A>,
    max_blob_bytes: u64,
) -> CallAuditFixture<A> {
    let (host, broker_task, sender, module) = call_audit_module_peer().await;
    let environment = call_audit_environment().await;
    let callers = Arc::new(CallAuditCallerDirectory {
        peer: sender.clone(),
        module: module.clone(),
        hide_peer: std::sync::atomic::AtomicBool::new(false),
    });
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::clone(&audit),
        callers: callers.clone(),
        context: HostStorageContext::new(RelayContext),
        lease: fixed_lease_claims(91, sender.clone(), &module),
        limits: GovernedEndpointLimits { max_blob_bytes },
        binding_slots: Arc::new(Semaphore::new(4)),
        runtime: tokio::runtime::Handle::current(),
    }));

    CallAuditFixture {
        endpoint,
        environment,
        callers,
        sender,
        _host: host,
        _broker_task: broker_task,
    }
}

fn audit_get<'a>(sender: Option<&'a BusName>, collection: &str) -> StorageCall<'a> {
    StorageCall {
        object: Object::Documents,
        selector: "Get",
        request: json!({"collection":collection,"document_id":"audit-record"}),
        sender,
        admission: Admission::Private,
    }
}

#[tokio::test]
async fn record_only_audit_sinks_receive_early_refusals_once() {
    let legacy_audit = Arc::new(RecordOnlyAudit::default());
    let legacy_fixture = call_audit_fixture(Arc::clone(&legacy_audit), 1024).await;
    assert_eq!(
        legacy_fixture
            .endpoint
            .call(audit_get(Some(&legacy_fixture.sender), "not-granted"))
            .await,
        Err(HostFailure::Denied("target-not-granted"))
    );
    let legacy_events = legacy_audit.events();
    assert_eq!(
        legacy_events.len(),
        1,
        "default record-only sinks see refusals"
    );
    assert_eq!(legacy_events[0].outcome, "target-not-granted");
}

#[tokio::test]
async fn call_audit_records_success_and_sanitizes_denied_targets_and_selectors() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(Arc::clone(&audit), 1024).await;

    fixture
        .endpoint
        .call(audit_get(Some(&fixture.sender), "notes"))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .endpoint
            .call(audit_get(Some(&fixture.sender), "not-granted"))
            .await,
        Err(HostFailure::Denied("target-not-granted"))
    );

    assert_eq!(
        fixture
            .endpoint
            .call(StorageCall {
                object: Object::Documents,
                selector: "not-a-protocol-selector:selector-marker",
                request: serde_json::Value::Null,
                sender: Some(&fixture.sender),
                admission: Admission::Private,
            })
            .await,
        Err(HostFailure::Denied("malformed-request"))
    );

    let events = audit.events();
    assert_eq!(events.len(), 3, "each completed call emits one record");
    assert_eq!(events[0].event.outcome, "allowed");
    assert!(events[0].has_module);
    assert_eq!(
        events[0].canonical_scope.as_deref(),
        Some("audit-fixture-private-scope")
    );
    assert_eq!(events[0].selector.as_deref(), Some("Get"));
    assert_eq!(events[0].target.as_deref(), Some("notes"));
    assert_eq!(events[1].event.outcome, "target-not-granted");
    assert_eq!(events[1].target, None, "a denied target is not audited");
    assert_eq!(events[2].event.outcome, "malformed-request");
    assert_eq!(events[2].selector.as_deref(), Some("unknown"));
    assert_eq!(events[2].target, None);
    let rendered = format!("{events:?}");
    assert!(!rendered.contains("selector-marker"));
}

#[tokio::test]
async fn call_audit_leaves_unvalidated_resolution_metadata_absent() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(Arc::clone(&audit), 1024).await;

    fixture.callers.hide_peer.store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .endpoint
            .call(audit_get(Some(&fixture.sender), "notes"))
            .await,
        Err(HostFailure::Denied("unknown-caller"))
    ));
    fixture.callers.hide_peer.store(false, Ordering::Release);

    fixture
        .environment
        .fail_resolve
        .store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .endpoint
            .call(audit_get(Some(&fixture.sender), "notes"))
            .await,
        Err(HostFailure::Denied("test-resolution-refused"))
    ));
    fixture
        .environment
        .fail_resolve
        .store(false, Ordering::Release);

    let events = audit.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.outcome, "unknown-caller");
    assert!(!events[0].has_module);
    assert_eq!(events[0].canonical_scope, None);
    assert_eq!(events[0].target, None);
    assert_eq!(events[1].event.outcome, "test-resolution-refused");
    assert!(events[1].has_module);
    assert_eq!(events[1].canonical_scope, None);
    assert_eq!(events[1].target, None);
}

#[tokio::test]
async fn call_audit_records_fenced_writes_and_redacted_secret_targets() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(Arc::clone(&audit), 1024).await;

    fixture
        .environment
        .fail_write
        .store(true, Ordering::Release);
    assert_eq!(
        fixture
            .endpoint
            .call(StorageCall {
                object: Object::Queues,
                selector: "Enqueue",
                request: json!({
                    "queue":"inbound",
                    "messages":[{"payload_json":"{\"audit\":true}"}]
                }),
                sender: Some(&fixture.sender),
                admission: Admission::Private,
            })
            .await,
        Err(HostFailure::Storage(StorageFailure {
            kind: StorageKind::Fenced,
        }))
    );
    fixture
        .environment
        .fail_write
        .store(false, Ordering::Release);

    assert!(matches!(
        fixture
            .endpoint
            .call(StorageCall {
                object: Object::Secrets,
                selector: "Get",
                request: json!({"name":"private/provider-key"}),
                sender: Some(&fixture.sender),
                admission: Admission::Confidential,
            })
            .await,
        Err(HostFailure::Storage(_))
    ));

    let events = audit.events();
    assert_eq!(events.len(), 2, "each completed call emits one record");
    assert_eq!(events[0].event.outcome, "fenced");
    assert!(events[0].event.write);
    assert_eq!(events[0].target.as_deref(), Some("inbound"));
    assert_eq!(events[1].event.outcome, "storage-error");
    assert_eq!(events[1].target.as_deref(), Some("[REDACTED]"));
    let rendered = format!("{events:?}");
    assert!(!rendered.contains("private/provider-key"));
}

#[tokio::test]
async fn call_audit_metadata_debug_redacts_scope_and_target() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(audit, 1024).await;
    let metadata = CallAuditMetadata {
        module: Some(&fixture.callers.module),
        canonical_scope: Some("scope-marker"),
        selector: "Get",
        target: Some("target-marker"),
    };
    let debug = format!("{metadata:?}");
    assert!(debug.contains("Get"));
    assert!(!debug.contains("scope-marker"));
    assert!(!debug.contains("target-marker"));
    assert!(debug.contains("[validated]"));
}

#[tokio::test]
async fn call_audit_records_guarded_panics_and_stream_entry_results_once() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(Arc::clone(&audit), 1).await;
    fixture
        .environment
        .panic_resolve
        .store(true, Ordering::Release);
    assert!(matches!(
        fixture
            .endpoint
            .call(audit_get(Some(&fixture.sender), "notes"))
            .await,
        Err(HostFailure::Storage(_))
    ));
    fixture
        .environment
        .panic_resolve
        .store(false, Ordering::Release);

    let oversized = tinystoragedrivers_bus::PutBlobStreamRequest {
        key: "attachments/large".to_string(),
        len: 2,
        content_type: None,
        ttl_ms: None,
    };
    assert!(
        fixture
            .endpoint
            .put_blob_stream(
                Some(&fixture.sender),
                oversized,
                Box::pin(tokio::io::empty()),
            )
            .await
            .is_err()
    );
    let oversized_until = tinystoragedrivers_bus::PutBlobStreamUntilRequest {
        key: "attachments/large".to_string(),
        len: 2,
        content_type: None,
        expires_at_ms: None,
    };
    assert!(
        fixture
            .endpoint
            .put_blob_stream_until(
                Some(&fixture.sender),
                oversized_until,
                Box::pin(tokio::io::empty()),
            )
            .await
            .is_err()
    );

    let missing = tinystoragedrivers_bus::BlobKeyRequest {
        key: "attachments/missing".to_string(),
    };
    let _ = fixture
        .endpoint
        .get_blob_stream(Some(&fixture.sender), missing.clone())
        .await;
    let _ = fixture
        .endpoint
        .get_blob_stream_with_expiry(Some(&fixture.sender), missing)
        .await;

    let events = audit.events();
    assert_eq!(
        events.len(),
        5,
        "each public request and stream entry logs once"
    );
    assert_eq!(
        events[0].event.outcome, "storage-error",
        "guarded panic is sanitized"
    );
    assert_eq!(
        events[1].event.outcome, "denied",
        "oversized upload is refused"
    );
    assert_eq!(events[1].selector.as_deref(), Some("PutStream"));
    assert!(events[1].event.write);
    assert_eq!(
        events[2].event.outcome, "denied",
        "oversized upload is refused"
    );
    assert_eq!(events[2].selector.as_deref(), Some("PutStreamUntil"));
    assert!(events[2].event.write);
    assert_eq!(events[3].selector.as_deref(), Some("GetStream"));
    assert_eq!(events[4].selector.as_deref(), Some("GetStreamWithExpiry"));
}

#[tokio::test]
async fn cancelling_a_governed_call_does_not_emit_a_synthetic_audit_event() {
    let audit = Arc::new(RecordingCallAudit::default());
    let fixture = call_audit_fixture(Arc::clone(&audit), 1024).await;
    let pause = Arc::new(ResolvePause {
        entered: tokio::sync::Notify::new(),
        release: Semaphore::new(0),
    });
    *fixture.environment.resolve_pause.lock().unwrap() = Some(Arc::clone(&pause));

    let endpoint = Arc::clone(&fixture.endpoint);
    let sender = fixture.sender.clone();
    let call = tokio::spawn(async move { endpoint.call(audit_get(Some(&sender), "notes")).await });
    tokio::time::timeout(Duration::from_secs(2), pause.entered.notified())
        .await
        .expect("the call reached the gated resolution");
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert!(
        audit.events().is_empty(),
        "cancellation is not a completed call"
    );
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
        lease: fixed_lease_claims(1, peer.clone(), &module),
        limits: GovernedEndpointLimits::default(),
        binding_slots: Arc::new(Semaphore::new(2)),
        runtime: tokio::runtime::Handle::current(),
    }));
    let (paused_tx, mut paused_rx) = mpsc::unbounded_channel();
    let (reset_sent_tx, mut reset_sent_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let release_backlog = Arc::new(Semaphore::new(0));
    *endpoint.relay_recv_test_gate.lock().unwrap() = Some(RelayRecvTestGate {
        reached: paused_tx,
        release: Arc::clone(&release),
        after_lag: Some(RelayRecvTestStage {
            reached: reset_sent_tx,
            release: Arc::clone(&release_backlog),
        }),
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
    tokio::time::timeout(Duration::from_secs(2), reset_sent_rx.recv())
        .await
        .expect("lagged relay did not publish its backend reset")
        .unwrap();
    let reset = tokio::time::timeout(Duration::from_secs(2), signals.recv())
        .await
        .expect("outward signal observer did not receive backend reset")
        .unwrap_or_else(|error| panic!("outward signal observer lagged: {error:?}"));
    assert_eq!(reset.watch_id, watch_id);
    assert_eq!(
        reset.notice,
        WatchNotice::ResetRequired {
            reason: WatchReset::Backend
        }
    );
    release_backlog.add_permits(1);
    for _ in 0..256 {
        let signal = tokio::time::timeout(Duration::from_secs(2), signals.recv())
            .await
            .expect("retained backend invalidations did not drain")
            .unwrap_or_else(|error| panic!("outward signal observer lagged: {error:?}"));
        assert_eq!(signal.watch_id, watch_id);
    }
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

#[cfg(feature = "test-stream-diagnostics")]
struct NormalShutdownErrorFixture {
    _broker_task: tokio::task::JoinHandle<tinybus::Result<()>>,
    host: ModuleHost,
    caller: tinybus::Connection,
    service: tinybus::Connection,
    observer: tinybus::Connection,
    service_peer: BusName,
    registration: StorageRegistration<RelayEnvironment, RelayAudit, RelayContext>,
    shutdown_stages: mpsc::UnboundedReceiver<crate::test_support::RegistrationShutdownStage>,
    name_changes: tokio::sync::broadcast::Receiver<tinybus::Message>,
}

#[cfg(feature = "test-stream-diagnostics")]
async fn normal_shutdown_error_fixture() -> NormalShutdownErrorFixture {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        relay_test_caller::linked_module().unwrap(),
        json!({}),
        move |connection| async move {
            caller_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("shutdown caller receiver closed"))
        },
    )
    .await
    .unwrap();
    let caller = tokio::time::timeout(Duration::from_secs(2), caller_rx)
        .await
        .expect("linked caller setup did not finish")
        .unwrap();
    let caller_peer = caller.unique_name().unwrap();
    let module = host
        .sender_lookup()
        .module_for_sender(&caller_peer)
        .unwrap();

    let service = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_peer = service.unique_name().unwrap();
    let observer = tinybus::Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let name_changes = observer
        .add_match(
            tinybus::MatchRule::new()
                .signals()
                .sender(tinybus::BusName::new(tinybus::BUS_NAME).unwrap())
                .interface(tinybus::InterfaceName::new(tinybus::BUS_INTERFACE).unwrap())
                .member(tinybus::MemberName::new("NameOwnerChanged").unwrap()),
        )
        .await
        .unwrap();
    let collection = WireCollectionSpec {
        name: "records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let storage = MemoryStorage::new();
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
        servers: [(1, server)].into_iter().collect(),
    });
    let policy = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(RelayAudit),
        GovernedEndpointLimits::default(),
    )
    .with_legacy_endpoint(Arc::new(DropCountingLegacyEndpoint(Arc::new(
        AtomicU64::new(0),
    ))));
    let (shutdown_probe, shutdown_stages) = mpsc::unbounded_channel();
    let registration = policy.with_shutdown_probe(shutdown_probe).register(
        service.clone(),
        Arc::new(RelayCallerDirectory {
            peer: caller_peer,
            module,
        }),
        tokio::runtime::Handle::current(),
    );
    registration.install_legacy().await.unwrap();

    NormalShutdownErrorFixture {
        _broker_task: broker_task,
        host,
        caller,
        service,
        observer,
        service_peer,
        registration,
        shutdown_stages,
        name_changes,
    }
}

#[cfg(feature = "test-stream-diagnostics")]
async fn wait_for_service_peer_detach(
    changes: &mut tokio::sync::broadcast::Receiver<tinybus::Message>,
    service_peer: &BusName,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let message = changes
                .recv()
                .await
                .expect("broker name-change stream closed");
            let serde_json::Value::Array(arguments) = message.body else {
                continue;
            };
            if arguments.len() == 3
                && arguments[0].as_str() == Some(service_peer.as_str())
                && arguments[1].as_str() == Some(service_peer.as_str())
                && arguments[2].is_null()
            {
                break;
            }
        }
    })
    .await
    .expect("connection close must detach the service peer");
}

#[cfg(feature = "test-stream-diagnostics")]
#[tokio::test(flavor = "multi_thread")]
async fn normal_shutdown_closes_connection_after_real_legacy_release_error() {
    let NormalShutdownErrorFixture {
        _broker_task,
        host,
        caller,
        service,
        observer,
        service_peer,
        registration,
        mut shutdown_stages,
        mut name_changes,
    } = normal_shutdown_error_fixture().await;

    // This is a successful broker operation from the actual name owner. The
    // subsequent registration shutdown therefore receives a real
    // NameHasNoOwner reply from its ReleaseName call.
    service.release_name(crate::HOST_NAME).await.unwrap();
    let shutdown = registration.shutdown().await;
    assert!(
        matches!(
            &shutdown,
            Err(tinybus::Error::MethodFailed { name, .. })
                if name == tinybus::Error::NameHasNoOwner(
                    tinybus::BusName::new(crate::HOST_NAME).unwrap()
                ).wire_name()
        ),
        "normal shutdown should preserve the real ReleaseName failure: {shutdown:?}"
    );

    let mut observed_stages = Vec::new();
    while let Ok(stage) = shutdown_stages.try_recv() {
        observed_stages.push(stage);
    }
    assert_eq!(
        observed_stages,
        [
            crate::test_support::RegistrationShutdownStage::LegacyShutdownStarted,
            crate::test_support::RegistrationShutdownStage::LegacyShutdownReturned,
            crate::test_support::RegistrationShutdownStage::ConnectionCloseStarted,
            crate::test_support::RegistrationShutdownStage::ConnectionCloseReturned,
        ],
        "normal shutdown must attempt connection close after legacy ReleaseName fails"
    );
    wait_for_service_peer_detach(&mut name_changes, &service_peer).await;
    assert_eq!(observer.name_owner(crate::HOST_NAME).await.unwrap(), None);
    assert_eq!(
        observer.name_owner(service_peer.as_str()).await.unwrap(),
        None
    );
    assert!(
        host.sender_lookup()
            .module_for_sender(&caller.unique_name().unwrap())
            .is_some()
    );
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
    let lease = fixed_lease_claims(1, peer.clone(), &module);
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
    let lease = fixed_lease_claims(1, peer.clone(), &module);
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
    assert_eq!(same_epoch_events.len(), 0);
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
        module: module.clone(),
    });
    let endpoint = Arc::new(GovernedEndpoint::for_lease(LeaseEndpointConfig {
        environment: Arc::clone(&environment),
        audit: Arc::new(RelayAudit),
        callers,
        context: HostStorageContext::new(RelayContext),
        lease: fixed_lease_claims(1, peer.clone(), &module),
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
        lease: fixed_lease_claims(1, peer.clone(), &module),
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
        lease: fixed_lease_claims(1, peer.clone(), &module),
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
