use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{CollectionSpec, Precondition, PutRequest, StorageNeeds};
use tinystoragedrivers_busproxy::{
    AuditEvent, AuditSink, Environment, GovernedEndpointLimits, GovernedStorage, GrantPolicy,
    HostContext, HostFailure, ResolvedStorage, StorageGrant,
};
use tinystoragedrivers_client::{StorageServer, StorageTransport};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use tinystoragedrivers_secrets::{DerivedKeys, DocumentSecrets};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio::sync::mpsc;

#[test]
fn stream_error_test_diagnostic_is_content_free_across_crate_boundary() {
    let sdk_error = tinybus::Error::StreamAborted {
        reason: "private stream reason".to_owned(),
    };
    let error = tinystoragedrivers_busproxy::test_support::diagnostic_io_error(&sdk_error);

    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(error.to_string(), "storage blob stream failed");
    assert_eq!(
        tinystoragedrivers_busproxy::test_support::stream_error_category(&error),
        Some("StreamAborted/Other")
    );
    assert!(!format!("{error:?}").contains("private stream reason"));
}

mod caller_export {
    async fn setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinystorage.TestCaller"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

struct AliasMarkerEndpoint {
    hits: Arc<Mutex<Vec<String>>>,
    drop_counter: Option<Arc<AtomicUsize>>,
}

impl Drop for AliasMarkerEndpoint {
    fn drop(&mut self) {
        if let Some(counter) = &self.drop_counter {
            counter.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl AliasMarkerEndpoint {
    fn record(&self, route: impl Into<String>) {
        self.hits.lock().unwrap().push(route.into());
    }
}

#[async_trait]
impl tinystoragedrivers_busproxy::StorageEndpoint for AliasMarkerEndpoint {
    async fn call(
        &self,
        call: tinystoragedrivers_busproxy::StorageCall<'_>,
    ) -> Result<Value, HostFailure> {
        self.record(format!("call:{:?}:{}", call.object, call.selector));
        Ok(json!({"service":"alias-b"}))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&tinybus::BusName>,
        _head: tinystoragedrivers_bus::PutBlobStreamRequest,
        _body: tinystoragedrivers_core::BlobReader,
    ) -> Result<Value, HostFailure> {
        self.record("upload");
        Ok(json!({"service":"alias-b"}))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&tinybus::BusName>,
        _request: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> Result<
        Option<(
            tinystoragedrivers_bus::BlobStreamHead,
            tinystoragedrivers_core::BlobReader,
        )>,
        HostFailure,
    > {
        self.record("download");
        Ok(Some((
            tinystoragedrivers_bus::BlobStreamHead {
                meta: tinystoragedrivers_bus::BlobInfo {
                    key: "alias-b".to_owned(),
                    len: 0,
                    content_type: None,
                },
            },
            Box::pin(std::io::Cursor::new(Vec::new())),
        )))
    }

    fn watch_signals(
        &self,
        _sender: &tinybus::BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged>> {
        None
    }
}

async fn attach_named_test_module(
    host: &ModuleHost,
    module_id: &str,
    bus_name: &str,
    object_path: &str,
) -> Connection {
    let (connection_tx, connection_rx) = tokio::sync::oneshot::channel();
    host.attach_linked_with_setup(
        named_test_export(module_id, bus_name, object_path),
        json!({}),
        move |connection| async move {
            connection_tx
                .send(connection.clone())
                .map_err(|_| tinybus::Error::failed("test connection receiver closed"))
        },
    )
    .await
    .unwrap();
    connection_rx.await.unwrap()
}

fn named_test_export(
    module_id: &str,
    bus_name: &str,
    object_path: &str,
) -> tinybus::module::LinkedModule {
    let mut export = caller_export::linked_module().unwrap();
    export.descriptor.module_name = [0; 64];
    export.descriptor.module_name[..module_id.len()].copy_from_slice(module_id.as_bytes());
    export.manifest.module.name = module_id.to_owned();
    export.manifest.bus_name = bus_name.parse().unwrap();
    export.manifest.object_path = object_path.parse().unwrap();
    export
}

#[derive(Debug)]
struct Profile(&'static str);
impl HostContext for Profile {}

struct ToggleCallerDirectory {
    lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory,
    enabled: Arc<AtomicBool>,
    revoked_peer: tinybus::BusName,
}

/// Explicit fixture admission map for real broker connections that are not
/// separately loaded as 257 module workers.
struct PeerMapDirectory(Mutex<HashMap<tinybus::BusName, tinybus::module::ModuleId>>);

impl tinystoragedrivers_busproxy::CallerDirectory for PeerMapDirectory {
    fn module_for_sender(&self, sender: &tinybus::BusName) -> Option<tinybus::module::ModuleId> {
        self.0.lock().unwrap().get(sender).cloned()
    }
}

impl tinystoragedrivers_busproxy::CallerDirectory for ToggleCallerDirectory {
    fn module_for_sender(&self, sender: &tinybus::BusName) -> Option<tinybus::module::ModuleId> {
        if sender == &self.revoked_peer && !self.enabled.load(Ordering::Acquire) {
            return None;
        }
        tinystoragedrivers_busproxy::CallerDirectory::module_for_sender(&self.lookup, sender)
    }
}

struct SelectiveCallerDirectory {
    lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory,
    service_peer: tinybus::BusName,
    service_live: Arc<AtomicBool>,
}

struct RevokingCallerDirectory {
    lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory,
    revoked_peer: tinybus::BusName,
    revoked: Arc<AtomicBool>,
}

impl tinystoragedrivers_busproxy::CallerDirectory for RevokingCallerDirectory {
    fn module_for_sender(&self, sender: &tinybus::BusName) -> Option<tinybus::module::ModuleId> {
        if sender == &self.revoked_peer && self.revoked.load(Ordering::Acquire) {
            return None;
        }
        tinystoragedrivers_busproxy::CallerDirectory::module_for_sender(&self.lookup, sender)
    }
}

impl tinystoragedrivers_busproxy::CallerDirectory for SelectiveCallerDirectory {
    fn module_for_sender(&self, sender: &tinybus::BusName) -> Option<tinybus::module::ModuleId> {
        if sender == &self.service_peer && !self.service_live.load(Ordering::Acquire) {
            return None;
        }
        tinystoragedrivers_busproxy::CallerDirectory::module_for_sender(&self.lookup, sender)
    }
}

struct TestEnvironment {
    backend: Arc<MemoryStorage>,
    collection: CollectionSpec,
    servers: Mutex<std::collections::HashMap<String, Arc<StorageServer>>>,
    schema: tokio::sync::OnceCell<()>,
    clock_gate: Arc<TestClockGate>,
    binding_epoch: std::sync::atomic::AtomicU64,
    resolve_gate: Arc<TestResolveGate>,
    write_gate: Arc<TestResolveGate>,
    panic_next_resolution: AtomicBool,
    scope_overrides: Mutex<std::collections::HashMap<String, String>>,
    scope_epochs: Mutex<std::collections::HashMap<String, u64>>,
    resolve_calls: AtomicUsize,
    schema_initializations: AtomicUsize,
    key_provider_calls: AtomicUsize,
    write_admissions: AtomicUsize,
    writable_grant: AtomicBool,
    write_admitted: AtomicBool,
    fail_key_provider: AtomicBool,
}

struct TestResolveGate {
    armed: AtomicBool,
    started: Mutex<Option<mpsc::UnboundedSender<()>>>,
    release: Arc<tokio::sync::Semaphore>,
}

impl Default for TestResolveGate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            started: Mutex::new(None),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl TestResolveGate {
    fn arm(&self) -> mpsc::UnboundedReceiver<()> {
        let (started, receiver) = mpsc::unbounded_channel();
        *self.started.lock().unwrap() = Some(started);
        self.armed.store(true, Ordering::Release);
        receiver
    }

    fn release(&self) {
        self.release.add_permits(1);
    }

    async fn wait_if_armed(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            self.release.acquire().await.unwrap().forget();
        }
    }
}

#[derive(Default)]
struct TestClockGate {
    armed: AtomicBool,
    state: Mutex<(bool, bool)>,
    changed: Condvar,
    started: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

impl TestClockGate {
    fn arm(&self) -> mpsc::UnboundedReceiver<()> {
        let (started, receiver) = mpsc::unbounded_channel();
        let mut state = self.state.lock().unwrap();
        state.0 = false;
        state.1 = false;
        *self.started.lock().unwrap() = Some(started);
        self.armed.store(true, Ordering::Release);
        receiver
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }

    fn now(&self) -> u64 {
        if self.armed.swap(false, Ordering::AcqRel) {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).unwrap();
            }
        }
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .expect("system time fits the test backend clock")
    }
}

#[async_trait]
impl Environment<Profile> for TestEnvironment {
    async fn resolve(
        &self,
        context: &Profile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<ResolvedStorage, HostFailure> {
        self.resolve_calls.fetch_add(1, Ordering::AcqRel);
        if self.panic_next_resolution.swap(false, Ordering::AcqRel) {
            std::panic::panic_any(PrivateBackendPanic);
        }
        let canonical_scope = self
            .scope_overrides
            .lock()
            .unwrap()
            .get(context.0)
            .cloned()
            .unwrap_or_else(|| context.0.to_owned());
        let scope =
            Scope::new(&canonical_scope).map_err(|_| HostFailure::Denied("invalid-test-scope"))?;
        let storage = self
            .backend
            .for_scope(&scope)
            .map_err(|_| HostFailure::Denied("test-backend-unavailable"))?;
        self.schema
            .get_or_try_init(|| async {
                self.schema_initializations.fetch_add(1, Ordering::AcqRel);
                storage
                    .documents()
                    .ensure_collection(&tinystoragedrivers_core::CollectionSpec::new(
                        self.collection.name.clone(),
                    ))
                    .await
                    .map_err(|_| HostFailure::Denied("test-schema-unavailable"))
            })
            .await?;
        let exact_secret_names = ["provider.key"];
        let blob_prefixes = ["canary/"];
        let stream_prefixes = ["events/"];
        let needs = StorageNeeds {
            collections: std::slice::from_ref(&self.collection),
            blob_prefixes: &blob_prefixes,
            stream_prefixes: &stream_prefixes,
            ..StorageNeeds::default()
        }
        .with_exact_secret_names(&exact_secret_names);
        let grant = StorageGrant::from_needs_with_exact_secrets(
            needs,
            &GrantPolicy {
                writable: self.writable_grant.load(Ordering::Acquire),
                allow_secrets: true,
                ..GrantPolicy::default()
            },
        )
        .map_err(|_| HostFailure::Denied("test-grant-invalid"))?;
        self.key_provider_calls.fetch_add(1, Ordering::AcqRel);
        if self.fail_key_provider.load(Ordering::Acquire) {
            return Err(HostFailure::Denied("key-provider-unavailable"));
        }
        let keys = Arc::new(
            DerivedKeys::from_hex(
                "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            )
            .unwrap(),
        );
        let secrets = Arc::new(DocumentSecrets::new(&storage, keys));
        let key = canonical_scope;
        let candidate = Arc::new(StorageServer::new(storage).with_secrets(secrets));
        let server = {
            let mut servers = self.servers.lock().unwrap();
            Arc::clone(servers.entry(key.clone()).or_insert(candidate))
        };
        let binding_epoch = self
            .scope_epochs
            .lock()
            .unwrap()
            .get(context.0)
            .copied()
            .unwrap_or_else(|| self.binding_epoch.load(Ordering::Acquire));
        let resolved = ResolvedStorage {
            backend_identity: format!("memory-test-backend-v{binding_epoch}"),
            binding_epoch,
            canonical_scope: key,
            grant,
            server,
        };
        self.resolve_gate.wait_if_armed().await;
        Ok(resolved)
    }

    async fn admit_write(
        &self,
        _context: &Profile,
        _module: &tinybus::module::ModuleId,
    ) -> Result<(), HostFailure> {
        self.write_admissions.fetch_add(1, Ordering::AcqRel);
        if self.write_admitted.load(Ordering::Acquire) {
            self.write_gate.wait_if_armed().await;
            Ok(())
        } else {
            Err(HostFailure::Denied("write-admission-closed"))
        }
    }
}

impl TestEnvironment {
    fn server_for(&self, profile: &str) -> Arc<StorageServer> {
        Arc::clone(self.servers.lock().unwrap().get(profile).unwrap())
    }

    fn set_scope_epoch(&self, context: &str, epoch: u64) {
        self.scope_epochs
            .lock()
            .unwrap()
            .insert(context.to_owned(), epoch);
    }

    fn clear_scope_epoch(&self, context: &str) {
        self.scope_epochs.lock().unwrap().remove(context);
    }
}

fn test_environment(collection: CollectionSpec) -> Arc<TestEnvironment> {
    let clock_gate = Arc::new(TestClockGate::default());
    let clock = Arc::clone(&clock_gate);
    Arc::new(TestEnvironment {
        backend: Arc::new(
            MemoryStorage::with_clock(Arc::new(move || clock.now())).enable_native_watch(),
        ),
        collection,
        servers: Mutex::new(std::collections::HashMap::new()),
        schema: tokio::sync::OnceCell::new(),
        clock_gate,
        binding_epoch: std::sync::atomic::AtomicU64::new(1),
        resolve_gate: Arc::new(TestResolveGate::default()),
        write_gate: Arc::new(TestResolveGate::default()),
        panic_next_resolution: AtomicBool::new(false),
        scope_overrides: Mutex::new(std::collections::HashMap::new()),
        scope_epochs: Mutex::new(std::collections::HashMap::new()),
        resolve_calls: AtomicUsize::new(0),
        schema_initializations: AtomicUsize::new(0),
        key_provider_calls: AtomicUsize::new(0),
        write_admissions: AtomicUsize::new(0),
        writable_grant: AtomicBool::new(true),
        write_admitted: AtomicBool::new(true),
        fail_key_provider: AtomicBool::new(false),
    })
}

async fn governed_mutation_errors(
    transport: &tinystoragedrivers_busproxy::BusTransport,
    marker: &str,
) -> [Option<String>; 6] {
    let document = transport
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "shared.records".to_owned(),
                id: format!("denied-doc-{marker}"),
                doc: json!({"marker":marker}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    let stream = transport
        .call(
            tinystoragedrivers_client::Object::Streams,
            "Append",
            serde_json::to_value(tinystoragedrivers_bus::AppendRequest {
                stream: format!("events/{marker}"),
                record: json!({"marker":marker}),
            })
            .unwrap(),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    let secret = transport
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Set",
            serde_json::to_value(tinystoragedrivers_bus::SetSecretRequest {
                name: "provider.key".to_owned(),
                value: marker.as_bytes().to_vec(),
            })
            .unwrap(),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    let blob = transport
        .put_blob_stream(
            tinystoragedrivers_bus::PutBlobStreamRequest {
                key: format!("canary/{marker}"),
                len: 1,
                content_type: None,
                ttl_ms: None,
            },
            Box::pin(std::io::Cursor::new(vec![marker.as_bytes()[0]])),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    let blob_until = transport
        .call(
            tinystoragedrivers_client::Object::Blobs,
            "PutUntil",
            json!({
                "key": format!("canary/{marker}-absolute"),
                "bytes": [marker.as_bytes()[0]],
                "expires_at_ms": null
            }),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    let blob_stream_until = transport
        .put_blob_stream_until(
            tinystoragedrivers_bus::PutBlobStreamUntilRequest {
                key: format!("canary/{marker}-absolute-stream"),
                len: 1,
                content_type: None,
                expires_at_ms: None,
            },
            Box::pin(std::io::Cursor::new(vec![marker.as_bytes()[0]])),
        )
        .await
        .err()
        .map(|error| format!("{error:?}"));
    [
        document,
        stream,
        secret,
        blob,
        blob_until,
        blob_stream_until,
    ]
}

#[derive(Default)]
struct TestAudit(Mutex<Vec<AuditEvent>>);
impl AuditSink for TestAudit {
    fn record(&self, event: AuditEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[derive(Debug)]
struct PrivateBackendPanic;

struct FailsAfterPrefix(Vec<u8>);

impl AsyncRead for FailsAfterPrefix {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.0.is_empty() {
            return Poll::Ready(Err(std::io::Error::other(
                "fixture source failed after its prefix",
            )));
        }
        let amount = self.0.len().min(buffer.remaining());
        let prefix = self.0.drain(..amount).collect::<Vec<_>>();
        buffer.put_slice(&prefix);
        Poll::Ready(Ok(()))
    }
}

struct PendingAfterPrefix {
    prefix: Vec<u8>,
    started: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl AsyncRead for PendingAfterPrefix {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.prefix.is_empty() {
            let amount = self.prefix.len().min(buffer.remaining());
            let prefix = self.prefix.drain(..amount).collect::<Vec<_>>();
            buffer.put_slice(&prefix);
            return Poll::Ready(Ok(()));
        }
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        Poll::Pending
    }
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This linked broker test keeps two owners, a real peer, and their cleanup alive through the cross-owner assertions."
)]
async fn a_lease_handle_from_another_registration_cannot_revoke_equal_id() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    let mut caller_export = caller_export::linked_module().unwrap();
    let caller_id = "governed-revoke-caller";
    caller_export.descriptor.module_name = [0; 64];
    caller_export.descriptor.module_name[..caller_id.len()].copy_from_slice(caller_id.as_bytes());
    caller_export.manifest.module.name = caller_id.to_owned();
    caller_export.manifest.bus_name = "ai.tinyhumans.test.governedrevoke".parse().unwrap();
    caller_export.manifest.object_path = "/ai/tinyhumans/test/governed_revoke".parse().unwrap();
    host.attach_linked_with_setup(caller_export, json!({}), move |connection| async move {
        caller_tx
            .send(connection.clone())
            .map_err(|_| tinybus::Error::failed("test caller receiver closed"))
    })
    .await
    .unwrap();
    let caller = caller_rx.await.unwrap();
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let governed = GovernedStorage::new(
        test_environment(collection.clone()),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .with_max_leases(1)
    .with_max_concurrent_bindings(1);
    let caller_admission = Arc::new(AtomicBool::new(true));
    let callers: Arc<dyn tinystoragedrivers_busproxy::CallerDirectory> =
        Arc::new(ToggleCallerDirectory {
            lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory(host.sender_lookup()),
            enabled: Arc::clone(&caller_admission),
            revoked_peer: caller.unique_name().unwrap(),
        });
    let owner_a = governed.clone().register(
        Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap(),
        Arc::clone(&callers),
        tokio::runtime::Handle::current(),
    );
    let owner_b = governed.register(
        Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap(),
        Arc::clone(&callers),
        tokio::runtime::Handle::current(),
    );
    let peer = caller.unique_name().unwrap();
    let (lease_a, descriptor_a) = owner_a
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-shared")),
            peer.clone(),
        )
        .await
        .unwrap();
    assert!(
        owner_a
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-overflow")),
                caller.unique_name().unwrap(),
            )
            .await
            .is_err(),
        "active lease capacity must bound published and pending leases"
    );
    let (lease_b, descriptor_b) = owner_b
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-shared")),
            peer,
        )
        .await
        .unwrap();
    let transport_b =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_b)
            .await
            .unwrap();
    let transport_a =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_a)
            .await
            .unwrap();

    let opened = transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            json!({
                "collection":"shared.records",
                "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                "interval_ms":1000
            }),
        )
        .await
        .unwrap();
    let watch_id = opened["watch_id"].as_str().unwrap().to_owned();
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "WatchClose",
                json!({"watch_id":watch_id}),
            )
            .await
            .is_err(),
        "a second lease must not close another lease's watch"
    );
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Documents,
                "WatchClose",
                json!({"watch_id":watch_id}),
            )
            .await
            .is_ok(),
        "the owning lease must still be able to close its watch"
    );

    assert!(!owner_b.revoke(&lease_a).await);
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_ok(),
        "a foreign handle must not withdraw the equal-id lease"
    );

    assert!(owner_a.revoke(&lease_a).await);
    assert!(owner_b.revoke(&lease_b).await);

    let owner_c_service = attach_named_test_module(
        &host,
        "governed-revoke-service",
        "ai.tinyhumans.test.governedrevokeservice",
        "/ai/tinyhumans/test/governed_revoke_service",
    )
    .await;
    let owner_c_service_peer = owner_c_service.unique_name().unwrap();
    assert!(
        host.sender_lookup()
            .module_for_sender(&owner_c_service_peer)
            .is_some(),
        "the observed service connection must be a live admitted module peer"
    );
    let owner_c = Arc::new(governed.register(
        owner_c_service,
        Arc::clone(&callers),
        tokio::runtime::Handle::current(),
    ));
    let context_c =
        tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-dropped"));
    assert_eq!(format!("{context_c:?}"), "HostStorageContext(..)");
    let (lease_c, descriptor_c) = owner_c
        .register_lease(context_c, caller.unique_name().unwrap())
        .await
        .unwrap();
    let rendered_lease = format!("{lease_c:?}");
    assert_eq!(rendered_lease, "StorageLease(..)");
    assert!(!rendered_lease.contains("profile-dropped"));
    let rendered_owner = format!("{owner_c:?}");
    assert!(!rendered_owner.contains("profile-dropped"));
    let transport_c =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_c)
            .await
            .unwrap();
    let retained_lease = lease_c.clone();
    drop(lease_c);
    assert!(
        transport_c
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_ok(),
        "dropping one clone must not revoke a lease retained by another clone"
    );
    drop(retained_lease);
    assert!(
        transport_c
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_err(),
        "dropping the only lease handle must revoke its published route"
    );
    let (lease_d, descriptor_d) = owner_c
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-churn")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_d =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_d)
            .await
            .unwrap();
    caller_admission.store(false, Ordering::Release);
    let (events, received) = tokio::sync::broadcast::channel(1);
    let name_owner_changed = || {
        tinybus::Message::signal(
            tinybus::ObjectPath::new("/org/freedesktop/DBus").unwrap(),
            tinybus::InterfaceName::new(tinybus::BUS_INTERFACE).unwrap(),
            tinybus::MemberName::new("NameOwnerChanged").unwrap(),
            Value::Null,
        )
    };
    events.send(name_owner_changed()).unwrap();
    events.send(name_owner_changed()).unwrap();
    let (reconciled_tx, mut reconciled_rx) = mpsc::unbounded_channel();
    let monitor = tokio::spawn(crate::storage_host::monitor_owner_changes(
        received,
        owner_c_service_peer,
        Arc::clone(&owner_c),
        Some(reconciled_tx),
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), reconciled_rx.recv())
            .await
            .unwrap()
            .unwrap(),
        1,
        "a lagged observer stream must reconcile and revoke stale callers"
    );
    caller_admission.store(true, Ordering::Release);
    assert!(
        transport_d
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_err(),
        "caller reconciliation must withdraw leases after admission loss"
    );
    monitor.abort();
    drop(lease_d);
    host.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This fixture distinguishes linked service-peer loss from a still-live caller across observer lag."
)]
async fn a_lagged_service_peer_detach_with_live_callers_withdraws_the_registration() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_connection = attach_named_test_module(
        &host,
        "governed-lagged-service",
        "ai.tinyhumans.test.laggedservice",
        "/ai/tinyhumans/test/lagged_service",
    )
    .await;
    let caller = attach_named_test_module(
        &host,
        "governed-lagged-caller",
        "ai.tinyhumans.test.laggedcaller",
        "/ai/tinyhumans/test/lagged_caller",
    )
    .await;
    let service_peer = service_connection.unique_name().unwrap();
    let caller_peer = caller.unique_name().unwrap();
    let service_live = Arc::new(AtomicBool::new(true));
    let callers: Arc<dyn tinystoragedrivers_busproxy::CallerDirectory> =
        Arc::new(SelectiveCallerDirectory {
            lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory(host.sender_lookup()),
            service_peer: service_peer.clone(),
            service_live: Arc::clone(&service_live),
        });
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let endpoint = GovernedStorage::new(
        test_environment(collection.clone()),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let registration = Arc::new(endpoint.register(
        service_connection,
        Arc::clone(&callers),
        tokio::runtime::Handle::current(),
    ));
    let (_lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-lagged-service")),
            caller_peer.clone(),
        )
        .await
        .unwrap();
    let transport =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor)
            .await
            .unwrap();
    assert!(
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"before-detach"}),
            )
            .await
            .is_ok()
    );

    service_live.store(false, Ordering::Release);
    let (events, received) = tokio::sync::broadcast::channel(1);
    let event = |peer: &str, new_owner: Value| {
        tinybus::Message::signal(
            tinybus::ObjectPath::new("/org/freedesktop/DBus").unwrap(),
            tinybus::InterfaceName::new(tinybus::BUS_INTERFACE).unwrap(),
            tinybus::MemberName::new("NameOwnerChanged").unwrap(),
            Value::Array(vec![
                Value::String(peer.to_owned()),
                Value::String("owner".to_owned()),
                new_owner,
            ]),
        )
    };
    assert!(callers.module_for_sender(&caller_peer).is_some());
    events
        .send(event(service_peer.as_str(), Value::Null))
        .unwrap();
    events
        .send(event(
            ":1.999",
            Value::String("ai.tinyhumans.test.retained_alias".to_owned()),
        ))
        .unwrap();
    let (reconciled_tx, mut reconciled_rx) = mpsc::unbounded_channel();
    let monitor = tokio::spawn(crate::storage_host::monitor_owner_changes(
        received,
        service_peer,
        Arc::clone(&registration),
        Some(reconciled_tx),
    ));
    let caller_reconciliation = tokio::time::timeout(Duration::from_secs(1), reconciled_rx.recv())
        .await
        .expect("observer lag was not recovered")
        .unwrap();
    assert_eq!(caller_reconciliation, 0, "the caller remains live");
    assert!(callers.module_for_sender(&caller_peer).is_some());

    let mut withdrawn = false;
    for _ in 0..100 {
        if transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"after-detach"}),
            )
            .await
            .is_err()
        {
            withdrawn = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        withdrawn,
        "a lagged service-peer detach must withdraw routes even while caller peers remain live"
    );
    assert!(matches!(
        registration
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                    "profile-after-service-detach"
                )),
                caller_peer.clone(),
            )
            .await,
        Err(HostFailure::Denied("registration-shutdown"))
    ));
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    monitor.abort();
    host.shutdown(Duration::from_secs(1)).await;
    let _ = registration;
}

#[tokio::test(flavor = "multi_thread")]
async fn attach_rolls_back_legacy_name_collisions_and_observer_refusal() {
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let blocker = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    blocker
        .request_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let blocked_endpoint = GovernedStorage::new(
        test_environment(collection.clone()),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    assert!(
        attach_storage(
            &host,
            &observer,
            blocked_endpoint,
            tokio::runtime::Handle::current(),
        )
        .await
        .is_err(),
        "the linked registration must roll back if its compatibility name is occupied"
    );
    assert_eq!(
        observer
            .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
            .await
            .unwrap(),
        blocker.unique_name(),
        "rollback must preserve the existing owner of the occupied alias"
    );
    blocker.close().await.unwrap();
    host.shutdown(Duration::from_secs(1)).await;

    let observer_bus = MemoryBus::new();
    let observer_broker = Broker::new();
    let _observer_broker_task = observer_broker.clone().spawn(observer_bus.clone());
    let observer_host = ModuleHost::new(observer_broker);
    let rejected_observer = Connection::connect(observer_bus.connect().await.unwrap())
        .await
        .unwrap();
    rejected_observer.close().await.unwrap();
    let observer_endpoint = GovernedStorage::new(
        test_environment(collection),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    assert!(
        attach_storage(
            &observer_host,
            &rejected_observer,
            observer_endpoint,
            tokio::runtime::Handle::current(),
        )
        .await
        .is_err(),
        "failure to subscribe to owner changes must roll back the registration"
    );
    let probe = Connection::connect(observer_bus.connect().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        probe
            .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
            .await
            .unwrap(),
        None,
        "failed setup must not leave the storage well-known name claimed"
    );
    assert!(probe.name_owner(tinybus::BUS_NAME).await.is_ok());
    observer_host.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This test orders malformed notifications, a caller sentinel, and service detach."
)]
async fn owner_monitor_ignores_malformed_and_non_detach_messages() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = attach_named_test_module(
        &host,
        "governed-monitor-service",
        "ai.tinyhumans.test.monitorservice",
        "/ai/tinyhumans/test/monitor_service",
    )
    .await;
    let caller_a = attach_named_test_module(
        &host,
        "governed-monitor-caller-a",
        "ai.tinyhumans.test.monitorcallera",
        "/ai/tinyhumans/test/monitor_caller_a",
    )
    .await;
    let caller_b = attach_named_test_module(
        &host,
        "governed-monitor-caller-b",
        "ai.tinyhumans.test.monitorcallerb",
        "/ai/tinyhumans/test/monitor_caller_b",
    )
    .await;
    let service_peer = service.unique_name().unwrap();
    let first_caller_peer = caller_a.unique_name().unwrap();
    let second_caller_peer = caller_b.unique_name().unwrap();
    let callers: Arc<dyn tinystoragedrivers_busproxy::CallerDirectory> = Arc::new(
        tinystoragedrivers_busproxy::TinyBusCallerDirectory(host.sender_lookup()),
    );
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let registration = Arc::new(
        GovernedStorage::new(
            Arc::clone(&environment),
            Arc::new(TestAudit::default()),
            GovernedEndpointLimits::default(),
        )
        .register(
            service,
            Arc::clone(&callers),
            tokio::runtime::Handle::current(),
        ),
    );
    let (_lease_a, descriptor_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("monitor-a")),
            first_caller_peer.clone(),
        )
        .await
        .unwrap();
    let (_lease_b, descriptor_b) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("monitor-b")),
            second_caller_peer.clone(),
        )
        .await
        .unwrap();
    let transport_a =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller_a, descriptor_a)
            .await
            .unwrap();
    let transport_b =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller_b, descriptor_b)
            .await
            .unwrap();
    for transport in [&transport_a, &transport_b] {
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"before-events"}),
            )
            .await
            .unwrap();
    }

    let (events, received) = tokio::sync::broadcast::channel(8);
    let event = |body| {
        tinybus::Message::signal(
            tinybus::ObjectPath::new("/org/freedesktop/DBus").unwrap(),
            tinybus::InterfaceName::new(tinybus::BUS_INTERFACE).unwrap(),
            tinybus::MemberName::new("NameOwnerChanged").unwrap(),
            body,
        )
    };
    events.send(event(Value::Null)).unwrap();
    events
        .send(event(Value::Array(vec![Value::String(
            service_peer.to_string(),
        )])))
        .unwrap();
    events
        .send(event(Value::Array(vec![
            Value::String("not a valid bus name".to_owned()),
            Value::String("old-owner".to_owned()),
            Value::Null,
        ])))
        .unwrap();
    events
        .send(event(Value::Array(vec![
            Value::String("ai.tinyhumans.test.alias".to_owned()),
            Value::String("old-owner".to_owned()),
            Value::Null,
        ])))
        .unwrap();
    events
        .send(event(Value::Array(vec![
            Value::String(service_peer.to_string()),
            Value::String("old-owner".to_owned()),
            Value::String(":1.900".to_owned()),
        ])))
        .unwrap();
    events
        .send(event(Value::Array(vec![
            Value::String(first_caller_peer.to_string()),
            Value::String("old-owner".to_owned()),
            Value::Null,
        ])))
        .unwrap();

    let monitor = tokio::spawn(crate::storage_host::monitor_owner_changes(
        received,
        service_peer.clone(),
        Arc::clone(&registration),
        None,
    ));
    let caller_a_withdrawn = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if transport_a
                .call(
                    tinystoragedrivers_client::Object::Documents,
                    "Get",
                    json!({
                        "collection":"shared.records",
                        "document_id":"after-malformed-events"
                    }),
                )
                .await
                .is_err()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    assert!(
        caller_a_withdrawn,
        "the valid caller detach sentinel was not processed"
    );
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"malformed-events-kept-registration-live"}),
            )
            .await
            .is_ok(),
        "malformed or non-detach notifications must not close the whole registration"
    );
    events
        .send(event(Value::Array(vec![
            Value::String(service_peer.to_string()),
            Value::String("old-owner".to_owned()),
            Value::Null,
        ])))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), monitor)
        .await
        .expect("service detach was not processed")
        .unwrap();
    assert!(matches!(
        registration
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile("after-shutdown")),
                second_caller_peer,
            )
            .await,
        Err(HostFailure::Denied("registration-shutdown"))
    ));
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    host.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_syntactically_valid_forged_descriptor_never_reaches_storage_resolution() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = attach_named_test_module(
        &host,
        "governed-forged-route-caller",
        "ai.tinyhumans.test.governedforgedroutecaller",
        "/ai/tinyhumans/test/governed_forged_route_caller",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let registration = attach_storage(
        &host,
        &observer,
        GovernedStorage::new(
            Arc::clone(&environment),
            Arc::new(TestAudit::default()),
            GovernedEndpointLimits::default(),
        ),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("forged-route")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();

    let forged_id = 9_001_u64;
    let mut forged_wire = serde_json::to_value(&descriptor).unwrap();
    forged_wire["registration_id"] = json!(forged_id);
    forged_wire["object_root"] = json!(format!(
        "{}/Leases/{forged_id}",
        tinystoragedrivers_busproxy::OBJECT_ROOT
    ));
    let forged_descriptor: tinystoragedrivers_busproxy::StorageDescriptor =
        serde_json::from_value(forged_wire).unwrap();
    let forged_route =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, forged_descriptor)
            .await
            .unwrap();
    let before_forged_call = environment.resolve_calls.load(Ordering::Acquire);
    assert!(
        forged_route
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"forged-route"}),
            )
            .await
            .is_err(),
        "a parseable descriptor has no authority unless its exact route is served"
    );
    assert_eq!(
        environment.resolve_calls.load(Ordering::Acquire),
        before_forged_call,
        "a forged route must be rejected before backend selection or reads"
    );
    registration.shutdown().await.unwrap();
    drop(lease);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    host.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This integration test exercises the admission, rotation, and refusal matrix end to end."
)]
async fn governed_admission_and_grant_rotation_refuse_mutations_before_backend_effects() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = attach_named_test_module(
        &host,
        "governed-admission-caller",
        "ai.tinyhumans.test.governedadmissioncaller",
        "/ai/tinyhumans/test/governed_admission_caller",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let audit = Arc::new(TestAudit::default());
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::clone(&audit),
        GovernedEndpointLimits::default(),
    );
    let registration = attach_storage(
        &host,
        &observer,
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("governed-admission")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport = Arc::new(
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor)
            .await
            .unwrap(),
    );

    let schema_calls_before = environment.schema_initializations.load(Ordering::Acquire);
    let backend = environment
        .backend
        .for_scope(&Scope::new("governed-admission").unwrap())
        .unwrap();
    let inline_write = transport
        .put_blob_until(tinystoragedrivers_bus::PutBlobUntilRequest {
            key: "canary/absolute-inline".to_owned(),
            bytes: b"absolute".to_vec(),
            content_type: Some("text/plain".to_owned()),
            expires_at_ms: None,
        })
        .await
        .unwrap();
    let inline_meta: tinystoragedrivers_bus::BlobInfo =
        serde_json::from_value(inline_write).unwrap();
    assert_eq!(inline_meta.key, "canary/absolute-inline");
    let inline_source = transport
        .get_blob_with_expiry(tinystoragedrivers_bus::BlobKeyRequest {
            key: inline_meta.key.clone(),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inline_source.bytes, b"absolute");
    assert_eq!(inline_source.expires_at_ms, None);

    let streamed_write = transport
        .put_blob_stream_until(
            tinystoragedrivers_bus::PutBlobStreamUntilRequest {
                key: "canary/absolute-streamed".to_owned(),
                len: 8,
                content_type: Some("text/plain".to_owned()),
                expires_at_ms: None,
            },
            Box::pin(std::io::Cursor::new(b"streamed".to_vec())),
        )
        .await
        .unwrap();
    let streamed_meta: tinystoragedrivers_bus::BlobInfo =
        serde_json::from_value(streamed_write).unwrap();
    let (streamed_head, mut streamed_body) = transport
        .get_blob_stream_with_expiry(tinystoragedrivers_bus::BlobKeyRequest {
            key: streamed_meta.key,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed_head.expires_at_ms, None);
    let mut streamed_bytes = Vec::new();
    streamed_body
        .read_to_end(&mut streamed_bytes)
        .await
        .unwrap();
    assert_eq!(streamed_bytes, b"streamed");

    let undeclared_schema = transport
        .call(
            tinystoragedrivers_client::Object::Documents,
            "EnsureCollection",
            serde_json::to_value(tinystoragedrivers_bus::EnsureCollectionRequest {
                spec: CollectionSpec {
                    name: "unapproved.records".to_owned(),
                    indexes: vec![],
                    ttl_field: None,
                    search: None,
                },
            })
            .unwrap(),
        )
        .await;
    assert!(undeclared_schema.is_err());
    assert_eq!(
        environment.schema_initializations.load(Ordering::Acquire),
        schema_calls_before,
        "an undeclared schema must not initialize backend schema"
    );

    environment.write_admitted.store(false, Ordering::Release);
    let before_admission_denial = environment.write_admissions.load(Ordering::Acquire);
    assert_eq!(
        governed_mutation_errors(&transport, "admission-marker")
            .await
            .map(|error| error.is_some()),
        [true; 6],
        "all mutations must be refused"
    );
    assert_eq!(
        environment.write_admissions.load(Ordering::Acquire),
        before_admission_denial + 6,
        "each routed mutation should reach write admission; failures={:?}",
        governed_mutation_errors(&transport, "admission-diagnostic").await
    );
    assert!(
        backend
            .documents()
            .get("shared.records", "denied-doc-admission-marker")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        backend
            .streams()
            .read_window("events/admission-marker", 0, 10)
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(
        backend
            .blobs()
            .get("canary/admission-marker")
            .await
            .unwrap()
            .is_none()
    );
    let absent_secret = transport
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Get",
            json!({"name":"provider.key"}),
        )
        .await
        .unwrap();
    assert!(!absent_secret.to_string().contains("admission-marker"));

    environment.write_admitted.store(true, Ordering::Release);
    environment.writable_grant.store(false, Ordering::Release);
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    let before_read_only_denial = environment.write_admissions.load(Ordering::Acquire);
    assert_eq!(
        governed_mutation_errors(&transport, "readonly-marker")
            .await
            .map(|error| error.is_some()),
        [true; 6],
        "a rotated read-only grant refuses every mutation path"
    );
    assert_eq!(
        environment.write_admissions.load(Ordering::Acquire),
        before_read_only_denial,
        "grant authorization denies writes before the write-admission callback"
    );
    assert!(
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"denied-doc-readonly-marker"}),
            )
            .await
            .is_ok()
    );
    assert!(
        transport
            .call(
                tinystoragedrivers_client::Object::Streams,
                "ReadWindow",
                serde_json::to_value(tinystoragedrivers_bus::ReadWindowRequest {
                    stream: "events/readonly-marker".to_owned(),
                    from: 0,
                    limit: 10,
                })
                .unwrap(),
            )
            .await
            .is_ok()
    );
    assert!(
        transport
            .get_blob_stream(tinystoragedrivers_bus::BlobKeyRequest {
                key: "canary/readonly-marker".to_owned(),
            })
            .await
            .unwrap()
            .is_none()
    );

    environment.writable_grant.store(true, Ordering::Release);
    environment.fail_key_provider.store(true, Ordering::Release);
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    let key_calls_before = environment.key_provider_calls.load(Ordering::Acquire);
    let key_failure = transport
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Get",
            json!({
                "collection":"shared.records",
                "document_id":"key-provider-marker"
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(
        environment.key_provider_calls.load(Ordering::Acquire),
        key_calls_before + 1
    );
    assert!(!format!("{key_failure:?}").contains("key-provider-marker"));
    environment
        .fail_key_provider
        .store(false, Ordering::Release);
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    assert!(
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({
                    "collection":"shared.records",
                    "document_id":"key-provider-marker"
                }),
            )
            .await
            .is_ok(),
        "a later key-provider retry must not reuse the failed resolution"
    );

    let audit_dump = format!("{:?}", audit.0.lock().unwrap());
    for marker in ["admission-marker", "readonly-marker", "key-provider-marker"] {
        assert!(!audit_dump.contains(marker));
    }
    assert!(registration.shutdown().await.is_ok());
    drop(lease);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The test gates a real admitted write immediately before native fenced commit."
)]
async fn governed_native_fence_refuses_after_admission_before_document_commit() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-fence-caller",
        "ai.tinyhumans.test.governedfencecaller",
        "/ai/tinyhumans/test/governed_fence_caller",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let guard_scope = Scope::new("governed-fence-guards").unwrap();
    let guard = environment.backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .ensure_collection(&tinystoragedrivers_core::CollectionSpec {
            name: "leases".to_owned(),
            indexes: vec![],
            ttl_field: None,
            search: None,
        })
        .await
        .unwrap();
    guard
        .documents()
        .put(
            "leases",
            "governed-caller",
            json!({"epoch":1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    let fence =
        tinystoragedrivers_core::Fence::epoch(guard_scope, "leases", "governed-caller", "epoch", 1);
    let fenced_storage = environment
        .backend
        .for_scope_fenced(&Scope::new("governed-fenced-profile").unwrap(), &fence)
        .unwrap();
    environment.servers.lock().unwrap().insert(
        "governed-fenced-profile".to_owned(),
        Arc::new(StorageServer::new(fenced_storage)),
    );
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let registration = attach_storage(
        &host,
        &observer,
        GovernedStorage::new(
            Arc::clone(&environment),
            Arc::new(TestAudit::default()),
            GovernedEndpointLimits::default(),
        ),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                "governed-fenced-profile",
            )),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport = tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor)
        .await
        .unwrap();

    let admission_before = environment.write_admissions.load(Ordering::Acquire);
    let mut admitted = environment.write_gate.arm();
    let pending_transport = Arc::new(transport);
    let pending = tokio::spawn(async move {
        pending_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Put",
                serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                    collection: "shared.records".to_owned(),
                    id: "fenced-after-admit".to_owned(),
                    doc: json!({"epoch":1}),
                    precondition: Precondition::Absent,
                    schema: 0,
                    expires_ms: None,
                })
                .unwrap(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), admitted.recv())
        .await
        .expect("write did not pass host admission")
        .unwrap();
    assert_eq!(
        environment.write_admissions.load(Ordering::Acquire),
        admission_before + 1
    );
    guard
        .documents()
        .put(
            "leases",
            "governed-caller",
            json!({"epoch":2}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    environment.write_gate.release();
    let result = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .expect("fenced write did not settle")
        .unwrap();
    assert!(
        matches!(
            result,
            Err(tinystoragedrivers_client::WireError::Storage(
                tinystoragedrivers_bus::StorageFailure {
                    kind: tinystoragedrivers_bus::StorageKind::Fenced
                }
            ))
        ),
        "the native fence, not task cancellation, must refuse the stale write: {result:?}"
    );
    assert!(
        environment
            .backend
            .for_scope(&Scope::new("governed-fenced-profile").unwrap())
            .unwrap()
            .documents()
            .get("shared.records", "fenced-after-admit")
            .await
            .unwrap()
            .is_none()
    );
    assert!(registration.shutdown().await.is_ok());
    drop(lease);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This integration test orders async resolve cancellation and resource recovery."
)]
async fn governed_binding_slot_recovers_after_cancelled_caller_resolve_settles() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-binding-pressure-caller",
        "ai.tinyhumans.test.governedbindingpressure",
        "/ai/tinyhumans/test/governed_binding_pressure",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .with_max_leases(2)
    .with_max_concurrent_bindings(1);
    let registration = attach_storage(
        &host,
        &Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap(),
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let peer = caller.unique_name().unwrap();
    let (lease_a, descriptor_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("pressure-a")),
            peer.clone(),
        )
        .await
        .unwrap();
    let transport_a = Arc::new(
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_a)
            .await
            .unwrap(),
    );

    let mut entered = environment.resolve_gate.arm();
    let blocked_transport = Arc::clone(&transport_a);
    let blocked = tokio::spawn(async move {
        blocked_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"pending-resolution"}),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.recv())
        .await
        .expect("governed resolver did not acquire its single binding slot")
        .unwrap();

    assert!(matches!(
        registration
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile("pressure-b")),
                peer.clone(),
            )
            .await,
        Err(HostFailure::Denied("binding-capacity"))
    ));
    blocked.abort();
    assert!(blocked.await.unwrap_err().is_cancelled());
    environment.resolve_gate.release();

    let (lease_b, descriptor_b) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match registration
                .register_lease(
                    tinystoragedrivers_busproxy::HostStorageContext::new(Profile("pressure-b")),
                    peer.clone(),
                )
                .await
            {
                Ok(lease) => break lease,
                Err(HostFailure::Denied("binding-capacity")) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected lease registration failure: {error:?}"),
            }
        }
    })
    .await
    .expect("the canceled caller's resolver did not release its slot");
    let transport_b =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_b)
            .await
            .unwrap();
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"after-cancel"}),
            )
            .await
            .is_ok()
    );
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"still-live"}),
            )
            .await
            .is_ok()
    );
    assert!(registration.shutdown().await.is_ok());
    drop((lease_a, lease_b));
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The test keeps 257 live broker peers and their lease paths observable through teardown."
)]
async fn more_than_256_live_bus_peers_release_bounded_governed_lease_resources() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let admitted_module = attach_named_test_module(
        &host,
        "governed-peer-churn-module",
        "ai.tinyhumans.test.governedpeerchurnmodule",
        "/ai/tinyhumans/test/governed_peer_churn_module",
    )
    .await;
    let admitted_module_id = host
        .sender_lookup()
        .module_for_sender(&admitted_module.unique_name().unwrap())
        .unwrap();
    let mut peers = Vec::with_capacity(258);
    let directory = Arc::new(PeerMapDirectory(Mutex::new(HashMap::new())));
    for _ in 0..258 {
        let peer = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let name = peer.unique_name().unwrap();
        directory
            .0
            .lock()
            .unwrap()
            .insert(name.clone(), admitted_module_id.clone());
        peers.push((peer, name));
    }

    let service = attach_named_test_module(
        &host,
        "governed-peer-churn-service",
        "ai.tinyhumans.test.governedpeerchurnservice",
        "/ai/tinyhumans/test/governed_peer_churn_service",
    )
    .await;
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let endpoint = GovernedStorage::new(
        test_environment(CollectionSpec {
            name: "shared.records".to_owned(),
            indexes: vec![],
            ttl_field: None,
            search: None,
        }),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .with_max_leases(257)
    .with_max_concurrent_bindings(8);
    let owner = endpoint.register(
        service,
        directory.clone(),
        tokio::runtime::Handle::current(),
    );

    // These are 258 real, simultaneous broker peers. The fixture directory
    // deliberately maps them to one admitted ModuleId; it tests lease/cache
    // lifecycle, not native module admission or 258 independent module workers.
    let mut leases = Vec::with_capacity(257);
    for (_, peer) in peers.iter().take(257) {
        leases.push(
            owner
                .register_lease(
                    tinystoragedrivers_busproxy::HostStorageContext::new(Profile("peer-churn")),
                    peer.clone(),
                )
                .await
                .unwrap(),
        );
    }
    assert!(matches!(
        owner
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile("peer-over-bound")),
                peers[257].1.clone(),
            )
            .await,
        Err(HostFailure::Denied("lease-capacity"))
    ));

    // Exercise real disconnects plus authoritative-directory loss. Keep one
    // peer connected after its admission is removed so its old route can be
    // probed after reconciliation.
    for (index, (peer, name)) in peers.iter().take(257).enumerate() {
        directory.0.lock().unwrap().remove(name);
        if index != 1 {
            peer.close().await.unwrap();
        }
    }
    assert_eq!(owner.reconcile_callers().await, 257);
    assert_eq!(owner.reconcile_callers().await, 0);
    let (surviving_peer, _) = &peers[1];
    let withdrawn_transport = tinystoragedrivers_busproxy::BusTransport::connect_bound(
        surviving_peer.clone(),
        leases[1].1.clone(),
    )
    .await
    .unwrap();
    assert!(
        withdrawn_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"after-churn-reconcile"}),
            )
            .await
            .is_err()
    );
    let (reused_lease, _) = owner
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("peer-capacity-reused")),
            peers[257].1.clone(),
        )
        .await
        .expect("revocation returns the bounded lease slot");
    assert!(owner.revoke(&reused_lease).await);
    peers[1].0.close().await.unwrap();
    let mut refilled = Vec::with_capacity(257);
    for _ in 0..257 {
        refilled.push(
            owner
                .register_lease(
                    tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                        "peer-capacity-refill",
                    )),
                    peers[257].1.clone(),
                )
                .await
                .unwrap(),
        );
    }
    assert!(matches!(
        owner
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                    "peer-refill-over-bound",
                )),
                peers[257].1.clone(),
            )
            .await,
        Err(HostFailure::Denied("lease-capacity"))
    ));
    directory.0.lock().unwrap().remove(&peers[257].1);
    peers[257].0.close().await.unwrap();
    assert_eq!(owner.reconcile_callers().await, 257);
    assert_eq!(owner.reconcile_callers().await, 0);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    owner.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The upload test covers ceiling, framing, cancellation, and destination ownership."
)]
async fn governed_upload_checks_declared_body_length_and_exact_ceiling() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-upload-bounds-caller",
        "ai.tinyhumans.test.governeduploadbounds",
        "/ai/tinyhumans/test/governed_upload_bounds",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits { max_blob_bytes: 4 },
    );
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let registration = attach_storage(
        &host,
        &observer,
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("upload-bounds")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport = Arc::new(
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor)
            .await
            .unwrap(),
    );
    let backend = environment
        .backend
        .for_scope(&Scope::new("upload-bounds").unwrap())
        .unwrap();

    let exact_result = transport
        .put_blob_stream(
            tinystoragedrivers_bus::PutBlobStreamRequest {
                key: "canary/bounded-exact".to_owned(),
                len: 4,
                content_type: None,
                ttl_ms: None,
            },
            Box::pin(std::io::Cursor::new(vec![0x10, 0x20, 0x30, 0x40])),
        )
        .await;
    assert!(
        exact_result.is_ok(),
        "exact-ceiling upload failed: {exact_result:?}"
    );
    let exact = backend
        .blobs()
        .get("canary/bounded-exact")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.bytes, vec![0x10, 0x20, 0x30, 0x40]);

    for (key, body) in [("canary/bounded-short", vec![1, 2, 3])] {
        assert!(
            transport
                .put_blob_stream(
                    tinystoragedrivers_bus::PutBlobStreamRequest {
                        key: key.to_owned(),
                        len: 4,
                        content_type: None,
                        ttl_ms: None,
                    },
                    Box::pin(std::io::Cursor::new(body)),
                )
                .await
                .is_err(),
            "body bytes must equal the declared stream length"
        );
        assert!(backend.blobs().get(key).await.unwrap().is_none());
    }
    assert!(
        transport
            .put_blob_stream(
                tinystoragedrivers_bus::PutBlobStreamRequest {
                    key: "canary/bounded-extra-source".to_owned(),
                    len: 4,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(std::io::Cursor::new(vec![1, 2, 3, 4, 5])),
            )
            .await
            .is_ok()
    );
    assert_eq!(
        backend
            .blobs()
            .get("canary/bounded-extra-source")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![1, 2, 3, 4],
        "the transport sends exactly the declared length and leaves extra source bytes unread"
    );

    assert!(
        transport
            .put_blob_stream(
                tinystoragedrivers_bus::PutBlobStreamRequest {
                    key: "canary/bounded-source-error".to_owned(),
                    len: 8,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(FailsAfterPrefix(vec![1, 2, 3, 4])),
            )
            .await
            .is_err()
    );
    assert!(
        backend
            .blobs()
            .get("canary/bounded-source-error")
            .await
            .unwrap()
            .is_none()
    );

    let admissions_before_cancel = environment.write_admissions.load(Ordering::Acquire);
    let (source_started, mut source_entered) = tokio::sync::mpsc::unbounded_channel();
    let pending_transport = Arc::clone(&transport);
    let pending_upload = tokio::spawn(async move {
        pending_transport
            .put_blob_stream(
                tinystoragedrivers_bus::PutBlobStreamRequest {
                    key: "canary/bounded-cancelled".to_owned(),
                    len: 4,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(PendingAfterPrefix {
                    prefix: vec![1, 2],
                    started: Some(source_started),
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), source_entered.recv())
        .await
        .expect("upload source did not pause after sending its first bytes")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while environment.write_admissions.load(Ordering::Acquire) == admissions_before_cancel {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("host did not admit the upload before its source was cancelled");
    pending_upload.abort();
    assert!(pending_upload.await.unwrap_err().is_cancelled());
    assert!(
        backend
            .blobs()
            .get("canary/bounded-cancelled")
            .await
            .unwrap()
            .is_none()
    );

    let admissions_before_ceiling = environment.write_admissions.load(Ordering::Acquire);
    assert!(
        transport
            .put_blob_stream(
                tinystoragedrivers_bus::PutBlobStreamRequest {
                    key: "canary/bounded-over-ceiling".to_owned(),
                    len: 5,
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(std::io::Cursor::new(vec![1, 2, 3, 4, 5])),
            )
            .await
            .is_err()
    );
    assert_eq!(
        environment.write_admissions.load(Ordering::Acquire),
        admissions_before_ceiling,
        "the declared over-ceiling body is refused before backend admission"
    );
    assert!(
        backend
            .blobs()
            .get("canary/bounded-over-ceiling")
            .await
            .unwrap()
            .is_none()
    );
    assert!(registration.shutdown().await.is_ok());
    drop(lease);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn revocation_during_delayed_blob_open_cannot_publish_a_stale_reader() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-delayed-download-caller",
        "ai.tinyhumans.test.governeddelayeddownload",
        "/ai/tinyhumans/test/governed_delayed_download",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let registration = attach_storage(
        &host,
        &observer,
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("delayed-download")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport = Arc::new(
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor)
            .await
            .unwrap(),
    );
    let backend = environment
        .backend
        .for_scope(&Scope::new("delayed-download").unwrap())
        .unwrap();
    backend
        .blobs()
        .put(
            "canary/delayed-reader",
            b"must-not-escape-after-revoke".to_vec(),
            None,
        )
        .await
        .unwrap();

    let mut backend_entered = environment.clock_gate.arm();
    let pending_transport = Arc::clone(&transport);
    let pending = tokio::spawn(async move {
        pending_transport
            .get_blob_stream(tinystoragedrivers_bus::BlobKeyRequest {
                key: "canary/delayed-reader".to_owned(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), backend_entered.recv())
        .await
        .expect("GetBlob did not reach the gated backend read")
        .unwrap();
    assert!(registration.revoke(&lease).await);
    environment.clock_gate.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("revoked GetBlob did not settle")
            .unwrap()
            .is_err()
    );
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    assert!(registration.shutdown().await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn another_peer_cannot_accept_a_governed_download_stream() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-download-owner",
        "ai.tinyhumans.test.governeddownloadowner",
        "/ai/tinyhumans/test/governed_download_owner",
    )
    .await;
    let attacker = attach_named_test_module(
        &host,
        "governed-download-attacker",
        "ai.tinyhumans.test.governeddownloadattacker",
        "/ai/tinyhumans/test/governed_download_attacker",
    )
    .await;
    let environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let registration = attach_storage(
        &host,
        &observer,
        GovernedStorage::new(
            Arc::clone(&environment),
            Arc::new(TestAudit::default()),
            GovernedEndpointLimits::default(),
        ),
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("download-theft")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let payload = b"only the admitted peer can receive this body".to_vec();
    environment
        .backend
        .for_scope(&Scope::new("download-theft").unwrap())
        .unwrap()
        .blobs()
        .put("canary/download-theft", payload.clone(), None)
        .await
        .unwrap();

    let service_peer = tinybus::BusName::new(descriptor.service_peer().to_owned()).unwrap();
    let proxy = caller
        .proxy(
            service_peer.as_str(),
            format!("{}/Blobs", descriptor.object_root()),
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )
        .unwrap();
    let reply: Value = proxy
        .call("GetStream", (json!({"key":"canary/download-theft"}),))
        .await
        .unwrap();
    let stream_ref: tinybus::StreamRef = serde_json::from_value(reply[1].clone()).unwrap();
    assert!(
        attacker
            .accept_stream_from(&stream_ref, &service_peer)
            .is_err(),
        "a different unique peer cannot accept the owner-directed stream"
    );
    let mut reader = caller
        .accept_stream_from(&stream_ref, &service_peer)
        .unwrap();
    let received = reader.read_to_end_capped(1024).await.unwrap();
    assert_eq!(
        received, payload,
        "the admitted owner still drains the stream"
    );

    assert!(registration.shutdown().await.is_ok());
    drop(lease);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
}

async fn assert_canceled_owner_transfer_cleanup(
    observer: &Connection,
    endpoint_drops: &AtomicUsize,
    drop_stages: &mut mpsc::UnboundedReceiver<crate::storage_host::StorageDropStage>,
) {
    let mut last_name_state = "not observed";
    let mut last_endpoint_drops = 0;
    let mut observed_drop_stages = Vec::new();
    let cleanup = async {
        loop {
            let name_released = match observer
                .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
                .await
            {
                Ok(None) => {
                    last_name_state = "released";
                    true
                }
                Ok(Some(_)) => {
                    last_name_state = "owned";
                    false
                }
                Err(_) => {
                    last_name_state = "query error";
                    false
                }
            };
            last_endpoint_drops = endpoint_drops.load(Ordering::Acquire);
            while let Ok(stage) = drop_stages.try_recv() {
                observed_drop_stages.push(stage);
            }
            if name_released && last_endpoint_drops == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    };
    let cleanup_result = tokio::time::timeout(Duration::from_secs(2), cleanup).await;
    while let Ok(stage) = drop_stages.try_recv() {
        observed_drop_stages.push(stage);
    }
    let current_endpoint_drops = endpoint_drops.load(Ordering::Acquire);
    assert!(
        cleanup_result.is_ok(),
        "canceled owner-transfer cleanup timed out: name_owner_state={last_name_state}, \
         last_observed_endpoint_drops={last_endpoint_drops}, \
         current_endpoint_drops={current_endpoint_drops}, \
         cleanup_stages={observed_drop_stages:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_attach_before_owner_transfer_cleans_registration_and_keeps_host_live() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = Arc::new(ModuleHost::new(broker));
    let survivor = attach_named_test_module(
        &host,
        "governed-transfer-survivor",
        "ai.tinyhumans.test.governedtransfersurvivor",
        "/ai/tinyhumans/test/governed_transfer_survivor",
    )
    .await;
    let survivor_peer = survivor.unique_name().unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let observer_peer = observer.unique_name().unwrap();
    let endpoint_drops = Arc::new(AtomicUsize::new(0));
    let endpoint = GovernedStorage::new(
        test_environment(CollectionSpec {
            name: "shared.records".to_owned(),
            indexes: vec![],
            ttl_field: None,
            search: None,
        }),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    )
    .with_legacy_endpoint(Arc::new(AliasMarkerEndpoint {
        hits: Arc::new(Mutex::new(Vec::new())),
        drop_counter: Some(Arc::clone(&endpoint_drops)),
    }));
    let (setup_ready, setup_wait) = tokio::sync::oneshot::channel();
    let (continue_setup, continue_wait) = tokio::sync::oneshot::channel();
    let (cleanup_probe, mut drop_stages) = mpsc::unbounded_channel();
    let observer_for_attach = observer.clone();
    let host_for_attach = Arc::clone(&host);
    let attaching = tokio::spawn(async move {
        crate::storage_host::attach_storage_before_result_transfer(
            &host_for_attach,
            &observer_for_attach,
            endpoint,
            tokio::runtime::Handle::current(),
            move || async move {
                let _ = setup_ready.send(());
                let _ = continue_wait.await;
            },
            cleanup_probe,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), setup_wait)
        .await
        .expect("setup did not reach its owner-transfer gate")
        .unwrap();

    assert!(
        observer
            .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
            .await
            .unwrap()
            .is_some()
    );

    attaching.abort();
    assert!(attaching.await.unwrap_err().is_cancelled());
    let _ = continue_setup.send(());
    assert_canceled_owner_transfer_cleanup(&observer, &endpoint_drops, &mut drop_stages).await;
    assert_eq!(observer.unique_name().unwrap(), observer_peer);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    assert!(
        host.sender_lookup()
            .module_for_sender(&survivor_peer)
            .is_some()
    );
    assert!(
        observer
            .name_owner("ai.tinyhumans.test.governedtransfersurvivor")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_a_lease_closes_its_watch_on_a_retained_server() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let caller = attach_named_test_module(
        &host,
        "governed-watch-cleanup-caller",
        "ai.tinyhumans.test.watchcleanupcaller",
        "/ai/tinyhumans/test/watch_cleanup_caller",
    )
    .await;
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let environment = test_environment(collection);
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let registration = attach_storage(
        &host,
        &observer,
        endpoint,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-watch-cleanup")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport = tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor)
        .await
        .unwrap();
    transport
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            json!({
                "collection":"shared.records",
                "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                "interval_ms":1000
            }),
        )
        .await
        .unwrap();
    let server = environment.server_for("profile-watch-cleanup");
    assert_eq!(server.open_watches(), 1);

    let empty_owner = crate::storage_host::tests::empty_registration_for_test::<
        TestEnvironment,
        TestAudit,
        Profile,
    >(tokio::runtime::Handle::current());
    assert!(!empty_owner.revoke(&lease).await);
    assert!(
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"after-empty-owner-revoke"}),
            )
            .await
            .is_ok(),
        "an already-empty registration wrapper cannot revoke another live owner's lease"
    );
    empty_owner.shutdown().await.unwrap();

    assert!(registration.revoke(&lease).await);
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.open_watches() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("lease revocation did not close its native watch");
    assert_eq!(
        server.open_watches(),
        0,
        "revoking a lease must close its native watch while the shared server remains alive"
    );
    assert!(
        observer.unique_name().is_some(),
        "observer is borrowed by registration"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The detach test verifies shared-server watch isolation and a positive survivor canary."
)]
async fn caller_detach_closes_only_that_callers_watch_on_a_shared_server() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = attach_named_test_module(
        &host,
        "governed-watch-detach-service",
        "ai.tinyhumans.test.watchdetachservice",
        "/ai/tinyhumans/test/watch_detach_service",
    )
    .await;
    let caller_a = attach_named_test_module(
        &host,
        "governed-watch-detach-a",
        "ai.tinyhumans.test.watchdetacha",
        "/ai/tinyhumans/test/watch_detach_a",
    )
    .await;
    let caller_b = attach_named_test_module(
        &host,
        "governed-watch-detach-b",
        "ai.tinyhumans.test.watchdetachb",
        "/ai/tinyhumans/test/watch_detach_b",
    )
    .await;
    let service_peer = service.unique_name().unwrap();
    let peer_a = caller_a.unique_name().unwrap();
    let peer_b = caller_b.unique_name().unwrap();
    let revoked = Arc::new(AtomicBool::new(false));
    let callers: Arc<dyn tinystoragedrivers_busproxy::CallerDirectory> =
        Arc::new(RevokingCallerDirectory {
            lookup: tinystoragedrivers_busproxy::TinyBusCallerDirectory(host.sender_lookup()),
            revoked_peer: peer_a.clone(),
            revoked: Arc::clone(&revoked),
        });
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let environment = test_environment(collection);
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let registration = Arc::new(endpoint.register(
        service,
        Arc::clone(&callers),
        tokio::runtime::Handle::current(),
    ));
    let (_lease_a, descriptor_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-shared-watches")),
            peer_a.clone(),
        )
        .await
        .unwrap();
    let (_lease_b, descriptor_b) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-shared-watches")),
            peer_b.clone(),
        )
        .await
        .unwrap();
    let transport_a =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller_a, descriptor_a)
            .await
            .unwrap();
    let transport_b =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller_b, descriptor_b)
            .await
            .unwrap();
    let mut signals_b = transport_b.signals();
    let open_watch = json!({
        "collection":"shared.records",
        "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
        "interval_ms":1000
    });
    let watch_a = transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch.clone(),
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let watch_b = transport_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch,
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let server = environment.server_for("profile-shared-watches");
    assert_eq!(server.open_watches(), 2);

    // This fixture makes the authoritative directory report peer A gone and
    // delivers its matching NameOwnerChanged event; it does not claim that a
    // module-side Connection::close physically stops its owning ModuleHost.
    revoked.store(true, Ordering::Release);
    let (events, received) = tokio::sync::broadcast::channel(2);
    events
        .send(tinybus::Message::signal(
            tinybus::ObjectPath::new("/org/freedesktop/DBus").unwrap(),
            tinybus::InterfaceName::new(tinybus::BUS_INTERFACE).unwrap(),
            tinybus::MemberName::new("NameOwnerChanged").unwrap(),
            Value::Array(vec![
                Value::String(peer_a.to_string()),
                Value::String("owner".to_owned()),
                Value::Null,
            ]),
        ))
        .unwrap();
    let monitor = tokio::spawn(crate::storage_host::monitor_owner_changes(
        received,
        service_peer,
        Arc::clone(&registration),
        None,
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.open_watches() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("caller detach did not close its own native watch");
    assert_eq!(server.open_watches(), 1);
    assert!(matches!(
        signals_b.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    transport_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "shared.records".to_owned(),
                id: "surviving-watch-canary".to_owned(),
                doc: json!({"still_open":true}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let signal = tokio::time::timeout(Duration::from_secs(1), signals_b.recv())
        .await
        .expect("remaining caller watch did not receive its canary")
        .unwrap();
    assert_eq!(signal.watch_id, watch_b);
    assert_ne!(watch_a, signal.watch_id);
    assert!(matches!(
        signal.notice,
        tinystoragedrivers_bus::WatchNotice::Invalidated { .. }
    ));
    assert_eq!(server.open_watches(), 1);
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    monitor.abort();
    drop(registration);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "The handover test preserves a stale close request across two real bindings."
)]
async fn stale_watch_close_cannot_close_equal_id_on_new_backend() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let service = attach_named_test_module(
        &host,
        "governed-watch-close-service",
        "ai.tinyhumans.test.watchcloseservice",
        "/ai/tinyhumans/test/watch_close_service",
    )
    .await;
    let caller = attach_named_test_module(
        &host,
        "governed-watch-close-caller",
        "ai.tinyhumans.test.watchclosecaller",
        "/ai/tinyhumans/test/watch_close_caller",
    )
    .await;
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let environment = test_environment(collection);
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let registration = endpoint.register(
        service,
        Arc::new(tinystoragedrivers_busproxy::TinyBusCallerDirectory(
            host.sender_lookup(),
        )),
        tokio::runtime::Handle::current(),
    );
    let peer = caller.unique_name().unwrap();
    let (_lease_a, descriptor_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-close-source")),
            peer.clone(),
        )
        .await
        .unwrap();
    let (_lease_b, descriptor_b) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-close-target")),
            peer,
        )
        .await
        .unwrap();
    let transport_a =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_a)
            .await
            .unwrap();
    let transport_b =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller, descriptor_b)
            .await
            .unwrap();
    let mut signals_b = transport_b.signals();
    let open_watch = json!({
        "collection":"shared.records",
        "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
        "interval_ms":1000
    });
    let watch_a = transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch.clone(),
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let watch_b = transport_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch,
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(
        watch_a, watch_b,
        "separate caller leases need distinct outward watch handles"
    );
    let target_server = environment.server_for("profile-close-target");
    assert_eq!(target_server.open_watches(), 1);
    environment.scope_overrides.lock().unwrap().insert(
        "profile-close-source".to_owned(),
        "profile-close-target".to_owned(),
    );
    environment.set_scope_epoch(
        "profile-close-source",
        environment.binding_epoch.load(Ordering::Acquire) + 1,
    );

    let close = transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchClose",
            json!({"watch_id":watch_a}),
        )
        .await;
    assert!(
        close.is_err(),
        "a stale handle must not route to a new backend"
    );
    assert_eq!(target_server.open_watches(), 1);
    assert!(matches!(
        signals_b.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    transport_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "shared.records".to_owned(),
                id: "watch-close-handover-canary".to_owned(),
                doc: json!({"target_still_open":true}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let signal = tokio::time::timeout(Duration::from_secs(1), signals_b.recv())
        .await
        .expect("target watch must receive a fresh post-handover canary")
        .unwrap();
    assert_eq!(signal.watch_id, watch_b);
    assert!(matches!(
        signal.notice,
        tinystoragedrivers_bus::WatchNotice::Invalidated { .. }
    ));
    assert_eq!(target_server.open_watches(), 1);
}

#[cfg(not(windows))]
fn stage_allowlisted_cdylib(source: &std::path::Path) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir_in(source.parent().expect("fixture has a parent"))
        .expect("create private module staging directory");
    let staged = directory
        .path()
        .join(source.file_name().expect("fixture has a name"));
    std::fs::copy(source, &staged).expect("stage module artifact");
    let digest = tinybus::module::sha256_file(&staged).expect("hash module artifact");
    let file_name = staged.file_name().unwrap().to_string_lossy();
    std::fs::write(
        directory.path().join("modules.toml"),
        format!("{file_name:?} = {digest:?}\n"),
    )
    .expect("write exact module artifact allowlist");
    (directory, staged)
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "This loaded-cdylib proof exercises the complete native consumer route matrix."
)]
#[ignore = "requires TSD_GOVERNED_TEST_MODULE to point at the built consumer cdylib"]
async fn a_loaded_cdylib_uses_the_governed_descriptor_and_never_falls_back_to_host_name() {
    let source = std::path::PathBuf::from(
        std::env::var_os("TSD_GOVERNED_TEST_MODULE")
            .expect("TSD_GOVERNED_TEST_MODULE points to the built fixture"),
    );
    let (_staging, staged) = stage_allowlisted_cdylib(&source);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let (service_tx, service_rx) = tokio::sync::oneshot::channel();
    let service_info = host
        .attach_linked_with_setup(
            named_test_export(
                "governed-cdylib-service",
                "ai.tinyhumans.test.governedcdylibservice",
                "/ai/tinyhumans/test/governed_cdylib_service",
            ),
            json!({}),
            move |connection| async move {
                service_tx
                    .send(connection.clone())
                    .map_err(|_| tinybus::Error::failed("service receiver closed"))
            },
        )
        .await
        .unwrap();
    let service_a = service_rx.await.unwrap();
    let service_a_peer = service_a.unique_name().unwrap();
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let environment = test_environment(collection);
    let audit = Arc::new(TestAudit::default());
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::clone(&audit),
        GovernedEndpointLimits {
            max_blob_bytes: tinystoragedrivers_bus::MAX_FRAME_BYTES as u64 + 1024 * 1024 + 17,
        },
    );
    let registration = Arc::new(endpoint.register(
        service_a.clone(),
        Arc::new(tinystoragedrivers_busproxy::TinyBusCallerDirectory(
            host.sender_lookup(),
        )),
        tokio::runtime::Handle::current(),
    ));

    let consumer_info = host.load_file_with_config(&staged, json!({})).unwrap();
    let consumer_peer = observer
        .name_owner(consumer_info.manifest.bus_name.as_str())
        .await
        .unwrap()
        .expect("loaded consumer has an authoritative unique peer");
    let (lease, descriptor) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("loaded-cdylib-profile")),
            consumer_peer.clone(),
        )
        .await
        .unwrap();
    let control = observer
        .proxy(
            consumer_info.manifest.bus_name.as_str(),
            consumer_info.manifest.object_path.as_str(),
            "ai.tinyhumans.test.GovernedTestConsumer",
        )
        .unwrap();
    control
        .call::<()>("Configure", (descriptor.clone(),))
        .await
        .unwrap();

    let outsider = attach_named_test_module(
        &host,
        "governed-cdylib-outsider",
        "ai.tinyhumans.test.governedcdyliboutsider",
        "/ai/tinyhumans/test/governed_cdylib_outsider",
    )
    .await;
    let before_refusal = (
        environment.resolve_calls.load(Ordering::Acquire),
        environment.schema_initializations.load(Ordering::Acquire),
        environment.key_provider_calls.load(Ordering::Acquire),
        environment.write_admissions.load(Ordering::Acquire),
    );
    let forged_route = tinystoragedrivers_busproxy::BusTransport::connect_bound(
        outsider.clone(),
        descriptor.clone(),
    )
    .await
    .unwrap();
    let forged = forged_route
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Get",
            json!({
                "collection":"shared.records",
                "document_id":"loaded-cdylib",
                "scope":"forged-scope-marker"
            }),
        )
        .await
        .unwrap_err();
    assert!(!format!("{forged:?}").contains("forged-scope-marker"));
    assert_eq!(
        before_refusal,
        (
            environment.resolve_calls.load(Ordering::Acquire),
            environment.schema_initializations.load(Ordering::Acquire),
            environment.key_provider_calls.load(Ordering::Acquire),
            environment.write_admissions.load(Ordering::Acquire),
        ),
        "a mismatched module peer is refused before resolve, schema, keys, or write admission"
    );

    let exercised: Value = control.call("Exercise", ()).await.unwrap();
    assert_eq!(exercised["secret"], json!({"value":[17,18,19]}));
    assert_eq!(
        exercised["blob_len"],
        json!(tinystoragedrivers_bus::MAX_FRAME_BYTES + 1024 * 1024 + 17)
    );
    assert!(
        control
            .call::<bool>("RejectScopeArgument", ())
            .await
            .unwrap()
    );
    assert!(
        control
            .call::<bool>("RejectSecretSuffix", ())
            .await
            .unwrap()
    );
    let audit_before_oversize = audit.0.lock().unwrap().len();
    assert!(
        control
            .call::<bool>("RejectOversizedUpload", ())
            .await
            .unwrap()
    );
    assert!(
        audit.0.lock().unwrap()[audit_before_oversize..]
            .iter()
            .any(|event| {
                event.object == tinystoragedrivers_client::Object::Blobs
                    && event.outcome == "denied"
                    && event.write
            }),
        "the over-ceiling body reached the governed server refusal path"
    );
    assert!(
        control
            .call::<bool>("RejectMismatchedUploadLengths", ())
            .await
            .unwrap()
    );
    let stolen_stream: Value = control.call("OpenStagedStream", ()).await.unwrap();
    let stolen_stream: tinybus::StreamRef = serde_json::from_value(stolen_stream).unwrap();
    let stolen_path = format!("{}/Blobs", descriptor.object_root());
    let stolen_proxy = outsider
        .proxy(
            descriptor.service_peer(),
            stolen_path,
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )
        .unwrap();
    assert!(
        stolen_proxy
            .call::<Value>(
                "PutStream",
                (
                    tinystoragedrivers_bus::PutBlobStreamRequest {
                        key: "canary/stolen-stream".to_owned(),
                        len: 4,
                        content_type: None,
                        ttl_ms: None,
                    },
                    stolen_stream,
                ),
            )
            .await
            .is_err(),
        "another module cannot consume the loaded consumer's upload stream"
    );
    control.call::<()>("AbortStagedStream", ()).await.unwrap();
    let audit_dump = format!("{:?}", audit.0.lock().unwrap());
    assert!(!audit_dump.contains("forged-scope-marker"));
    assert!(!audit_dump.contains("provider.key.suffix-marker"));
    let direct = environment
        .backend
        .for_scope(&Scope::new("loaded-cdylib-profile").unwrap())
        .unwrap();
    let stored_blob = direct
        .blobs()
        .get("canary/loaded-cdylib.bin")
        .await
        .unwrap()
        .unwrap();
    let expected: Vec<u8> = (0..tinystoragedrivers_bus::MAX_FRAME_BYTES + 1024 * 1024 + 17)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    assert_eq!(stored_blob.bytes, expected);
    assert!(
        direct
            .blobs()
            .get("canary/stolen-stream")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        direct
            .blobs()
            .get("canary/mismatched-length")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        direct
            .blobs()
            .get("canary/over-ceiling")
            .await
            .unwrap()
            .is_none()
    );

    service_a
        .request_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let watch_id: String = control.call("OpenWatch", ()).await.unwrap();
    control.call::<()>("PokeWatch", ()).await.unwrap();
    let watch_notice: Value = control
        .call("WaitWatch", (watch_id.clone(),))
        .await
        .unwrap();
    assert_eq!(watch_notice["watch_id"], watch_id);
    assert_eq!(watch_notice["notice"]["kind"], "invalidated");

    service_a
        .release_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let (alias_tx, alias_rx) = tokio::sync::oneshot::channel();
    let _alias_info = host
        .attach_linked_with_setup(
            named_test_export(
                "governed-cdylib-alias-host",
                "ai.tinyhumans.test.governedcdylibaliashost",
                "/ai/tinyhumans/test/governed_cdylib_alias_host",
            ),
            json!({}),
            move |connection| async move {
                alias_tx
                    .send(connection.clone())
                    .map_err(|_| tinybus::Error::failed("alias owner receiver closed"))
            },
        )
        .await
        .unwrap();
    let alias_connection = alias_rx.await.unwrap();
    let alias_environment = test_environment(CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    });
    let alias_endpoint = GovernedStorage::new(
        Arc::clone(&alias_environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits {
            max_blob_bytes: tinystoragedrivers_bus::MAX_FRAME_BYTES as u64 + 1024 * 1024 + 17,
        },
    );
    let alias_registration = Arc::new(alias_endpoint.register(
        alias_connection.clone(),
        Arc::new(tinystoragedrivers_busproxy::TinyBusCallerDirectory(
            host.sender_lookup(),
        )),
        tokio::runtime::Handle::current(),
    ));
    let (alias_lease, alias_descriptor) = alias_registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("alias-b-profile")),
            consumer_peer.clone(),
        )
        .await
        .unwrap();
    assert_eq!(alias_descriptor.object_root(), descriptor.object_root());
    assert_ne!(alias_descriptor.service_peer(), descriptor.service_peer());
    let alias_resolve_at_registration = alias_environment.resolve_calls.load(Ordering::Acquire);
    let alias_root_hits = Arc::new(Mutex::new(Vec::new()));
    let _alias_owner = tinystoragedrivers_busproxy::serve(
        &alias_connection,
        Arc::new(AliasMarkerEndpoint {
            hits: Arc::clone(&alias_root_hits),
            drop_counter: None,
        }),
    )
    .await
    .unwrap();
    let alias_peer = observer
        .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap()
        .expect("B owns the public alias");
    let alias_service_peer = alias_connection.unique_name().unwrap();
    assert_eq!(alias_peer, alias_service_peer);
    assert_eq!(
        observer.name_owner(service_a_peer.as_str()).await.unwrap(),
        Some(service_a_peer.clone()),
        "A remains connected while B owns HOST_NAME"
    );
    control
        .call::<()>("Configure", (alias_descriptor.clone(),))
        .await
        .unwrap();
    let alias_probe: Value = control.call("Exercise", ()).await.unwrap();
    assert_eq!(alias_probe["secret"], json!({"value":[17,18,19]}));
    assert_eq!(
        alias_probe["blob_len"],
        json!(tinystoragedrivers_bus::MAX_FRAME_BYTES + 1024 * 1024 + 17)
    );
    let alias_watch: String = control.call("OpenWatch", ()).await.unwrap();
    control.call::<()>("PokeWatch", ()).await.unwrap();
    let alias_notice: Value = control
        .call("WaitWatch", (alias_watch.clone(),))
        .await
        .unwrap();
    assert_eq!(alias_notice["watch_id"], alias_watch);
    assert_eq!(alias_notice["notice"]["kind"], "invalidated");
    control
        .call::<()>("CloseWatch", (alias_watch,))
        .await
        .unwrap();
    control
        .call::<()>("Configure", (descriptor.clone(),))
        .await
        .unwrap();
    let alias_resolve_baseline = alias_environment.resolve_calls.load(Ordering::Acquire);
    assert!(alias_resolve_baseline > alias_resolve_at_registration);
    let verified: Value = control.call("VerifyRoutes", ()).await.unwrap();
    assert_eq!(verified["secret"], json!({"value":[17,18,19]}));
    assert_eq!(verified["upload_len"], 4097);
    assert!(alias_root_hits.lock().unwrap().is_empty());
    assert_eq!(
        alias_environment.resolve_calls.load(Ordering::Acquire),
        alias_resolve_baseline,
        "the bound route suite must not reach B's same-root lease handler"
    );
    assert_eq!(
        control.call::<Value>("LegacyMarker", ()).await.unwrap(),
        json!({"service":"alias-b"})
    );
    let mut expected_alias_root_hits = 1;
    assert_eq!(
        alias_root_hits.lock().unwrap().len(),
        expected_alias_root_hits,
        "the sole B root call so far is the positive legacy control"
    );
    let handover_watch: String = control.call("OpenWatch", ()).await.unwrap();
    control.call::<()>("PokeWatch", ()).await.unwrap();
    let handover_notice: Value = control
        .call("WaitWatch", (handover_watch.clone(),))
        .await
        .unwrap();
    assert_eq!(handover_notice["watch_id"], handover_watch);
    assert_eq!(handover_notice["notice"]["kind"], "invalidated");
    assert_eq!(
        alias_root_hits.lock().unwrap().len(),
        expected_alias_root_hits
    );
    assert_eq!(
        alias_environment.resolve_calls.load(Ordering::Acquire),
        alias_resolve_baseline,
        "calls, streams, Secrets, and watch traffic remain on A after alias handover"
    );

    // Stop the actual linked service module through the ModuleHost control
    // plane while A's lease is still active. This proves its native lifetime
    // ended; revoking the lease first would only prove path withdrawal.
    let stopped = observer
        .stop_module(&service_info.name, Duration::from_secs(3))
        .await
        .unwrap();
    assert!(matches!(
        stopped.state,
        tinybus::module::ModuleState::Stopped
    ));
    let failures: Value = control.call("BoundRoutesFail", ()).await.unwrap();
    assert!(
        failures
            .as_object()
            .unwrap()
            .values()
            .all(|value| value.as_bool() == Some(true))
    );
    assert_eq!(
        alias_root_hits.lock().unwrap().len(),
        expected_alias_root_hits
    );
    assert_eq!(
        alias_environment.resolve_calls.load(Ordering::Acquire),
        alias_resolve_baseline,
        "no bound route reaches B after the actual ModuleHost stop"
    );
    assert_eq!(
        control.call::<Value>("LegacyMarker", ()).await.unwrap(),
        json!({"service":"alias-b"})
    );
    expected_alias_root_hits += 1;
    assert_eq!(
        alias_root_hits.lock().unwrap().len(),
        expected_alias_root_hits
    );

    let _ = alias_lease;
    let _ = lease;
    drop(registration);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "This integration keeps one native linked service alive across authorization, stream, watch, epoch, and cleanup races."
)]
async fn one_authenticated_peer_can_hold_two_isolated_context_leases() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    let (caller_tx, caller_rx) = tokio::sync::oneshot::channel();
    let mut caller_export = caller_export::linked_module().unwrap();
    let caller_id = "governed-test-caller";
    caller_export.descriptor.module_name = [0; 64];
    caller_export.descriptor.module_name[..caller_id.len()].copy_from_slice(caller_id.as_bytes());
    caller_export.manifest.module.name = caller_id.to_owned();
    caller_export.manifest.bus_name = "ai.tinyhumans.test.governedcaller".parse().unwrap();
    caller_export.manifest.object_path = "/ai/tinyhumans/test/governed_caller".parse().unwrap();
    host.attach_linked_with_setup(caller_export, json!({}), move |connection| async move {
        caller_tx
            .send(connection.clone())
            .map_err(|_| tinybus::Error::failed("test caller receiver closed"))
    })
    .await
    .unwrap();
    let caller = caller_rx.await.unwrap();
    let collection = CollectionSpec {
        name: "shared.records".to_owned(),
        indexes: vec![],
        ttl_field: None,
        search: None,
    };
    let environment = test_environment(collection.clone());
    let endpoint = GovernedStorage::new(
        Arc::clone(&environment),
        Arc::new(TestAudit::default()),
        GovernedEndpointLimits::default(),
    );
    let endpoint_for_second_owner = endpoint.clone();
    let observer_peer = observer.unique_name().unwrap();
    let registration = Arc::new(
        attach_storage(
            &host,
            &observer,
            endpoint,
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap(),
    );
    let (lease_a, descriptor_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-alpha")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let (_lease_b, descriptor_b) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-beta")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let registration_for_caller_runtime = Arc::clone(&registration);
    let caller_peer = caller.unique_name().unwrap();
    let (lease_c, descriptor_c) = tokio::task::spawn_blocking(move || {
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        caller_runtime
            .block_on(registration_for_caller_runtime.register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-alpha")),
                caller_peer,
            ))
            .unwrap()
    })
    .await
    .unwrap();

    // A second registration also begins at lease id 1. A handle from the
    // first owner must not be able to revoke the second owner's id 1.
    let second_service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let second_owner = endpoint_for_second_owner.register(
        second_service,
        Arc::new(tinystoragedrivers_busproxy::TinyBusCallerDirectory(
            host.sender_lookup(),
        )),
        tokio::runtime::Handle::current(),
    );
    let (_lease_other, descriptor_other) = second_owner
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-gamma")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let other_transport =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_other)
            .await
            .unwrap();
    assert!(
        !second_owner.revoke(&lease_a).await,
        "a different registration must not accept this lease handle"
    );
    assert!(
        other_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_ok(),
        "cross-owner revoke must leave the second registration live"
    );

    let transport_a = tokio::time::timeout(
        Duration::from_secs(5),
        tinystoragedrivers_busproxy::BusTransport::connect_bound(
            caller.clone(),
            descriptor_a.clone(),
        ),
    )
    .await
    .expect("connect A timed out")
    .unwrap();
    let transport_b = tokio::time::timeout(
        Duration::from_secs(5),
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_b),
    )
    .await
    .expect("connect B timed out")
    .unwrap();
    let transport_c =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_c)
            .await
            .unwrap();

    let (_lease_handover_a, descriptor_handover_a) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-close-source")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let (_lease_handover_b, descriptor_handover_b) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-close-target")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_handover_a = tinystoragedrivers_busproxy::BusTransport::connect_bound(
        caller.clone(),
        descriptor_handover_a,
    )
    .await
    .unwrap();
    let transport_handover_b = tinystoragedrivers_busproxy::BusTransport::connect_bound(
        caller.clone(),
        descriptor_handover_b,
    )
    .await
    .unwrap();
    let mut handover_b_signals = transport_handover_b.signals();
    let close_watch_open = json!({
        "collection":"shared.records",
        "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
        "interval_ms":1000
    });
    let close_watch_a = transport_handover_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            close_watch_open.clone(),
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let close_watch_b = transport_handover_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            close_watch_open,
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(
        close_watch_a, close_watch_b,
        "rebound and surviving leases need distinct outward watch handles"
    );
    let target_server = environment.server_for("profile-close-target");
    assert_eq!(target_server.open_watches(), 1);
    environment.scope_overrides.lock().unwrap().insert(
        "profile-close-source".to_owned(),
        "profile-close-target".to_owned(),
    );
    environment.set_scope_epoch(
        "profile-close-source",
        environment.binding_epoch.load(Ordering::Acquire) + 1,
    );
    let _stale_close = transport_handover_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchClose",
            json!({"watch_id":close_watch_a}),
        )
        .await;
    assert_eq!(
        target_server.open_watches(),
        1,
        "a stale WatchClose must not close the equal-ID watch on its new backend"
    );
    transport_handover_b
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "shared.records".to_owned(),
                id: "close-handover-canary".to_owned(),
                doc: json!({"still_open":true}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        target_server.open_watches(),
        1,
        "the unaffected context must retain its native backend watch"
    );
    let target_signal = tokio::time::timeout(Duration::from_secs(2), handover_b_signals.recv())
        .await
        .expect("target lease lost its watch signal after stale close")
        .unwrap();
    assert_eq!(target_signal.watch_id, close_watch_b);
    assert!(matches!(
        target_signal.notice,
        tinystoragedrivers_bus::WatchNotice::Invalidated { .. }
    ));
    environment
        .scope_overrides
        .lock()
        .unwrap()
        .remove("profile-close-source");
    environment.clear_scope_epoch("profile-close-source");

    for (transport, bytes) in [(&transport_a, vec![1, 2, 3]), (&transport_b, vec![9])] {
        transport
            .put_blob_stream(
                tinystoragedrivers_bus::PutBlobStreamRequest {
                    key: "canary/shared".to_owned(),
                    len: u64::try_from(bytes.len()).unwrap(),
                    content_type: None,
                    ttl_ms: None,
                },
                Box::pin(std::io::Cursor::new(bytes)),
            )
            .await
            .unwrap();
    }
    for (transport, expected) in [
        (&transport_a, vec![1, 2, 3]),
        (&transport_c, vec![1, 2, 3]),
        (&transport_b, vec![9]),
    ] {
        let (head, mut body) = transport
            .get_blob_stream(tinystoragedrivers_bus::BlobKeyRequest {
                key: "canary/shared".to_owned(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.meta.len, u64::try_from(expected.len()).unwrap());
        let mut received = Vec::new();
        if let Err(error) = body.read_to_end(&mut received).await {
            let category = tinystoragedrivers_busproxy::test_support::stream_error_category(&error)
                .unwrap_or("unavailable");
            panic!("blob stream read failed (SDK error category: {category})");
        }
        assert_eq!(received, expected);
    }

    let append = |profile| {
        serde_json::to_value(tinystoragedrivers_bus::AppendRequest {
            stream: "events/shared".to_owned(),
            record: json!({"profile":profile}),
        })
        .unwrap()
    };
    transport_a
        .call(
            tinystoragedrivers_client::Object::Streams,
            "Append",
            append("alpha"),
        )
        .await
        .unwrap();
    transport_b
        .call(
            tinystoragedrivers_client::Object::Streams,
            "Append",
            append("beta"),
        )
        .await
        .unwrap();
    for (transport, expected_profile) in [
        (&transport_a, "alpha"),
        (&transport_c, "alpha"),
        (&transport_b, "beta"),
    ] {
        let items = transport
            .call(
                tinystoragedrivers_client::Object::Streams,
                "ReadWindow",
                serde_json::to_value(tinystoragedrivers_bus::ReadWindowRequest {
                    stream: "events/shared".to_owned(),
                    from: 0,
                    limit: 10,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let items: Vec<tinystoragedrivers_bus::StreamItem> = serde_json::from_value(items).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].record, json!({"profile":expected_profile}));
    }

    let mut signals_a = transport_a.signals();
    let mut signals_c = transport_c.signals();
    let open_watch = json!({
        "collection":"shared.records",
        "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
        "interval_ms":1000
    });
    let watch_a = transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch.clone(),
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let watch_c = transport_c
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            open_watch,
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let put = serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
        collection: "shared.records".to_owned(),
        id: "watch-canary".to_owned(),
        doc: json!({"changed":true}),
        precondition: Precondition::Absent,
        schema: 0,
        expires_ms: None,
    })
    .unwrap();
    transport_c
        .call(tinystoragedrivers_client::Object::Documents, "Put", put)
        .await
        .unwrap();
    let watch_update_a = tokio::time::timeout(Duration::from_secs(3), signals_a.recv())
        .await
        .expect("A watch signal timed out")
        .unwrap();
    let watch_update_c = tokio::time::timeout(Duration::from_secs(3), signals_c.recv())
        .await
        .expect("C watch signal timed out")
        .unwrap();
    assert_eq!(watch_update_a.watch_id, watch_a);
    assert_eq!(watch_update_c.watch_id, watch_c);

    let (lease_j, descriptor_j) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-alpha")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_j =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_j)
            .await
            .unwrap();
    let watch_j = transport_j
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            json!({
                "collection":"shared.records",
                "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                "interval_ms":1000
            }),
        )
        .await
        .unwrap()["watch_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(environment.server_for("profile-alpha").open_watches(), 3);
    assert!(registration.revoke(&lease_j).await);
    tokio::time::timeout(Duration::from_secs(1), async {
        while environment.server_for("profile-alpha").open_watches() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("revoking lease J did not close only its native watch");
    assert_eq!(
        environment.server_for("profile-alpha").open_watches(),
        2,
        "ordinary lease revoke must close only its watch on a retained shared server"
    );
    assert_ne!(watch_j, watch_a);
    transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            serde_json::to_value(tinystoragedrivers_bus::PutDocumentRequest {
                collection: "shared.records".to_owned(),
                id: "after-revoke-canary".to_owned(),
                doc: json!({"still_open":true}),
                precondition: Precondition::Absent,
                schema: 0,
                expires_ms: None,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let after_revoke_signal = tokio::time::timeout(Duration::from_secs(2), signals_a.recv())
        .await
        .expect("lease A watch did not survive lease J revoke")
        .unwrap();
    assert_eq!(after_revoke_signal.watch_id, watch_a);

    let shared_server = environment.server_for("profile-alpha");
    assert_eq!(shared_server.open_watches(), 2);
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    for transport in [&transport_a, &transport_c] {
        transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        shared_server.open_watches(),
        0,
        "replacing a binding must close its old native watches"
    );

    let (lease_d, descriptor_d) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-delayed-open")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_d =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_d)
            .await
            .unwrap();
    let mut open_entered = environment.clock_gate.arm();
    let open_task = tokio::spawn(async move {
        transport_d
            .call(
                tinystoragedrivers_client::Object::Documents,
                "WatchOpen",
                json!({
                    "collection":"shared.records",
                    "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                    "interval_ms":1000
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), open_entered.recv())
        .await
        .expect("backend WatchOpen did not reach its gated native operation")
        .unwrap();
    assert!(registration.revoke(&lease_d).await);
    environment.clock_gate.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), open_task)
            .await
            .expect("revoked WatchOpen did not finish")
            .unwrap()
            .is_err(),
        "a WatchOpen completed after revocation must not be published"
    );
    assert_eq!(
        environment
            .server_for("profile-delayed-open")
            .open_watches(),
        0
    );

    let (lease_e, descriptor_e) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-epoch-change")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_e =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_e)
            .await
            .unwrap();
    let mut epoch_open_entered = environment.clock_gate.arm();
    let epoch_open_task = tokio::spawn(async move {
        transport_e
            .call(
                tinystoragedrivers_client::Object::Documents,
                "WatchOpen",
                json!({
                    "collection":"shared.records",
                    "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                    "interval_ms":1000
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), epoch_open_entered.recv())
        .await
        .expect("epoch-race WatchOpen did not reach its gated native operation")
        .unwrap();
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    environment.clock_gate.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), epoch_open_task)
            .await
            .expect("stale-epoch WatchOpen did not finish")
            .unwrap()
            .is_err(),
        "a WatchOpen resolved against a replaced binding must not be published"
    );
    assert_eq!(
        environment
            .server_for("profile-epoch-change")
            .open_watches(),
        0
    );
    assert!(registration.revoke(&lease_e).await);

    let (_lease_f, descriptor_f) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                "profile-resolution-race",
            )),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_f = Arc::new(
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_f)
            .await
            .unwrap(),
    );
    let mut resolution_entered = environment.resolve_gate.arm();
    let first_transport = Arc::clone(&transport_f);
    let late_resolution = tokio::spawn(async move {
        first_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), resolution_entered.recv())
        .await
        .expect("first backend resolution did not reach its gate")
        .unwrap();
    environment.binding_epoch.fetch_add(1, Ordering::AcqRel);
    assert!(
        transport_f
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_ok(),
        "the newer binding should serve while the older resolution is pending"
    );
    environment.resolve_gate.release();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), late_resolution)
            .await
            .expect("late backend resolution did not finish")
            .unwrap()
            .is_err(),
        "a late binding resolution must not publish or execute after a newer ticket"
    );

    let (_lease_g, descriptor_g) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile("profile-panic")),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_g =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_g)
            .await
            .unwrap();
    environment
        .panic_next_resolution
        .store(true, Ordering::Release);
    let panic_error = transport_g
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Get",
            json!({"collection":"shared.records","document_id":"same-id"}),
        )
        .await
        .unwrap_err();
    assert!(!format!("{panic_error:?}").contains("PrivateBackendPanic"));

    let outsider = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let forged_transport =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(outsider, descriptor_a.clone())
            .await
            .unwrap();
    assert!(
        forged_transport
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Capabilities",
                Value::Null,
            )
            .await
            .is_err()
    );
    let put = |profile: &'static str| {
        let mut request = serde_json::to_value(PutRequest {
            id: "same-id".to_owned(),
            doc: json!({"profile": profile}),
            precondition: Precondition::Absent,
        })
        .unwrap();
        request.as_object_mut().unwrap().insert(
            "collection".to_owned(),
            Value::String("shared.records".to_owned()),
        );
        request
    };
    transport_a
        .call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            put("alpha"),
        )
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(8),
        transport_b.call(
            tinystoragedrivers_client::Object::Documents,
            "Put",
            put("beta"),
        ),
    )
    .await
    .expect("put B timed out")
    .unwrap();

    let get = json!({"collection":"shared.records","document_id":"same-id"});
    let reply_a = tokio::time::timeout(
        Duration::from_secs(5),
        transport_a.call(
            tinystoragedrivers_client::Object::Documents,
            "Get",
            get.clone(),
        ),
    )
    .await
    .expect("get A timed out")
    .unwrap();
    let reply_b = tokio::time::timeout(
        Duration::from_secs(5),
        transport_b.call(tinystoragedrivers_client::Object::Documents, "Get", get),
    )
    .await
    .expect("get B timed out")
    .unwrap();
    let decoded_a: tinystoragedrivers_bus::Versioned = serde_json::from_value(reply_a).unwrap();
    let decoded_b: tinystoragedrivers_bus::Versioned = serde_json::from_value(reply_b).unwrap();
    assert_eq!(decoded_a.doc, json!({"profile":"alpha"}));
    assert_eq!(decoded_b.doc, json!({"profile":"beta"}));

    let secret_a = transport_a
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Set",
            json!({"name":"provider.key","value":[1,2,3]}),
        )
        .await
        .unwrap();
    let secret_b = transport_b
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Set",
            json!({"name":"provider.key","value":[4,5,6]}),
        )
        .await
        .unwrap();
    assert!(secret_a.is_null());
    assert!(secret_b.is_null());
    let secret_a = transport_a
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Get",
            json!({"name":"provider.key"}),
        )
        .await
        .unwrap();
    let secret_b = transport_b
        .call(
            tinystoragedrivers_client::Object::Secrets,
            "Get",
            json!({"name":"provider.key"}),
        )
        .await
        .unwrap();
    assert_eq!(secret_a, json!({"value":[1,2,3]}));
    assert_eq!(secret_b, json!({"value":[4,5,6]}));
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Secrets,
                "Get",
                json!({"name":"provider.key.suffix"}),
            )
            .await
            .is_err()
    );
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Secrets,
                "List",
                json!({"prefix":"provider.key"}),
            )
            .await
            .is_err()
    );

    let forged_scope =
        json!({"collection":"shared.records","document_id":"same-id","scope":"profile-beta"});
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                forged_scope
            )
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), registration.revoke(&lease_a))
            .await
            .expect("revoke A timed out")
    );
    assert!(
        transport_a
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"})
            )
            .await
            .is_err()
    );
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"})
            )
            .await
            .is_ok()
    );
    tokio::task::spawn_blocking(move || drop(lease_c))
        .await
        .unwrap();
    let mut route_withdrawn = false;
    for _ in 0..100 {
        match transport_c
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
        {
            Err(tinystoragedrivers_client::WireError::Transport(error))
                if !error.contains("denied") =>
            {
                route_withdrawn = true;
                break;
            }
            _ => tokio::task::yield_now().await,
        }
    }
    assert!(
        route_withdrawn,
        "dropping a lease after the caller runtime exited must withdraw its route on the captured service runtime"
    );
    let (lease_k, descriptor_k) = registration
        .register_lease(
            tinystoragedrivers_busproxy::HostStorageContext::new(Profile(
                "profile-handle-drop-watch",
            )),
            caller.unique_name().unwrap(),
        )
        .await
        .unwrap();
    let transport_k =
        tinystoragedrivers_busproxy::BusTransport::connect_bound(caller.clone(), descriptor_k)
            .await
            .unwrap();
    transport_k
        .call(
            tinystoragedrivers_client::Object::Documents,
            "WatchOpen",
            json!({
                "collection":"shared.records",
                "filter":serde_json::to_value(tinystoragedrivers_core::Filter::All).unwrap(),
                "interval_ms":1000
            }),
        )
        .await
        .unwrap();
    let dropped_server = environment.server_for("profile-handle-drop-watch");
    assert_eq!(dropped_server.open_watches(), 1);
    tokio::task::spawn_blocking(move || drop(lease_k))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while dropped_server.open_watches() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping an active lease handle did not close its native watch");
    assert_eq!(
        dropped_server.open_watches(),
        0,
        "dropping an active lease handle must close its backend watch on the captured runtime"
    );
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    let registration = Arc::try_unwrap(registration)
        .unwrap_or_else(|_| panic!("registration owner still has a caller-runtime reference"));
    tokio::time::timeout(Duration::from_secs(10), registration.shutdown())
        .await
        .expect("registration shutdown timed out")
        .unwrap();
    assert!(
        transport_b
            .call(
                tinystoragedrivers_client::Object::Documents,
                "Get",
                json!({"collection":"shared.records","document_id":"same-id"}),
            )
            .await
            .is_err()
    );
    assert_eq!(observer.unique_name().as_ref(), Some(&observer_peer));
    assert!(observer.name_owner(tinybus::BUS_NAME).await.is_ok());
    let peer_owner = observer.name_owner(descriptor_a.service_peer()).await;
    // The bus peer close does not assert TinyBus ModuleHost's separate
    // admission/lifetime state; the enclosing host retains that ownership.
    let host_admission_state = host
        .list()
        .into_iter()
        .find(|module| module.name == "tinystoragedrivers-tinybus");
    eprintln!(
        "post-registration shutdown diagnostics: peer_owner={peer_owner:?}, module_state={:?}",
        host_admission_state.map(|module| module.state)
    );
    tokio::time::timeout(
        Duration::from_secs(10),
        host.shutdown(Duration::from_secs(1)),
    )
    .await
    .expect("module host shutdown timed out");
    observer.close().await.unwrap();
}
