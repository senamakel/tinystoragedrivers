use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

struct TestContext;
impl HostContext for TestContext {}

struct TestEnvironment {
    resolve_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Environment<TestContext> for TestEnvironment {
    async fn resolve(
        &self,
        _context: &TestContext,
        _module: &tinybus::module::ModuleId,
    ) -> Result<tinystoragedrivers_busproxy::ResolvedStorage, HostFailure> {
        self.resolve_calls.fetch_add(1, Ordering::AcqRel);
        Err(HostFailure::Denied("unused-test-environment"))
    }

    async fn admit_write(
        &self,
        _context: &TestContext,
        _module: &tinybus::module::ModuleId,
    ) -> Result<(), HostFailure> {
        Err(HostFailure::Denied("unused-test-environment"))
    }
}

struct TestAudit;
impl AuditSink for TestAudit {
    fn record(&self, _event: tinystoragedrivers_busproxy::AuditEvent) {}
}

struct RejectAllCallers;
impl CallerDirectory for RejectAllCallers {
    fn module_for_sender(&self, _sender: &BusName) -> Option<tinybus::module::ModuleId> {
        None
    }
}

pub(crate) fn empty_registration_for_test<E, A, C>(runtime: Handle) -> StorageRegistration<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    StorageRegistration {
        inner: None,
        monitor: None,
        runtime,
        cleanup_probe: None,
    }
}

#[tokio::test]
async fn registration_debug_and_shutdown_cover_the_missing_monitor_path() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    let broker = tinybus::broker::Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service_peer = service.unique_name().unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let survivor = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let survivor_name = "ai.tinyhumans.test.storagehostsurvivor";
    survivor.request_name(survivor_name).await.unwrap();
    let resolve_calls = Arc::new(AtomicUsize::new(0));
    let inner = GovernedStorage::new(
        Arc::new(TestEnvironment {
            resolve_calls: Arc::clone(&resolve_calls),
        }),
        Arc::new(TestAudit),
        tinystoragedrivers_busproxy::GovernedEndpointLimits::default(),
    )
    .register(service, Arc::new(RejectAllCallers), Handle::current());
    let registration = StorageRegistration {
        inner: Some(Arc::new(inner)),
        monitor: None,
        runtime: Handle::current(),
        cleanup_probe: None,
    };
    let rendered = format!("{registration:?}");
    assert!(rendered.contains("StorageRegistration"));
    assert!(!rendered.contains("unused-test-environment"));

    // This is the internal empty-owner teardown state, not a promise that the
    // public consuming shutdown method can be called repeatedly.
    let empty_registration: StorageRegistration<TestEnvironment, TestAudit, TestContext> =
        StorageRegistration {
            inner: None,
            monitor: None,
            runtime: Handle::current(),
            cleanup_probe: None,
        };
    empty_registration.shutdown().await.unwrap();
    assert_eq!(resolve_calls.load(Ordering::Acquire), 0);
    assert_eq!(
        observer.name_owner(service_peer.as_str()).await.unwrap(),
        Some(service_peer),
        "empty-owner cleanup must not close another live service registration"
    );
    assert_eq!(
        observer.name_owner(survivor_name).await.unwrap(),
        survivor.unique_name(),
        "the borrowed observer and unrelated live peer remain usable"
    );
    registration.shutdown().await.unwrap();
}
