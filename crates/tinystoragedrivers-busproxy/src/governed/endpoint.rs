//! Per-call authorization adapters for host-owned storage context.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::{BusName, Connection, module::ModuleId};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, BlobStreamHeadWithExpiry, PutBlobStreamRequest,
    PutBlobStreamUntilRequest, StorageFailure, StorageKind, WatchChanged, WatchNotice, WatchReset,
};
use tinystoragedrivers_client::{Admission, Object, StorageServer};
use tinystoragedrivers_core::BlobReader;
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinHandle;

use crate::host::{ServedLease, serve_lease_on, serve_on};
use crate::{
    Denied, HostFailure, HostStorageContext, ServedStorage, StorageCall, StorageEndpoint,
    StorageGrant,
};

use super::types::{LeaseClaims, LeaseDropGuard};
use super::{HostContext, StorageDescriptor, StorageLease};

type LeaseEntries = Mutex<
    HashMap<
        u64,
        (
            ServedLease,
            Arc<AtomicBool>,
            Arc<AtomicBool>,
            BusName,
            ModuleId,
        ),
    >,
>;

/// Authoritative broker-peer to module admission lookup.
pub trait CallerDirectory: Send + Sync + 'static {
    /// Return the module admitted for this live unique peer.
    fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId>;
}

/// Adapter over `TinyBus` U5's weak, host-owned sender lookup.
pub struct TinyBusCallerDirectory(pub tinybus::module::ModuleSenderLookup);

impl std::fmt::Debug for TinyBusCallerDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TinyBusCallerDirectory(..)")
    }
}

impl CallerDirectory for TinyBusCallerDirectory {
    fn module_for_sender(&self, sender: &BusName) -> Option<ModuleId> {
        self.0.module_for_sender(sender)
    }
}

/// Storage resources selected by trusted host policy for one context/module.
pub struct ResolvedStorage {
    /// Stable identity/version for cache and replacement checks.
    pub backend_identity: String,
    /// Monotonic revision for this context/module binding. Increment when the
    /// selected backend, scope, fence, grant, or key set changes.
    pub binding_epoch: u64,
    /// Canonical host-resolved scope (never received from the request).
    pub canonical_scope: String,
    /// Existing validated grant with prefix and exact-secret rules.
    pub grant: StorageGrant,
    /// Server already bound to the resolved scope/backend/keys.
    pub server: Arc<StorageServer>,
}

impl std::fmt::Debug for ResolvedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedStorage")
            .field("backend_identity", &self.backend_identity)
            .field("canonical_scope", &"[REDACTED]")
            .field("grant", &self.grant)
            .finish_non_exhaustive()
    }
}

/// Host-specific authority resolution and write admission.
#[async_trait]
pub trait Environment<C: HostContext>: Send + Sync + 'static {
    /// Resolve context/module to the installed backend, canonical scope, grant,
    /// and server. Implementations must fail closed for ambiguous contexts.
    async fn resolve(&self, context: &C, module: &ModuleId)
    -> Result<ResolvedStorage, HostFailure>;

    /// Recheck host write admission immediately before each mutating operation.
    async fn admit_write(&self, context: &C, module: &ModuleId) -> Result<(), HostFailure>;
}

/// Content-free audit observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditEvent {
    /// Stable lease registration id.
    pub registration_id: u64,
    /// Storage object involved.
    pub object: Object,
    /// Closed outcome vocabulary; never request/backend text.
    pub outcome: &'static str,
    /// Whether the request was a mutation.
    pub write: bool,
}

/// Host audit sink. It receives no request, resource name, or error message.
pub trait AuditSink: Send + Sync + 'static {
    /// Record one content-free event.
    fn record(&self, event: AuditEvent);
}

/// Limits applied before a governed binding is made visible.
#[derive(Clone, Copy, Debug)]
pub struct GovernedEndpointLimits {
    /// Maximum individual blob body accepted by the storage bus.
    pub max_blob_bytes: u64,
}

impl Default for GovernedEndpointLimits {
    fn default() -> Self {
        Self {
            max_blob_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Reusable host policy bundle used to create a registration on an attested
/// service connection.
pub struct GovernedStorage<E, A, C> {
    environment: Arc<E>,
    audit: Arc<A>,
    limits: GovernedEndpointLimits,
    max_leases: usize,
    max_concurrent_bindings: usize,
    legacy_endpoint: Option<Arc<dyn StorageEndpoint>>,
    marker: std::marker::PhantomData<fn() -> C>,
}

impl<E, A, C> std::fmt::Debug for GovernedStorage<E, A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernedStorage")
            .field("limits", &self.limits)
            .field("legacy_endpoint", &self.legacy_endpoint.is_some())
            .finish_non_exhaustive()
    }
}

impl<E, A, C> Clone for GovernedStorage<E, A, C> {
    fn clone(&self) -> Self {
        Self {
            environment: Arc::clone(&self.environment),
            audit: Arc::clone(&self.audit),
            limits: self.limits,
            max_leases: self.max_leases,
            max_concurrent_bindings: self.max_concurrent_bindings,
            legacy_endpoint: self.legacy_endpoint.clone(),
            marker: std::marker::PhantomData,
        }
    }
}

struct LeaseReservation {
    count: Arc<AtomicUsize>,
    committed: bool,
}

impl LeaseReservation {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for LeaseReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn reserve_lease_slot(
    count: &Arc<AtomicUsize>,
    limit: usize,
) -> Result<LeaseReservation, HostFailure> {
    let mut current = count.load(Ordering::Acquire);
    loop {
        if current >= limit {
            return Err(HostFailure::Denied("lease-capacity"));
        }
        let next = current
            .checked_add(1)
            .ok_or(HostFailure::Denied("lease-capacity"))?;
        match count.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
    Ok(LeaseReservation {
        count: Arc::clone(count),
        committed: false,
    })
}

impl<E, A, C> GovernedStorage<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    /// Create host-owned policy for subsequent lease registrations.
    pub fn new(environment: Arc<E>, audit: Arc<A>, limits: GovernedEndpointLimits) -> Self {
        Self {
            environment,
            audit,
            limits,
            max_leases: 1024,
            max_concurrent_bindings: 64,
            legacy_endpoint: None,
            marker: std::marker::PhantomData,
        }
    }

    /// Opt into the compatibility fixed-root route for one explicitly trusted
    /// context. This route cannot detect a different business-profile request.
    #[must_use]
    pub fn with_legacy_endpoint(mut self, endpoint: Arc<dyn StorageEndpoint>) -> Self {
        self.legacy_endpoint = Some(endpoint);
        self
    }

    /// Bound live plus in-progress lease registrations for this service.
    #[must_use]
    pub fn with_max_leases(mut self, max_leases: usize) -> Self {
        self.max_leases = max_leases;
        self
    }

    /// Bound concurrent asynchronous backend resolutions across its leases.
    #[must_use]
    pub fn with_max_concurrent_bindings(mut self, max_bindings: usize) -> Self {
        self.max_concurrent_bindings = max_bindings;
        self
    }

    /// Bind this policy bundle to the linked service connection.
    pub fn register(
        &self,
        connection: Connection,
        callers: Arc<dyn CallerDirectory>,
        runtime: Handle,
    ) -> StorageRegistration<E, A, C> {
        StorageRegistration::new(RegistrationConfig {
            connection,
            environment: Arc::clone(&self.environment),
            audit: Arc::clone(&self.audit),
            callers,
            limits: self.limits,
            max_leases: self.max_leases,
            binding_slots: Arc::new(Semaphore::new(self.max_concurrent_bindings)),
            legacy_endpoint: self.legacy_endpoint.clone(),
            runtime,
        })
    }
}

struct RegistrationConfig<E, A> {
    connection: Connection,
    environment: Arc<E>,
    audit: Arc<A>,
    callers: Arc<dyn CallerDirectory>,
    limits: GovernedEndpointLimits,
    max_leases: usize,
    binding_slots: Arc<Semaphore>,
    legacy_endpoint: Option<Arc<dyn StorageEndpoint>>,
    runtime: Handle,
}

/// Owns all lease object paths served by one linked service connection.
pub struct StorageRegistration<E, A, C> {
    connection: Connection,
    environment: Arc<E>,
    audit: Arc<A>,
    callers: Arc<dyn CallerDirectory>,
    limits: GovernedEndpointLimits,
    next_id: AtomicU64,
    owner: Arc<()>,
    max_leases: usize,
    lease_count: Arc<AtomicUsize>,
    binding_slots: Arc<Semaphore>,
    leases: Arc<LeaseEntries>,
    closed: AtomicBool,
    cleanup_complete: AtomicBool,
    shutdown_lock: tokio::sync::Mutex<()>,
    legacy_endpoint: Option<Arc<dyn StorageEndpoint>>,
    legacy: Mutex<Option<ServedStorage>>,
    runtime: Handle,
    marker: std::marker::PhantomData<fn() -> C>,
}

impl<E, A, C> std::fmt::Debug for StorageRegistration<E, A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageRegistration")
            .finish_non_exhaustive()
    }
}

impl<E, A, C> StorageRegistration<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    /// Create an empty owner for per-lease objects served by `connection`.
    fn new(config: RegistrationConfig<E, A>) -> Self {
        Self {
            connection: config.connection,
            environment: config.environment,
            audit: config.audit,
            callers: config.callers,
            limits: config.limits,
            next_id: AtomicU64::new(1),
            owner: Arc::new(()),
            max_leases: config.max_leases,
            lease_count: Arc::new(AtomicUsize::new(0)),
            binding_slots: config.binding_slots,
            leases: Arc::new(Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
            cleanup_complete: AtomicBool::new(false),
            shutdown_lock: tokio::sync::Mutex::new(()),
            legacy_endpoint: config.legacy_endpoint,
            legacy: Mutex::new(None),
            runtime: config.runtime,
            marker: std::marker::PhantomData,
        }
    }

    /// Install the opt-in fixed-root objects on this same linked connection.
    ///
    /// # Errors
    /// Returns a content-free failure if the fixed-root interface cannot be served.
    pub async fn install_legacy(&self) -> Result<(), HostFailure> {
        let Some(endpoint) = &self.legacy_endpoint else {
            return Ok(());
        };
        let served = serve_on(&self.connection, Arc::clone(endpoint), self.runtime.clone())
            .await
            .map_err(|_| unavailable())?;
        let mut legacy = self
            .legacy
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        *legacy = Some(served);
        Ok(())
    }

    /// Publish the four exact objects for a captured context and intended peer.
    /// The descriptor is returned only after every object has been served.
    ///
    /// # Errors
    /// Returns a content-free failure when the caller is unknown, capacity is
    /// exhausted, setup fails, or the registration is shutting down.
    #[allow(
        clippy::too_many_lines,
        reason = "The lease publication transaction keeps its rollback and commit points ordered."
    )]
    pub async fn register_lease(
        &self,
        context: HostStorageContext<C>,
        intended_peer: BusName,
    ) -> Result<(StorageLease, StorageDescriptor), HostFailure> {
        if self.closed.load(Ordering::Acquire) {
            return Err(HostFailure::Denied("registration-shutdown"));
        }
        let reservation = reserve_lease_slot(&self.lease_count, self.max_leases)?;
        if !intended_peer.is_unique() {
            return Err(HostFailure::Denied("unique-caller-required"));
        }
        let module = self
            .callers
            .module_for_sender(&intended_peer)
            .ok_or(HostFailure::Denied("unknown-caller"))?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(HostFailure::Denied("lease-capacity"));
        }
        let revoked = Arc::new(AtomicBool::new(true));
        let published = Arc::new(AtomicBool::new(false));
        let leases = Arc::downgrade(&self.leases);
        let count = Arc::clone(&self.lease_count);
        let runtime = self.runtime.clone();
        let revoked_on_drop = Arc::clone(&revoked);
        let published_on_drop = Arc::clone(&published);
        let drop_guard = Arc::new(LeaseDropGuard::new(move || {
            revoked_on_drop.store(true, Ordering::Release);
            published_on_drop.store(false, Ordering::Release);
            let Some(leases) = leases.upgrade() else {
                return;
            };
            let removed = leases.lock().ok().and_then(|mut leases| {
                let entry = leases.get(&id)?;
                if !Arc::ptr_eq(&entry.1, &revoked_on_drop)
                    || !Arc::ptr_eq(&entry.2, &published_on_drop)
                {
                    return None;
                }
                leases.remove(&id)
            });
            if let Some((served, _, _, _, _)) = removed {
                count.fetch_sub(1, Ordering::AcqRel);
                runtime.spawn(served.shutdown());
            }
        }));
        let lease = StorageLease {
            registration_id: id,
            owner: Arc::clone(&self.owner),
            epoch: id,
            caller_peer: intended_peer,
            module,
            // Closed while paths are installed; activated only after all
            // four objects and the registration owner are published.
            revoked,
            published,
            _drop_guard: drop_guard,
        };
        let endpoint = GovernedEndpoint::for_lease(LeaseEndpointConfig {
            environment: Arc::clone(&self.environment),
            audit: Arc::clone(&self.audit),
            callers: Arc::clone(&self.callers),
            context,
            lease: LeaseClaims::from(&lease),
            limits: self.limits,
            binding_slots: Arc::clone(&self.binding_slots),
            runtime: self.runtime.clone(),
        });
        endpoint.warm(&lease.caller_peer).await?;
        let root = format!("{}/Leases/{id}", crate::names::OBJECT_ROOT);
        let served = serve_lease_on(
            &self.connection,
            &root,
            Arc::new(endpoint),
            self.runtime.clone(),
        )
        .await
        .map_err(|_| unavailable())?;
        if self.callers.module_for_sender(&lease.caller_peer).as_ref() != Some(&lease.module) {
            lease.revoked.store(true, Ordering::Release);
            served.shutdown().await;
            return Err(HostFailure::Denied("unknown-caller"));
        }
        if self.closed.load(Ordering::Acquire) {
            lease.revoked.store(true, Ordering::Release);
            served.shutdown().await;
            return Err(HostFailure::Denied("registration-shutdown"));
        }
        let Some(service_peer) = self.connection.unique_name() else {
            lease.revoked.store(true, Ordering::Release);
            served.shutdown().await;
            return Err(unavailable());
        };
        let descriptor = StorageDescriptor {
            service_peer: service_peer.as_str().to_owned(),
            registration_id: id,
            object_root: root,
        };
        let Ok(mut leases) = self.leases.lock() else {
            lease.revoked.store(true, Ordering::Release);
            served.shutdown().await;
            return Err(HostFailure::Denied("host-unavailable"));
        };
        if self.closed.load(Ordering::Acquire) {
            lease.revoked.store(true, Ordering::Release);
            drop(leases);
            served.shutdown().await;
            return Err(HostFailure::Denied("registration-shutdown"));
        }
        if leases
            .insert(
                id,
                (
                    served,
                    Arc::clone(&lease.revoked),
                    Arc::clone(&lease.published),
                    lease.caller_peer.clone(),
                    lease.module.clone(),
                ),
            )
            .is_some()
        {
            lease.revoked.store(true, Ordering::Release);
            return Err(HostFailure::Denied("lease-capacity"));
        }
        lease.published.store(true, Ordering::Release);
        lease.revoked.store(false, Ordering::Release);
        reservation.commit();
        Ok((lease, descriptor))
    }

    /// Revoke and withdraw one lease; calls already in flight finish only if
    /// their checks completed before revocation.
    pub async fn revoke(&self, lease: &StorageLease) -> bool {
        if !Arc::ptr_eq(&self.owner, &lease.owner) {
            return false;
        }
        lease.revoked.store(true, Ordering::Release);
        let served = self.leases.lock().ok().and_then(|mut leases| {
            let entry = leases.get(&lease.registration_id)?;
            if !Arc::ptr_eq(&entry.1, &lease.revoked)
                || !Arc::ptr_eq(&entry.2, &lease.published)
                || entry.3 != lease.caller_peer
            {
                return None;
            }
            leases.remove(&lease.registration_id)
        });
        if let Some((served, revoked, published, _peer, _module)) = served {
            self.lease_count.fetch_sub(1, Ordering::AcqRel);
            revoked.store(true, Ordering::Release);
            published.store(false, Ordering::Release);
            served.shutdown().await;
            true
        } else {
            false
        }
    }

    /// Revoke every lease for one broker-reported disconnected unique peer.
    pub async fn revoke_peer(&self, peer: &BusName) -> usize {
        let removed = self
            .leases
            .lock()
            .ok()
            .map(|mut leases| {
                let ids = leases
                    .iter()
                    .filter_map(|(id, (_, _, _, caller, _))| (caller == peer).then_some(*id))
                    .collect::<Vec<_>>();
                ids.into_iter()
                    .filter_map(|id| leases.remove(&id))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let count = removed.len();
        self.lease_count.fetch_sub(count, Ordering::AcqRel);
        for (served, revoked, published, _, _) in removed {
            revoked.store(true, Ordering::Release);
            published.store(false, Ordering::Release);
            served.shutdown().await;
        }
        count
    }

    /// Revoke leases whose authenticated unique peer is no longer admitted as
    /// the same module, for recovery after observer-signal lag.
    pub async fn reconcile_callers(&self) -> usize {
        let stale = self
            .leases
            .lock()
            .map(|leases| {
                leases
                    .values()
                    .filter_map(|(_, _, _, peer, module)| {
                        (self.callers.module_for_sender(peer).as_ref() != Some(module))
                            .then_some(peer.clone())
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let mut count = 0;
        for peer in stale {
            count += self.revoke_peer(&peer).await;
        }
        count
    }

    /// Whether the linked storage service peer is still admitted by the
    /// authoritative module directory. Used to recover service detach after
    /// the observer stream has lagged.
    pub fn service_peer_is_live(&self, peer: &BusName) -> bool {
        peer.is_unique() && self.callers.module_for_sender(peer).is_some()
    }

    /// Withdraw every lease and stop forwarding signals.
    ///
    /// # Errors
    /// Returns a sanitized bus failure if the linked service connection cannot be closed.
    pub async fn shutdown(&self) -> tinybus::Result<()> {
        let _shutdown = self.shutdown_lock.lock().await;
        self.closed.store(true, Ordering::Release);
        let drained = {
            let mut leases = self
                .leases
                .lock()
                .map_err(|_| tinybus::Error::failed("storage registration lock unavailable"))?;
            leases.drain().map(|(_, lease)| lease).collect::<Vec<_>>()
        };
        self.lease_count.fetch_sub(drained.len(), Ordering::AcqRel);
        for (served, revoked, published, _, _) in drained {
            revoked.store(true, Ordering::Release);
            published.store(false, Ordering::Release);
            served.shutdown().await;
        }
        let legacy = self.legacy.lock().ok().and_then(|mut legacy| legacy.take());
        if let Some(legacy) = legacy {
            legacy.shutdown().await?;
        }
        let result = self.connection.close().await;
        if result.is_ok() {
            self.cleanup_complete.store(true, Ordering::Release);
        }
        result
    }
}

impl<E, A, C> Drop for StorageRegistration<E, A, C> {
    fn drop(&mut self) {
        if self.cleanup_complete.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closed.store(true, Ordering::Release);
        let leases = self
            .leases
            .lock()
            .ok()
            .map(|mut leases| std::mem::take(&mut *leases))
            .unwrap_or_default();
        let mut served_leases = Vec::with_capacity(leases.len());
        for (_, (served, revoked, published, _peer, _module)) in leases {
            revoked.store(true, Ordering::Release);
            published.store(false, Ordering::Release);
            served_leases.push(served);
        }
        let legacy = self.legacy.get_mut().ok().and_then(Option::take);
        let connection = self.connection.clone();
        self.runtime.spawn(async move {
            for served in served_leases {
                served.shutdown().await;
            }
            if let Some(legacy) = legacy {
                let _ = legacy.shutdown().await;
            }
            let _ = connection.close().await;
        });
    }
}

fn unavailable() -> HostFailure {
    HostFailure::Storage(StorageFailure {
        kind: StorageKind::Unavailable,
    })
}

fn backend_failure() -> HostFailure {
    HostFailure::Storage(StorageFailure {
        kind: StorageKind::Backend,
    })
}

struct CatchUnwind<F> {
    future: Pin<Box<F>>,
}

impl<F> CatchUnwind<F> {
    fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F> Unpin for CatchUnwind<F> {}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, ()>;

    fn poll(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            this.future.as_mut().poll(context)
        })) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

async fn guarded<F: Future>(future: F) -> Result<F::Output, HostFailure> {
    CatchUnwind::new(future)
        .await
        .map_err(|()| backend_failure())
}

struct BoundServer {
    peer: BusName,
    backend_identity: String,
    canonical_scope: String,
    binding_epoch: u64,
    grant: StorageGrant,
    server: Arc<StorageServer>,
}

#[derive(Clone)]
struct WatchOwner {
    server: Arc<StorageServer>,
    binding_epoch: u64,
    backend_watch_id: String,
}

struct ResolutionGate {
    next: AtomicU64,
    latest: AtomicU64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RelayPublicationPause {
    BeforeCommit,
}

#[cfg(test)]
pub(super) struct RelayInstallTestGate {
    pub(super) epoch: u64,
    pub(super) pause: RelayPublicationPause,
    pub(super) reached: tokio::sync::mpsc::UnboundedSender<()>,
    pub(super) release: Arc<Semaphore>,
}

#[cfg(test)]
pub(super) struct RelayRecvTestGate {
    pub(super) reached: tokio::sync::mpsc::UnboundedSender<()>,
    pub(super) release: Arc<Semaphore>,
    pub(super) after_lag: Option<RelayRecvTestStage>,
}

#[cfg(test)]
pub(super) struct RelayRecvTestStage {
    pub(super) reached: tokio::sync::mpsc::UnboundedSender<()>,
    pub(super) release: Arc<Semaphore>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RelayTestEvent {
    Installed(u64),
    Forwarded { epoch: u64, watch_id: String },
}

impl ResolutionGate {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            latest: AtomicU64::new(0),
        }
    }

    fn begin(&self) -> u64 {
        let ticket = self.next.fetch_add(1, Ordering::AcqRel);
        self.latest.fetch_max(ticket, Ordering::AcqRel);
        ticket
    }

    fn is_latest(&self, ticket: u64) -> bool {
        self.latest.load(Ordering::Acquire) == ticket
    }
}

/// Per-lease typed adapter implementing the unchanged object-safe endpoint.
pub struct GovernedEndpoint<E, A, C> {
    environment: Arc<E>,
    audit: Arc<A>,
    callers: Arc<dyn CallerDirectory>,
    context: Arc<C>,
    lease: Arc<LeaseClaims>,
    limits: GovernedEndpointLimits,
    binding_slots: Arc<Semaphore>,
    bound_server: Mutex<Option<BoundServer>>,
    resolution_gate: ResolutionGate,
    watch_ids: Arc<Mutex<HashMap<String, WatchOwner>>>,
    next_watch_id: AtomicU64,
    signals: broadcast::Sender<WatchChanged>,
    signal_relay: Mutex<Option<JoinHandle<()>>>,
    runtime: Handle,
    #[cfg(test)]
    relay_install_test_gate: Mutex<Option<RelayInstallTestGate>>,
    #[cfg(test)]
    relay_test_events: Mutex<Option<tokio::sync::mpsc::UnboundedSender<RelayTestEvent>>>,
    #[cfg(test)]
    relay_recv_test_gate: Arc<Mutex<Option<RelayRecvTestGate>>>,
}

struct LeaseEndpointConfig<E, A, C> {
    environment: Arc<E>,
    audit: Arc<A>,
    callers: Arc<dyn CallerDirectory>,
    context: HostStorageContext<C>,
    lease: LeaseClaims,
    limits: GovernedEndpointLimits,
    binding_slots: Arc<Semaphore>,
    runtime: Handle,
}

impl<E, A, C> std::fmt::Debug for GovernedEndpoint<E, A, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernedEndpoint").finish_non_exhaustive()
    }
}

impl<E, A, C> Drop for GovernedEndpoint<E, A, C> {
    fn drop(&mut self) {
        if let Ok(mut relay) = self.signal_relay.lock()
            && let Some(relay) = relay.take()
        {
            relay.abort();
        }
        let watches = self
            .watch_ids
            .lock()
            .map(|mut watches| std::mem::take(&mut *watches))
            .unwrap_or_default();
        self.runtime.spawn(async move {
            for owner in watches.into_values() {
                let _ = guarded(owner.server.handle(
                    Admission::Private,
                    Object::Documents.as_str(),
                    "WatchClose",
                    serde_json::json!({"watch_id": owner.backend_watch_id}),
                ))
                .await;
            }
        });
    }
}

impl<E, A, C> GovernedEndpoint<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    /// Construct one endpoint adapter for a context and already admitted lease.
    fn for_lease(config: LeaseEndpointConfig<E, A, C>) -> Self {
        Self {
            environment: config.environment,
            audit: config.audit,
            callers: config.callers,
            context: Arc::new(config.context.into_inner()),
            lease: Arc::new(config.lease),
            limits: config.limits,
            binding_slots: config.binding_slots,
            bound_server: Mutex::new(None),
            resolution_gate: ResolutionGate::new(),
            watch_ids: Arc::new(Mutex::new(HashMap::new())),
            next_watch_id: AtomicU64::new(1),
            signals: broadcast::channel(256).0,
            signal_relay: Mutex::new(None),
            runtime: config.runtime,
            #[cfg(test)]
            relay_install_test_gate: Mutex::new(None),
            #[cfg(test)]
            relay_test_events: Mutex::new(None),
            #[cfg(test)]
            relay_recv_test_gate: Arc::new(Mutex::new(None)),
        }
    }

    fn module(&self, sender: Option<&BusName>) -> Result<ModuleId, HostFailure> {
        if self.lease.revoked.load(Ordering::Acquire)
            || !self.lease.published.load(Ordering::Acquire)
        {
            return Err(HostFailure::Denied("lease-revoked"));
        }
        let sender = sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        if sender != &self.lease.caller_peer {
            return Err(HostFailure::Denied("lease-caller-mismatch"));
        }
        let module = self
            .callers
            .module_for_sender(sender)
            .ok_or(HostFailure::Denied("unknown-caller"))?;
        if module != self.lease.module {
            return Err(HostFailure::Denied("lease-module-mismatch"));
        }
        Ok(module)
    }

    async fn resolved(
        &self,
        sender: Option<&BusName>,
    ) -> Result<(ModuleId, ResolvedStorage, u64), HostFailure> {
        let module = self.module(sender)?;
        let ticket = self.resolution_gate.begin();
        let _binding = self
            .binding_slots
            .try_acquire()
            .map_err(|_| HostFailure::Denied("binding-capacity"))?;
        let mut resolved = guarded(self.environment.resolve(&self.context, &module)).await??;
        let sender = sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        self.module(Some(sender))?;
        if let Ok(held) = self.bound_server.lock()
            && let Some(binding) = held.as_ref()
            && binding.peer == *sender
            && binding.binding_epoch == resolved.binding_epoch
        {
            if binding.backend_identity != resolved.backend_identity
                || binding.canonical_scope != resolved.canonical_scope
                || binding.grant != resolved.grant
            {
                return Err(HostFailure::Denied("binding-epoch-not-advanced"));
            }
            resolved.server = Arc::clone(&binding.server);
        }
        if !self.resolution_gate.is_latest(ticket) {
            return Err(HostFailure::Denied("stale-binding-resolution"));
        }
        Ok((module, resolved, ticket))
    }

    async fn warm(&self, sender: &BusName) -> Result<(), HostFailure> {
        let module = self
            .callers
            .module_for_sender(sender)
            .ok_or(HostFailure::Denied("unknown-caller"))?;
        if sender != &self.lease.caller_peer || module != self.lease.module {
            return Err(HostFailure::Denied("lease-module-mismatch"));
        }
        let _binding = self
            .binding_slots
            .try_acquire()
            .map_err(|_| HostFailure::Denied("binding-capacity"))?;
        let resolved = guarded(self.environment.resolve(&self.context, &module)).await??;
        let server = Arc::clone(&resolved.server);
        let mut held = self
            .bound_server
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        *held = Some(BoundServer {
            peer: sender.clone(),
            backend_identity: resolved.backend_identity,
            canonical_scope: resolved.canonical_scope,
            binding_epoch: resolved.binding_epoch,
            grant: resolved.grant,
            server: resolved.server,
        });
        drop(held);
        self.start_signal_relay(&server, resolved.binding_epoch);
        Ok(())
    }

    async fn revalidate_after_backend(
        &self,
        sender: &BusName,
        resolved: &ResolvedStorage,
        ticket: u64,
    ) -> Result<(), HostFailure> {
        let module = self.module(Some(sender))?;
        if !self.resolution_gate.is_latest(ticket) {
            return Err(HostFailure::Denied("stale-binding-resolution"));
        }
        let current = guarded(self.environment.resolve(&self.context, &module)).await??;
        self.module(Some(sender))?;
        if !self.resolution_gate.is_latest(ticket)
            || current.binding_epoch != resolved.binding_epoch
            || current.backend_identity != resolved.backend_identity
            || current.canonical_scope != resolved.canonical_scope
            || current.grant != resolved.grant
        {
            return Err(HostFailure::Denied("stale-binding-resolution"));
        }
        Ok(())
    }

    fn audit(&self, object: Object, outcome: &'static str, write: bool) {
        self.audit.record(AuditEvent {
            registration_id: self.lease.registration_id,
            object,
            outcome,
            write,
        });
    }

    async fn publish_server(
        &self,
        sender: Option<&BusName>,
        resolved: &ResolvedStorage,
        ticket: u64,
    ) -> Result<(), HostFailure> {
        let sender = sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        // Recheck revocation/admission immediately before making a completed
        // async resolution observable to watch forwarding.
        self.module(Some(sender))?;
        if !self.resolution_gate.is_latest(ticket) {
            return Err(HostFailure::Denied("stale-binding-resolution"));
        }
        // Test barriers are deliberately before the commit critical section:
        // no test (or production caller) may suspend while binding/watch/relay
        // state is only partially published.
        #[cfg(test)]
        self.pause_publication_for_test(
            resolved.binding_epoch,
            RelayPublicationPause::BeforeCommit,
        )
        .await;

        let (changed, previous, stale_watches) = {
            let mut held = self
                .bound_server
                .lock()
                .map_err(|_| HostFailure::Denied("host-unavailable"))?;
            if !self.resolution_gate.is_latest(ticket) {
                return Err(HostFailure::Denied("stale-binding-resolution"));
            }
            if let Some(binding) = held.as_ref() {
                if resolved.binding_epoch < binding.binding_epoch {
                    return Err(HostFailure::Denied("stale-binding-resolution"));
                }
                if resolved.binding_epoch == binding.binding_epoch
                    && (binding.backend_identity != resolved.backend_identity
                        || binding.canonical_scope != resolved.canonical_scope
                        || binding.grant != resolved.grant)
                {
                    return Err(HostFailure::Denied("binding-epoch-not-advanced"));
                }
            }
            let changed = held.as_ref().is_none_or(|binding| {
                binding.binding_epoch != resolved.binding_epoch
                    || !Arc::ptr_eq(&binding.server, &resolved.server)
            });
            let previous = if changed {
                let previous = held.take();
                *held = Some(BoundServer {
                    peer: sender.clone(),
                    backend_identity: resolved.backend_identity.clone(),
                    canonical_scope: resolved.canonical_scope.clone(),
                    binding_epoch: resolved.binding_epoch,
                    grant: resolved.grant.clone(),
                    server: Arc::clone(&resolved.server),
                });
                let mut watches = self
                    .watch_ids
                    .lock()
                    .map_err(|_| HostFailure::Denied("host-unavailable"))?;
                let stale_watches = std::mem::take(&mut *watches);
                self.install_signal_relay_locked(&resolved.server, resolved.binding_epoch);
                for watch_id in stale_watches.keys() {
                    let _ = self.signals.send(WatchChanged {
                        watch_id: watch_id.clone(),
                        notice: WatchNotice::ResetRequired {
                            reason: WatchReset::Backend,
                        },
                    });
                }
                (previous, stale_watches)
            } else {
                (None, HashMap::new())
            };
            (changed, previous.0, previous.1)
        };
        if changed && previous.is_some() {
            for owner in stale_watches.into_values() {
                let _ = guarded(owner.server.handle(
                    Admission::Private,
                    Object::Documents.as_str(),
                    "WatchClose",
                    serde_json::json!({"watch_id": owner.backend_watch_id}),
                ))
                .await;
            }
        }
        self.module(Some(sender))?;
        if !self.resolution_gate.is_latest(ticket) {
            return Err(HostFailure::Denied("stale-binding-resolution"));
        }
        Ok(())
    }

    fn start_signal_relay(&self, server: &Arc<StorageServer>, binding_epoch: u64) {
        // Serialize relay installation with backend publication. Keep the
        // bound lock until replacement commits so a newer publication cannot
        // pass between validation and installation.
        let Ok(bound) = self.bound_server.lock() else {
            return;
        };
        if !bound.as_ref().is_some_and(|current| {
            current.binding_epoch == binding_epoch && Arc::ptr_eq(&current.server, server)
        }) {
            return;
        }
        self.install_signal_relay_locked(server, binding_epoch);
    }

    /// Replace the relay while the caller holds `bound_server`, making relay
    /// state part of the same visible binding commit.
    fn install_signal_relay_locked(&self, server: &Arc<StorageServer>, binding_epoch: u64) {
        let Ok(mut current) = self.signal_relay.lock() else {
            return;
        };
        if let Some(old) = current.take() {
            old.abort();
        }
        let mut input = server.signals();
        let server = Arc::clone(server);
        let output = self.signals.clone();
        let watch_ids = Arc::clone(&self.watch_ids);
        #[cfg(test)]
        let recv_test_gate = Arc::clone(&self.relay_recv_test_gate);
        #[cfg(test)]
        let test_events = self
            .relay_test_events
            .lock()
            .ok()
            .and_then(|events| events.clone());
        *current = Some(self.runtime.spawn(async move {
            #[cfg(test)]
            let mut after_lag_test_gate = None;
            loop {
                #[cfg(test)]
                {
                    let gate = recv_test_gate.lock().ok().and_then(|mut gate| gate.take());
                    if let Some(gate) = gate {
                        after_lag_test_gate = gate.after_lag;
                        let _ = gate.reached.send(());
                        if let Ok(permit) = gate.release.acquire().await {
                            permit.forget();
                        }
                    }
                }
                match input.recv().await {
                    Ok(signal) => {
                        let outward_id = watch_ids.lock().ok().and_then(|ids| {
                            ids.iter().find_map(|(watch_id, owner)| {
                                (owner.backend_watch_id == signal.watch_id
                                    && owner.binding_epoch == binding_epoch
                                    && Arc::ptr_eq(&owner.server, &server))
                                .then(|| watch_id.clone())
                            })
                        });
                        if let Some(watch_id) = outward_id {
                            #[cfg(test)]
                            if let Some(events) = &test_events {
                                let _ = events.send(RelayTestEvent::Forwarded {
                                    epoch: binding_epoch,
                                    watch_id: watch_id.clone(),
                                });
                            }
                            let mut signal = signal;
                            signal.watch_id = watch_id;
                            let _ = output.send(signal);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let ids = watch_ids
                            .lock()
                            .map(|ids| {
                                ids.iter()
                                    .filter(|(_, owner)| {
                                        owner.binding_epoch == binding_epoch
                                            && Arc::ptr_eq(&owner.server, &server)
                                    })
                                    .map(|(watch_id, _)| watch_id.clone())
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        for watch_id in ids {
                            let _ = output.send(WatchChanged {
                                watch_id,
                                notice: WatchNotice::ResetRequired {
                                    reason: WatchReset::Backend,
                                },
                            });
                        }
                        #[cfg(test)]
                        if let Some(gate) = after_lag_test_gate.take() {
                            let _ = gate.reached.send(());
                            if let Ok(permit) = gate.release.acquire().await {
                                permit.forget();
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }));
        #[cfg(test)]
        if let Ok(events) = self.relay_test_events.lock()
            && let Some(events) = &*events
        {
            let _ = events.send(RelayTestEvent::Installed(binding_epoch));
        }
    }

    #[cfg(test)]
    async fn pause_publication_for_test(&self, binding_epoch: u64, pause: RelayPublicationPause) {
        let gate = self
            .relay_install_test_gate
            .lock()
            .ok()
            .and_then(|mut gate| {
                gate.as_ref()
                    .is_some_and(|gate| gate.epoch == binding_epoch && gate.pause == pause)
                    .then(|| gate.take())
                    .flatten()
            });
        if let Some(gate) = gate {
            let _ = gate.reached.send(());
            if let Ok(permit) = gate.release.acquire().await {
                permit.forget();
            }
        }
    }

    fn record_watch_if_current(
        &self,
        sender: &BusName,
        resolved: &ResolvedStorage,
        ticket: u64,
        backend_watch_id: &str,
    ) -> Option<String> {
        if !self.resolution_gate.is_latest(ticket) {
            return None;
        }
        let Ok(bound) = self.bound_server.lock() else {
            return None;
        };
        let current = bound.as_ref().is_some_and(|binding| {
            binding.peer == *sender
                && binding.binding_epoch == resolved.binding_epoch
                && binding.backend_identity == resolved.backend_identity
                && binding.canonical_scope == resolved.canonical_scope
                && binding.grant == resolved.grant
                && Arc::ptr_eq(&binding.server, &resolved.server)
        });
        if !current || !self.resolution_gate.is_latest(ticket) {
            return None;
        }
        let Ok(mut watches) = self.watch_ids.lock() else {
            return None;
        };
        let mut sequence = self.next_watch_id.load(Ordering::Acquire);
        loop {
            let next = sequence.checked_add(1)?;
            match self.next_watch_id.compare_exchange_weak(
                sequence,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => sequence = actual,
            }
        }
        let watch_id = format!(
            "host-watch-{:016x}-{:016x}",
            self.lease.registration_id, sequence
        );
        watches.insert(
            watch_id.clone(),
            WatchOwner {
                server: Arc::clone(&resolved.server),
                binding_epoch: resolved.binding_epoch,
                backend_watch_id: backend_watch_id.to_owned(),
            },
        );
        Some(watch_id)
    }
}

fn map_denied(denied: Denied) -> HostFailure {
    HostFailure::Denied(denied.label())
}
fn invalid() -> HostFailure {
    HostFailure::Storage(StorageFailure {
        kind: StorageKind::InvalidInput,
    })
}

#[async_trait]
impl<E, A, C> StorageEndpoint for GovernedEndpoint<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    #[allow(
        clippy::too_many_lines,
        reason = "The per-call policy, backend fence, and watch ownership transaction must stay ordered."
    )]
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure> {
        let opens_watch = call.object == Object::Documents && call.selector == "WatchOpen";
        let closes_watch = if call.object == Object::Documents && call.selector == "WatchClose" {
            call.request
                .get("watch_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            None
        };
        let (module, resolved, ticket) = self.resolved(call.sender).await?;
        let plan = resolved
            .grant
            .authorize(call.object, call.selector, &call.request)
            .map_err(map_denied)?;
        let (mut request, filter, write) = match plan {
            crate::Plan::Answer(answer) => {
                self.audit(call.object, "allowed", false);
                return Ok(answer);
            }
            crate::Plan::Forward { filter, write, .. } => (call.request, filter, write),
        };
        if write {
            guarded(self.environment.admit_write(&self.context, &module)).await??;
        }
        let mut close_owner = None;
        if let Some(watch_id) = closes_watch.as_deref() {
            self.publish_server(call.sender, &resolved, ticket).await?;
            let owner = self
                .watch_ids
                .lock()
                .ok()
                .and_then(|ids| ids.get(watch_id).cloned())
                .filter(|owner| {
                    owner.binding_epoch == resolved.binding_epoch
                        && Arc::ptr_eq(&owner.server, &resolved.server)
                });
            let Some(owner) = owner else {
                self.audit(call.object, "denied", false);
                return Err(HostFailure::Denied("watch-handle-not-owned"));
            };
            request["watch_id"] = serde_json::json!(owner.backend_watch_id);
            close_owner = Some((watch_id.to_owned(), owner));
        } else {
            self.publish_server(call.sender, &resolved, ticket).await?;
        }
        let result = guarded(resolved.server.handle(
            call.admission,
            call.object.as_str(),
            call.selector,
            request,
        ))
        .await??;
        let sender = call
            .sender
            .ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        if let Err(error) = self
            .revalidate_after_backend(sender, &resolved, ticket)
            .await
        {
            if opens_watch && let Some(watch_id) = result.get("watch_id").and_then(Value::as_str) {
                let _ = guarded(resolved.server.handle(
                    call.admission,
                    Object::Documents.as_str(),
                    "WatchClose",
                    serde_json::json!({"watch_id": watch_id}),
                ))
                .await;
            }
            return Err(error);
        }
        let opened_watch_id = opens_watch
            .then(|| {
                result
                    .get("watch_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        let mut filtered = match resolved.grant.filter_reply(filter, result) {
            Ok(filtered) => filtered,
            Err(error) => {
                if let Some(watch_id) = opened_watch_id {
                    let _ = guarded(resolved.server.handle(
                        call.admission,
                        Object::Documents.as_str(),
                        "WatchClose",
                        serde_json::json!({"watch_id": watch_id}),
                    ))
                    .await;
                }
                return Err(map_denied(error));
            }
        };
        if opens_watch
            && let Some(backend_watch_id) = filtered.get("watch_id").and_then(Value::as_str)
        {
            let backend_watch_id = backend_watch_id.to_owned();
            let Some(sender) = call.sender else {
                let _ = guarded(resolved.server.handle(
                    call.admission,
                    Object::Documents.as_str(),
                    "WatchClose",
                    serde_json::json!({"watch_id": backend_watch_id}),
                ))
                .await;
                return Err(HostFailure::Denied("unauthenticated-caller"));
            };
            let Some(watch_id) =
                self.record_watch_if_current(sender, &resolved, ticket, &backend_watch_id)
            else {
                let _ = guarded(resolved.server.handle(
                    call.admission,
                    Object::Documents.as_str(),
                    "WatchClose",
                    serde_json::json!({"watch_id": backend_watch_id}),
                ))
                .await;
                return Err(HostFailure::Denied("stale-binding-resolution"));
            };
            filtered["watch_id"] = serde_json::json!(watch_id);
        }
        if let Some((watch_id, closed_owner)) = close_owner
            && let Ok(mut ids) = self.watch_ids.lock()
            && ids.get(&watch_id).is_some_and(|owner| {
                owner.binding_epoch == closed_owner.binding_epoch
                    && owner.backend_watch_id == closed_owner.backend_watch_id
                    && Arc::ptr_eq(&owner.server, &closed_owner.server)
            })
        {
            ids.remove(&watch_id);
        }
        self.audit(call.object, "allowed", write);
        Ok(filtered)
    }

    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        if head.len > self.limits.max_blob_bytes {
            self.audit(Object::Blobs, "denied", true);
            return Err(invalid());
        }
        let (module, resolved, ticket) = self.resolved(sender).await?;
        let request = serde_json::to_value(&head).map_err(|_| invalid())?;
        let plan = resolved
            .grant
            .authorize(Object::Blobs, "PutStream", &request)
            .map_err(map_denied)?;
        let write = matches!(plan, crate::Plan::Forward { write: true, .. });
        if write {
            guarded(self.environment.admit_write(&self.context, &module)).await??;
        }
        self.publish_server(sender, &resolved, ticket).await?;
        let value = guarded(resolved.server.put_blob_stream(head, body)).await??;
        self.revalidate_after_backend(
            sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?,
            &resolved,
            ticket,
        )
        .await?;
        serde_json::to_value(value).map_err(|_| invalid())
    }

    async fn put_blob_stream_until(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamUntilRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        if head.len > self.limits.max_blob_bytes {
            self.audit(Object::Blobs, "denied", true);
            return Err(invalid());
        }
        let (module, resolved, ticket) = self.resolved(sender).await?;
        let request = serde_json::to_value(&head).map_err(|_| invalid())?;
        let plan = resolved
            .grant
            .authorize(Object::Blobs, "PutStreamUntil", &request)
            .map_err(map_denied)?;
        let write = matches!(plan, crate::Plan::Forward { write: true, .. });
        if write {
            guarded(self.environment.admit_write(&self.context, &module)).await??;
        }
        self.publish_server(sender, &resolved, ticket).await?;
        let value = guarded(resolved.server.put_blob_stream_until(head, body)).await??;
        self.revalidate_after_backend(
            sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?,
            &resolved,
            ticket,
        )
        .await?;
        serde_json::to_value(value).map_err(|_| invalid())
    }

    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        let (_module, resolved, ticket) = self.resolved(sender).await?;
        let value = serde_json::to_value(&request).map_err(|_| invalid())?;
        resolved
            .grant
            .authorize(Object::Blobs, "GetStream", &value)
            .map_err(map_denied)?;
        self.publish_server(sender, &resolved, ticket).await?;
        let found = guarded(resolved.server.get_blob_stream(request)).await??;
        self.revalidate_after_backend(
            sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?,
            &resolved,
            ticket,
        )
        .await?;
        if found
            .as_ref()
            .is_some_and(|(head, _)| head.meta.len > self.limits.max_blob_bytes)
        {
            self.audit(Object::Blobs, "denied", false);
            return Err(invalid());
        }
        Ok(found)
    }

    async fn get_blob_stream_with_expiry(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHeadWithExpiry, BlobReader)>, HostFailure> {
        let (_module, resolved, ticket) = self.resolved(sender).await?;
        let value = serde_json::to_value(&request).map_err(|_| invalid())?;
        resolved
            .grant
            .authorize(Object::Blobs, "GetStreamWithExpiry", &value)
            .map_err(map_denied)?;
        self.publish_server(sender, &resolved, ticket).await?;
        let found = guarded(resolved.server.get_blob_stream_with_expiry(request)).await??;
        self.revalidate_after_backend(
            sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?,
            &resolved,
            ticket,
        )
        .await?;
        if found
            .as_ref()
            .is_some_and(|(head, _)| head.meta.len > self.limits.max_blob_bytes)
        {
            self.audit(Object::Blobs, "denied", false);
            return Err(invalid());
        }
        Ok(found)
    }

    fn watch_signals(&self, sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        if self.module(Some(sender)).is_err() {
            return None;
        }
        Some(self.signals.subscribe())
    }

    fn watch_epoch(&self, _sender: &BusName) -> u64 {
        self.lease.epoch
    }
}

#[cfg(test)]
#[path = "endpoint_tests.rs"]
mod tests;
