//! Lifecycle-owned `TinyBus` attachment for governed storage registrations.

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;
use tinybus::{BusName, Connection, InterfaceName, MatchRule, MemberName, module::ModuleHost};
use tinystoragedrivers_busproxy::{
    AuditSink, CallerDirectory, Environment, GovernedStorage, HostContext, HostFailure,
    StorageDescriptor, StorageLease, TinyBusCallerDirectory,
};
use tokio::runtime::Handle;
#[cfg(test)]
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{broadcast, oneshot};

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageDropStage {
    OwnerDropEntered,
    CleanupTaskStarted,
    ShutdownFinished(ShutdownStatus),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShutdownStatus {
    Success,
    Failure,
}

/// TSD-owned storage registration returned after the linked module is ready.
pub struct StorageRegistration<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    inner: Option<Arc<tinystoragedrivers_busproxy::governed::StorageRegistration<E, A, C>>>,
    monitor: Option<tokio::task::JoinHandle<()>>,
    runtime: Handle,
    #[cfg(test)]
    cleanup_probe: Option<UnboundedSender<StorageDropStage>>,
}

impl<E, A, C> std::fmt::Debug for StorageRegistration<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
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
    /// Register one context against a live, authenticated unique module peer.
    ///
    /// # Errors
    /// Returns a content-free failure when the caller is unknown, capacity is
    /// exhausted, setup fails, or the registration is shutting down.
    pub async fn register_lease(
        &self,
        context: tinystoragedrivers_busproxy::HostStorageContext<C>,
        peer: tinybus::BusName,
    ) -> Result<(StorageLease, StorageDescriptor), HostFailure> {
        self.inner
            .as_ref()
            .ok_or(HostFailure::Denied("registration-shutdown"))?
            .register_lease(context, peer)
            .await
    }

    /// Revoke and withdraw one lease.
    pub async fn revoke(&self, lease: &StorageLease) -> bool {
        match self.inner.as_ref() {
            Some(inner) => inner.revoke(lease).await,
            None => false,
        }
    }

    /// Revoke all leases and withdraw their exact paths before closing the
    /// linked service connection. The borrowed observer remains caller-owned.
    ///
    /// # Errors
    /// Returns a sanitized bus failure if the linked service connection cannot be closed.
    pub async fn shutdown(mut self) -> tinybus::Result<()> {
        if let Some(monitor) = self.monitor.take() {
            monitor.abort();
        }
        if let Some(inner) = self.inner.take() {
            inner.shutdown().await?;
        }
        Ok(())
    }
}

impl<E, A, C> Drop for StorageRegistration<E, A, C>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    fn drop(&mut self) {
        let inner = self.inner.take();
        #[cfg(test)]
        if let Some(probe) = &self.cleanup_probe {
            let _ = probe.send(StorageDropStage::OwnerDropEntered);
        }
        if let Some(monitor) = self.monitor.take() {
            monitor.abort();
        }
        #[cfg(test)]
        let cleanup_probe = self.cleanup_probe.clone();
        self.runtime.spawn(async move {
            #[cfg(test)]
            if let Some(probe) = &cleanup_probe {
                let _ = probe.send(StorageDropStage::CleanupTaskStarted);
            }
            if let Some(inner) = inner {
                #[cfg(test)]
                let shutdown_succeeded = matches!(inner.shutdown().await, Ok(()));
                #[cfg(not(test))]
                let _ = inner.shutdown().await;
                #[cfg(test)]
                if let Some(probe) = cleanup_probe {
                    let status = if shutdown_succeeded {
                        ShutdownStatus::Success
                    } else {
                        ShutdownStatus::Failure
                    };
                    let _ = probe.send(StorageDropStage::ShutdownFinished(status));
                }
            }
        });
    }
}

/// Attach TSD's statically linked storage service and return its cleanup owner.
///
/// The setup callback captures the attested linked connection and sends the
/// completed owner through an owned one-shot channel. `ModuleInfo` remains an
/// internal U5 admission result; callers retain this TSD registration. The
/// borrowed observer stays owned by its caller; this function retains only its
/// broker signal receiver and monitor task.
///
/// # Errors
/// Returns a content-free failure if the linked module cannot be admitted or
/// the service registration cannot be established.
pub async fn attach_storage<E, A, C>(
    host: &ModuleHost,
    observer_connection: &Connection,
    endpoint: GovernedStorage<E, A, C>,
    runtime: Handle,
) -> Result<StorageRegistration<E, A, C>, HostFailure>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    attach_storage_inner(
        host,
        observer_connection,
        endpoint,
        runtime,
        || async {},
        #[cfg(test)]
        None,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn attach_storage_before_result_transfer<E, A, C, H, Fut>(
    host: &ModuleHost,
    observer_connection: &Connection,
    endpoint: GovernedStorage<E, A, C>,
    runtime: Handle,
    before_transfer: H,
    cleanup_probe: UnboundedSender<StorageDropStage>,
) -> Result<StorageRegistration<E, A, C>, HostFailure>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
    H: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    attach_storage_inner(
        host,
        observer_connection,
        endpoint,
        runtime,
        before_transfer,
        Some(cleanup_probe),
    )
    .await
}

async fn attach_storage_inner<E, A, C, H, Fut>(
    host: &ModuleHost,
    observer_connection: &Connection,
    endpoint: GovernedStorage<E, A, C>,
    runtime: Handle,
    before_transfer: H,
    #[cfg(test)] cleanup_probe: Option<UnboundedSender<StorageDropStage>>,
) -> Result<StorageRegistration<E, A, C>, HostFailure>
where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
    H: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let caller_lookup: Arc<dyn CallerDirectory> =
        Arc::new(TinyBusCallerDirectory(host.sender_lookup()));
    let (owner_tx, owner_rx) = oneshot::channel();
    let runtime_for_setup = runtime.clone();
    let observer_for_setup = Connection::clone(observer_connection);
    let attached = host
        .attach_linked_with_setup(
            storage_module::linked_module().map_err(|_| unavailable())?,
            serde_json::json!({}),
            move |connection| {
                let callers = Arc::clone(&caller_lookup);
                let endpoint = endpoint.clone();
                let runtime = runtime_for_setup.clone();
                let observer = observer_for_setup;
                async move {
                    let service_peer = connection.unique_name().ok_or_else(|| {
                        tinybus::Error::failed("linked storage service has no unique peer")
                    })?;
                    let inner = Arc::new(endpoint.register(connection, callers, runtime.clone()));
                    if inner.install_legacy().await.is_err() {
                        let _ = inner.shutdown().await;
                        return Err(tinybus::Error::failed(
                            "legacy storage registration unavailable",
                        ));
                    }
                    let rule = MatchRule::new()
                        .signals()
                        .sender(BusName::new(tinybus::BUS_NAME)?)
                        .interface(InterfaceName::new(tinybus::BUS_INTERFACE)?)
                        .member(MemberName::new("NameOwnerChanged")?);
                    let changes = match observer.add_match(rule).await {
                        Ok(changes) => changes,
                        Err(error) => {
                            let _ = inner.shutdown().await;
                            return Err(error);
                        }
                    };
                    let monitored = Arc::clone(&inner);
                    let monitor_runtime = runtime.clone();
                    let monitor = monitor_runtime.spawn(async move {
                        monitor_owner_changes(changes, service_peer, monitored, None).await;
                    });
                    let owner = StorageRegistration {
                        inner: Some(inner),
                        monitor: Some(monitor),
                        runtime,
                        #[cfg(test)]
                        cleanup_probe,
                    };
                    before_transfer().await;
                    owner_tx
                        .send(owner)
                        .map_err(|_| tinybus::Error::failed("storage registration receiver closed"))
                }
            },
        )
        .await;
    match attached {
        Ok(_module_info) => match owner_rx.await {
            Ok(owner) => Ok(owner),
            Err(_) => Err(unavailable()),
        },
        Err(_error) => {
            if let Ok(owner) = owner_rx.await {
                let _ = owner.shutdown().await;
            }
            Err(unavailable())
        }
    }
}

pub(crate) async fn monitor_owner_changes<E, A, C>(
    mut changes: broadcast::Receiver<tinybus::Message>,
    service_peer: BusName,
    monitored: Arc<tinystoragedrivers_busproxy::governed::StorageRegistration<E, A, C>>,
    lag_reporter: Option<tokio::sync::mpsc::UnboundedSender<usize>>,
) where
    C: HostContext,
    E: Environment<C>,
    A: AuditSink,
{
    loop {
        let message = match changes.recv().await {
            Ok(message) => message,
            Err(broadcast::error::RecvError::Lagged(_)) => {
                if !monitored.service_peer_is_live(&service_peer) {
                    if let Some(reporter) = &lag_reporter {
                        let _ = reporter.send(0);
                    }
                    let _ = monitored.shutdown().await;
                    return;
                }
                let revoked = monitored.reconcile_callers().await;
                if let Some(reporter) = &lag_reporter {
                    let _ = reporter.send(revoked);
                }
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let Value::Array(args) = message.body else {
            continue;
        };
        let (Some(Value::String(name)), Some(_old), Some(new)) =
            (args.first(), args.get(1), args.get(2))
        else {
            continue;
        };
        let Ok(peer) = BusName::new(name.clone()) else {
            continue;
        };
        if !peer.is_unique() || !new.is_null() {
            continue;
        }
        if peer == service_peer {
            let _ = monitored.shutdown().await;
            return;
        }
        let _ = monitored.revoke_peer(&peer).await;
    }
}

fn unavailable() -> HostFailure {
    HostFailure::Storage(tinystoragedrivers_bus::StorageFailure {
        kind: tinystoragedrivers_bus::StorageKind::Unavailable,
    })
}

mod storage_module {
    async fn default_setup(_: tinybus::Connection) -> tinybus::Result<()> {
        Ok(())
    }

    tinybus_module::module_export_static! {
        setup = default_setup,
        worker_threads = 1,
        provides = ["ai.tinyhumans.tinystorage.Storage"],
        methods = [],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[cfg(test)]
#[path = "storage_host_unit_tests.rs"]
pub(crate) mod tests;
