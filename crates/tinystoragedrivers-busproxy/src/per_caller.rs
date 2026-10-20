//! A ready-made [`StorageEndpoint`] that gives each authenticated caller its
//! own [`StorageServer`].
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::BusName;
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, BlobStreamHeadWithExpiry, PutBlobStreamRequest,
    PutBlobStreamUntilRequest, StorageFailure, WatchChanged,
};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::BlobReader;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{Semaphore, broadcast};

use crate::host::{HostFailure, StorageCall, StorageEndpoint};

type BindFuture = Pin<Box<dyn Future<Output = Option<StorageServer>> + Send + 'static>>;
type PendingBindings = Mutex<HashMap<BusName, u64>>;

struct PendingBinding {
    bindings: Arc<PendingBindings>,
    sender: BusName,
    ticket: u64,
    armed: bool,
}

impl PendingBinding {
    fn new(bindings: Arc<PendingBindings>, sender: BusName, ticket: u64) -> Self {
        Self {
            bindings,
            sender,
            ticket,
            armed: true,
        }
    }

    fn complete(&mut self, bindings: &mut HashMap<BusName, u64>) -> bool {
        if bindings.get(&self.sender) != Some(&self.ticket) {
            return false;
        }
        bindings.remove(&self.sender);
        self.armed = false;
        true
    }
}

impl Drop for PendingBinding {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut bindings) = self.bindings.lock()
            && bindings.get(&self.sender) == Some(&self.ticket)
        {
            bindings.remove(&self.sender);
        }
    }
}

struct BoundBlobReader {
    reader: BlobReader,
    _server: Arc<StorageServer>,
}

impl AsyncRead for BoundBlobReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.reader.as_mut().poll_read(context, buffer)
    }
}

/// Serves each caller from the [`StorageServer`] returned by its binder.
///
/// The sync constructor remains available. New host integrations can use
/// [`PerCallerEndpoint::new_async`] for a fallible binding source that may
/// await. Bindings run outside the endpoint's cache mutex; concurrent misses
/// for one caller race to install and the later result is discarded.
pub struct PerCallerEndpoint<F> {
    bind: Arc<dyn Fn(BusName) -> BindFuture + Send + Sync>,
    servers: Mutex<HashMap<BusName, (Arc<StorageServer>, u64)>>,
    next_epoch: AtomicU64,
    next_binding_ticket: AtomicU64,
    pending_bindings: Arc<PendingBindings>,
    max_callers: usize,
    binding_slots: Arc<Semaphore>,
    marker: PhantomData<fn() -> F>,
}

impl<F> std::fmt::Debug for PerCallerEndpoint<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerCallerEndpoint").finish_non_exhaustive()
    }
}

impl<F> PerCallerEndpoint<F>
where
    F: Fn(&BusName) -> Option<StorageServer> + Send + Sync + 'static,
{
    /// An endpoint that binds callers synchronously with `bind`.
    #[must_use]
    pub fn new(bind: F) -> Self {
        let bind = Arc::new(bind);
        let async_bind = Arc::clone(&bind);
        Self {
            bind: Arc::new(move |sender| {
                let result = async_bind(&sender);
                Box::pin(async move { result })
            }),
            servers: Mutex::new(HashMap::new()),
            next_epoch: AtomicU64::new(1),
            next_binding_ticket: AtomicU64::new(1),
            pending_bindings: Arc::new(Mutex::new(HashMap::new())),
            max_callers: 1024,
            binding_slots: Arc::new(Semaphore::new(64)),
            marker: PhantomData,
        }
    }
}

impl PerCallerEndpoint<()> {
    /// Create an endpoint whose caller binding can await host policy or
    /// backend setup.
    #[must_use]
    pub fn new_async<F, Fut>(bind: F) -> PerCallerEndpoint<F>
    where
        F: Fn(BusName) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<StorageServer>> + Send + 'static,
    {
        let bind = Arc::new(bind);
        let async_bind = Arc::clone(&bind);
        PerCallerEndpoint {
            bind: Arc::new(move |sender| Box::pin(async_bind(sender))),
            servers: Mutex::new(HashMap::new()),
            next_epoch: AtomicU64::new(1),
            next_binding_ticket: AtomicU64::new(1),
            pending_bindings: Arc::new(Mutex::new(HashMap::new())),
            max_callers: 1024,
            binding_slots: Arc::new(Semaphore::new(64)),
            marker: PhantomData,
        }
    }
}

impl<F> PerCallerEndpoint<F> {
    /// Bound the number of caller bindings retained at once. Disconnected
    /// callers should be removed with [`PerCallerEndpoint::release`]; new
    /// callers are refused while this bound is full.
    #[must_use]
    pub fn with_max_callers(mut self, max_callers: usize) -> Self {
        self.max_callers = max_callers;
        self
    }

    /// Bound concurrent asynchronous binder executions. Excess misses fail
    /// closed until a later request retries.
    #[must_use]
    pub fn with_max_concurrent_bindings(mut self, max_bindings: usize) -> Self {
        self.binding_slots = Arc::new(Semaphore::new(max_bindings));
        self
    }

    /// Drop the server bound to `caller`, for when it leaves the bus, and
    /// return whether it had one. The caller's watches and signals end with
    /// it; a later call from the same name binds afresh.
    pub fn release(&self, caller: &BusName) -> bool {
        let Ok(mut servers) = self.servers.lock() else {
            return false;
        };
        let removed = servers.remove(caller).is_some();
        if let Ok(mut pending) = self.pending_bindings.lock() {
            pending.remove(caller);
        }
        removed
    }

    fn cached(&self, sender: &BusName) -> Result<Option<(Arc<StorageServer>, u64)>, HostFailure> {
        let servers = self
            .servers
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        Ok(servers
            .get(sender)
            .map(|(server, epoch)| (Arc::clone(server), *epoch)))
    }

    async fn bound(
        &self,
        sender: Option<&BusName>,
    ) -> Result<(Arc<StorageServer>, u64), HostFailure> {
        let sender = sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        if let Some(cached) = self.cached(sender)? {
            return Ok(cached);
        }
        let _binding = self
            .binding_slots
            .try_acquire()
            .map_err(|_| HostFailure::Denied("binding-capacity"))?;
        let ticket = self.next_binding_ticket.fetch_add(1, Ordering::AcqRel);
        let mut pending =
            PendingBinding::new(Arc::clone(&self.pending_bindings), sender.clone(), ticket);
        self.pending_bindings
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?
            .insert(sender.clone(), ticket);
        let server = Arc::new(
            (self.bind)(sender.clone())
                .await
                .ok_or(HostFailure::Denied("unknown-caller"))?,
        );
        let mut servers = self
            .servers
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        let mut pending_bindings = self
            .pending_bindings
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        if !pending.complete(&mut pending_bindings) {
            return Err(HostFailure::Denied("stale-caller-binding"));
        }
        if let Some((existing, epoch)) = servers.get(sender) {
            return Ok((Arc::clone(existing), *epoch));
        }
        if servers.len() >= self.max_callers {
            let evictable = servers
                .iter()
                .find(|(_, (server, _))| {
                    Arc::strong_count(server) == 1 && server.open_watches() == 0
                })
                .map(|(caller, _)| caller.clone());
            if let Some(evictable) = evictable {
                servers.remove(&evictable);
            } else {
                return Err(HostFailure::Denied("caller-capacity"));
            }
        }
        let epoch = self.next_epoch.fetch_add(1, Ordering::Relaxed);
        servers.insert(sender.clone(), (Arc::clone(&server), epoch));
        Ok((server, epoch))
    }

    fn server_cached(&self, sender: &BusName) -> Option<Arc<StorageServer>> {
        self.servers
            .lock()
            .ok()
            .and_then(|servers| servers.get(sender).map(|(server, _)| Arc::clone(server)))
    }
}

fn refused(failure: StorageFailure) -> HostFailure {
    HostFailure::Storage(failure)
}

#[async_trait]
impl<F: Send + Sync + 'static> StorageEndpoint for PerCallerEndpoint<F> {
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure> {
        let (server, _) = self.bound(call.sender).await?;
        server
            .handle(
                call.admission,
                call.object.as_str(),
                call.selector,
                call.request,
            )
            .await
            .map_err(refused)
    }

    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        let (server, _) = self.bound(sender).await?;
        let info = server.put_blob_stream(head, body).await.map_err(refused)?;
        serde_json::to_value(info).map_err(|_| HostFailure::Denied("encode-failed"))
    }

    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        let (server, _) = self.bound(sender).await?;
        let Some((head, reader)) = server.get_blob_stream(request).await.map_err(refused)? else {
            return Ok(None);
        };
        Ok(Some((
            head,
            Box::pin(BoundBlobReader {
                reader,
                _server: server,
            }),
        )))
    }

    async fn put_blob_stream_until(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamUntilRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        let (server, _) = self.bound(sender).await?;
        let info = server
            .put_blob_stream_until(head, body)
            .await
            .map_err(refused)?;
        serde_json::to_value(info).map_err(|_| HostFailure::Denied("encode-failed"))
    }

    async fn get_blob_stream_with_expiry(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHeadWithExpiry, BlobReader)>, HostFailure> {
        let (server, _) = self.bound(sender).await?;
        let Some((head, reader)) = server
            .get_blob_stream_with_expiry(request)
            .await
            .map_err(refused)?
        else {
            return Ok(None);
        };
        Ok(Some((
            head,
            Box::pin(BoundBlobReader {
                reader,
                _server: server,
            }),
        )))
    }

    fn watch_signals(&self, sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        self.server_cached(sender).map(|server| server.signals())
    }

    fn watch_epoch(&self, sender: &BusName) -> u64 {
        self.cached(sender)
            .ok()
            .flatten()
            .map_or(0, |(_, epoch)| epoch)
    }
}

#[cfg(test)]
#[path = "per_caller_tests.rs"]
mod tests;
