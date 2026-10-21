//! The host side: register the six `Storage` objects on a `TinyBus`
//! connection and hand every call to a [`StorageEndpoint`].
//!
//! The host decides everything. The endpoint is given the broker-authenticated
//! sender and must derive the caller's scope, grant and audit from it; nothing
//! in a request can name a scope (the wire types reject unknown fields).
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::StorageAddress;
use crate::transport::{Admission, Object};
use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::module::{
    HostCallbackBinding, HostCallbackEndpoint, HostCallbackSignal, HostStreamCallbackEndpoint,
    ModuleHost,
};
use tinybus::{
    BusName, CallContext, Connection, Interface, InterfaceName, MemberName, StreamDescriptor,
    StreamReader, StreamRef, StreamWriter, WeakConnection,
};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, StorageKind, WatchChanged,
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::chunks::{blob_reader, pump};
use crate::names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, callback_watch_path, interface, member,
    object_path, watch_path, wire_value,
};
use tinystoragedrivers_api::BlobReader;

/// How a call failed, before it becomes a bus error.
#[derive(Debug, PartialEq, Eq)]
pub enum HostFailure {
    /// Not allowed. The label says why and must hold no request content.
    Denied(&'static str),
    /// The backend refused or failed, as a content-free category.
    Storage(StorageFailure),
}

impl From<StorageFailure> for HostFailure {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure)
    }
}

impl From<HostFailure> for tinybus::Error {
    fn from(error: HostFailure) -> Self {
        match error {
            HostFailure::Denied(label) => Self::MethodFailed {
                name: ERROR_DENIED.to_owned(),
                message: label.to_owned(),
            },
            HostFailure::Storage(failure) => Self::MethodFailed {
                name: ERROR_STORAGE.to_owned(),
                message: serde_json::to_value(failure.kind)
                    .ok()
                    .and_then(|kind| kind.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "backend".to_owned()),
            },
        }
    }
}

/// One request/reply call, as the bus delivered it.
#[derive(Debug)]
pub struct StorageCall<'a> {
    /// Which of the five objects.
    pub object: Object,
    /// The member name; the endpoint must treat unknown ones as invalid.
    pub selector: &'a str,
    /// The single request argument (`null` when there is none).
    pub request: Value,
    /// The broker-authenticated unique sender; `None` when the delivery path
    /// cannot prove one, in which case the endpoint must refuse.
    pub sender: Option<&'a BusName>,
    /// `Confidential` only for `Secrets`, which the bus delivers on a
    /// confidential route only.
    pub admission: Admission,
}

/// What a host serves the `Storage` objects from.
#[async_trait]
pub trait StorageEndpoint: Send + Sync + 'static {
    /// Answer one request/reply call.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure>;

    /// `Blobs.PutStream`: store `body`, which must be exactly `head.len`
    /// bytes. Replies with the stored `BlobInfo`.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure>;

    /// `Blobs.GetStream`: the blob's metadata and body, or `None`.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure>;

    /// The watch invalidations of `sender`'s binding. The host calls this just
    /// before it forwards a caller's first `WatchOpen`, so no signal is missed;
    /// `None` means the endpoint has no signals for that caller.
    fn watch_signals(&self, sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>>;

    /// Which binding `sender`'s signals currently come from. It must change
    /// whenever the endpoint rebinds the caller (for example after a release),
    /// so the host replaces a forwarder still relaying the old binding instead
    /// of leaving the new one without signals. Endpoints that never rebind can
    /// keep the default.
    fn watch_epoch(&self, _sender: &BusName) -> u64 {
        0
    }
}

#[derive(Clone)]
enum HostTransport {
    Connection(WeakConnection),
    Addressed {
        connection: WeakConnection,
        watch_path: tinybus::ObjectPath,
        active: Arc<AtomicBool>,
    },
    Callback(Weak<HostCallbackBinding>),
}

enum ServedMode {
    Connection(Connection),
    Addressed {
        connection: WeakConnection,
        paths: Vec<tinybus::ObjectPath>,
        active: Arc<AtomicBool>,
        runtime: tokio::runtime::Handle,
        watch_path: tinybus::ObjectPath,
    },
    Callback(Arc<HostCallbackBinding>),
}

struct Shared {
    transport: HostTransport,
    endpoint: Arc<dyn StorageEndpoint>,
    forwarders: Mutex<HashMap<BusName, (u64, JoinHandle<()>)>>,
}

impl Shared {
    fn is_active(&self) -> bool {
        match &self.transport {
            HostTransport::Addressed { active, .. } => active.load(Ordering::Acquire),
            HostTransport::Connection(_) | HostTransport::Callback(_) => true,
        }
    }

    /// Start relaying `sender`'s watch signals if they are not already.
    fn forward_watch_signals(&self, sender: &BusName) {
        let Ok(mut forwarders) = self.forwarders.lock() else {
            return;
        };
        forwarders.retain(|_, (_, task)| !task.is_finished());
        let epoch = self.endpoint.watch_epoch(sender);
        if forwarders
            .get(sender)
            .is_some_and(|(held, _)| *held == epoch)
        {
            return;
        }
        let Some(mut signals) = self.endpoint.watch_signals(sender) else {
            return;
        };
        let (path, transport) = match &self.transport {
            HostTransport::Connection(_) => {
                let Ok(path) = watch_path(sender) else {
                    return;
                };
                (path, self.transport.clone())
            }
            HostTransport::Addressed { watch_path, .. } => {
                (watch_path.clone(), self.transport.clone())
            }
            HostTransport::Callback(_) => (callback_watch_path(), self.transport.clone()),
        };
        // A forwarder for a binding that was released is replaced, not kept.
        if let Some((_, stale)) = forwarders.remove(sender) {
            stale.abort();
        }
        let (interface, member) = (interface(), member(WATCH_MEMBER));
        let recipient = sender.clone();
        let task = tokio::spawn(async move {
            loop {
                match signals.recv().await {
                    Ok(signal) => {
                        let emitted = match &transport {
                            HostTransport::Connection(connection)
                            | HostTransport::Addressed { connection, .. } => {
                                let Some(connection) = connection.upgrade() else {
                                    return;
                                };
                                // Unicast: a peer that subscribed to this path (or the
                                // whole interface) never sees another caller's signal.
                                connection
                                    .emit_to(
                                        recipient.clone(),
                                        path.clone(),
                                        interface.clone(),
                                        member.clone(),
                                        (signal,),
                                    )
                                    .await
                            }
                            HostTransport::Callback(binding) => {
                                let Some(binding) = binding.upgrade() else {
                                    return;
                                };
                                binding
                                    .emit_signal(&path, &interface, &member, (signal,))
                                    .await
                            }
                        };
                        if emitted.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        forwarders.insert(sender.clone(), (epoch, task));
    }
}

/// One storage object on the bus.
struct ObjectService {
    object: Object,
    shared: Arc<Shared>,
}

fn selectors(object: Object) -> &'static [&'static str] {
    use tinystoragedrivers_bus as wire;
    match object {
        Object::Documents => &wire::DOCUMENTS_SELECTORS,
        Object::Streams => &wire::STREAMS_SELECTORS,
        Object::Blobs => &wire::BLOBS_SELECTORS,
        Object::Secrets => &wire::SECRETS_SELECTORS,
        Object::Queues => &wire::QUEUES_SELECTORS,
        Object::Kv => &wire::KV_SELECTORS,
    }
}

fn invalid() -> HostFailure {
    HostFailure::Storage(StorageFailure {
        kind: StorageKind::InvalidInput,
    })
}

/// The positional arguments of a call as a list.
fn arguments(args: Value) -> Result<Vec<Value>, HostFailure> {
    match args {
        Value::Array(items) => Ok(items),
        Value::Null => Ok(Vec::new()),
        _ => Err(invalid()),
    }
}

/// The request out of positional bus arguments: none is `null`, one is itself.
fn request_of(args: Value) -> Result<Value, HostFailure> {
    let mut items = arguments(args)?;
    if items.len() > 1 {
        return Err(invalid());
    }
    Ok(items.pop().unwrap_or(Value::Null))
}

impl ObjectService {
    fn connection(&self) -> Result<Connection, HostFailure> {
        match &self.shared.transport {
            HostTransport::Connection(connection) | HostTransport::Addressed { connection, .. } => {
                connection
                    .upgrade()
                    .ok_or(HostFailure::Storage(StorageFailure {
                        kind: StorageKind::Unavailable,
                    }))
            }
            HostTransport::Callback(_) => Err(HostFailure::Storage(StorageFailure {
                kind: StorageKind::Unavailable,
            })),
        }
    }

    fn accept_stream(
        &self,
        context: &CallContext,
        stream: &StreamRef,
    ) -> Result<StreamReader, HostFailure> {
        match &self.shared.transport {
            HostTransport::Connection(_) | HostTransport::Addressed { .. } => {
                let caller = context
                    .authenticated_sender()
                    .ok_or(HostFailure::Denied("unauthenticated-caller"))?;
                self.connection()?
                    .accept_stream_from(stream, caller)
                    .map_err(|_| invalid())
            }
            HostTransport::Callback(binding) => binding
                .upgrade()
                .ok_or(HostFailure::Storage(StorageFailure {
                    kind: StorageKind::Unavailable,
                }))?
                .accept_stream_from_module(context, stream)
                .map_err(|_| invalid()),
        }
    }

    async fn open_stream(
        &self,
        context: &CallContext,
        descriptor: StreamDescriptor,
    ) -> Result<StreamWriter, HostFailure> {
        match &self.shared.transport {
            HostTransport::Connection(_) | HostTransport::Addressed { .. } => {
                let caller = context
                    .authenticated_sender()
                    .ok_or(HostFailure::Denied("unauthenticated-caller"))?;
                self.connection()?
                    .open_stream(caller, descriptor)
                    .await
                    .map_err(|_| invalid())
            }
            HostTransport::Callback(binding) => binding
                .upgrade()
                .ok_or(HostFailure::Storage(StorageFailure {
                    kind: StorageKind::Unavailable,
                }))?
                .open_stream_to_module(context, descriptor, Duration::from_secs(30))
                .await
                .map_err(|_| invalid()),
        }
    }

    /// `PutStream`: `[PutBlobStreamRequest, StreamRef]`.
    async fn put_stream(&self, args: Value, context: &CallContext) -> Result<Value, HostFailure> {
        let mut items = arguments(args)?;
        let (Some(stream), Some(head), None) = (items.pop(), items.pop(), items.pop()) else {
            return Err(invalid());
        };
        let head: PutBlobStreamRequest = serde_json::from_value(head).map_err(|_| invalid())?;
        let stream: StreamRef = serde_json::from_value(stream).map_err(|_| invalid())?;
        if stream.len.is_some_and(|declared| declared != head.len) {
            return Err(invalid());
        }
        // The body is read only from a stream the authenticated caller opened,
        // so a guessed stream id of another peer is refused.
        let Some(sender) = context.authenticated_sender() else {
            return Err(HostFailure::Denied("unauthenticated-caller"));
        };
        let reader = self.accept_stream(context, &stream)?;
        self.shared
            .endpoint
            .put_blob_stream(Some(sender), head, blob_reader(reader))
            .await
    }

    /// `GetStream`: `[BlobKeyRequest]`, replying `null` or `[head, StreamRef]`
    /// with the body streamed to the caller.
    async fn get_stream(&self, args: Value, context: &CallContext) -> Result<Value, HostFailure> {
        let request: BlobKeyRequest =
            serde_json::from_value(request_of(args)?).map_err(|_| invalid())?;
        // The body is sent to the broker-authenticated caller, so without one
        // there is nowhere safe to send it.
        let Some(sender) = context.authenticated_sender() else {
            return Err(HostFailure::Denied("unauthenticated-caller"));
        };
        let found = self
            .shared
            .endpoint
            .get_blob_stream(Some(sender), request)
            .await?;
        let Some((head, mut body)) = found else {
            return Ok(Value::Null);
        };
        let len = head.meta.len;
        let writer = self
            .open_stream(context, StreamDescriptor::with_len(len))
            .await
            .map_err(|_| {
                HostFailure::Storage(StorageFailure {
                    kind: StorageKind::Unavailable,
                })
            })?;
        let reply = json!([wire_value(&head), wire_value(&writer.stream_ref())]);
        // The body follows the reply: the caller accepts the stream only once it
        // has the reference, and the stream's window holds the first chunks.
        tokio::spawn(async move {
            let _ = pump(writer, &mut body, len).await;
        });
        Ok(reply)
    }
}

#[async_trait]
impl Interface for ObjectService {
    fn name(&self) -> InterfaceName {
        interface()
    }

    fn members(&self) -> Vec<MemberName> {
        selectors(self.object)
            .iter()
            .map(|selector| member(selector))
            .collect()
    }

    fn requires_confidential(&self, _member: &MemberName) -> bool {
        self.object == Object::Secrets
    }

    async fn call(&self, _member: &MemberName, _args: Value) -> tinybus::Result<Value> {
        // Without the broker's context there is no caller to authorize.
        Err(HostFailure::Denied("unauthenticated-caller").into())
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> tinybus::Result<Value> {
        if !self.shared.is_active() {
            return Err(HostFailure::Denied("storage-endpoint-closed").into());
        }
        let sender = context.authenticated_sender();
        let selector = member.as_str();
        let outcome = match (self.object, selector) {
            (Object::Blobs, "PutStream") => self.put_stream(args, context).await,
            (Object::Blobs, "GetStream") => self.get_stream(args, context).await,
            _ => {
                if let (Object::Documents, "WatchOpen", Some(sender)) =
                    (self.object, selector, sender)
                {
                    self.shared.forward_watch_signals(sender);
                }
                let admission = if self.object == Object::Secrets {
                    Admission::Confidential
                } else {
                    Admission::Private
                };
                match request_of(args) {
                    Ok(request) => {
                        self.shared
                            .endpoint
                            .call(StorageCall {
                                object: self.object,
                                selector,
                                request,
                                sender,
                                admission,
                            })
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
        };
        outcome.map_err(Into::into)
    }
}

/// The registered `Storage` objects. Call [`ServedStorage::shutdown`] to
/// withdraw them. Ordinary serving leaves registrations on the caller-owned
/// connection when this handle is dropped; callback serving owns a private
/// binding whose final drop revokes its registrations.
pub struct ServedStorage {
    mode: ServedMode,
    shared: Arc<Shared>,
}

impl ServedMode {
    fn transport(&self) -> HostTransport {
        match self {
            Self::Connection(connection) => HostTransport::Connection(connection.downgrade()),
            Self::Addressed {
                connection,
                paths: _,
                active,
                runtime: _,
                watch_path,
            } => HostTransport::Addressed {
                connection: connection.clone(),
                watch_path: watch_path.clone(),
                active: Arc::clone(active),
            },
            Self::Callback(binding) => HostTransport::Callback(Arc::downgrade(binding)),
        }
    }
}

impl std::fmt::Debug for ServedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServedStorage").finish_non_exhaustive()
    }
}

impl ServedStorage {
    /// Withdraw the objects, release the name and stop relaying signals.
    ///
    /// # Errors
    /// The bus refuses to release the name.
    pub async fn shutdown(self) -> tinybus::Result<()> {
        if let Ok(mut forwarders) = self.shared.forwarders.lock() {
            for (_, (_, task)) in forwarders.drain() {
                task.abort();
            }
        }
        match &self.mode {
            ServedMode::Connection(connection) => {
                for object in ALL {
                    connection.unserve(&object_path(object)).await;
                }
                connection.release_name(HOST_NAME).await
            }
            ServedMode::Addressed { active, .. } => {
                if active.swap(false, Ordering::AcqRel) {
                    self.unregister_addressed().await;
                }
                Ok(())
            }
            ServedMode::Callback(binding) => binding.close().await,
        }
    }

    async fn unregister_addressed(&self) {
        if let ServedMode::Addressed {
            connection, paths, ..
        } = &self.mode
            && let Some(connection) = connection.upgrade()
        {
            for path in paths {
                connection.unserve(path).await;
            }
        }
    }
}

impl Drop for ServedStorage {
    fn drop(&mut self) {
        if let ServedMode::Addressed {
            connection,
            paths,
            active,
            runtime,
            ..
        } = &self.mode
        {
            if !active.swap(false, Ordering::AcqRel) {
                return;
            }
            let connection = connection.clone();
            let paths = paths.clone();
            runtime.spawn(async move {
                if let Some(connection) = connection.upgrade() {
                    for path in paths {
                        connection.unserve(&path).await;
                    }
                }
            });
        }
    }
}

const ALL: [Object; 6] = [
    Object::Documents,
    Object::Streams,
    Object::Blobs,
    Object::Secrets,
    Object::Queues,
    Object::Kv,
];

/// Serve the `Storage` objects on `connection` and claim [`HOST_NAME`].
///
/// # Errors
/// The name is taken or the bus refuses a registration.
pub async fn serve(
    connection: &Connection,
    endpoint: Arc<dyn StorageEndpoint>,
) -> tinybus::Result<ServedStorage> {
    let shared = Arc::new(Shared {
        transport: HostTransport::Connection(connection.downgrade()),
        endpoint,
        forwarders: Mutex::new(HashMap::new()),
    });
    for object in ALL {
        connection
            .serve_at(
                object_path(object),
                ObjectService {
                    object,
                    shared: Arc::clone(&shared),
                },
            )
            .await?;
    }
    if let Err(error) = connection.request_name(HOST_NAME).await {
        // Do not leave unreachable objects registered behind a name we lack.
        for object in ALL {
            connection.unserve(&object_path(object)).await;
        }
        return Err(error);
    }
    Ok(ServedStorage {
        mode: ServedMode::Connection(connection.clone()),
        shared,
    })
}

/// Serve a request-scoped storage endpoint beneath an invocation root on an
/// already-attested host connection.
///
/// The address host must be this connection's unique broker name. This does
/// not claim a name or alter the normal host service; each invocation gets
/// distinct object paths. Dropping the returned guard immediately disables
/// dispatch and asynchronously unregisters only those paths. Call
/// [`ServedStorage::shutdown`] when synchronous cleanup is required.
///
/// # Errors
/// The host identity does not match the connection or `TinyBus` refuses a path
/// registration because an interface already owns it.
pub async fn serve_at_address(
    connection: &Connection,
    address: StorageAddress,
    endpoint: Arc<dyn StorageEndpoint>,
) -> tinybus::Result<ServedStorage> {
    serve_at_address_with_registration_hook(connection, address, endpoint, |_| async {}).await
}

async fn serve_at_address_with_registration_hook<F, Fut>(
    connection: &Connection,
    address: StorageAddress,
    endpoint: Arc<dyn StorageEndpoint>,
    mut after_registration: F,
) -> tinybus::Result<ServedStorage>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    if connection.unique_name().as_ref() != Some(address.host()) {
        return Err(tinybus::Error::failed(
            "storage address host does not match this connection",
        ));
    }
    let paths: Vec<_> = ALL
        .into_iter()
        .map(|object| address.port_path(object).clone())
        .collect();
    let active = Arc::new(AtomicBool::new(false));
    let transport = HostTransport::Addressed {
        connection: connection.downgrade(),
        watch_path: address.watch_path().clone(),
        active: Arc::clone(&active),
    };
    let shared = Arc::new(Shared {
        transport,
        endpoint,
        forwarders: Mutex::new(HashMap::new()),
    });
    let mut registered = Vec::new();
    for (index, (object, path)) in ALL.into_iter().zip(paths.iter()).enumerate() {
        if let Err(error) = connection
            .try_serve_at(
                path.clone(),
                ObjectService {
                    object,
                    shared: Arc::clone(&shared),
                },
            )
            .await
        {
            for path in registered {
                connection.unserve(&path).await;
            }
            return Err(error);
        }
        registered.push(path.clone());
        after_registration(index).await;
    }
    active.store(true, Ordering::Release);
    Ok(ServedStorage {
        mode: ServedMode::Addressed {
            connection: connection.downgrade(),
            paths,
            active,
            runtime: tokio::runtime::Handle::current(),
            watch_path: address.watch_path().clone(),
        },
        shared,
    })
}

/// Prepare the exact confidential RPC, ordinary blob-stream, and watch-signal
/// declarations used by a host-callback storage server.
///
/// Pass the returned binding to [`serve_with_host_callbacks`], then provide
/// its unique name to the loaded module and connect it with
/// [`crate::connect_storage_at`]. Port RPC bodies, including Secrets, use the
/// confidential callback route. Blob stream payloads use the separately
/// declared ordinary stream endpoint.
///
/// # Errors
/// Returns the SDK error if the host refuses to reserve the callback
/// interface or its endpoint declarations conflict.
pub async fn prepare_storage_host_callbacks(
    host: &ModuleHost,
) -> tinybus::Result<Arc<HostCallbackBinding>> {
    let endpoints = ALL
        .into_iter()
        .map(|object| HostCallbackEndpoint {
            path: object_path(object),
            interface: interface(),
            members: selectors(object)
                .iter()
                .copied()
                .filter(|selector| {
                    object != Object::Blobs || !matches!(*selector, "PutStream" | "GetStream")
                })
                .map(member)
                .collect(),
        })
        .collect();
    let signals = vec![HostCallbackSignal {
        path: callback_watch_path(),
        interface: interface(),
        members: vec![member(WATCH_MEMBER)],
    }];
    let stream_endpoints = vec![HostStreamCallbackEndpoint {
        path: object_path(Object::Blobs),
        interface: interface(),
        members: vec![member("PutStream"), member("GetStream")],
    }];
    host.prepare_host_callbacks_with_signals_and_stream_endpoints(
        endpoints,
        signals,
        stream_endpoints,
    )
    .await
    .map(Arc::new)
}

/// Serve the six storage objects on an exact module-bound callback binding.
///
/// The binding must come from [`prepare_storage_host_callbacks`]. Ordinary
/// [`serve`] remains available for the well-known host service. Callback mode
/// has no name-based fallback and closes the private callback connection on
/// shutdown.
///
/// # Errors
/// Returns the SDK error if any of the six storage objects cannot be served.
pub async fn serve_with_host_callbacks(
    binding: Arc<HostCallbackBinding>,
    endpoint: Arc<dyn StorageEndpoint>,
) -> tinybus::Result<ServedStorage> {
    let mode = ServedMode::Callback(binding.clone());
    let shared = Arc::new(Shared {
        transport: mode.transport(),
        endpoint,
        forwarders: Mutex::new(HashMap::new()),
    });
    for object in ALL {
        if let Err(error) = binding
            .serve_at(
                object_path(object),
                ObjectService {
                    object,
                    shared: Arc::clone(&shared),
                },
            )
            .await
        {
            binding.revoke();
            return Err(error);
        }
    }
    Ok(ServedStorage { mode, shared })
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
