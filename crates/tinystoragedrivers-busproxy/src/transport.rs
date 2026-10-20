//! The module-side [`StorageTransport`] over a real `TinyBus` connection.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{BusName, Connection, MatchRule, Message, StreamDescriptor, StreamRef};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, BlobStreamHeadWithExpiry, BlobWithExpiry, PutBlobStreamRequest,
    PutBlobStreamUntilRequest, PutBlobUntilRequest, StorageFailure, StorageKind, WatchChanged,
};
use tinystoragedrivers_client::{Object, StorageTransport, WireError};
use tinystoragedrivers_core::{BlobReader, Scope};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::StorageDescriptor;
use crate::chunks::{
    PumpError, PumpFailureState, PumpSourceFailure, blob_reader, pump_with_failure_state,
};
use crate::names::OBJECT_ROOT;
use crate::names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, host_name, interface, member, wire_value,
};

/// Signals buffered for a slow watcher before the oldest are dropped.
const SIGNAL_BUFFER: usize = 256;
/// How long one call, or one stream chunk, may take. A large streamed blob is
/// bounded per chunk, not as a whole.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The label a module gives the one scope its host bound it to. The host never
/// reads it; the scope itself comes from the broker-authenticated caller.
#[must_use]
pub fn module_scope_label() -> Scope {
    Scope::local()
}

/// A [`StorageTransport`] that calls the `Storage` objects of the host that
/// owns [`HOST_NAME`], over one `TinyBus` connection.
///
/// * Documents, Streams, Blobs and Secrets calls are bus method calls; Secrets
///   use the confidential route, which the broker only completes to an
///   attested host.
/// * `PutStream` opens a bus stream on the host and writes the body while the
///   call is outstanding; `GetStream` accepts the stream the host opens on this
///   connection. Neither buffers a whole blob.
/// * Watch invalidations arrive as signals on a path derived from this
///   connection's unique name, accepted only from the host's unique name.
pub struct BusTransport {
    connection: Connection,
    host: BusName,
    object_root: String,
    /// The unique name of the peer that owns [`HOST_NAME`]; streams are
    /// accepted only from it.
    owner: BusName,
    timeout: Duration,
    signals: broadcast::Sender<WatchChanged>,
    forwarder: JoinHandle<()>,
}

/// Join the host call and body pump while preserving a classified local reader
/// error over the backend response induced by aborting that stream. If the host
/// fails before a local reader error is known, the other future is cancelled.
async fn join_streamed_put<C, S>(
    calling: C,
    sending: S,
    failure_state: PumpFailureState,
) -> Result<Value, WireError>
where
    C: std::future::Future<Output = Result<Value, WireError>>,
    S: std::future::Future<Output = Result<(), WireError>>,
{
    match tokio::try_join!(calling, sending) {
        Ok((reply, ())) => Ok(reply),
        Err(error) => Err(failure_state
            .known()
            .map_or(error, source_failure_wire_error)),
    }
}

fn source_failure_wire_error(failure: PumpSourceFailure) -> WireError {
    match failure {
        PumpSourceFailure::Short => WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput,
        }),
        PumpSourceFailure::Source => WireError::Transport("blob source failed".into()),
    }
}

fn pump_wire_error(error: PumpError) -> WireError {
    match error {
        PumpError::Short => source_failure_wire_error(PumpSourceFailure::Short),
        PumpError::Source => source_failure_wire_error(PumpSourceFailure::Source),
        PumpError::Bus(error) => transport_error(&error),
    }
}

impl fmt::Debug for BusTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusTransport")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl Drop for BusTransport {
    fn drop(&mut self) {
        self.forwarder.abort();
    }
}

impl BusTransport {
    /// Attach to the storage host on `connection`.
    ///
    /// # Errors
    /// No peer owns [`HOST_NAME`], the connection has no unique name yet, or
    /// the broker refuses the signal subscription.
    pub async fn connect(connection: Connection) -> tinybus::Result<Self> {
        let owner = connection
            .name_owner(HOST_NAME)
            .await?
            .ok_or_else(|| tinybus::Error::NameHasNoOwner(host_name()))?;
        Self::connect_to(connection, host_name(), owner, OBJECT_ROOT.to_owned()).await
    }

    /// Attach to a host-issued lease route, pinned to its unique service peer.
    /// The descriptor is routing configuration only; the host validates the
    /// authenticated caller against its live lease registry on every call.
    ///
    /// # Errors
    /// The descriptor is malformed, its service peer has disconnected, or the
    /// bus refuses the signal subscription.
    pub async fn connect_bound(
        connection: Connection,
        descriptor: StorageDescriptor,
    ) -> tinybus::Result<Self> {
        let owner = BusName::new(descriptor.service_peer().to_owned())?;
        if !owner.is_unique() {
            return Err(tinybus::Error::failed(
                "storage descriptor service peer must be unique",
            ));
        }
        validate_object_root(descriptor.object_root())?;
        let expected_root = format!("{OBJECT_ROOT}/Leases/{}", descriptor.registration_id);
        if descriptor.object_root() != expected_root {
            return Err(tinybus::Error::failed(
                "storage descriptor route does not match its registration",
            ));
        }
        if connection.name_owner(owner.as_str()).await?.as_ref() != Some(&owner) {
            return Err(tinybus::Error::NameHasNoOwner(owner));
        }
        Self::connect_to(
            connection,
            owner.clone(),
            owner,
            descriptor.object_root().to_owned(),
        )
        .await
    }

    async fn connect_to(
        connection: Connection,
        host: BusName,
        owner: BusName,
        object_root: String,
    ) -> tinybus::Result<Self> {
        let me = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("the connection has no unique name"))?;
        let mut rule = MatchRule::new()
            .signals()
            .sender(owner.clone())
            .interface(interface())
            .member(member(WATCH_MEMBER));
        let expected_watch_path = watch_path_at(&object_root, &me)?;
        rule.path = Some(expected_watch_path.clone());
        let expected_signal_sender = owner.clone();
        // Subscribe before returning so no watch signal can be missed.
        let mut received = connection.add_match(rule).await?;
        let (signals, _) = broadcast::channel(SIGNAL_BUFFER);
        let relay = signals.clone();
        let forwarder = tokio::spawn(async move {
            loop {
                match received.recv().await {
                    Ok(message) => {
                        if let Some(signal) =
                            decode_signal(&message, &expected_watch_path, &expected_signal_sender)
                        {
                            // No receiver just means nobody is watching.
                            let _ = relay.send(signal);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(Self {
            connection,
            host,
            object_root,
            owner,
            timeout: CALL_TIMEOUT,
            signals,
            forwarder,
        })
    }

    /// Use `timeout` for calls and for each stream chunk instead of 30 s.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn proxy(&self, object: Object) -> Result<tinybus::Proxy, WireError> {
        tinybus::Proxy::new(
            self.connection.clone(),
            self.host.clone(),
            route_object_path(&self.object_root, object)
                .map_err(|error| transport_error(&error))?,
            interface(),
        )
        .map(|proxy| proxy.with_timeout(self.timeout))
        .map_err(|error| transport_error(&error))
    }

    fn streamed_call(&self, selector: &str, body: Value) -> Result<Message, WireError> {
        Ok(Message::method_call(
            self.host.clone(),
            route_object_path(&self.object_root, Object::Blobs)
                .map_err(|error| transport_error(&error))?,
            interface(),
            member(selector),
            body,
        ))
    }
}

fn validate_object_root(root: &str) -> tinybus::Result<()> {
    let prefix = format!("{OBJECT_ROOT}/Leases/");
    let tail = root
        .strip_prefix(&prefix)
        .ok_or_else(|| tinybus::Error::failed("storage descriptor has an invalid lease root"))?;
    if tail.is_empty() || tail.contains('/') || !tail.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(tinybus::Error::failed(
            "storage descriptor has an invalid lease root",
        ));
    }
    tinybus::ObjectPath::new(root.to_owned()).map(|_| ())
}

fn route_object_path(root: &str, object: Object) -> tinybus::Result<tinybus::ObjectPath> {
    tinybus::ObjectPath::new(format!("{root}/{}", object.as_str()))
}

fn watch_path_at(root: &str, recipient: &BusName) -> tinybus::Result<tinybus::ObjectPath> {
    if !recipient.is_unique() {
        return Err(tinybus::Error::failed(
            "watch signals address a unique bus name",
        ));
    }
    let element: String = recipient
        .as_str()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect();
    tinybus::ObjectPath::new(format!("{root}/Watch/{element}"))
}

fn transport_error(error: &tinybus::Error) -> WireError {
    WireError::Transport(error.to_string())
}

/// A failed call as the client sees it: a backend category stays a category,
/// everything else is a transport failure.
fn wire_error(error: tinybus::Error) -> WireError {
    match error {
        tinybus::Error::MethodFailed { name, message } if name == ERROR_STORAGE => {
            let kind = serde_json::from_value::<StorageKind>(Value::String(message))
                .unwrap_or(StorageKind::Backend);
            WireError::Storage(StorageFailure { kind })
        }
        tinybus::Error::MethodFailed { name, message } if name == ERROR_DENIED => {
            WireError::Transport(format!("the storage host denied the call: {message}"))
        }
        other => transport_error(&other),
    }
}

fn decode_signal(
    message: &Message,
    expected_path: &tinybus::ObjectPath,
    expected_sender: &BusName,
) -> Option<WatchChanged> {
    if message.header.path.as_ref() != Some(expected_path)
        || message.header.sender.as_ref() != Some(expected_sender)
    {
        return None;
    }
    let first = match &message.body {
        Value::Array(items) if items.len() == 1 => items.first()?.clone(),
        _ => return None,
    };
    serde_json::from_value(first).ok()
}

/// The two-element `[head, StreamRef]` reply of `GetStream`.
fn decode_stream_reply(reply: Value) -> Result<Option<(BlobStreamHead, StreamRef)>, WireError> {
    if reply.is_null() {
        return Ok(None);
    }
    let Value::Array(mut items) = reply else {
        return Err(WireError::Transport("malformed GetStream reply".into()));
    };
    let (Some(stream), Some(head), None) = (items.pop(), items.pop(), items.pop()) else {
        return Err(WireError::Transport("malformed GetStream reply".into()));
    };
    let malformed = |_| WireError::Transport("malformed GetStream reply".into());
    Ok(Some((
        serde_json::from_value(head).map_err(malformed)?,
        serde_json::from_value(stream).map_err(malformed)?,
    )))
}

fn decode_expiry_stream_reply(
    reply: Value,
) -> Result<Option<(BlobStreamHeadWithExpiry, StreamRef)>, WireError> {
    if reply.is_null() {
        return Ok(None);
    }
    let Value::Array(mut items) = reply else {
        return Err(WireError::Transport(
            "malformed GetStreamWithExpiry reply".into(),
        ));
    };
    let (Some(stream), Some(head), None) = (items.pop(), items.pop(), items.pop()) else {
        return Err(WireError::Transport(
            "malformed GetStreamWithExpiry reply".into(),
        ));
    };
    let malformed = |_| WireError::Transport("malformed GetStreamWithExpiry reply".into());
    Ok(Some((
        serde_json::from_value(head).map_err(malformed)?,
        serde_json::from_value(stream).map_err(malformed)?,
    )))
}

#[async_trait]
impl StorageTransport for BusTransport {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError> {
        let proxy = self.proxy(object)?;
        let reply = if object == Object::Secrets {
            proxy.call_confidential::<Value>(selector, (request,)).await
        } else {
            proxy.call::<Value>(selector, (request,)).await
        };
        reply.map_err(wire_error)
    }

    async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        mut body: BlobReader,
    ) -> Result<Value, WireError> {
        let len = head.len;
        let mut descriptor = StreamDescriptor::with_len(len);
        if let Some(content_type) = &head.content_type {
            descriptor = descriptor.content_type(content_type.clone());
        }
        let writer = self
            .connection
            .open_stream_with_timeout(&self.host, descriptor, self.timeout)
            .await
            .map_err(|error| transport_error(&error))?;
        let message = self.streamed_call(
            "PutStream",
            json!([wire_value(&head), wire_value(&writer.stream_ref())]),
        )?;
        // The host reads the stream while this call is in flight, so neither
        // the call nor the body can wait for the other to start.
        let failure_state = PumpFailureState::default();
        let send_failure_state = failure_state.clone();
        let sending = async {
            pump_with_failure_state(writer, &mut body, len, &send_failure_state)
                .await
                .map_err(pump_wire_error)
        };
        let calling = async {
            self.connection
                .call_raw(message, self.timeout)
                .await
                .map_err(wire_error)
        };
        join_streamed_put(calling, sending, failure_state).await
    }

    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_core::BlobReader)>, WireError> {
        let reply = self
            .proxy(Object::Blobs)?
            .call::<Value>("GetStream", (wire_value(&request),))
            .await
            .map_err(wire_error)?;
        let Some((head, stream)) = decode_stream_reply(reply)? else {
            return Ok(None);
        };
        let reader = self
            .connection
            .accept_stream_from(&stream, &self.owner)
            .map_err(|error| transport_error(&error))?;
        Ok(Some((head, blob_reader(reader))))
    }

    async fn put_blob_until(&self, request: PutBlobUntilRequest) -> Result<Value, WireError> {
        <Self as StorageTransport>::call(self, Object::Blobs, "PutUntil", wire_value(&request))
            .await
    }

    async fn put_blob_stream_until(
        &self,
        head: PutBlobStreamUntilRequest,
        mut body: BlobReader,
    ) -> Result<Value, WireError> {
        let len = head.len;
        let mut descriptor = StreamDescriptor::with_len(len);
        if let Some(content_type) = &head.content_type {
            descriptor = descriptor.content_type(content_type.clone());
        }
        let writer = self
            .connection
            .open_stream_with_timeout(&self.host, descriptor, self.timeout)
            .await
            .map_err(|error| transport_error(&error))?;
        let message = self.streamed_call(
            "PutStreamUntil",
            json!([wire_value(&head), wire_value(&writer.stream_ref())]),
        )?;
        let failure_state = PumpFailureState::default();
        let send_failure_state = failure_state.clone();
        let sending = async {
            pump_with_failure_state(writer, &mut body, len, &send_failure_state)
                .await
                .map_err(pump_wire_error)
        };
        let calling = async {
            self.connection
                .call_raw(message, self.timeout)
                .await
                .map_err(wire_error)
        };
        join_streamed_put(calling, sending, failure_state).await
    }

    async fn get_blob_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<BlobWithExpiry>, WireError> {
        let reply = <Self as StorageTransport>::call(
            self,
            Object::Blobs,
            "GetWithExpiry",
            wire_value(&request),
        )
        .await?;
        if reply.is_null() {
            return Ok(None);
        }
        serde_json::from_value(reply)
            .map(Some)
            .map_err(|_| WireError::Transport("malformed GetWithExpiry reply".into()))
    }

    async fn get_blob_stream_with_expiry(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHeadWithExpiry, BlobReader)>, WireError> {
        let reply = self
            .proxy(Object::Blobs)?
            .call::<Value>("GetStreamWithExpiry", (wire_value(&request),))
            .await
            .map_err(wire_error)?;
        let Some((head, stream)) = decode_expiry_stream_reply(reply)? else {
            return Ok(None);
        };
        let reader = self
            .connection
            .accept_stream_from(&stream, &self.owner)
            .map_err(|error| transport_error(&error))?;
        Ok(Some((head, blob_reader(reader))))
    }

    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.signals.subscribe()
    }
}

/// Storage for a module: connect to the host's `Storage` objects over
/// `connection` and return the one scoped handle the host bound this module to.
///
/// The returned [`BusStorage`](tinystoragedrivers_client::BusStorage) also
/// gives `secrets()` and the capabilities the host admitted.
///
/// # Errors
/// The host is absent, refuses the capability calls, or the bus fails.
pub async fn connect_storage(
    connection: Connection,
) -> tinystoragedrivers_core::Result<tinystoragedrivers_client::BusStorage> {
    let transport = BusTransport::connect(connection)
        .await
        .map_err(|error| tinystoragedrivers_core::StorageError::unavailable(error.to_string()))?;
    tinystoragedrivers_client::BusStorage::connect(Arc::new(transport), module_scope_label()).await
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
