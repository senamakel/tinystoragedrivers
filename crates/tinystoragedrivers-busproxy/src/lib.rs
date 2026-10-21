//! The real `TinyBus` transport for the storage bus.
//!
//! `tinystoragedrivers-client` turns the storage ports into typed bus messages
//! and knows nothing about `TinyBus`; this crate is the other half, on both
//! ends of a connection:
//!
//! * **Module side.** [`BusTransport`] implements
//!   [`StorageTransport`](tinystoragedrivers_client::StorageTransport) over a
//!   [`tinybus::Connection`]: async JSON calls to the
//!   `ai.tinyhumans.tinystorage.Storage` objects, blobs over `tinybus` streams
//!   in bounded chunks, watch invalidations as signals. [`connect_storage`]
//!   is the one call a module makes to obtain its
//!   [`BusStorage`](tinystoragedrivers_client::BusStorage).
//! * **Host side.** [`serve`] registers the five objects on the host's
//!   connection and hands every call, with the broker-authenticated sender, to
//!   a [`StorageEndpoint`]. [`PerCallerEndpoint`] is the ready-made endpoint
//!   that binds each caller to its own `StorageServer`; a host that builds
//!   that server `with_queue_manifest` serves exactly the queues its manifest
//!   declares.
//!
//! ```no_run
//! # async fn module(connection: tinybus::Connection) -> tinystoragedrivers_core::Result<()> {
//! use tinystoragedrivers_core::{DocumentStore, CollectionSpec};
//!
//! let storage = tinystoragedrivers_busproxy::connect_storage(connection).await?;
//! let scoped = tinystoragedrivers_core::StorageBackend::for_scope(
//!     &storage,
//!     &tinystoragedrivers_busproxy::module_scope_label(),
//! )?;
//! scoped.documents().ensure_collection(&CollectionSpec::new("notes")).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Wire conventions
//!
//! * Request/reply members take one positional argument (`[request]`) and reply
//!   with the typed reply as JSON. `Secrets` is served on the confidential
//!   route only.
//! * `Blobs.PutStream` takes `[PutBlobStreamRequest, StreamRef]`: the caller
//!   opens a stream on the host and writes the body while the call is
//!   outstanding. `Blobs.GetStream` takes `[BlobKeyRequest]` and replies `null`
//!   or `[BlobStreamHead, StreamRef]`; the host opens the stream on the caller.
//! * Watch invalidations are the `WatchChanged` signal carrying `[WatchChanged]`
//!   on `/ai/tinyhumans/tinystorage/Watch/<caller>`; see [`watch_path`].
//! * Failures are `MethodFailed` errors named [`ERROR_DENIED`] (a label, no
//!   content) or [`ERROR_STORAGE`] (a `snake_case` `StorageKind`).
//!
//! ## Peer binding
//!
//! * Streams are taken with `Connection::accept_stream_from`: the host reads an
//!   upload only from the authenticated caller that opened it, and a client
//!   reads a download only from the host's unique name. A guessed stream id of
//!   another peer is refused and stays available to its owner.
//! * Watch signals are unicast with `Connection::emit_to` to the subscribing
//!   caller, so a peer that matches the same path or interface never sees them.
mod chunks;
pub mod governed;
pub mod grant;
mod host;
mod names;
mod per_caller;
#[cfg(any(test, feature = "test-stream-diagnostics"))]
#[doc(hidden)]
pub mod test_support;
mod transport;

pub use governed::{
    AuditEvent, AuditSink, CallAuditMetadata, CallerDirectory, Environment, GovernedEndpoint,
    GovernedEndpointLimits, GovernedStorage, HostContext, HostStorageContext, ResolvedStorage,
    StorageDescriptor, StorageLease, StorageRegistration, TinyBusCallerDirectory,
};
pub use grant::{
    Denied, GrantError, GrantPolicy, Plan, QueueGrantError, QueueManifest, ReplyFilter,
    StorageGrant,
};
pub use host::{HostFailure, ServedStorage, StorageCall, StorageEndpoint, serve};
pub use names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, OBJECT_ROOT, WATCH_MEMBER, object_path, watch_path,
};
pub use per_caller::PerCallerEndpoint;
pub use transport::{BusTransport, connect_storage, module_scope_label};

#[cfg(test)]
#[path = "bus_tests.rs"]
mod bus_tests;
#[cfg(test)]
#[path = "watching_fake.rs"]
mod watching_fake;
