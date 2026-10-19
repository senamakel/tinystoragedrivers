//! Storage bus client and reference server adapter.
//!
//! [`BusStorage`] implements the same ports as a native driver
//! ([`StorageBackend`](tinystoragedrivers_core::StorageBackend),
//! `DocumentStore`, `StreamStore`, `BlobStore`, `QueueStore`, `SecretStore`) by turning each
//! call into a typed `tinystoragedrivers-bus` message sent through a
//! [`StorageTransport`]. A transport is the only thing that knows about
//! `TinyBus`; this crate has no dependency on it.
//!
//! [`StorageServer`] is the host side: it is generic over an installed
//! backend, holds the caller's scope, and answers the same messages. The scope
//! lives in the server and never in a request: every request type rejects
//! unknown fields, so a forged `scope` is a decode failure. [`Loopback`] joins
//! a client to a server in one process through a JSON text frame, the way a
//! bus does, so the shared conformance suite can run against the client.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use std::sync::Arc;
//! use tinystoragedrivers_client::{Admission, BusStorage, Loopback, StorageServer};
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! let backend = MemoryStorage::new();
//! let storage = backend.for_scope(&Scope::local())?;
//! let server = Arc::new(StorageServer::new(storage));
//! let bus = BusStorage::connect(
//!     Arc::new(Loopback::new(server, Admission::Private)),
//!     Scope::local(),
//! )
//! .await?;
//! let docs = bus.for_scope(&Scope::local())?;
//! docs.documents().ensure_collection(&tinystoragedrivers_core::CollectionSpec::new("notes")).await?;
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
mod client;
mod convert;
mod loopback;
mod server;
mod transport;

pub use client::{BusBlobs, BusDocuments, BusQueues, BusSecrets, BusStorage, BusStreams};
pub use loopback::Loopback;
pub use server::StorageServer;
pub use transport::{Admission, BlobBody, Object, StorageTransport, WireError};

#[cfg(test)]
#[path = "ops_tests.rs"]
mod ops_tests;
