//! The MongoDB driver for `tinystoragedrivers`: the multi-tenant cloud
//! backend.
//!
//! One MongoDB database is shared by every tenant. Each stored record carries
//! the [`Scope`](tinystoragedrivers_core::Scope) it belongs to in a `_scope`
//! field, and the driver ANDs `_scope` into every query, write, aggregation
//! and index, so a handle opened for one scope cannot reach another's data.
//!
//! The entry point is [`MongoStorage::connect`]; most hosts reach it through
//! the `tinystoragedrivers` facade with a `mongodb://…/<db>` URL and the
//! `mongodb` feature.
//!
//! ```no_run
//! # async fn demo() -> tinystoragedrivers_core::Result<()> {
//! use serde_json::json;
//! use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};
//! use tinystoragedrivers_mongodb::MongoStorage;
//!
//! let backend = MongoStorage::connect("mongodb://db.internal/openhuman", "openhuman").await?;
//! let alice = backend.for_scope(&Scope::new("alice")?)?;
//! alice
//!     .documents()
//!     .put("pending", "req-1", json!({"tool": "shell"}), Precondition::Absent)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Capabilities
//!
//! - **Ttl**: expiry is a read-time rule (an expired document reads as
//!   absent), and [`MongoStorage::sweep_expired`] reclaims the space. A Mongo
//!   TTL index is not used: it needs a BSON date and deletes lazily.
//! - **`FullText`**: a `$text` index over the declared search fields, without
//!   stemming or stop words.
//! - **Transactions**: only on replica sets and sharded clusters, detected at
//!   connect.
//! - **Fencing**: with transactions only. Document and stream writes run in a
//!   transaction that reads and touches the fence's guard document; blob
//!   writes are refused, because GridFS cannot join a transaction.
//!
//! - **`ScopePurge`**: replica sets support resumable purge under trusted host
//!   writer quiescence. Persistent Pending admission blocks all generations.
//!   GridFS prevents atomic or fenced all-port purge; those requests are refused.
//!
//! Payload and admission reads select the primary. Retired-generation reads
//! fail closed before returning replacement data.
//!
//! See the crate `README.md` for the storage layout and every place this
//! driver's behavior is driver-defined.
//!
//! This crate holds no typed repository and no URL parsing; the facade owns
//! `StorageConfig`, and records belong to the crates that own them.

mod backend;
mod blobs;
mod convert;
mod documents;
mod errors;
mod fence;
mod naming;
mod purge;
mod scoped;
mod scopes;
mod streams;
mod translate;

pub use backend::{MongoOpenOptions, MongoStorage};
