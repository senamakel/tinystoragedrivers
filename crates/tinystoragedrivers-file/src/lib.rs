//! The plain-file driver for tinystoragedrivers.
//!
//! [`FileStorage`] keeps every record as an ordinary file under one directory,
//! so the data stays human-inspectable and easy to back up or diff:
//!
//! - documents: one pretty-printed JSON file each, `{"id", "version", "doc"}`
//!   (a deleted document leaves a `{"id", "version"}` tombstone so its id never
//!   reuses a version);
//! - streams: one JSONL file each, a `{"offset", "value"}` line per entry;
//! - blobs: the raw bytes, with a small JSON sidecar for content type and expiry.
//!
//! It is meant for desktop JSON stores (artifacts, costs, app state, turn
//! states, thread transcripts) that are small per user and want to stay
//! readable on disk. It passes the same conformance suite as every other
//! driver.
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::{Scope, StorageBackend};
//! use tinystoragedrivers_file::FileStorage;
//!
//! let dir = tempfile::tempdir().unwrap();
//! let storage = FileStorage::open(dir.path())?;
//! let threads = storage.for_scope(&Scope::local())?;
//! threads.streams().append("t1", json!({"role": "user", "text": "hi"})).await?;
//! assert_eq!(threads.streams().len("t1").await?, 1);
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Capabilities and limits
//!
//! - Expiry ([`Capability::Ttl`](tinystoragedrivers_core::Capability::Ttl))
//!   and full-text search
//!   ([`Capability::FullText`](tinystoragedrivers_core::Capability::FullText))
//!   are provided. Cross-document transactions are not: a batch spanning
//!   several files cannot be made crash-atomic with renames, so
//!   [`atomic_batch`](tinystoragedrivers_core::DocumentStore::atomic_batch)
//!   returns `Unsupported`.
//! - [`scopes`](tinystoragedrivers_core::StorageBackend::scopes) walks the
//!   `scopes/` tree and lists every scope that has a file. `maintain` and
//!   `checkpoint` are `Unsupported`: plain files have nothing to reclaim or
//!   flush, and a caller treats the refusal as nothing to do.
//! - Queries read the whole collection; there are no on-disk indexes.
//! - Operations are serialized per directory within one process. Several
//!   processes writing one directory are not coordinated.
//! - Names are percent-encoded into file names that are distinct on
//!   case-insensitive filesystems. Encodings over 200 bytes become a hash, with
//!   the real name stored inside the file. Windows' 260-character path limit
//!   applies unless long paths are enabled.
//!
//! This crate holds no repository for any particular record type; those belong
//! to the crates that own the records.

mod blobs;
mod documents;
mod encode;
mod fsio;
mod scopes;
mod storage;
mod streams;

pub use blobs::FileBlobs;
pub use documents::FileDocuments;
pub use storage::FileStorage;
pub use streams::FileStreams;
