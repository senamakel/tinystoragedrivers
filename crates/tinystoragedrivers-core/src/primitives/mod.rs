//! Shared primitives (owner `tsd`): leases, key-value, counters and the typed
//! [`Repo`] with its compare-and-swap [`update`](Repo::update) loop.
//!
//! Every owner needs the same four things, and each used to rebuild them. They
//! all run on a [`DocumentStore`](crate::DocumentStore) handle the caller opened for the right
//! scope (a tenant for [`Kv`] and [`Counters`], usually `cluster` for
//! [`Leases`] that place processes), so scope isolation and write fencing
//! apply to them exactly as to any other record.
//!
//! The three collections they use, [`LEASES`], [`KV`] and [`COUNTERS`], are
//! declared by [`collection_specs`]; call [`ensure`] once per scope at boot
//! (the schema registry does it for a host that composes one).
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use tinystoragedrivers_core::primitives::{self, Counters};
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! let storage = MemoryStorage::new().for_scope(&Scope::local())?;
//! primitives::ensure(&storage).await?;
//! let counters = Counters::new(storage.documents());
//! assert_eq!(counters.next("cron", "runs").await?, 1);
//! assert_eq!(counters.next("cron", "runs").await?, 2);
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```

mod counters;
mod kv;
mod leases;
mod repo;

use crate::backend::ScopedStorage;
use crate::document::{CollectionSpec, IndexSpec};
use crate::error::Result;

pub use counters::Counters;
pub use kv::Kv;
pub use leases::{Acquire, Acquisition, Lease, LeaseRecord, Leases};
pub use repo::{Repo, RetryPolicy};

/// The lease collection.
pub const LEASES: &str = "tsd.leases";
/// The key-value collection.
pub const KV: &str = "tsd.kv";
/// The counter collection.
pub const COUNTERS: &str = "tsd.counters";

/// The collections the primitives need, with their indexes.
///
/// [`Kv`] declares an index over `(owner, ns, key)` so a namespace lists in
/// key order without scanning other owners' entries. Leases and counters are
/// only ever read by id.
#[must_use]
pub fn collection_specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(LEASES),
        CollectionSpec::new(KV).index(IndexSpec::new("kv_by_key", ["owner", "ns", "key"])),
        CollectionSpec::new(COUNTERS),
    ]
}

/// Declare [`collection_specs`] on `storage`'s scope. Idempotent.
///
/// # Errors
///
/// The first failing [`ensure_collection`](crate::DocumentStore::ensure_collection).
pub async fn ensure(storage: &ScopedStorage) -> Result<()> {
    for spec in collection_specs() {
        storage.documents().ensure_collection(&spec).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
