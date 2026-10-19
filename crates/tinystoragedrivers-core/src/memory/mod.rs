//! The in-memory driver: the reference implementation of every port.
//!
//! It provides every [`Capability`](crate::Capability), including
//! [`Maintenance`](crate::Capability::Maintenance) (it has nothing to reclaim,
//! so `maintain` and `checkpoint` succeed and report zero),
//! [`Fencing`](crate::Capability::Fencing) on every write (the fence is
//! checked under the same lock as the write), keeps nothing across process
//! restarts, and is what tests and stateless embedders run on. Because
//! its semantics are the simplest correct reading of the port docs, the
//! conformance suite is written against it first and every other driver must
//! agree with it.

mod blobs;
mod documents;
mod purge;
mod state;
mod streams;

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{
    Checkpoint, MaintenanceBudget, MaintenanceReport, ScopedStorage, StorageBackend,
};
use crate::capabilities::Capabilities;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::scope::Scope;

pub use blobs::MemoryBlobs;
pub use documents::MemoryDocuments;
pub use streams::MemoryStreams;

use state::DbState;

/// Milliseconds since the Unix epoch. The memory driver reads time through
/// this so tests can drive document expiry deterministically.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub(crate) fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// The in-memory database behind a [`MemoryStorage`].
#[derive(Debug, Default)]
pub(crate) struct MemoryDb {
    state: Mutex<DbState>,
}

impl MemoryDb {
    fn lock(&self) -> Result<MutexGuard<'_, DbState>> {
        self.state
            .lock()
            .map_err(|_| StorageError::backend("memory storage lock poisoned by a panicked writer"))
    }
    fn guard(state: &DbState, fence: Option<&MemoryFence>, now: u64) -> Result<()> {
        match fence {
            None => Ok(()),
            Some(binding) => state.guard(Some(&binding.fence), now),
        }
    }
}

#[derive(Debug)]
pub(super) struct MemoryFence {
    fence: Fence,
}

/// An in-memory [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
///
/// let storage = MemoryStorage::new();
/// let alice = storage.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "memory");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct MemoryStorage {
    db: Arc<MemoryDb>,
    clock: Clock,
    existing: bool,
}

impl MemoryStorage {
    /// An empty backend reading the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(system_clock))
    }

    /// An empty backend reading time from `clock`.
    #[must_use]
    pub fn with_clock(clock: Clock) -> Self {
        Self {
            db: Arc::new(MemoryDb::default()),
            clock,
            existing: false,
        }
    }

    fn handles(&self, scope: &Scope, fence: Option<&Arc<MemoryFence>>) -> ScopedStorage {
        self.handles_generation(scope, fence, crate::ScopeGeneration::default())
    }
    fn handles_generation(
        &self,
        scope: &Scope,
        fence: Option<&Arc<MemoryFence>>,
        generation: crate::ScopeGeneration,
    ) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                MemoryDocuments::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                    self.existing,
                )
                .with_generation(generation),
            ),
            Arc::new(
                MemoryStreams::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                )
                .with_generation(generation),
            ),
            Arc::new(
                MemoryBlobs::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                )
                .with_generation(generation),
            ),
        )
        .with_purge(Arc::new(purge::MemoryPurge {
            db: Arc::clone(&self.db),
            scope: scope.clone(),
            generation,
            clock: Arc::clone(&self.clock),
            fence: fence.cloned(),
        }))
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StorageBackend for MemoryStorage {
    fn driver(&self) -> &'static str {
        "memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
            .without(crate::Capability::Watch)
            .without(crate::Capability::PresignedUrls)
            .without(crate::Capability::StreamingBlobs)
            .without(crate::Capability::SharedAcrossProcesses)
    }

    fn supports_scope_purge_binding_validation(&self) -> bool {
        true
    }

    fn supports_scope_purge_generation_validation(&self) -> bool {
        true
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                crate::Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        Ok(())
    }

    /// Nothing to reclaim: memory is freed as records go.
    async fn maintain(&self, _: MaintenanceBudget) -> Result<MaintenanceReport> {
        Ok(MaintenanceReport::default())
    }

    /// Nothing to flush: there is no log.
    async fn checkpoint(&self, _: Checkpoint) -> Result<()> {
        Ok(())
    }

    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        self.db
            .lock()?
            .scopes(prefix)
            .into_iter()
            .map(Scope::new)
            .collect()
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let mut storage = self.clone();
        storage.existing = true;
        Ok(Arc::new(storage))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(MemoryFence {
                fence: fence.clone(),
            })),
        ))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: crate::ScopeGeneration,
    ) -> Result<ScopedStorage> {
        Ok(self.handles_generation(scope, None, generation))
    }

    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: crate::ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles_generation(
            scope,
            Some(&Arc::new(MemoryFence {
                fence: fence.clone(),
            })),
            generation,
        ))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "generation_tests.rs"]
mod generation_tests;

#[cfg(test)]
#[path = "blob_ttl_tests.rs"]
mod blob_ttl_tests;

#[cfg(test)]
#[path = "meta_tests.rs"]
mod meta_tests;

#[cfg(test)]
#[path = "seal_tests.rs"]
mod seal_tests;
