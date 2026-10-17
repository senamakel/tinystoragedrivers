//! The in-memory driver: the reference implementation of every port.
//!
//! It is the reference implementation for the storage ports, keeps nothing
//! across process restarts, and is what tests and stateless embedders run on.
//! [`Maintenance`](crate::Capability::Maintenance) succeeds and reports zero
//! because there is nothing to reclaim. [`Fencing`](crate::Capability::Fencing)
//! is checked under the same lock as each write. Native `Watch` stays disabled
//! by default; the `test-util` feature provides an explicit opt-in for tests
//! that need that capability.

mod blobs;
mod documents;
mod purge;
mod state;
mod streams;
#[cfg(feature = "test-util")]
mod watch;

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
    #[cfg(feature = "test-util")]
    watchers: Arc<watch::MemoryWatchRegistry>,
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

    #[cfg(feature = "test-util")]
    fn subscribe_watch(
        &self,
        key: (String, String),
        documents: MemoryDocuments,
        interval: std::time::Duration,
    ) -> Result<watch::MemoryWatch> {
        self.watchers.subscribe(key, documents, interval)
    }

    #[cfg(feature = "test-util")]
    pub(super) fn invalidate_watch(&self, scope: &str, collection: &str) {
        self.watchers.invalidate(scope, collection);
    }

    #[cfg(feature = "test-util")]
    pub(super) fn invalidate_scope_watches(&self, scope: &str) {
        self.watchers.invalidate_scope(scope);
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
    #[cfg(feature = "test-util")]
    native_watch: bool,
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
            #[cfg(feature = "test-util")]
            native_watch: false,
        }
    }

    /// Opt in to native document invalidations for tests that require `Watch`.
    ///
    /// Ordinary memory storage stays watch-free. The test watch coalesces
    /// payload-free invalidations for writes in the same scope and collection,
    /// and periodically invalidates at the watch interval for logical expiry.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_native_watch() -> Self {
        Self::new().enable_native_watch()
    }

    /// Enable native document invalidations on an existing test backend.
    ///
    /// Use this when a test starts with `with_clock`; `new`, `with_clock`, and
    /// `Default` otherwise leave native Watch disabled.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn enable_native_watch(mut self) -> Self {
        self.native_watch = true;
        self
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
        let documents = MemoryDocuments::new(
            Arc::clone(&self.db),
            scope.clone(),
            Arc::clone(&self.clock),
            fence.cloned(),
            self.existing,
        );
        #[cfg(feature = "test-util")]
        let documents = documents.with_native_watch(self.native_watch);
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(documents.with_generation(generation)),
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
        let capabilities = Capabilities::all()
            .without(crate::Capability::Watch)
            .without(crate::Capability::PresignedUrls)
            .without(crate::Capability::StreamingBlobs);
        #[cfg(feature = "test-util")]
        let capabilities = if self.native_watch {
            capabilities.with(crate::Capability::Watch)
        } else {
            capabilities
        };
        capabilities
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
