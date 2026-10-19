//! Compose document/stream storage with an independently scoped blob driver.

use crate::{
    BlobStore, Capabilities, Capability, Checkpoint, Fence, MaintenanceBudget, MaintenanceReport,
    Result, Scope, ScopeGeneration, ScopedStorage, StorageBackend, StorageError, async_trait,
};
use std::sync::Arc;

mod admission;
mod backend_blobs;
mod pending_ports;
mod purge;
use admission::{FencedBlobs, GenerationBlobs};
use pending_ports::{PendingDocuments, PendingStreams};
use purge::{CompositePurge, coordinator_scope_for_layer, ensure_public_scope, is_public_scope};

/// A connected backend that provides only scoped blob handles.
#[async_trait]
pub trait BlobBackend: Send + Sync + std::fmt::Debug {
    /// Optional blob abilities supplied by every unfenced handle.
    fn capabilities(&self) -> Capabilities;
    /// Blob handle bound to one tenant.
    ///
    /// # Errors
    ///
    /// Unsupported scope or backend failure.
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>>;
    /// Whether this backend can isolate and erase one physical blob generation.
    /// Composite scope purge is advertised only when this is true. A backend
    /// must provide distinct, non-overlapping physical namespaces; merely
    /// forwarding the records generation number is not sufficient.
    fn supports_generation_purge(&self) -> bool {
        false
    }
    /// Blob handle bound to one scope generation.
    ///
    /// # Errors
    ///
    /// Unsupported generation binding or backend failure.
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<Arc<dyn BlobStore>> {
        let _ = (scope, generation);
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "blob backend cannot bind generations",
        ))
    }
    /// Erase only blobs in the specified physical generation. Writes already
    /// in flight may finish in the old namespace after this returns; they are
    /// inaccessible from later generations and may require a later orphan
    /// sweep for physical reclamation.
    ///
    /// # Errors
    ///
    /// Unsupported generation purge or backend failure.
    async fn purge_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<()> {
        let _ = (scope, generation);
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "blob backend cannot purge an isolated generation",
        ))
    }
    /// Erase only this scope's blobs. Implementations must not touch records.
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        let _ = scope;
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "blob backend cannot purge a scope",
        ))
    }
    /// Enumerate scopes containing blobs.
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let _ = prefix;
        Err(StorageError::unsupported(
            Capability::ScopeEnumeration,
            "blob backend cannot enumerate scopes",
        ))
    }
}

pub use backend_blobs::BackendBlobs;

/// Documents and streams from one backend, blobs from another.
///
/// Upload through an unfenced scope first, then commit its reference through
/// a fenced document handle. Fenced blob mutations always fail rather than
/// silently omit the document fence; reads remain available.
#[derive(Debug)]
pub struct CompositeStorage {
    records: Arc<dyn StorageBackend>,
    blobs: Arc<dyn BlobBackend>,
    // Serializes runners sharing this composition instance. Durable CAS
    // coordinator state still governs retries across reopened instances.
    purge_lock: Arc<tokio::sync::Mutex<()>>,
}

impl CompositeStorage {
    /// Compose already connected backends.
    #[must_use]
    pub fn new(records: Arc<dyn StorageBackend>, blobs: Arc<dyn BlobBackend>) -> Self {
        Self {
            records,
            blobs,
            purge_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn combine(
        &self,
        records: &ScopedStorage,
        generation: ScopeGeneration,
        fenced: bool,
    ) -> Result<ScopedStorage> {
        let scope = records.scope().clone();
        let blobs = if self.blobs.supports_generation_purge() {
            self.blobs.for_scope_generation(&scope, generation)?
        } else if generation == ScopeGeneration(0) {
            self.blobs.for_scope(&scope)?
        } else {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "blob backend cannot bind scope generations",
            ));
        };
        let can_purge = self.records.capabilities().contains(Capability::ScopePurge)
            && self.records.supports_scope_purge_binding_validation()
            && self.records.supports_scope_purge_generation_validation()
            && records.supports_purge_binding_validation()
            && records.supports_purge_generation_validation()
            && self.blobs.supports_generation_purge()
            && self.blobs.capabilities().contains(Capability::ScopePurge);
        if generation != ScopeGeneration(0) && !can_purge {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "composite backend cannot validate scope generation bindings",
            ));
        }
        let coordinator = can_purge
            .then(|| {
                let depth = self.records.composite_depth().ok_or_else(|| {
                    StorageError::backend("composite storage nesting depth overflowed")
                })?;
                self.records
                    .for_composite_internal_scope(&coordinator_scope_for_layer(depth)?)
            })
            .transpose()?;
        let blobs: Arc<dyn BlobStore> = Arc::new(GenerationBlobs {
            inner: if fenced {
                Arc::new(FencedBlobs(blobs))
            } else {
                blobs
            },
            records: records.clone(),
            coordinator: coordinator.clone(),
            validate_generation: records.supports_purge_generation_validation(),
        });
        let mut combined = ScopedStorage::new(
            scope,
            "composite",
            coordinator.as_ref().map_or_else(
                || records.documents().clone(),
                |coordinator| {
                    Arc::new(PendingDocuments {
                        inner: records.documents().clone(),
                        coordinator: coordinator.clone(),
                        scope: records.scope().clone(),
                    }) as Arc<dyn crate::DocumentStore>
                },
            ),
            coordinator.as_ref().map_or_else(
                || records.streams().clone(),
                |coordinator| {
                    Arc::new(PendingStreams {
                        inner: records.streams().clone(),
                        coordinator: coordinator.clone(),
                        scope: records.scope().clone(),
                    }) as Arc<dyn crate::StreamStore>
                },
            ),
            blobs,
        );
        if let Some(coordinator) = coordinator {
            combined = combined.with_purge(Arc::new(CompositePurge {
                records: records.clone(),
                blobs: Arc::clone(&self.blobs),
                scope: records.scope().clone(),
                generation,
                coordinator,
                purge_lock: Arc::clone(&self.purge_lock),
            }));
        }
        Ok(combined)
    }
}

#[async_trait]
impl StorageBackend for CompositeStorage {
    fn driver(&self) -> &'static str {
        "composite"
    }
    fn document_driver(&self) -> &'static str {
        self.records.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        let mut caps = self
            .records
            .capabilities()
            .without(Capability::ExistingSchema)
            .without(Capability::ScopePurge)
            .without(Capability::AtomicScopePurge)
            // Blobs live in an independent store, so the records backend
            // cannot tell which scopes hold only blobs.
            .without(Capability::ScopeEnumeration)
            .without(Capability::PresignedUrls)
            .without(Capability::StreamingBlobs)
            .without(Capability::BlobTtl);
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
        ] {
            if self.blobs.capabilities().contains(cap) {
                caps = caps.with(cap);
            }
        }
        if self.records.capabilities().contains(Capability::ScopePurge)
            && self.records.supports_scope_purge_binding_validation()
            && self.records.supports_scope_purge_generation_validation()
            && self.blobs.supports_generation_purge()
            && self.blobs.capabilities().contains(Capability::ScopePurge)
        {
            caps = caps.with(Capability::ScopePurge);
        }
        if self
            .records
            .capabilities()
            .contains(Capability::ScopeEnumeration)
            && self
                .blobs
                .capabilities()
                .contains(Capability::ScopeEnumeration)
        {
            caps = caps.with(Capability::ScopeEnumeration);
        }
        caps
    }
    fn composite_depth(&self) -> Option<usize> {
        self.records
            .composite_depth()
            .and_then(|depth| depth.checked_add(1))
    }
    fn supports_scope_purge_binding_validation(&self) -> bool {
        self.capabilities().contains(Capability::ScopePurge)
    }
    fn supports_scope_purge_generation_validation(&self) -> bool {
        self.capabilities().contains(Capability::ScopePurge)
    }
    fn for_composite_internal_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        if scope.as_str() != "__tsd_composite_purge_coordinator"
            && !scope
                .as_str()
                .starts_with("__tsd_composite_purge_coordinator/")
        {
            return Err(StorageError::invalid_input(
                "invalid internal composite storage scope",
            ));
        }
        self.records.for_composite_internal_scope(scope)
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        ensure_public_scope(scope)?;
        self.combine(&self.records.for_scope(scope)?, ScopeGeneration(0), false)
    }
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        ensure_public_scope(scope)?;
        self.combine(
            &self.records.for_scope_fenced(scope, fence)?,
            ScopeGeneration(0),
            true,
        )
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<ScopedStorage> {
        ensure_public_scope(scope)?;
        let can_purge = self.records.capabilities().contains(Capability::ScopePurge)
            && self.records.supports_scope_purge_binding_validation()
            && self.records.supports_scope_purge_generation_validation()
            && self.blobs.supports_generation_purge()
            && self.blobs.capabilities().contains(Capability::ScopePurge);
        if !can_purge && generation != ScopeGeneration(0) {
            let _ = (scope, generation);
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "composite backend cannot bind scope generations",
            ));
        }
        self.combine(
            &self.records.for_scope_generation(scope, generation)?,
            generation,
            false,
        )
    }
    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        ensure_public_scope(scope)?;
        let can_purge = self.records.capabilities().contains(Capability::ScopePurge)
            && self.records.supports_scope_purge_binding_validation()
            && self.records.supports_scope_purge_generation_validation()
            && self.blobs.supports_generation_purge()
            && self.blobs.capabilities().contains(Capability::ScopePurge);
        if !can_purge && generation != ScopeGeneration(0) {
            let _ = (scope, generation, fence);
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "composite backend cannot bind scope generations",
            ));
        }
        self.combine(
            &self
                .records
                .for_scope_fenced_generation(scope, generation, fence)?,
            generation,
            true,
        )
    }
    /// Maintains the records backend; the blob backend is a separate store a
    /// host maintains on its own.
    async fn maintain(&self, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
        self.records.maintain(budget).await
    }
    async fn checkpoint(&self, mode: Checkpoint) -> Result<()> {
        self.records.checkpoint(mode).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let mut scopes = self.records.scopes(prefix).await?;
        scopes.extend(self.blobs.scopes(prefix).await?);
        scopes.retain(is_public_scope);
        scopes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        scopes.dedup();
        Ok(scopes)
    }
}

#[cfg(test)]
#[path = "composite_tests.rs"]
mod tests;
