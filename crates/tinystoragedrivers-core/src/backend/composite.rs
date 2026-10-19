//! Compose document/stream storage with an independently scoped blob driver.

use crate::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability, Checkpoint, Fence,
    MaintenanceBudget, MaintenanceReport, Result, Scope, ScopedStorage, StorageBackend,
    StorageError, async_trait,
};
use std::{ops::Range, sync::Arc, time::Duration};

/// A connected backend that provides only scoped blob handles.
pub trait BlobBackend: Send + Sync + std::fmt::Debug {
    /// Optional blob abilities supplied by every unfenced handle.
    fn capabilities(&self) -> Capabilities;
    /// Blob handle bound to one tenant.
    ///
    /// # Errors
    ///
    /// Unsupported scope or backend failure.
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>>;
}

/// Use the blob port of an existing complete backend independently.
#[derive(Debug)]
pub struct BackendBlobs(Arc<dyn StorageBackend>);

impl BackendBlobs {
    /// Wrap an existing backend's blob port.
    #[must_use]
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self(backend)
    }
}

impl BlobBackend for BackendBlobs {
    fn capabilities(&self) -> Capabilities {
        let source = self.0.capabilities();
        let mut caps = Capabilities::none();
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
            Capability::BlobSweep,
        ] {
            if source.contains(cap) {
                caps = caps.with(cap);
            }
        }
        caps
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        Ok(self.0.for_scope(scope)?.blobs().clone())
    }
}

/// Documents and streams from one backend, blobs from another.
///
/// Upload through an unfenced scope first, then commit its reference through
/// a fenced document handle. Fenced blob mutations always fail rather than
/// silently omit the document fence; reads remain available.
#[derive(Debug)]
pub struct CompositeStorage {
    records: Arc<dyn StorageBackend>,
    blobs: Arc<dyn BlobBackend>,
}

impl CompositeStorage {
    /// Compose already connected backends.
    #[must_use]
    pub fn new(records: Arc<dyn StorageBackend>, blobs: Arc<dyn BlobBackend>) -> Self {
        Self { records, blobs }
    }

    fn combine(&self, records: &ScopedStorage, fenced: bool) -> Result<ScopedStorage> {
        let scope = records.scope().clone();
        let blobs = self.blobs.for_scope(&scope)?;
        let blobs: Arc<dyn BlobStore> = if fenced {
            Arc::new(FencedBlobs(blobs))
        } else {
            blobs
        };
        Ok(ScopedStorage::new(
            scope,
            "composite",
            records.documents().clone(),
            records.streams().clone(),
            blobs,
        ))
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
            .without(Capability::BlobTtl)
            .without(Capability::BlobSweep);
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
            Capability::BlobSweep,
        ] {
            if self.blobs.capabilities().contains(cap) {
                caps = caps.with(cap);
            }
        }
        caps
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        self.combine(&self.records.for_scope(scope)?, false)
    }
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        self.combine(&self.records.for_scope_fenced(scope, fence)?, true)
    }
    /// Maintains the records backend; the blob backend is a separate store a
    /// host maintains on its own.
    async fn maintain(&self, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
        self.records.maintain(budget).await
    }
    async fn checkpoint(&self, mode: Checkpoint) -> Result<()> {
        self.records.checkpoint(mode).await
    }
}

#[derive(Debug)]
struct FencedBlobs(Arc<dyn BlobStore>);

fn unfenceable() -> StorageError {
    StorageError::unsupported(
        Capability::Fencing,
        "independent blob writes cannot join the document fence",
    )
}

#[async_trait]
impl BlobStore for FencedBlobs {
    fn max_key_len(&self) -> usize {
        self.0.max_key_len()
    }
    fn capabilities(&self) -> Capabilities {
        // Presigned PUT cannot enforce the document fence.
        self.0
            .capabilities()
            .without(Capability::ScopePurge)
            .without(Capability::AtomicScopePurge)
            .without(Capability::PresignedUrls)
            .without(Capability::BlobTtl)
            // Fenced blob writes cannot coordinate a sweep with the document
            // reference check, so this handle must not advertise cleanup.
            .without(Capability::BlobSweep)
    }
    async fn put(&self, _: &str, _: Vec<u8>, _: Option<&str>) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_expiring(
        &self,
        _: &str,
        _: Vec<u8>,
        _: Option<&str>,
        _: Duration,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_stream_expiring(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
        _: Duration,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_stream(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn delete(&self, _: &str) -> Result<bool> {
        Err(unfenceable())
    }
    async fn presign_put(&self, _: &str, _: Duration) -> Result<String> {
        Err(unfenceable())
    }
    async fn sweep_expired(&self) -> Result<u64> {
        Err(unfenceable())
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.0.get(key).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        self.0.get_stream(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.0.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.0.head(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.0.list(prefix).await
    }
    async fn list_sweep_page(
        &self,
        _prefix: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<crate::BlobSweepPage> {
        Err(StorageError::unsupported(
            Capability::BlobSweep,
            "fenced blob handles cannot sweep document references",
        ))
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.0.presign_get(key, ttl).await
    }
}

#[cfg(test)]
#[path = "composite_tests.rs"]
mod tests;
