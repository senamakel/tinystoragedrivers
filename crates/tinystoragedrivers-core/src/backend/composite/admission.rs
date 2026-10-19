//! Admission-fenced blob wrappers used by the composite backend.
use crate::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability, Result,
    ScopedStorage, StorageError, async_trait,
};
use std::sync::Arc;
use std::{ops::Range, time::Duration};

/// Couple independent blob access to the records handle's native admission
/// check, so a generation revoked by purge revokes old blob handles too.
#[derive(Debug)]
pub(super) struct GenerationBlobs {
    pub(super) inner: Arc<dyn BlobStore>,
    pub(super) records: ScopedStorage,
    pub(super) coordinator: Option<ScopedStorage>,
    pub(super) validate_generation: bool,
}

impl GenerationBlobs {
    async fn admit(&self) -> Result<()> {
        if let Some(coordinator) = &self.coordinator
            && super::purge::pending(coordinator, self.records.scope()).await?
        {
            return Err(StorageError::scope_generation_rejected(
                "scope purge is pending",
            ));
        }
        if self.validate_generation {
            self.records.validate_purge_generation().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for GenerationBlobs {
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn put(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put(key, bytes, ct).await
    }
    async fn put_stream(
        &self,
        key: &str,
        reader: crate::BlobReader,
        len: u64,
        ct: Option<&str>,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put_stream(key, reader, len, ct).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put_expiring(key, bytes, ct, ttl).await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: crate::BlobReader,
        len: u64,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner
            .put_stream_expiring(key, reader, len, ct, ttl)
            .await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.admit().await?;
        self.inner.get(key).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        self.admit().await?;
        self.inner.get_stream(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.admit().await?;
        self.inner.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.admit().await?;
        self.inner.head(key).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        self.admit().await?;
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.admit().await?;
        self.inner.list(prefix).await
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.admit().await?;
        self.inner.presign_get(key, ttl).await
    }
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.admit().await?;
        self.inner.presign_put(key, ttl).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        self.admit().await?;
        self.inner.sweep_expired().await
    }
    async fn list_dir(&self, dir: &str) -> Result<Vec<crate::TreeEntry>> {
        self.admit().await?;
        self.inner.list_dir(dir).await
    }
    async fn put_tree(&self, dir: &str, files: Vec<crate::TreeFile>) -> Result<Vec<BlobMeta>> {
        self.admit().await?;
        self.inner.put_tree(dir, files).await
    }
    async fn delete_tree(&self, dir: &str) -> Result<u64> {
        self.admit().await?;
        self.inner.delete_tree(dir).await
    }
}

#[derive(Debug)]
pub(super) struct FencedBlobs(pub(super) Arc<dyn BlobStore>);

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
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.0.presign_get(key, ttl).await
    }
}
