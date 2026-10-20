//! [`BlobStore`] for the in-memory driver.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::MemoryFence;
use super::state::DbState;
use super::{Clock, MemoryDb};
use crate::Capabilities;
use crate::Capability;
use crate::blob::{
    Blob, BlobMeta, BlobStore, clamp_range, validate_user_blob_key, validate_user_blob_prefix,
};
use crate::error::Result;
use crate::scope::Scope;

/// In-memory blobs bound to one scope.
#[derive(Clone)]
pub struct MemoryBlobs {
    db: Arc<MemoryDb>,
    scope: Scope,
    generation: crate::ScopeGeneration,
    clock: Clock,
    /// Checked under the lock before every write, when set.
    fence: Option<Arc<MemoryFence>>,
}

impl std::fmt::Debug for MemoryBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBlobs")
            .field("scope", &self.scope)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl MemoryBlobs {
    pub(super) fn new(
        db: Arc<MemoryDb>,
        scope: Scope,
        clock: Clock,
        fence: Option<Arc<MemoryFence>>,
    ) -> Self {
        Self {
            db,
            scope,
            generation: crate::ScopeGeneration::default(),
            clock,
            fence,
        }
    }

    pub(super) fn with_generation(mut self, generation: crate::ScopeGeneration) -> Self {
        self.generation = generation;
        self
    }

    /// Refuse a write unless this handle's fence (if any) holds. An
    /// unfenced handle never reads the clock.
    fn guard(&self, state: &DbState) -> Result<()> {
        state.check_generation(self.scope.as_str(), self.generation)?;
        match self.fence.as_deref() {
            None => Ok(()),
            fence => MemoryDb::guard(state, fence, (self.clock)()),
        }
    }

    fn key(&self, key: &str) -> (String, String) {
        (self.scope.as_str().to_owned(), key.to_owned())
    }

    /// Store a blob that expires `ttl` after the clock's current reading.
    fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
    ) -> Result<BlobMeta> {
        self.store_with_expiry(key, bytes, content_type, move |this| {
            ttl.map(|ttl| {
                (this.clock)().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
            })
        })
    }

    fn store_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.store_with_expiry(key, bytes, content_type, |_| expires_at_ms)
    }

    fn store_with_expiry<F>(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expiry: F,
    ) -> Result<BlobMeta>
    where
        F: FnOnce(&Self) -> Option<u64>,
    {
        validate_user_blob_key(key)?;
        let meta = BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        };
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        // A plain write never reads the clock; only a lifetime needs it.
        let expires_at = expiry(self);
        if let Some(at) = expires_at {
            // Expired blobs are invisible; drop them where a lifetime write
            // already reads the clock so they do not accumulate.
            let now = (self.clock)();
            state
                .blobs
                .retain(|_, (_, _, expiry)| expiry.is_none_or(|expires| expires > now));
            if at <= now {
                // A zero lifetime is expired on arrival: replace, then vanish.
                state.blobs.remove(&self.key(key));
                return Ok(meta);
            }
        }
        state
            .blobs
            .insert(self.key(key), (meta.clone(), bytes, expires_at));
        Ok(meta)
    }

    /// The live entry for `key`, judged against the clock.
    fn live<'a>(
        &self,
        state: &'a DbState,
        key: &str,
    ) -> Option<&'a (BlobMeta, Vec<u8>, Option<u64>)> {
        let now = (self.clock)();
        state
            .blobs
            .get(&self.key(key))
            .filter(|(_, _, expiry)| expiry.is_none_or(|at| at > now))
    }
}

#[async_trait]
impl BlobStore for MemoryBlobs {
    async fn sweep_expired(&self) -> Result<u64> {
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        let now = (self.clock)();
        let scope = self.scope.as_str();
        let before = state.blobs.len();
        state.blobs.retain(|(owner, _), (_, _, expiry)| {
            owner != scope || expiry.is_none_or(|expires| expires > now)
        });
        Ok((before - state.blobs.len()) as u64)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::none().with(Capability::BlobTtl)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None)
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl))
    }

    async fn put_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.store_until(key, bytes, content_type, expires_at_ms)
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_user_blob_key(key)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(self.live(&state, key).map(|(meta, bytes, _)| Blob {
            meta: meta.clone(),
            bytes: bytes.clone(),
        }))
    }

    async fn get_with_expiry(&self, key: &str) -> Result<Option<crate::BlobSource>> {
        validate_user_blob_key(key)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(self
            .live(&state, key)
            .map(|(meta, bytes, expires_at_ms)| crate::BlobSource {
                blob: Blob {
                    meta: meta.clone(),
                    bytes: bytes.clone(),
                },
                expires_at_ms: *expires_at_ms,
            }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_user_blob_key(key)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let Some((_, bytes, _)) = self.live(&state, key) else {
            return Ok(None);
        };
        let range = clamp_range(&range, bytes.len())?;
        Ok(Some(
            bytes.get(range).map(<[u8]>::to_vec).unwrap_or_default(),
        ))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_user_blob_key(key)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(self.live(&state, key).map(|(meta, _, _)| meta.clone()))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_user_blob_key(key)?;
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        // Only a blob with a lifetime needs the clock to say it still exists.
        Ok(state
            .blobs
            .remove(&self.key(key))
            .is_some_and(|(_, _, expiry)| expiry.is_none_or(|at| at > (self.clock)())))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_user_blob_prefix(prefix)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let now = (self.clock)();
        Ok(state
            .blobs
            .iter()
            .filter(|((scope, key), (_, _, expiry))| {
                scope == self.scope.as_str()
                    && key.starts_with(prefix)
                    && !crate::is_reserved_blob_key(key)
                    && expiry.is_none_or(|at| at > now)
            })
            .map(|(_, (meta, _, _))| meta.clone())
            .collect())
    }
}
