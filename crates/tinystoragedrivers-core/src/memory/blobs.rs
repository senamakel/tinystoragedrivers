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
    Blob, BlobMeta, BlobStore, BlobSweepEntry, BlobSweepPage, clamp_range,
    validate_sweep_page_request, validate_user_blob_key, validate_user_blob_prefix,
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
        validate_user_blob_key(key)?;
        let meta = BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        };
        let mut state = self.db.lock()?;
        self.guard(&state)?;
        let written_at_ms = (self.clock)();
        let expires_at = ttl.map(|ttl| {
            written_at_ms.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
        });
        if let Some(at) = expires_at {
            // Expired blobs are invisible; drop them where a lifetime write
            // already reads the clock so they do not accumulate.
            let now = (self.clock)();
            state
                .blobs
                .retain(|_, (_, _, expiry)| expiry.is_none_or(|expires| expires > now));
            let DbState {
                blobs,
                blob_written_ms,
                ..
            } = &mut *state;
            blob_written_ms.retain(|key, _| blobs.contains_key(key));
            if at <= now {
                // A zero lifetime is expired on arrival: replace, then vanish.
                state.blobs.remove(&self.key(key));
                state.blob_written_ms.remove(&self.key(key));
                return Ok(meta);
            }
        }
        state
            .blobs
            .insert(self.key(key), (meta.clone(), bytes, expires_at));
        state.blob_written_ms.insert(self.key(key), written_at_ms);
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
        let DbState {
            blobs,
            blob_written_ms,
            ..
        } = &mut *state;
        blob_written_ms.retain(|key, _| blobs.contains_key(key));
        Ok((before - state.blobs.len()) as u64)
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::BlobTtl)
            .with(Capability::BlobSweep)
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

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_user_blob_key(key)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(self.live(&state, key).map(|(meta, bytes, _)| Blob {
            meta: meta.clone(),
            bytes: bytes.clone(),
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
        let scoped_key = self.key(key);
        let removed = state
            .blobs
            .remove(&scoped_key)
            .is_some_and(|(_, _, expiry)| expiry.is_none_or(|at| at > (self.clock)()));
        state.blob_written_ms.remove(&scoped_key);
        Ok(removed)
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

    async fn list_sweep_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobSweepPage> {
        validate_sweep_page_request(prefix, after, limit)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let mut items = Vec::with_capacity(limit);
        let mut next_after = None;
        for ((scope, key), (meta, _, _)) in &state.blobs {
            if scope != self.scope.as_str()
                || !key.starts_with(prefix)
                || crate::is_reserved_blob_key(key)
                || after.is_some_and(|after| key.as_str() <= after)
            {
                continue;
            }
            if items.len() == limit {
                next_after = items
                    .last()
                    .map(|entry: &BlobSweepEntry| entry.meta.key.clone());
                break;
            }
            items.push(BlobSweepEntry {
                meta: meta.clone(),
                written_at_ms: state
                    .blob_written_ms
                    .get(&(scope.clone(), key.clone()))
                    .copied(),
            });
        }
        Ok(BlobSweepPage { items, next_after })
    }
}
