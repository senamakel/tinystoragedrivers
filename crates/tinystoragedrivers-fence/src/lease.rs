//! Shared lease result types and store contract.

use async_trait::async_trait;
use tinystoragedrivers_core::{Fence, LeaseRecord as StoredLeaseRecord, StorageError, Version};

/// The canonical persisted lease record type from `tinystoragedrivers-core`.
pub type LeaseRecord = StoredLeaseRecord;

/// Why a lease operation failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// Another holder owns the lease.
    #[error("lease held by {} (epoch {})", .0.holder, .0.epoch)]
    Held(StoredLeaseRecord),
    /// The grant is no longer the latest live grant from this store.
    #[error("lease lost")]
    Lost,
    /// The backend or local store failed.
    #[error("lease storage: {0}")]
    Storage(#[from] StorageError),
}

/// The latest grant issued by a [`LeaseStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseGrant {
    /// The key this grant owns.
    pub key: String,
    /// The holder name recorded by the store.
    pub holder: String,
    /// The fencing epoch of this tenure.
    pub epoch: u64,
    /// The latest operation generation for this tenure.
    pub version: Version,
    /// Unix epoch milliseconds when the lease expires.
    pub expires_ms: i64,
    /// Whether acquisition replaced an abandoned live tenure.
    pub previous_unclean: bool,
}

/// Exclusive ownership of named keys with optional storage-driver fences.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Acquire `key` for the configured holder at `now_ms`.
    async fn acquire(&self, key: &str, now_ms: u64) -> Result<LeaseGrant, LeaseError>;

    /// Renew the latest grant at `now_ms`.
    async fn renew(&self, grant: &LeaseGrant, now_ms: u64) -> Result<LeaseGrant, LeaseError>;

    /// Release the latest grant.
    async fn release(&self, grant: LeaseGrant) -> Result<(), LeaseError>;

    /// Inspect the stored record, including expired and released records.
    async fn inspect(&self, key: &str) -> Result<Option<StoredLeaseRecord>, LeaseError>;

    /// Observe the current holder when the backend can distinguish a live
    /// owner from a durable record left behind by a dead process.
    ///
    /// Stores without a separate liveness signal return their stored record.
    /// This does not mutate or erase the durable record.
    async fn holder(&self, key: &str) -> Result<Option<StoredLeaseRecord>, LeaseError> {
        self.inspect(key).await
    }

    /// Return the driver's atomic write fence when this store keeps lease
    /// records in the same storage backend as protected writes.
    fn driver_fence(&self, _key: &str, _holder: &str, _epoch: u64) -> Option<Fence> {
        None
    }
}

/// Whether a stored lease record excludes another holder at `now_ms`.
#[must_use]
pub fn is_live(record: &StoredLeaseRecord, now_ms: u64) -> bool {
    let Ok(now_ms) = i64::try_from(now_ms) else {
        return false;
    };
    !record.released && now_ms < record.expires_ms
}

/// Milliseconds until the stored lease record lapses.
#[must_use]
pub fn retry_after_ms(record: &StoredLeaseRecord, now_ms: u64) -> u64 {
    if record.released {
        return 0;
    }
    let Ok(now_ms) = i64::try_from(now_ms) else {
        return 0;
    };
    u64::try_from(record.expires_ms.saturating_sub(now_ms)).unwrap_or_default()
}
