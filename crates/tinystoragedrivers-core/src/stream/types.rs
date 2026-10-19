//! Stream sealing types: the outcome of a seal and the policy that states
//! when an owner seals.

use serde::{Deserialize, Serialize};

/// What [`StreamStore::seal`](crate::StreamStore::seal) leaves behind.
///
/// Sealing twice returns equal outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SealOutcome {
    /// The first retained offset: everything below it was truncated away
    /// before the seal.
    pub base: u64,
    /// The stream's length, the offset an append would have received. A
    /// stream truncated to nothing seals with `len == base`.
    pub len: u64,
    /// Size of the compressed blob holding the entries, in bytes.
    pub blob_bytes: u64,
}

impl SealOutcome {
    /// An outcome for a stream retaining offsets `base..len`, stored in a
    /// blob of `blob_bytes` bytes.
    #[must_use]
    pub const fn new(base: u64, len: u64, blob_bytes: u64) -> Self {
        Self {
            base,
            len,
            blob_bytes,
        }
    }

    /// How many entries the sealed stream holds.
    #[must_use]
    pub const fn retained(&self) -> u64 {
        self.len.saturating_sub(self.base)
    }
}

/// What [`StreamStore::info`](crate::StreamStore::info) reports about one
/// stream: its extent and the two instants the seal policy decides on.
///
/// Times are epoch milliseconds read from the driver's clock. A stream written
/// before its driver recorded times reports `None` for them, and a policy
/// treats `None` as "unknown, leave it alone".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StreamInfo {
    /// The first retained offset.
    pub base: u64,
    /// The stream's length, the offset the next append would receive.
    pub len: u64,
    /// When the stream was sealed; `None` while it accepts appends.
    pub sealed_ms: Option<i64>,
    /// When a record was last appended; `None` if unknown. Truncation does
    /// not change it.
    pub last_append_ms: Option<i64>,
}

impl StreamInfo {
    /// Info for a stream retaining `base..len`, with the given times.
    #[must_use]
    pub const fn new(
        base: u64,
        len: u64,
        sealed_ms: Option<i64>,
        last_append_ms: Option<i64>,
    ) -> Self {
        Self {
            base,
            len,
            sealed_ms,
            last_append_ms,
        }
    }

    /// Whether the stream is closed to appends.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.sealed_ms.is_some()
    }
}

pub use tinystoragedrivers_schema::{SealPolicy, SealWhen};

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
