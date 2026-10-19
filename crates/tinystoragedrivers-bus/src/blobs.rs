//! Wire shapes for the Blobs object.
//!
//! Small payloads travel inline (at most `MAX_INLINE_BLOB_BYTES`); larger ones
//! use `PutStream` / `GetStream`, whose bytes ride a `TinyBus` stream passed as
//! a separate call argument bound to the caller grant and one operation. The
//! messages here carry only metadata for those calls.
//!
//! Over a real bus the two streamed calls are:
//!
//! * `PutStream`: positional arguments `[PutBlobStreamRequest, StreamRef]`.
//!   The caller opens the stream on the host, writes exactly `len` bytes while
//!   the call is outstanding, and the reply is a [`BlobInfo`].
//! * `GetStream`: positional arguments `[BlobKeyRequest]`. The reply is
//!   `null` for an absent blob, otherwise `[BlobStreamHead, StreamRef]`: the
//!   host opens the stream on the *caller* and writes the body, which the caller
//!   accepts by the reference.
use serde::{Deserialize, Serialize};

/// Blobs object name under [`crate::STORAGE_INTERFACE`].
pub const BLOBS_OBJECT: &str = "Blobs";
/// Finite selector set; servers must reject every other selector.
pub const BLOBS_SELECTORS: [&str; 12] = [
    "Put",
    "PutStream",
    "Get",
    "GetStream",
    "GetRange",
    "Head",
    "Delete",
    "List",
    "ListSweepPage",
    "PresignGet",
    "PresignPut",
    "Capabilities",
];

/// Return whether `selector` belongs to the fixed Blobs selector set.
#[must_use]
pub fn is_blobs_selector(selector: &str) -> bool {
    BLOBS_SELECTORS.contains(&selector)
}

/// Blob metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobInfo {
    /// Key inside the captured grant.
    pub key: String,
    /// Size in bytes.
    pub len: u64,
    /// MIME type, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}
/// Inline write.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutBlobRequest {
    /// Blob key.
    pub key: String,
    /// Bytes, at most `MAX_INLINE_BLOB_BYTES`.
    pub bytes: Vec<u8>,
    /// MIME type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Lifetime in milliseconds from the write; absent for a permanent blob.
    /// A server whose blob handle lacks `ttl` in [`BlobCapabilities`] refuses
    /// the write as `Unsupported`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}
/// Streamed write metadata; the body is a `TinyBus` stream argument of exactly
/// `len` bytes. A mismatch fails before the object is published.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutBlobStreamRequest {
    /// Blob key.
    pub key: String,
    /// Declared body length.
    pub len: u64,
    /// MIME type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Lifetime in milliseconds from the write; see [`PutBlobRequest::ttl_ms`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}
/// Request addressing one blob (`Get`, `GetStream`, `Head`, `Delete`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobKeyRequest {
    /// Blob key.
    pub key: String,
}
/// Inline read result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobBody {
    /// Metadata.
    pub meta: BlobInfo,
    /// Bytes; a blob over `MAX_INLINE_BLOB_BYTES` must use `GetStream`.
    pub bytes: Vec<u8>,
}
/// Streamed read reply metadata; the body follows as a `TinyBus` stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobStreamHead {
    /// Metadata.
    pub meta: BlobInfo,
}
/// Ranged inline read of `[start, end)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetRangeRequest {
    /// Blob key.
    pub key: String,
    /// Inclusive start offset.
    pub start: u64,
    /// Exclusive end offset; the span is at most `MAX_INLINE_BLOB_BYTES`.
    pub end: u64,
}
/// List blobs by key prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListBlobsRequest {
    /// Literal key prefix.
    #[serde(default)]
    pub prefix: String,
}

/// One keyset-paged listing request for cleanup metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListBlobSweepPageRequest {
    /// Literal key prefix.
    #[serde(default)]
    pub prefix: String,
    /// The last key from the preceding page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Maximum number of entries to return.
    pub limit: usize,
}

/// Physical blob metadata used by a bounded orphan sweep.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobSweepInfo {
    /// Ordinary blob metadata.
    pub info: BlobInfo,
    /// Time the current bytes were written, in epoch milliseconds. Missing
    /// timestamps must be retained by cleanup callers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub written_at_ms: Option<u64>,
}

/// A bounded page of physical blobs, including objects hidden by expiry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobSweepPage {
    /// Entries in increasing key order.
    pub items: Vec<BlobSweepInfo>,
    /// Last key when another page may exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}
/// Request a bearer URL; only when the backend reports `PresignedUrls`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PresignRequest {
    /// Blob key.
    pub key: String,
    /// Lifetime in seconds, capped at `MAX_PRESIGN_TTL_SECS`.
    pub ttl_secs: u64,
}
/// A presigned URL. It is a credential and must never be logged.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PresignedUrl {
    /// Bearer URL.
    pub url: String,
}
impl std::fmt::Debug for PresignedUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresignedUrl").finish_non_exhaustive()
    }
}
/// Capabilities of the granted blob handle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobCapabilities {
    /// Whether `PresignGet` / `PresignPut` are available.
    pub presigned_urls: bool,
    /// Whether writes may carry `ttl_ms` (native blob expiry).
    #[serde(default)]
    pub ttl: bool,
    /// Whether bounded physical cleanup pages have trustworthy write times.
    #[serde(default)]
    pub blob_sweep: bool,
    /// Maximum logical key length in UTF-8 bytes.
    pub max_key_len: usize,
}

#[cfg(test)]
#[path = "blobs_tests.rs"]
mod tests;
