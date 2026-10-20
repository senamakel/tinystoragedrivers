//! Contract version and size limits shared by every Storage object.
//!
//! Servers enforce these before native I/O; clients may use them to split work.

/// Version of the whole `ai.tinyhumans.tinystorage.Storage` contract.
///
/// Changing any existing wire shape or selector requires a new interface
/// version; adding a selector or message bumps this constant.
///
/// Version 4 adds record metadata (`Meta` on every returned `Versioned`,
/// optional `schema` and `expires_ms` on `Put`) and the `Streams.Seal`
/// selector. Both are additive; see `docs/specs/storage-bus.md`.
///
/// Version 5 adds the `Queues` object (`QUEUES_SELECTORS`) and
/// [`MAX_QUEUE_MESSAGE_BYTES`]. Version 7 adds explicit atomic batch selectors
/// that carry uniform record metadata without changing the old batch shape.
pub const CONTRACT_VERSION: u32 = 7;
/// `TinyBus` frame cap. No request or reply may exceed it.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Encoded-size cap for one inline page, batch or record set (half the frame,
/// leaving room for envelope and JSON-in-string escaping overhead).
pub const MAX_INLINE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum records in one query page, search result or stream window.
pub const MAX_PAGE_ITEMS: usize = 1_000;
/// Maximum operations in one atomic (checked) batch or stream append batch.
pub const MAX_BATCH_OPS: usize = 1_000;
/// Largest blob moved inline by `Put`, `Get` or `GetRange`; larger payloads use
/// `PutStream` / `GetStream` over a `TinyBus` stream.
pub const MAX_INLINE_BLOB_BYTES: usize = 1024 * 1024;
/// Chunk size of blob transfers over a `TinyBus` stream.
pub const BLOB_STREAM_CHUNK_BYTES: usize = 512 * 1024;
/// Largest queue message payload, as JSON text. A `Receive` reply is filled
/// only while one more message of this size still fits the inline byte cap.
pub const MAX_QUEUE_MESSAGE_BYTES: usize = 256 * 1024;
/// Longest presigned URL lifetime a server may grant, in seconds.
pub const MAX_PRESIGN_TTL_SECS: u64 = 3_600;

#[cfg(test)]
#[path = "limits_tests.rs"]
mod tests;
