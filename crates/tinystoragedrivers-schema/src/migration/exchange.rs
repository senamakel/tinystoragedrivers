//! Version-one confidential migration exchange DTOs; no runtime or transport.
use crate::{DocumentsTransformRequest, TargetRecord, TransformResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current exchange protocol version; adapters must negotiate this before dispatch.
pub const MIGRATION_EXCHANGE_VERSION: u32 = 1;
/// Maximum raw chunk size in either direction.
pub const MIGRATION_CHUNK_BYTES: usize = 128 * 1024;

/// Metadata-only blob destination query; bytes remain in the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobTargetRequest {
    /// Owner operation.
    pub operation: String,
    /// Operation version.
    pub version: u32,
    /// Source key; content never crosses delivery.
    pub source: String,
}

/// Metadata-only query for the destination and entry mapping policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamPlanRequest {
    /// Owner operation.
    pub operation: String,
    /// Operation version.
    pub version: u32,
    /// Source stream name.
    pub source: String,
}

/// Owner-selected destination and stream mapping policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamPlan {
    /// Destination stream.
    pub target: String,
    /// Independent mapping policy.
    pub mode: StreamMapMode,
}

/// One ordered group for a transform explicitly declared entry-independent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamEntriesRequest {
    /// Owner operation.
    pub operation: String,
    /// Operation version.
    pub version: u32,
    /// One ordered independent group.
    pub entries: Vec<Value>,
}

/// Whether entries stay local or require independent owner conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamMapMode {
    /// Return original entries locally.
    Identity,
    /// Each entry is independent, allowing ordered groups.
    IndependentEntries,
}

/// Complete stateless owner input, serialized before fragmentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MigrationPayload {
    /// Documents
    Documents(DocumentsTransformRequest),
    /// A metadata-only blob destination.
    BlobTarget(BlobTargetRequest),
    /// A metadata-only stream policy.
    StreamPlan(StreamPlanRequest),
    /// An ordered independent group.
    StreamEntries(StreamEntriesRequest),
}

/// Complete stateless owner result, serialized before fragmentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MigrationPayloadReply {
    /// Documents
    Documents(TransformResult<Vec<TargetRecord>>),
    /// A metadata-only blob destination.
    BlobTarget(TransformResult<Option<String>>),
    /// A metadata-only stream policy.
    StreamPlan(TransformResult<Option<StreamPlan>>),
    /// An ordered independent group.
    StreamEntries(TransformResult<Vec<Value>>),
}

/// Version-one lifecycle and bounded raw fragment requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MigrationExchangeRequest {
    /// Reserve an empty assembly, without trusting the advertised length.
    Begin {
        /// Complete serialized request length.
        request_len: u64,
    },
    /// Append the next bounded raw fragment.
    Upload {
        /// Opaque server-issued token.
        transfer: String,
        /// Exact next byte offset.
        offset: u64,
        /// Raw JSON fragment.
        bytes: Vec<u8>,
    },
    /// Convert a completely uploaded request once.
    Execute {
        /// Opaque server-issued token.
        transfer: String,
    },
    /// Read the next bounded reply fragment.
    Download {
        /// Opaque server-issued token.
        transfer: String,
        /// Exact next byte offset.
        offset: u64,
    },
    /// Release a completely read reply.
    Finish {
        /// Opaque server-issued token.
        transfer: String,
    },
    /// Discard an exchange.
    Cancel {
        /// Opaque server-issued token.
        transfer: String,
    },
}

/// Version-one lifecycle acknowledgments and bounded reply fragments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MigrationExchangeReply {
    /// Reservation succeeded.
    Begun {
        /// Opaque server-issued token.
        transfer: String,
    },
    /// Fragment accepted.
    Uploaded {
        /// Next required upload offset.
        next_offset: u64,
    },
    /// Conversion completed.
    Ready {
        /// Complete serialized reply length.
        reply_len: u64,
    },
    /// Next reply fragment.
    Chunk {
        /// Fragment start.
        offset: u64,
        /// Bounded raw bytes.
        bytes: Vec<u8>,
    },
    /// Released after complete read.
    Finished,
    /// Discard requested.
    Cancelled,
    /// Content-free failure.
    Error {
        /// Static failure category.
        kind: ExchangeErrorKind,
    },
}

/// Static failures that never contain input, backend or credential details.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExchangeErrorKind {
    /// The fragment or request shape violates the contract.
    InvalidRequest,
    /// An allocation or runtime resource is unavailable.
    Unavailable,
    /// No live transfer belongs to this peer.
    UnknownTransfer,
    /// The phase or offset is not the exact expected next operation.
    WrongSequence,
    /// The upload or download has not completed.
    Incomplete,
    /// All admission slots are occupied.
    Busy,
    /// The idle exchange expired.
    Expired,
    /// The complete input is not a valid payload.
    InvalidPayload,
    /// The owner conversion failed or panicked.
    TransformFailed,
}

#[cfg(test)]
#[path = "exchange_tests.rs"]
mod tests;
