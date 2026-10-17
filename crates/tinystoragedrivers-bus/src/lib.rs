//! Confidential document data and CAS requests for compiled modules.
//!
//! Scope and collection authority remain captured by the host, outside the wire.
//! The data types are available without `TinyBus` by disabling default features;
//! the default-on `client` feature retains the legacy `DocumentClient` API.
//!
//! ```
//! use tinystoragedrivers_bus::{Precondition, PutRequest};
//! let request = PutRequest {
//!     id: "index".into(),
//!     doc: serde_json::json!({"nested": {"id": "business-data"}}),
//!     precondition: Precondition::Absent,
//! };
//! let wire = serde_json::to_value(request)?;
//! assert_eq!(wire["document_id"], "index");
//! assert!(wire["document_json"].is_string());
//! # Ok::<(), serde_json::Error>(())
//! ```
mod blobs;
#[cfg(feature = "client")]
mod client;
mod documents;
mod inline_json;
mod limits;
mod secrets;
mod streams;
mod types;
pub use blobs::{
    BLOBS_OBJECT, BLOBS_SELECTORS, BlobBody, BlobCapabilities, BlobInfo, BlobKeyRequest,
    BlobStreamHead, GetRangeRequest, ListBlobsRequest, PresignRequest, PresignedUrl,
    PutBlobRequest, PutBlobStreamRequest, is_blobs_selector,
};
#[cfg(feature = "client")]
pub use client::DocumentClient;
pub use documents::{
    BatchRequest, BatchResponse, CapabilitiesReply, ClaimRequest, CollectionRequest,
    CollectionSpec, CollectionsRequest, CountRequest, DOCUMENTS_OBJECT, DOCUMENTS_SELECTORS,
    DeleteDocumentRequest, DeleteWhereRequest, Direction, EnsureCollectionRequest,
    ExpectedDocument, Filter, GetDocumentRequest, IndexSpec, PutDocumentRequest, Query,
    QueryRequest, QueryResponse, ReadCheck, STORAGE_INTERFACE, SearchHit, SearchRequest,
    SearchSpec, Sort, StorageNeeds, StorageNeedsWithExactSecrets, ValidateCollectionSpecRequest,
    WatchChanged, WatchCloseRequest, WatchNotice, WatchOpenRequest, WatchOpenResponse, WatchReset,
    WriteOp, WriteResult, is_documents_selector,
};
pub use limits::{
    BLOB_STREAM_CHUNK_BYTES, CONTRACT_VERSION, MAX_BATCH_OPS, MAX_FRAME_BYTES,
    MAX_INLINE_BLOB_BYTES, MAX_INLINE_BYTES, MAX_PAGE_ITEMS, MAX_PRESIGN_TTL_SECS,
};
pub use secrets::{
    ListSecretsRequest, SECRETS_OBJECT, SECRETS_SELECTORS, SecretNameRequest, SecretValue,
    SetSecretRequest, is_secrets_selector,
};
pub use streams::{
    AppendBatchIfOffsetRequest, AppendBatchRequest, AppendRequest, AppendResponse,
    ListStreamsRequest, ReadWindowRequest, ReadWindowResponse, STREAMS_OBJECT, STREAMS_SELECTORS,
    SealResponse, StreamInfoResponse, StreamItem, StreamRecord, StreamRequest,
    TruncateBeforeRequest, is_streams_selector,
};
pub use types::{
    DeleteRequest, GetRequest, Meta, Precondition, PutRequest, StorageFailure, StorageKind,
    Version, Versioned,
};
/// Fixed callback interface.
pub const INTERFACE: &str = "ai.tinyhumans.storage.DocumentCollection1";
/// Finite callback member allowlist.
pub const MEMBERS: [&str; 3] = ["Get", "Put", "Delete"];
/// Native outcome, distinct from transport refusal.
pub type Outcome<T> = Result<T, StorageFailure>;
