//! Storage ports, the in-memory driver, and the driver conformance suite.
//!
//! Every crate that persists data talks to these ports instead of a database
//! library, and a host picks the backend (SQLite on a desktop, `MongoDB` in the
//! cloud, memory in tests) once at boot. The ports are deliberately primitive:
//!
//! - [`DocumentStore`]: versioned JSON objects in named collections, with
//!   filters, sorting, paging, compare-and-swap, atomic claims, and optional
//!   expiry, transactions and full-text search.
//! - [`StreamStore`]: append-only logs with dense offsets.
//! - [`BlobStore`]: opaque bytes by key.
//!
//! Typed repositories (sessions, approvals, workflow runs) belong to the crates
//! that own those records and are built on these ports; this crate knows
//! nothing about them.
//!
//! A [`StorageBackend`] hands out a [`ScopedStorage`] per [`Scope`], the
//! tenant key every record carries. [`StorageBackend::for_scope_fenced`] hands
//! out handles whose writes the driver checks atomically against a [`Fence`]
//! (a lease epoch), so a node that lost its lease cannot land a late write.
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_core::{
//!     Filter, MemoryStorage, Precondition, Query, Scope, StorageBackend,
//! };
//!
//! let storage = MemoryStorage::new();
//! let alice = storage.for_scope(&Scope::new("alice")?)?;
//! let docs = alice.documents();
//!
//! let v1 = docs.put("tasks", "t1", json!({"state": "open"}), Precondition::Absent).await?;
//! docs.put("tasks", "t1", json!({"state": "done"}), Precondition::Version(v1)).await?;
//!
//! let open = docs.query("tasks", &Query::filter(Filter::eq("state", "open"))).await?;
//! assert!(open.items.is_empty());
//!
//! let bob = storage.for_scope(&Scope::new("bob")?)?;
//! assert!(bob.documents().get("tasks", "t1").await?.is_none());
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Features
//!
//! - `migration-delivery`: confidential fragmented external transforms, without a bus dependency.
//! - `blocking`: [`Blocking`], a bridge for synchronous callers.
//! - `testkit`: the [`conformance`] suite drivers run in their tests.
//! - `polling-watch`: explicit conservative `PollingWatch` invalidations;
//!   never adds native [`Capability::Watch`] support.

mod backend;
mod blob;
mod capabilities;
mod document;
mod error;
mod fence;
mod filter;
mod key;
mod memory;
pub mod primitives;
pub mod schema;
mod scope;
mod stream;
pub mod value;
mod watch;

#[cfg(any(test, feature = "blocking"))]
mod blocking;
#[cfg(any(test, feature = "testkit"))]
pub mod conformance;

pub use backend::{
    BackendBlobs, BlobBackend, Checkpoint, CompositeStorage, MaintenanceBudget, MaintenanceReport,
    PurgeMode, PurgeReceipt, PurgeRequest, PurgeStage, PurgeStatus, ScopeGeneration, ScopePurge,
    ScopedStorage, StorageBackend,
};
pub use blob::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, BlobSweepEntry, BlobSweepPage,
    MAX_BLOB_KEY_LEN, MAX_BLOB_SWEEP_PAGE_ITEMS, TreeEntry, TreeFile, clamp_range,
    sweep_unreferenced, validate_blob_key, validate_sweep_page_request, validate_user_blob_key,
    validate_user_blob_prefix,
};
#[cfg(any(test, feature = "blocking"))]
pub use blocking::Blocking;
pub use capabilities::{Capabilities, Capability};
pub use document::{
    CollectionSpec, Cursor, CursorPosition, DocumentStore, DocumentStoreExt, ExpectedDocument,
    IndexSpec, MAX_COLLECTION_LEN, MAX_ID_LEN, Meta, Page, Precondition, Query, RESERVED_PREFIX,
    ReadCheck, SearchHit, SearchSpec, Version, Versioned, WriteOp, WriteOptions, WriteResult,
    validate_collection, validate_doc, validate_id,
};
pub use error::{ErrorKind, Result, StorageError};
pub use fence::Fence;
pub use filter::{
    CREATED_FIELD, Direction, EXPIRES_FIELD, Filter, ID_FIELD, SCHEMA_FIELD, Sort, UPDATED_FIELD,
    sort_documents, sort_documents_with_meta,
};
pub use key::Key;
pub use memory::{Clock, MemoryBlobs, MemoryDocuments, MemoryStorage, MemoryStreams};
pub use primitives::{
    Acquire, Acquisition, Counters, Kv, Lease, LeaseRecord, Leases, Repo, RetryPolicy,
};
pub use schema::{
    Expiry, Item, MIGRATIONS_COLLECTION, MigrateOptions, MigrationReport, Migrations, Owner,
    Progress, Registry, Schema, ScopeClass, SourceRecord, Step, StepFn, StepReport, TargetRecord,
};
pub use scope::{
    AGENT_INFIX, CLUSTER, MAX_SCOPE_LEN, MAX_SEGMENT_LEN, MODULE_INFIX, ParsedScope, Principal,
    Scope, Tenant, USER_PREFIX,
};
pub use stream::{
    SEALED_BLOB_PREFIX, SealOutcome, SealPolicy, SealWhen, SealedLog, StreamEntry, StreamInfo,
    StreamStore, is_reserved_blob_key, sealed_blob_key, validate_stream,
};

/// The `async_trait` attribute the port traits are declared with, re-exported
/// so a driver implements them without naming the dependency itself.
pub use async_trait::async_trait;

#[cfg(feature = "polling-watch")]
pub use watch::PollingWatch;
pub use watch::{
    DocumentWatch, MAX_WATCH_CURSOR_BYTES, WatchCursor, WatchNotice, WatchOptions, WatchReset,
};

/// Pure schema declarations and transformation wire contracts.
pub use tinystoragedrivers_schema as declarations;
pub use tinystoragedrivers_schema::{
    BlobTargetRequest, BlobTransformOutput, BlobTransformRequest, DocumentsTransformRequest,
    ExchangeErrorKind, MIGRATION_CHUNK_BYTES, MIGRATION_EXCHANGE_VERSION, MigrationExchangeReply,
    MigrationExchangeRequest, MigrationPayload, MigrationPayloadReply, SchemaDeclaration,
    StepDeclaration, StreamEntriesRequest, StreamMapMode, StreamPlan, StreamPlanRequest,
    StreamTransformOutput, StreamTransformRequest, TransformDescriptor, TransformError,
    TransformErrorKind, TransformKind, TransformResult,
};

#[cfg(feature = "migration-delivery")]
pub use schema::delivery::{
    CleanupGuard, DEFAULT_MAX_IN_FLIGHT, ExchangeClock, IDLE_TTL_MS, MigrationDeliveryClient,
    MigrationExchangeServer, MigrationPayloadHandler, MigrationTransport, PeerIdentity,
    TransferIds, TransformFuture,
};
