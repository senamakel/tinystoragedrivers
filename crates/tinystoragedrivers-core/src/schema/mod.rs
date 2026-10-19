//! The schema registry: owners declare what they store, the host composes the
//! declarations, prepares the backend and migrates older data forward.
//!
//! Every crate that owns a record type exports `storage_schema() -> Schema`:
//! its [`Item`]s (collections, streams, blob and secret prefixes, each with a
//! [`ScopeClass`]) and the [`Step`]s that rewrite older versions of a
//! collection. The host builds one [`Registry`] from all of them:
//!
//! 1. [`Registry::compose`] refuses bad names, names outside an owner's prefix,
//!    duplicate claims and owners, step gaps and descending versions. It is
//!    pure: nothing touches a backend.
//! 2. [`Registry::prepare`] declares every collection (trusted bootstrap).
//! 3. [`Registry::migrate`] runs each pending step over a [`ScopedStorage`](crate::ScopedStorage),
//!    recording progress in the `tsd.migrations` collection so a crash resumes
//!    at the last finished batch.
//! 4. Typed repositories call [`Registry::check_ready`] before they write;
//!    a collection whose steps have not finished answers
//!    [`ErrorKind::Unavailable`](crate::ErrorKind::Unavailable) with
//!    `migrating <owner>.<item>`. Drivers do not enforce it.
//!
//! # Example
//!
//! ```
//! use serde_json::json;
//! use tinystoragedrivers_core::schema::{
//!     MigrateOptions, Owner, Registry, Schema, ScopeClass, Step, TargetRecord,
//! };
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! let schema = Schema::new(Owner::new("shop")?)
//!     .collection("shop.orders", 2, ScopeClass::Tenant)
//!     .step(Step::new("rename", "shop.orders", 1, 2, |batch| {
//!         Ok(batch
//!             .into_iter()
//!             .map(|r| TargetRecord::new("shop.orders", r.id, json!({"total_ms": r.body["t"]})))
//!             .collect())
//!     }));
//! let registry = Registry::compose([schema])?;
//! let backend = MemoryStorage::new();
//! registry.prepare(&backend).await?;
//! let storage = backend.for_scope(&Scope::local())?;
//! assert!(registry.check_ready("shop.orders").is_err());
//! registry.migrate(&storage, &MigrateOptions::new()).await?;
//! registry.check_ready("shop.orders")?;
//! # Ok::<_, tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! A step targets one kind of item: [`Step::new`] rewrites collection
//! documents, [`Step::stream`] moves legacy streams (read whole, mapped,
//! rewritten, sealed again when the source was sealed, then deleted) and
//! [`Step::blobs`] moves legacy blobs. Stream and blob steps name their legacy
//! source with [`Step::source_prefix`]; see [`Registry::migrate`] for how they
//! resume.
//!
//! Out of scope here: legacy-file importers (steps with no source) are run by
//! the host, which records completion with [`Migrations::mark_done`].
//! A stream's [`SealPolicy`](crate::SealPolicy) is applied by [`Registry::maintain`].

#[cfg(feature = "migration-delivery")]
pub mod delivery;

mod compose;
mod executor;
mod metadata;
mod migrations;
mod policy;
mod prepared;
mod runner;
mod runner_blobs;
mod runner_streams;
mod types;

pub use compose::{ComposedRegistry, Registry};
pub use executor::{BoxFuture, TransformExecutor};
pub use metadata::{SchemaMetadata, StepExecution, StepMetadata};
pub use migrations::{MIGRATIONS_COLLECTION, Migrations, Progress};
pub use policy::PolicyReport;
pub use runner::{MigrateOptions, MigrationReport, StepReport};
pub use types::{
    BlobOutput, BlobStepFn, Expiry, Item, Owner, Schema, ScopeClass, SourceRecord, Step, StepBody,
    StepFn, StreamStepFn, TargetRecord,
};

pub use tinystoragedrivers_schema::{
    BlobTargetRequest, BlobTransformOutput, BlobTransformRequest, DocumentsTransformRequest,
    ExchangeErrorKind, MIGRATION_CHUNK_BYTES, MIGRATION_EXCHANGE_VERSION, MigrationExchangeReply,
    MigrationExchangeRequest, MigrationPayload, MigrationPayloadReply, SchemaDeclaration,
    StepDeclaration, StreamEntriesRequest, StreamMapMode, StreamPlan, StreamPlanRequest,
    StreamTransformOutput, StreamTransformRequest, TransformDescriptor, TransformError,
    TransformErrorKind, TransformKind, TransformResult,
};

#[cfg(test)]
#[path = "executor_tests.rs"]
mod executor_tests;
