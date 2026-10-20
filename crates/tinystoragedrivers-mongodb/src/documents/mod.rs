//! [`DocumentStore`] on MongoDB.
//!
//! A port collection `jobs` is the Mongo collection `jobs`, holding
//! `{_id: {s, k}, _scope, _key, _v, d}` per document. The `_id` makes each
//! `(scope, key)` unique without any declared index, and every access goes
//! through [`ScopedCollection`], which ANDs `_scope` into the query.
//!
//! - [`stored`]: the stored shape and the precondition, version and expiry
//!   rules, all pure.
//! - [`write`]: put, delete and removal with tombstones.
//! - [`read`]: queries, counts and search, server-side where the translation
//!   is exact and in Rust where it is not.
//!
//! Claims pick the first match and commit it with a compare-and-swap on its
//! version, retrying when another claimer won. The merge patch is applied in
//! Rust with [`value::merge_patch`], so RFC 7396 edge cases (a patch object
//! replacing a scalar) behave exactly as on the memory driver.

mod read;
pub(crate) mod stored;
mod watch;
mod write;

use crate::fence::Fencing;

use std::sync::Arc;

use async_trait::async_trait;
use mongodb::ClientSession;
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, CursorPosition, DocumentStore, ErrorKind, Filter,
    Page, Precondition, Query, RESERVED_PREFIX, ReadCheck, Result, Scope, SearchHit, Sort,
    StorageError, Version, Versioned, WriteOp, WriteOptions, WriteResult, validate_collection,
    validate_doc, validate_id, value,
};

use crate::backend::{Shared, specs::scope_key_model};
use crate::errors;
use crate::fence::in_write;
use crate::naming::{decode_cursor, encode_cursor};
use crate::scoped::ScopedCollection;

/// How many times a claim retries after another claimer took its pick.
const CLAIM_ATTEMPTS: usize = 64;

/// Retry an uncertain commit without replaying the transaction.
const TRANSACTION_ATTEMPTS: usize = 8;

/// What a failed commit means for the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitFailure {
    /// The server aborted the transaction: running the batch again is safe.
    Aborted,
    /// The commit may have applied. Replaying could apply the batch twice,
    /// so the caller gets a non-retryable error and must read first.
    Unknown,
    /// Any other failure, which did not commit.
    Failed,
}

/// Classify a commit failure by its labels. An outcome the driver could not
/// learn (even after retrying the commit itself) wins over a transient label,
/// because only an abort guarantees nothing was applied.
pub(crate) fn commit_failure(error: &mongodb::error::Error) -> CommitFailure {
    if errors::is_unknown_commit(error) {
        CommitFailure::Unknown
    } else if errors::is_transient(error) {
        CommitFailure::Aborted
    } else {
        CommitFailure::Failed
    }
}

/// MongoDB documents bound to one scope.
pub(crate) struct MongoDocuments {
    shared: Arc<Shared>,
    scope: Scope,
    generation: tinystoragedrivers_core::ScopeGeneration,
    /// Every write runs in a transaction that holds this fence, when set.
    fence: Option<Arc<Fencing>>,
}

impl std::fmt::Debug for MongoDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDocuments")
            .field("scope", &self.scope)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl MongoDocuments {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope, fence: Option<Arc<Fencing>>) -> Self {
        Self {
            shared,
            scope,
            generation: tinystoragedrivers_core::ScopeGeneration::default(),
            fence,
        }
    }
    pub(crate) fn with_generation(
        mut self,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Self {
        self.generation = generation;
        self
    }

    /// Validate `collection` and do what cannot run inside a transaction
    /// (index builds, loading the declaration) before a fenced write.
    async fn ready(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        self.prepare(collection).await?;
        self.shared.spec(collection).await?;
        self.shared.prepare_validation(Some(&self.scope)).await?;
        Ok(())
    }

    /// Every logical document mutation and checked batch writes this driver
    /// metadata in its transaction. This prevents Mongo snapshot write skew
    /// without rewriting dependencies or changing their logical versions.
    pub(crate) async fn hold_validation(&self, session: &mut ClientSession) -> Result<()> {
        self.shared
            .hold_validation(Some(&self.scope), &mut *session)
            .await?;
        crate::purge::check(&self.shared, &self.scope, self.generation, Some(session)).await
    }

    /// One claim attempt: pick the first match and compare-and-swap it.
    async fn claim_once(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<Versioned<Value>>> {
        let (first, _) = self.page(collection, filter, sort, None, Some(1)).await?;
        let Some(first) = first.into_iter().next() else {
            return Ok(None);
        };
        let mut doc = first.doc;
        value::merge_patch(&mut doc, patch);
        // A claim patches the body only: the schema and expiry carry over.
        let options = WriteOptions {
            schema: first.meta.schema,
            expires_ms: first.meta.expires_ms,
            created_ms: None,
        };
        let (version, meta) = self
            .put_one(
                collection,
                &first.id,
                &doc,
                Precondition::Version(first.version),
                options,
                session,
            )
            .await?;
        Ok(Some(Versioned {
            id: first.id,
            version,
            doc,
            meta,
        }))
    }

    fn collection(&self, collection: &str) -> ScopedCollection {
        self.shared.scoped(collection, &self.scope)
    }

    async fn prepare(&self, collection: &str) -> Result<()> {
        self.shared
            .prepare(collection, vec![scope_key_model()])
            .await
    }

    async fn run_batch(
        &self,
        ops: &[WriteOp],
        options: WriteOptions,
        session: &mut ClientSession,
    ) -> Result<Vec<WriteResult>> {
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            let (WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. }) = op;
            self.shared
                .spec_in_session(collection, &mut *session)
                .await?;
            results.push(match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    precondition,
                } => WriteResult::Put {
                    version: self
                        .put_one(
                            collection,
                            id,
                            doc,
                            *precondition,
                            options,
                            Some(&mut *session),
                        )
                        .await?
                        .0,
                },
                WriteOp::Delete {
                    collection,
                    id,
                    precondition,
                } => WriteResult::Delete {
                    removed: self
                        .delete_one(collection, id, *precondition, Some(&mut *session))
                        .await?,
                },
            });
        }
        Ok(results)
    }

    /// Commit, retrying a commit whose outcome the server could not report.
    pub(crate) async fn commit(
        session: &mut ClientSession,
    ) -> std::result::Result<(), mongodb::error::Error> {
        let mut outcome = session.commit_transaction().await;
        for _ in 0..TRANSACTION_ATTEMPTS {
            match &outcome {
                Err(error) if errors::is_unknown_commit(error) => {
                    outcome = session.commit_transaction().await;
                }
                _ => break,
            }
        }
        outcome
    }
}

#[async_trait]
impl DocumentStore for MongoDocuments {
    fn capabilities(&self) -> Capabilities {
        let base = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Ttl)
            .with(Capability::CollectionEnumeration)
            .with(Capability::ExistingSchema)
            .with(Capability::CollectionValidation)
            .with(Capability::RecordMetadata)
            .with(Capability::StreamSeal)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas)
            .with(Capability::ScopeEnumeration);
        let base = if self.shared.physical_transactions {
            base.with(Capability::Watch)
        } else {
            base
        };
        if self.shared.transactions {
            base.with(Capability::Transactions)
                .with(Capability::AtomicWriteOptions)
                .with(Capability::ValidatedBatch)
                .with(Capability::ConditionalStreamAppend)
                .with(Capability::Fencing)
        } else {
            base
        }
    }

    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &tinystoragedrivers_core::WatchOptions,
    ) -> Result<Box<dyn tinystoragedrivers_core::DocumentWatch>> {
        self.open_watch(collection, filter, options).await
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        if self.shared.existing {
            return self.shared.ensure(spec, &self.scope, self.generation).await;
        }
        crate::purge::admitted!(
            self,
            self.shared.ensure(spec, &self.scope, self.generation).await
        )
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        crate::purge::admitted!(self, {
            required.validate()?;
            let stored = self.shared.persisted_spec(&required.name).await?;
            if stored.merge(required)? != stored {
                return Err(StorageError::invalid_input(
                    "stored declaration lacks required features",
                ));
            }
            let mut models = vec![scope_key_model()];
            models.extend(crate::backend::specs::index_models(&stored)?);
            if let Some(model) = stored
                .search
                .as_ref()
                .map(crate::backend::specs::text_model)
                .transpose()?
                .flatten()
            {
                models.push(model);
            }
            self.shared.validate_indexes(&stored.name, &models).await
        })
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        crate::purge::admitted!(self, {
            validate_collection(collection)?;
            validate_id(id)?;
            let spec = self.shared.spec(collection).await?;
            let now = self.shared.now();
            Ok(self
                .read_raw(&self.collection(collection), id, None)
                .await?
                .filter(|found| stored::is_live(&spec, found, now))
                .map(stored::Stored::into_versioned))
        })
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_with(collection, id, doc, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        crate::purge::admitted!(self, {
            if !self.shared.physical_transactions {
                return self
                    .put_one(collection, id, &doc, precondition, options, None)
                    .await
                    .map(|(version, _)| version);
            }
            self.ready(collection).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(session).await?;
                self.shared.spec_in_session(collection, &mut *session).await?;
                self.put_one(collection, id, &doc, precondition, options, Some(session))
                    .await
                    .map(|(version, _)| version)
            })
        })
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        crate::purge::admitted!(self, {
            if !self.shared.physical_transactions {
                return self.delete_one(collection, id, precondition, None).await;
            }
            self.ready(collection).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(session).await?;
                self.shared.spec_in_session(collection, &mut *session).await?;
                self.delete_one(collection, id, precondition, Some(session)).await
            })
        })
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        crate::purge::admitted!(self, {
            query.validate()?;
            validate_collection(collection)?;
            let position = decode_cursor(collection, &self.scope, query)?;
            let (items, more) = self
                .page(
                    collection,
                    &query.filter,
                    &query.sort,
                    position,
                    query.limit,
                )
                .await?;
            let next = if more {
                let last = items
                    .last()
                    .ok_or_else(|| StorageError::backend("a continued page was empty"))?;
                Some(encode_cursor(
                    collection,
                    &self.scope,
                    query,
                    &CursorPosition::for_record(&last.id, &last.doc, &last.meta, &query.sort),
                )?)
            } else {
                None
            };
            Ok(Page { items, next })
        })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_collection(collection)?;
            filter.validate()?;
            self.count_matching(collection, filter).await
        })
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_collection(collection)?;
            filter.validate()?;
            if !self.shared.physical_transactions {
                let pairs = self.live_pairs(collection, filter).await?;
                return self.remove(collection, &pairs, None).await;
            }
            self.ready(collection).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(session).await?;
                self.shared.spec_in_session(collection, &mut *session).await?;
                match self.live_pairs(collection, filter).await {
                    Ok(pairs) => self.remove(collection, &pairs, Some(session)).await,
                    Err(error) => Err(error),
                }
            })
        })
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        crate::purge::admitted!(self, {
            validate_doc(patch)?;
            validate_collection(collection)?;
            filter.validate()?;
            if self.shared.physical_transactions {
                self.ready(collection).await?;
            }
            for _ in 0..CLAIM_ATTEMPTS {
                let outcome = if self.shared.physical_transactions {
                    in_write!(&self.shared, self.fence.as_deref(), session => {
                    self.hold_validation(session).await?;
                    self.shared.spec_in_session(collection, &mut *session).await?;
                    match self.claim_once(collection, filter, sort, patch, Some(session)).await {
                        Err(error) if error.kind() == ErrorKind::Conflict => Ok(None),
                        other => other.map(Some),
                    }
                })
                .and_then(|step| {
                    step.ok_or_else(|| StorageError::conflict("claim lost to a concurrent claimer"))
                })
                } else {
                    self.claim_once(collection, filter, sort, patch, None).await
                };
                match outcome {
                    Ok(claimed) => return Ok(claimed),
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => return Err(error),
                }
            }
            Err(StorageError::unavailable(
                "every claim attempt lost to a concurrent claimer; retry",
            ))
        })
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        crate::purge::admitted!(self, {
            if !self.shared.transactions {
                return Err(StorageError::unsupported(
                    Capability::Transactions,
                    "this MongoDB handle disables public transactions",
                ));
            }
            self.atomic_batch_checked(Vec::new(), ops).await
        })
    }

    async fn atomic_batch_with(
        &self,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        crate::purge::admitted!(self, {
            if !self.shared.transactions {
                return Err(StorageError::unsupported(
                    Capability::AtomicWriteOptions,
                    "this MongoDB handle cannot apply document write options in a transaction",
                ));
            }
            self.atomic_batch_checked_with(Vec::new(), ops, options)
                .await
        })
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        crate::purge::admitted!(self, {
            if !self.shared.transactions {
                return Err(StorageError::unsupported(
                    Capability::ValidatedBatch,
                    "this MongoDB handle disables validated batches",
                ));
            }
            self.atomic_batch_checked_with(checks, ops, WriteOptions::default())
                .await
        })
    }

    async fn atomic_batch_checked_with(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        crate::purge::admitted!(self, {
            if !self.shared.transactions {
                return Err(StorageError::unsupported(
                    Capability::AtomicWriteOptions,
                    "this MongoDB handle cannot validate and apply document write options in a transaction",
                ));
            }
            self.shared.prepare_validation(Some(&self.scope)).await?;
            for check in &checks {
                check.validate()?;
                self.ready(&check.collection).await?;
            }
            for op in &ops {
                let (WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. }) = op;
                self.ready(collection).await?;
            }
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(session).await?;
                let now = self.shared.now();
                for check in &checks {
                    let spec = self.shared.spec_in_session(&check.collection, &mut *session).await?;
                    let current = self.read_raw(&self.collection(&check.collection), &check.id, Some(&mut *session)).await?
                        .filter(|found| stored::is_live(&spec, found, now)).map(|found| found.version);
                    check.verify(current)?;
                }
                self.run_batch(&ops, options, session).await
            })
        })
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        crate::purge::admitted!(self, {
            validate_collection(collection)?;
            self.search_hits(collection, text, limit).await
        })
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        crate::purge::admitted!(self, {
            validate_collection(collection)?;
            if self.shared.existing {
                self.shared.spec(collection).await?;
            }
            if !self.shared.physical_transactions {
                let pairs = self.matching_pairs(collection, &Filter::All).await?;
                self.remove(collection, &pairs, None).await?;
                return Ok(());
            }
            self.ready(collection).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(session).await?;
                self.shared.spec_in_session(collection, &mut *session).await?;
                match self.matching_pairs(collection, &Filter::All).await {
                    Ok(pairs) => self.remove(collection, &pairs, Some(session)).await,
                    Err(error) => Err(error),
                }
            })?;
            Ok(())
        })
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        crate::purge::admitted!(self, {
            let names = self
                .shared
                .db
                .list_collection_names()
                .await
                .map_err(errors::failed("list document collections"))?;
            let mut found = Vec::new();
            for name in names {
                let name = name.as_str();
                if name.starts_with(RESERVED_PREFIX)
                    || !name.starts_with(prefix)
                    || validate_collection(name).is_err()
                {
                    continue;
                }
                let count = self
                    .shared
                    .raw(name)
                    .count_documents(mongodb::bson::doc! {"_scope": self.scope.as_str()})
                    .await
                    .map_err(errors::failed("list scoped document collections"))?;
                if count > 0 {
                    found.push(name.to_owned());
                }
            }
            found.sort();
            Ok(found)
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
