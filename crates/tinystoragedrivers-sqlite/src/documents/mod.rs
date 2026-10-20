//! [`DocumentStore`] on SQLite.
//!
//! All collections of a database share one `WITHOUT ROWID` table keyed by
//! `(scope, collection, id)`, with the body stored as JSON text (or, over 4 KiB,
//! a zstd blob, see [`crate::body`]) and the record
//! metadata (`schema`, `created_ms`, `updated_ms`, `expires_ms`) in columns of
//! their own. A deleted document keeps its row (`deleted = 1`, body `{}`) so
//! its version continues if the id is written again; every read skips it. Declared
//! index and search fields are projected into the `doc_fields` side table
//! ([`crate::fields`]), and search fields are mirrored into an FTS5 table. Filters push their exact
//! string and boolean equalities down to SQL and are re-checked in Rust with
//! [`Filter::matches_with`], so results agree with the memory driver.
//!
//! Every write runs in an immediate transaction under the shared connection,
//! which makes preconditions, unique indexes, claims and batches atomic.

pub(crate) mod ops;
mod order;
mod ordered;
mod pushdown;

use std::fmt;
use std::sync::Arc;

use rusqlite::Connection;
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, DocumentStore, Filter, Page,
    Precondition, Query, ReadCheck, Result, Scope, SearchHit, Sort, StorageError, Version,
    Versioned, WriteOp, WriteOptions, WriteResult, async_trait, validate_collection, validate_doc,
    validate_id, value,
};

use crate::connection::Db;
use crate::fence::{self, Fencing};
use crate::sql::Tables;
use crate::storage::Clock;

use ops::Ctx;

/// What this driver supports.
pub(crate) fn capabilities() -> Capabilities {
    Capabilities::none()
        .with(Capability::FullText)
        .with(Capability::Transactions)
        .with(Capability::ValidatedBatch)
        .with(Capability::ScopePurge)
        .with(Capability::AtomicScopePurge)
        .with(Capability::Ttl)
        .with(Capability::Fencing)
        .with(Capability::ConditionalStreamAppend)
        .with(Capability::CollectionEnumeration)
        .with(Capability::ExistingSchema)
        .with(Capability::CollectionValidation)
        .with(Capability::BlobTtl)
        .with(Capability::Maintenance)
        .with(Capability::ScopeEnumeration)
        .with(Capability::StreamSeal)
        .with(Capability::RecordMetadata)
        .with(Capability::AtomicWriteOptions)
        .with(Capability::SharedAcrossProcesses)
        .with(Capability::CrossProcessCas)
}

/// SQLite documents bound to one scope.
#[derive(Clone)]
pub struct SqliteDocuments {
    db: Arc<Db>,
    tables: Arc<Tables>,
    scope: Scope,
    generation: tinystoragedrivers_core::ScopeGeneration,
    clock: Clock,
    /// Checked inside every write's transaction, when set.
    fencing: Option<Arc<Fencing>>,
    existing: bool,
}

impl fmt::Debug for SqliteDocuments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteDocuments")
            .field("scope", &self.scope)
            .field("path", &self.db.path())
            .field("fenced", &self.fencing.is_some())
            .finish_non_exhaustive()
    }
}

/// How a call touches the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Access {
    /// A plain read.
    Read,
    /// A schema declaration: transactional, but never fenced.
    Declare,
    /// A data write: transactional and fenced.
    Write,
}

impl SqliteDocuments {
    pub(crate) fn new(
        db: Arc<Db>,
        tables: Arc<Tables>,
        scope: Scope,
        clock: Clock,
        fencing: Option<Arc<Fencing>>,
        existing: bool,
    ) -> Self {
        Self {
            db,
            tables,
            scope,
            generation: tinystoragedrivers_core::ScopeGeneration::default(),
            clock,
            fencing,
            existing,
        }
    }

    pub(crate) fn with_generation(
        mut self,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Self {
        self.generation = generation;
        self
    }

    /// Run `f` with a context for this handle, inside an immediate
    /// transaction unless it only reads. A fenced handle checks its fence in
    /// that transaction before a [`Access::Write`].
    async fn with<T, F>(&self, access: Access, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, Ctx<'_>) -> Result<T> + Send + 'static,
    {
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let generation = self.generation;
        let now_ms = (self.clock)();
        let fencing = self.fencing.clone();
        self.db
            .run(move |conn| {
                let ctx = Ctx {
                    tables: &tables,
                    scope: scope.as_str(),
                    now_ms,
                };
                if access == Access::Read {
                    let tx = conn
                        .transaction()
                        .map_err(crate::error::during("start read transaction"))?;
                    crate::purge::check(&tx, &tables, scope.as_str(), generation)?;
                    let value = f(&tx, ctx)?;
                    tx.commit()
                        .map_err(crate::error::during("finish read transaction"))?;
                    return Ok(value);
                }
                let fencing = if access == Access::Write {
                    fencing.as_deref()
                } else {
                    None
                };
                fence::write(conn, fencing, &tables, |tx| {
                    crate::purge::check(tx, &tables, scope.as_str(), generation)?;
                    f(tx, ctx)
                })
            })
            .await
    }
}

/// FNV-1a: a stable hash, so a cursor stays valid across processes.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A fingerprint of what decides a query's result order.
fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> Result<u64> {
    let shape = serde_json::to_string(&(collection, scope.as_str(), &query.filter, &query.sort))?;
    Ok(fnv1a(shape.as_bytes()))
}

fn parse_cursor(collection: &str, scope: &Scope, query: &Query) -> Result<Option<CursorPosition>> {
    let Some(cursor) = &query.cursor else {
        return Ok(None);
    };
    let position = cursor.decode_keyset("sqlite", fingerprint(collection, scope, query)?)?;
    position.validate(&query.sort)?;
    Ok(Some(position))
}

#[async_trait]
impl DocumentStore for SqliteDocuments {
    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                Capability::ExistingSchema,
                "existing-schema handles cannot declare collections",
            ));
        }
        spec.validate()?;
        let spec = spec.clone();
        self.with(Access::Declare, move |conn, ctx| {
            ops::declare(conn, ctx.tables, &spec, ctx.now_ms)
        })
        .await
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let required = required.clone();
        self.with(Access::Read, move |conn, ctx| {
            ops::validate_spec(conn, ctx.tables, &required)
        })
        .await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Read, move |conn, ctx| {
            ops::get(conn, ctx, &collection, &id)
        })
        .await
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
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Write, move |conn, ctx| {
            ops::put_with(conn, ctx, &collection, &id, &doc, precondition, options)
        })
        .await
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(Access::Write, move |conn, ctx| {
            ops::delete(conn, ctx, &collection, &id, precondition)
        })
        .await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let position = parse_cursor(collection, &self.scope, query)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let fingerprint = fingerprint(collection, &self.scope, query)?;
        let (collection, query) = (collection.to_owned(), query.clone());
        self.with(Access::Read, move |conn, ctx| {
            let found = ops::matching_after(
                conn,
                ctx,
                &collection,
                &query.filter,
                &query.sort,
                position.as_ref(),
                query.limit.map(|limit| limit.saturating_add(1)),
            )?;
            let end = query
                .limit
                .map_or(found.len(), |limit| limit.min(found.len()));
            let items = found.get(..end).map(<[_]>::to_vec).unwrap_or_default();
            let next = if end < found.len() {
                let last = items
                    .last()
                    .ok_or_else(|| StorageError::backend("a continued page was empty"))?;
                Some(Cursor::keyset(
                    "sqlite",
                    fingerprint,
                    &CursorPosition::for_record(&last.id, &last.doc, &last.meta, &query.sort),
                )?)
            } else {
                None
            };
            Ok(Page { items, next })
        })
        .await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(Access::Read, move |conn, ctx| {
            Ok(ops::matching(conn, ctx, &collection, &filter, &[])?.len() as u64)
        })
        .await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, filter) = (collection.to_owned(), filter.clone());
        self.with(Access::Write, move |conn, ctx| {
            ops::delete_where(conn, ctx, &collection, &filter)
        })
        .await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        for order in sort {
            Filter::exists(order.field.clone(), true).validate()?;
        }
        validate_doc(patch)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, filter, sort, patch) = (
            collection.to_owned(),
            filter.clone(),
            sort.to_vec(),
            patch.clone(),
        );
        self.with(Access::Write, move |conn, ctx| {
            let Some(first) =
                ops::matching_after(conn, ctx, &collection, &filter, &sort, None, Some(1))?
                    .into_iter()
                    .next()
            else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, &patch);
            // A claim patches the body only: the schema and expiry carry over.
            let version = ops::put_with(
                conn,
                ctx,
                &collection,
                &first.id,
                &doc,
                Precondition::Version(first.version),
                WriteOptions {
                    schema: first.meta.schema,
                    expires_ms: first.meta.expires_ms,
                    created_ms: None,
                },
            )?;
            let meta =
                ops::get(conn, ctx, &collection, &first.id)?.map_or(first.meta, |found| found.meta);
            Ok(Some(Versioned {
                id: first.id,
                version,
                doc,
                meta,
            }))
        })
        .await
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.atomic_batch_with(ops, WriteOptions::default()).await
    }

    async fn atomic_batch_with(
        &self,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        self.atomic_batch_checked_with(Vec::new(), ops, options)
            .await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.atomic_batch_checked_with(checks, ops, WriteOptions::default())
            .await
    }

    async fn atomic_batch_checked_with(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
        options: WriteOptions,
    ) -> Result<Vec<WriteResult>> {
        for check in &checks {
            check.validate()?;
        }
        for op in &ops {
            match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    ..
                } => {
                    validate_collection(collection)?;
                    validate_id(id)?;
                    validate_doc(doc)?;
                }
                WriteOp::Delete { collection, id, .. } => {
                    validate_collection(collection)?;
                    validate_id(id)?;
                }
            }
        }
        if self.existing {
            for check in &checks {
                self.validate_collection_spec(&CollectionSpec::new(&check.collection))
                    .await?;
            }
            for op in &ops {
                let (WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. }) = op;
                self.validate_collection_spec(&CollectionSpec::new(collection))
                    .await?;
            }
        }
        self.with(Access::Write, move |conn, ctx| {
            for check in &checks {
                check.validate()?;
                check.verify(
                    ops::get(conn, ctx, &check.collection, &check.id)?.map(|found| found.version),
                )?;
            }

            ops.into_iter()
                .map(|op| match op {
                    WriteOp::Put {
                        collection,
                        id,
                        doc,
                        precondition,
                    } => Ok(WriteResult::Put {
                        version: if options.is_default() {
                            ops::put(conn, ctx, &collection, &id, &doc, precondition)?
                        } else {
                            ops::put_with(conn, ctx, &collection, &id, &doc, precondition, options)?
                        },
                    }),
                    WriteOp::Delete {
                        collection,
                        id,
                        precondition,
                    } => Ok(WriteResult::Delete {
                        removed: ops::delete(conn, ctx, &collection, &id, precondition)?,
                    }),
                })
                .collect()
        })
        .await
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, text) = (collection.to_owned(), text.to_owned());
        let hits = self
            .with(Access::Read, move |conn, ctx| {
                ops::search(conn, ctx, &collection, &text, limit)
            })
            .await?;
        Ok(hits
            .into_iter()
            .map(|(id, score)| SearchHit { id, score })
            .collect())
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let collection = collection.to_owned();
        self.with(Access::Write, move |conn, ctx| {
            ops::drop_collection(conn, ctx, &collection)
        })
        .await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.with(Access::Read, move |conn, ctx| {
            ops::collections(conn, ctx, &prefix)
        })
        .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
