//! [`DocumentStore`] for the in-memory driver.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::MemoryFence;
use super::state::{DbState, Write};
use super::{Clock, MemoryDb};
use crate::capabilities::{Capabilities, Capability};
use crate::document::{
    CollectionSpec, Cursor, CursorPosition, DocumentStore, Page, Precondition, Query, ReadCheck,
    SearchHit, Version, Versioned, WriteOp, WriteOptions, WriteResult, validate_collection,
    validate_doc,
};
use crate::error::{Result, StorageError};
use crate::filter::{Filter, Sort, sort_documents_with_meta};
use crate::scope::Scope;
use crate::value;
#[cfg(feature = "test-util")]
use crate::{DocumentWatch, WatchOptions};

/// In-memory documents bound to one scope.
#[derive(Clone)]
pub struct MemoryDocuments {
    db: Arc<MemoryDb>,
    scope: Scope,
    generation: crate::ScopeGeneration,
    clock: Clock,
    /// Checked under the lock before every write, when set.
    fence: Option<Arc<MemoryFence>>,
    existing: bool,
    #[cfg(feature = "test-util")]
    native_watch: bool,
}

impl std::fmt::Debug for MemoryDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryDocuments")
            .field("scope", &self.scope)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl MemoryDocuments {
    pub(super) fn new(
        db: Arc<MemoryDb>,
        scope: Scope,
        clock: Clock,
        fence: Option<Arc<MemoryFence>>,
        existing: bool,
    ) -> Self {
        Self {
            db,
            scope,
            generation: crate::ScopeGeneration::default(),
            clock,
            fence,
            existing,
            #[cfg(feature = "test-util")]
            native_watch: false,
        }
    }

    fn check_schema(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        if self.existing && !self.db.lock()?.specs.contains_key(collection) {
            return Err(StorageError::not_found(
                "collection has no persisted declaration",
            ));
        }
        Ok(())
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    pub(super) fn with_generation(mut self, generation: crate::ScopeGeneration) -> Self {
        self.generation = generation;
        self
    }

    #[cfg(feature = "test-util")]
    pub(super) fn with_native_watch(mut self, enabled: bool) -> Self {
        self.native_watch = enabled;
        self
    }

    #[cfg(feature = "test-util")]
    pub(super) fn validate_watch(&self) -> Result<()> {
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        MemoryDb::guard(&state, self.fence.as_deref(), self.now())
    }

    /// Refuse a write unless this handle's fence (if any) holds.
    fn guard(&self, state: &DbState, now_ms: u64) -> Result<()> {
        state.check_generation(self.scope.as_str(), self.generation)?;
        MemoryDb::guard(state, self.fence.as_deref(), now_ms)
    }

    fn matching(
        &self,
        state: &DbState,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
    ) -> Result<Vec<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        let mut found: Vec<Versioned<Value>> = state
            .live(self.scope.as_str(), collection, self.now())
            .into_iter()
            .filter(|(id, stored)| filter.matches_with(id, &stored.doc, &stored.meta))
            .map(|(id, stored)| Versioned {
                id: id.clone(),
                version: stored.version,
                doc: stored.doc.clone(),
                meta: stored.meta,
            })
            .collect();
        sort_documents_with_meta(&mut found, sort, |item| {
            (item.id.as_str(), &item.doc, &item.meta)
        });
        Ok(found)
    }
}

/// A fingerprint of everything that decides a query's result order, so a
/// cursor only resumes the query that issued it.
pub(super) fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> u64 {
    let shape = serde_json::to_vec(&(collection, scope.as_str(), &query.filter, &query.sort))
        .unwrap_or_default();
    shape.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Decode a keyset cursor issued by [`MemoryDocuments::query`] for this scope and query.
fn parse_cursor(collection: &str, scope: &Scope, query: &Query) -> Result<Option<CursorPosition>> {
    let Some(cursor) = &query.cursor else {
        return Ok(None);
    };
    let position = cursor.decode_keyset("mem", fingerprint(collection, scope, query))?;
    position.validate(&query.sort)?;
    Ok(Some(position))
}

#[async_trait]
impl DocumentStore for MemoryDocuments {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Transactions)
            .with(Capability::ValidatedBatch)
            .with(Capability::Ttl)
            .with(Capability::Fencing)
            .with(Capability::ConditionalStreamAppend)
            .with(Capability::StreamSeal)
            .with(Capability::CollectionEnumeration)
            .with(Capability::ExistingSchema)
            .with(Capability::CollectionValidation)
            .with(Capability::RecordMetadata)
            .with(Capability::AtomicWriteOptions);
        #[cfg(feature = "test-util")]
        let capabilities = if self.native_watch {
            capabilities.with(Capability::Watch)
        } else {
            capabilities
        };
        capabilities
    }

    #[cfg(feature = "test-util")]
    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        if !self.native_watch {
            return Err(StorageError::unsupported(
                Capability::Watch,
                "this memory backend has native watch disabled",
            ));
        }
        validate_collection(collection)?;
        filter.validate()?;
        options.validate()?;
        if options.after.is_some() {
            return Err(StorageError::invalid_input(
                "memory watch does not support resume cursors",
            ));
        }
        self.check_schema(collection)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        MemoryDb::guard(&state, self.fence.as_deref(), self.now())?;
        let watch = self.db.subscribe_watch(
            (self.scope.as_str().to_owned(), collection.to_owned()),
            self.clone(),
            options.interval,
        )?;
        Ok(Box::new(watch))
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                Capability::ExistingSchema,
                "existing-schema handles cannot declare collections",
            ));
        }
        spec.validate()?;
        let mut state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let merged = match state.specs.get(&spec.name) {
            Some(existing) => existing.merge(spec)?,
            None => spec.clone(),
        };
        state.check_existing_unique(&merged, self.now())?;
        state.specs.insert(spec.name.clone(), merged);
        Ok(())
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let stored = state
            .specs
            .get(&required.name)
            .ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
        if stored.merge(required)? != *stored {
            return Err(StorageError::invalid_input(
                "stored declaration lacks required features",
            ));
        }
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        crate::document::validate_id(id)?;
        self.check_schema(collection)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(state.get(self.scope.as_str(), collection, id, self.now()))
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
        crate::document::validate_id(id)?;
        validate_doc(&doc)?;
        self.check_schema(collection)?;
        // The clock is read under the lock, where the guard check and the
        // write are serialized.
        let mut state = self.db.lock()?;
        let now = self.now();
        self.guard(&state, now)?;
        let write = Write {
            doc,
            precondition,
            options,
        };
        let version = state.put(self.scope.as_str(), collection, id, write, now)?;
        #[cfg(feature = "test-util")]
        self.db.invalidate_watch(self.scope.as_str(), collection);
        Ok(version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        validate_collection(collection)?;
        crate::document::validate_id(id)?;
        self.check_schema(collection)?;
        // The clock is read under the lock, where the guard check and the
        // write are serialized.
        let mut state = self.db.lock()?;
        let now = self.now();
        self.guard(&state, now)?;
        let removed = state.delete(self.scope.as_str(), collection, id, precondition, now)?;
        #[cfg(feature = "test-util")]
        if removed {
            self.db.invalidate_watch(self.scope.as_str(), collection);
        }
        Ok(removed)
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let position = parse_cursor(collection, &self.scope, query)?;
        self.check_schema(collection)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let found = self.matching(&state, collection, &query.filter, &query.sort)?;
        let start = position.as_ref().map_or(0, |cursor| {
            found.partition_point(|item| {
                CursorPosition::for_record(&item.id, &item.doc, &item.meta, &query.sort)
                    .compare(cursor, &query.sort)
                    != std::cmp::Ordering::Greater
            })
        });
        let end = query.limit.map_or(found.len(), |limit| {
            start.saturating_add(limit).min(found.len())
        });
        let items = found.get(start..end).map(<[_]>::to_vec).unwrap_or_default();
        let next = if end < found.len() {
            let last = items
                .last()
                .ok_or_else(|| StorageError::backend("a continued page was empty"))?;
            Some(Cursor::keyset(
                "mem",
                fingerprint(collection, &self.scope, query),
                &CursorPosition::for_record(&last.id, &last.doc, &last.meta, &query.sort),
            )?)
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        self.check_schema(collection)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        Ok(self.matching(&state, collection, filter, &[])?.len() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        self.check_schema(collection)?;
        // The clock is read under the lock, where the guard check and the
        // write are serialized.
        let mut state = self.db.lock()?;
        let now = self.now();
        self.guard(&state, now)?;
        let removed = state.delete_where(self.scope.as_str(), collection, filter, now);
        #[cfg(feature = "test-util")]
        if removed > 0 {
            self.db.invalidate_watch(self.scope.as_str(), collection);
        }
        Ok(removed)
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_doc(patch)?;
        Query {
            filter: filter.clone(),
            sort: sort.to_vec(),
            ..Query::default()
        }
        .validate()?;
        self.check_schema(collection)?;
        // The clock is read under the lock, where the guard check and the
        // write are serialized.
        let mut state = self.db.lock()?;
        let now = self.now();
        self.guard(&state, now)?;
        let Some(first) = self
            .matching(&state, collection, filter, sort)?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let mut doc = first.doc;
        value::merge_patch(&mut doc, patch);
        // A claim patches the body only: the schema and expiry carry over.
        let write = Write {
            doc: doc.clone(),
            precondition: Precondition::Version(first.version),
            options: WriteOptions {
                schema: first.meta.schema,
                expires_ms: first.meta.expires_ms,
                created_ms: None,
            },
        };
        let version = state.put(self.scope.as_str(), collection, &first.id, write, now)?;
        #[cfg(feature = "test-util")]
        self.db.invalidate_watch(self.scope.as_str(), collection);
        let meta = state
            .get(self.scope.as_str(), collection, &first.id, now)
            .map_or(first.meta, |found| found.meta);
        Ok(Some(Versioned {
            id: first.id,
            version,
            doc,
            meta,
        }))
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
                    crate::document::validate_id(id)?;
                    validate_doc(doc)?;
                }
                WriteOp::Delete { collection, id, .. } => {
                    validate_collection(collection)?;
                    crate::document::validate_id(id)?;
                }
            }
        }
        for check in &checks {
            self.check_schema(&check.collection)?;
        }
        for op in &ops {
            let (WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. }) = op;
            self.check_schema(collection)?;
        }
        // The clock is read under the lock, where the guard check and the
        // write are serialized.
        let mut state = self.db.lock()?;
        let now = self.now();
        self.guard(&state, now)?;
        for check in &checks {
            check.verify(
                state
                    .get(self.scope.as_str(), &check.collection, &check.id, now)
                    .map(|found| found.version),
            )?;
        }
        #[cfg(feature = "test-util")]
        let changed_collections: BTreeSet<String> = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. } => {
                    collection.clone()
                }
            })
            .collect();
        // A batch only writes documents; copying the rest would cost time
        // proportional to every stored stream and blob.
        let mut draft = DbState {
            specs: state.specs.clone(),
            docs: state.docs.clone(),
            tombstones: state.tombstones.clone(),
            ..DbState::default()
        };
        let scope = self.scope.as_str();
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            results.push(match op {
                WriteOp::Put {
                    collection,
                    id,
                    doc,
                    precondition,
                } => {
                    let write = Write {
                        doc,
                        precondition,
                        options,
                    };
                    WriteResult::Put {
                        version: draft.put(scope, &collection, &id, write, now)?,
                    }
                }
                WriteOp::Delete {
                    collection,
                    id,
                    precondition,
                } => WriteResult::Delete {
                    removed: draft.delete(scope, &collection, &id, precondition, now)?,
                },
            });
        }
        state.docs = draft.docs;
        state.tombstones = draft.tombstones;
        #[cfg(feature = "test-util")]
        for collection in changed_collections {
            self.db.invalidate_watch(scope, &collection);
        }
        Ok(results)
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        self.check_schema(collection)?;
        validate_collection(collection)?;
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let spec = state.spec(collection);
        let Some(search) = spec.search else {
            return Err(StorageError::invalid_input(
                "this collection declares no search fields",
            ));
        };
        let wanted = value::tokens(&Value::String(text.to_owned()));
        if wanted.is_empty() {
            return Ok(Vec::new());
        }
        let mut hits: Vec<SearchHit> = state
            .live(self.scope.as_str(), collection, self.now())
            .into_iter()
            .filter_map(|(id, stored)| {
                let have: Vec<String> = search
                    .fields
                    .iter()
                    .filter_map(|field| value::lookup(&stored.doc, field))
                    .flat_map(value::tokens)
                    .collect();
                let score = wanted.iter().filter(|token| have.contains(token)).count();
                // Token counts are tiny; the cast is exact.
                #[allow(clippy::cast_precision_loss)]
                let score = score as f64;
                (score > 0.0).then(|| SearchHit {
                    id: id.clone(),
                    score,
                })
            })
            .collect();
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        hits.truncate(limit);
        Ok(hits)
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.check_schema(collection)?;
        validate_collection(collection)?;
        let mut state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        // Only a fenced handle reads the clock here: dropping a collection
        // does not otherwise need it.
        if self.fence.is_some() {
            self.guard(&state, self.now())?;
        }
        let key = (self.scope.as_str().to_owned(), collection.to_owned());
        let ids: Vec<String> = state
            .docs
            .get(&key)
            .map(|docs| docs.keys().cloned().collect())
            .unwrap_or_default();
        for id in ids {
            state.remove(self.scope.as_str(), collection, &id);
        }
        #[cfg(feature = "test-util")]
        self.db.invalidate_watch(self.scope.as_str(), collection);
        Ok(())
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let state = self.db.lock()?;
        state.check_generation(self.scope.as_str(), self.generation)?;
        let scope = self.scope.as_str();
        let names = state
            .docs
            .keys()
            .chain(state.tombstones.keys())
            .filter(|(stored_scope, collection)| {
                stored_scope == scope && collection.starts_with(prefix)
            })
            .map(|(_, collection)| collection.clone())
            .collect::<BTreeSet<_>>();
        Ok(names.into_iter().collect())
    }
}
