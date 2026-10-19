//! [`DocumentStore`] for the file driver: one pretty-printed JSON file per
//! document.
//!
//! Queries, counts, deletes by filter and claims read every document of the
//! collection and evaluate with the core's [`Filter::matches`] and
//! [`sort_documents`](tinystoragedrivers_core::sort_documents), exactly as the
//! memory driver does. That is linear in the collection's size, which suits
//! the small per-user stores this driver is meant for.

mod store;

use crate::storage::Fencing;

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_core::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, DocumentStore, Filter, Page,
    Precondition, Query, Result, Scope, ScopeGeneration, SearchHit, Sort, StorageError, Version,
    Versioned, WriteOptions, validate_collection, validate_doc, validate_id, value,
};

use crate::encode::fnv1a_64;
use crate::storage::{CAPABILITIES, Db};
use store::Docs;

/// File-backed documents bound to one scope.
#[derive(Debug, Clone)]
pub struct FileDocuments {
    db: Arc<Db>,
    scope: Scope,
    /// Checked under the directory lock before every write, when set.
    fence: Option<Arc<Fencing>>,
    existing: bool,
    generation: ScopeGeneration,
}

/// Refuse a write unless `fence` (when there is one) holds. Called under the
/// directory lock, so the check and the write are one step for every handle
/// in this process.
///
/// # Errors
///
/// [`ErrorKind::Fenced`](tinystoragedrivers_core::ErrorKind::Fenced), or a
/// failure reading the guard document.
pub(crate) fn guard(db: &Db, fence: Option<&Fencing>) -> Result<()> {
    let Some(fence) = fence else {
        return Ok(());
    };
    let fence = &fence.fence;
    let docs = Docs {
        db,
        scope: fence.scope(),
        now: db.now(),
    };
    fence.check(docs.get(fence.collection(), fence.id())?.as_ref())
}

impl FileDocuments {
    pub(crate) fn new(
        db: Arc<Db>,
        scope: Scope,
        fence: Option<Arc<Fencing>>,
        existing: bool,
    ) -> Self {
        Self {
            db,
            scope,
            fence,
            existing,
            generation: ScopeGeneration::default(),
        }
    }

    pub(crate) fn with_generation(mut self, generation: ScopeGeneration) -> Self {
        self.generation = generation;
        self
    }

    /// Run `work` under the directory lock with this scope's document view.
    async fn with<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Docs<'_>) -> Result<T> + Send + 'static,
    {
        self.run(None, work).await
    }

    /// [`Self::with`] for a write: the fence (if any) is checked first,
    /// under the same lock.
    async fn write<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Docs<'_>) -> Result<T> + Send + 'static,
    {
        self.run(self.fence.clone(), work).await
    }

    async fn run<T, F>(&self, fence: Option<Arc<Fencing>>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Docs<'_>) -> Result<T> + Send + 'static,
    {
        let scope = self.scope.clone();
        let generation = self.generation;
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                guard(db, fence.as_deref())?;
                let docs = Docs {
                    db,
                    scope: &scope,
                    now: db.now(),
                };
                work(&docs)
            })
            .await
    }
}

/// A fingerprint of everything that decides a query's result order, so a
/// cursor only resumes the query that issued it. FNV-1a is stable across
/// processes, so a cursor survives a restart.
pub(crate) fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(collection.as_bytes());
    // A separator no collection name contains keeps the parts apart.
    bytes.push(0);
    bytes.extend_from_slice(scope.as_str().as_bytes());
    bytes.push(0);
    bytes.extend(serde_json::to_vec(&query.filter).unwrap_or_default());
    bytes.push(0);
    bytes.extend(serde_json::to_vec(&query.sort).unwrap_or_default());
    fnv1a_64(&bytes)
}

/// Decode a keyset cursor issued by [`FileDocuments::query`] for this scope and query.
fn parse_cursor(collection: &str, scope: &Scope, query: &Query) -> Result<Option<CursorPosition>> {
    let Some(cursor) = &query.cursor else {
        return Ok(None);
    };
    let position = cursor.decode_keyset("file", fingerprint(collection, scope, query))?;
    position.validate(&query.sort)?;
    Ok(Some(position))
}

#[async_trait]
impl DocumentStore for FileDocuments {
    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                Capability::ExistingSchema,
                "existing-schema handles cannot declare collections",
            ));
        }
        let spec = spec.clone();
        self.with(move |docs| docs.ensure(&spec)).await
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let required = required.clone();
        self.with(move |docs| docs.validate_spec(&required)).await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let (collection, id) = (collection.to_owned(), id.to_owned());
        self.with(move |docs| docs.get(&collection, &id)).await
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
        self.write(move |docs| docs.put(&collection, &id, doc, precondition, options))
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
        self.write(move |docs| docs.delete(&collection, &id, precondition))
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
        let fingerprint = fingerprint(collection, &self.scope, query);
        let (collection, query) = (collection.to_owned(), query.clone());
        self.with(move |docs| {
            let found = docs.matching(&collection, &query.filter, &query.sort)?;
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
                    "file",
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
        self.with(move |docs| Ok(docs.matching(&collection, &filter, &[])?.len() as u64))
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
        self.write(move |docs| docs.delete_where(&collection, &filter))
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
        self.write(move |docs| {
            let Some(first) = docs
                .matching(&collection, &filter, &sort)?
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            let mut doc = first.doc;
            value::merge_patch(&mut doc, &patch);
            // A claim patches the body only: the schema and expiry carry over.
            let options = WriteOptions {
                schema: first.meta.schema,
                expires_ms: first.meta.expires_ms,
                created_ms: None,
            };
            let version = docs.put(
                &collection,
                &first.id,
                doc.clone(),
                Precondition::Version(first.version),
                options,
            )?;
            let meta = docs
                .get(&collection, &first.id)?
                .map_or(first.meta, |found| found.meta);
            Ok(Some(Versioned {
                id: first.id,
                version,
                doc,
                meta,
            }))
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
        self.with(move |docs| {
            let spec = docs.spec(&collection)?;
            let Some(search) = spec.search.clone() else {
                return Err(StorageError::invalid_input(
                    "this collection declares no search fields",
                ));
            };
            let wanted = value::tokens(&Value::String(text));
            if wanted.is_empty() {
                return Ok(Vec::new());
            }
            let mut hits: Vec<SearchHit> = docs
                .live(&spec)?
                .into_iter()
                .filter_map(|stored| {
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
                    (score > 0.0).then_some(SearchHit {
                        id: stored.id,
                        score,
                    })
                })
                .collect();
            hits.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
            hits.truncate(limit);
            Ok(hits)
        })
        .await
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        if self.existing {
            self.validate_collection_spec(&CollectionSpec::new(collection))
                .await?;
        }
        let collection = collection.to_owned();
        self.write(move |docs| docs.drop_collection(&collection))
            .await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.with(move |docs| docs.collections(&prefix)).await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
