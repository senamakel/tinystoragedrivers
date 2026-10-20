//! [`MongoStorage`]: one MongoDB database shared by every tenant.
//!
//! The backend owns the client and, per named database (a collection
//! prefix), a [`Shared`] holding the declared collection specs and which
//! collections already carry the driver's indexes. Port handles are cheap
//! views over a `Shared` bound to one scope.

pub(crate) mod specs;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, ClientSession, Collection, Database};
use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, Result, Scope, ScopedStorage, StorageBackend,
    StorageError, validate_collection,
};

use crate::blobs::MongoBlobs;
use crate::documents::MongoDocuments;
use crate::errors;
use crate::fence::Fencing;
use crate::naming::{BLOBS, STREAM_HEADS, STREAMS, validate_mongo_database};
use crate::scoped::ScopedCollection;
use crate::streams::MongoStreams;

pub(crate) use specs::SpecCache;

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether a `hello` reply describes a deployment that runs multi-document
/// transactions: a replica set member or a `mongos` router, with sessions.
pub(crate) fn supports_transactions(hello: &Document) -> bool {
    let sessions = hello.contains_key("logicalSessionTimeoutMinutes");
    let replica_set = hello.get_str("setName").is_ok();
    let router = hello.get_str("msg").is_ok_and(|msg| msg == "isdbgrid");
    sessions && (replica_set || router)
}

/// What every handle of the database shares.
pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) watch_origin: mongodb::bson::oid::ObjectId,
    pub(crate) db: Database,
    pub(crate) clock: Clock,
    pub(crate) transactions: bool,
    /// Detected server support, retained when public capabilities are disabled.
    pub(crate) physical_transactions: bool,
    validation_ready: AtomicBool,
    validation_scopes: Mutex<BTreeSet<String>>,
    pub(crate) specs: SpecCache,
    pub(crate) existing: bool,
}

impl Shared {
    pub(crate) fn new(
        client: Client,
        db: Database,
        clock: Clock,
        transactions: bool,
        physical_transactions: bool,
    ) -> Self {
        Self {
            watch_origin: mongodb::bson::oid::ObjectId::new(),
            client,
            db,
            clock,
            transactions,
            physical_transactions,
            validation_ready: AtomicBool::new(false),
            validation_scopes: Mutex::new(BTreeSet::new()),
            specs: SpecCache::default(),
            existing: false,
        }
    }

    fn with_existing(mut self, existing: bool) -> Self {
        self.existing = existing;
        self
    }

    async fn prepare_validation_registry(&self) -> Result<()> {
        if self.validation_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.existing {
            self.require_collection("_tsd_validated_batch").await?;
        }
        let outcome = self
            .raw("_tsd_validated_batch")
            .update_one(
                doc! {"_id": "documents"},
                doc! {"$setOnInsert": {"epoch": 0_i64}},
            )
            .upsert(true)
            .await;
        if let Err(error) = outcome {
            let error = errors::map(error, "initialize document validation metadata");
            if error.kind() != tinystoragedrivers_core::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        self.validation_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Register a scope against collection-wide schema mutations. Existing
    /// scopes take no registration lock on their ordinary mutation paths.
    pub(crate) async fn prepare_validation(&self, scope: Option<&Scope>) -> Result<()> {
        self.prepare_validation_registry().await?;
        let Some(scope) = scope else {
            return Ok(());
        };
        let key = format!("scope:{}", scope.as_str());
        if self
            .validation_scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key)
        {
            return Ok(());
        }
        let metadata = self.raw("_tsd_validated_batch");
        if metadata
            .find_one(doc! {"_id": &key})
            .await
            .map_err(errors::failed("read validation scope"))?
            .is_none()
        {
            crate::fence::in_write!(self, None, session => {
                metadata.update_one(doc! {"_id": "documents"}, doc! {"$inc": {"epoch": 1_i64}})
                    .session(&mut *session).await.map_err(errors::failed("register validation scope"))?;
                metadata.update_one(doc! {"_id": &key}, doc! {"$setOnInsert": {"epoch": 0_i64, "scope": scope.as_str()}})
                    .upsert(true).session(&mut *session).await.map_err(errors::failed("initialize validation scope"))?;
                Ok(())
            })?;
        }
        self.validation_scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
        Ok(())
    }

    /// Ordinary mutations lock only their tenant. Collection-wide schema
    /// changes lock registration and every admitted tenant in one transaction.
    pub(crate) async fn hold_validation(
        &self,
        scope: Option<&Scope>,
        session: &mut ClientSession,
    ) -> Result<()> {
        let metadata = self.raw("_tsd_validated_batch");
        if let Some(scope) = scope {
            let key = format!("scope:{}", scope.as_str());
            let held = metadata
                .update_one(doc! {"_id": key}, doc! {"$inc": {"epoch": 1_i64}})
                .session(session)
                .await
                .map_err(errors::failed("hold document validation metadata"))?;
            if held.matched_count != 1 {
                return Err(StorageError::unavailable(
                    "document validation metadata is missing; retry",
                ));
            }
        } else {
            metadata
                .update_one(doc! {"_id": "documents"}, doc! {"$inc": {"epoch": 1_i64}})
                .session(&mut *session)
                .await
                .map_err(errors::failed("hold validation registration"))?;
            metadata
                .update_many(
                    doc! {"scope": {"$exists": true}},
                    doc! {"$inc": {"epoch": 1_i64}},
                )
                .session(session)
                .await
                .map_err(errors::failed("hold all document validation scopes"))?;
        }
        Ok(())
    }

    /// Milliseconds since the Unix epoch, from the configured clock.
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// The raw Mongo collection behind a driver or port collection name.
    /// Only collection-wide bookkeeping (specs, indexes) uses this directly.
    pub(crate) fn raw(&self, name: &str) -> Collection<Document> {
        self.db.collection(name)
    }

    /// The scoped handle to a port or driver collection.
    pub(crate) fn scoped(&self, name: &str, scope: &Scope) -> ScopedCollection {
        ScopedCollection::new(self.raw(name), scope.clone())
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("transactions", &self.transactions)
            .finish_non_exhaustive()
    }
}

/// A [`StorageBackend`] on MongoDB.
///
/// Every scope shares the database; each record carries a `_scope` field that
/// every query, write and index includes.
///
/// ```no_run
/// # async fn demo() -> tinystoragedrivers_core::Result<()> {
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_mongodb::MongoStorage;
///
/// let storage = MongoStorage::connect("mongodb://db.internal/openhuman", "openhuman").await?;
/// let alice = storage.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "mongodb");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MongoStorage {
    pub(crate) shared: Arc<Shared>,
}

impl MongoStorage {
    /// Connect to `uri` and use the database `database`.
    ///
    /// Connecting runs `hello` once, so an unreachable server fails here, and
    /// its reply decides whether the backend reports
    /// [`Capability::Transactions`] (replica sets and sharded clusters only).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for a malformed URI or database name,
    /// [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable)
    /// when no server answers. Messages never include the URI.
    pub async fn connect(uri: &str, database: &str) -> Result<Self> {
        validate_mongo_database(database)?;
        let mut options = ClientOptions::parse(uri).await.map_err(|error| {
            let mapped = errors::map(error, "parse the connection string");
            if mapped.is_retryable() {
                mapped
            } else {
                StorageError::invalid_input("the MongoDB connection string is malformed")
            }
        })?;
        if options.app_name.is_none() {
            options.app_name = Some("tinystoragedrivers".to_owned());
        }
        let client =
            Client::with_options(options).map_err(errors::failed("configure the client"))?;
        let hello = client
            .database("admin")
            .run_command(doc! {"hello": 1})
            .await
            .map_err(errors::failed("reach the server"))?;
        // Admission and payload reads must observe the primary.
        let db = client.database_with_options(
            database,
            mongodb::options::DatabaseOptions::builder()
                .selection_criteria(mongodb::options::SelectionCriteria::ReadPreference(
                    mongodb::options::ReadPreference::Primary,
                ))
                .build(),
        );
        let shared = Shared::new(
            client,
            db,
            Arc::new(system_clock),
            supports_transactions(&hello),
            supports_transactions(&hello),
        );
        Ok(Self {
            shared: Arc::new(shared),
        })
    }

    /// Read time from `clock` (milliseconds since the Unix epoch) instead of
    /// the system clock, so document expiry can be driven deterministically.
    #[must_use]
    pub fn with_clock(self, clock: Clock) -> Self {
        self.rebuild(clock, self.shared.transactions)
    }

    /// Disable public transactional capabilities, as if the deployment were a
    /// standalone server: [`Capability::Transactions`] is not reported and
    /// [`atomic_batch`](tinystoragedrivers_core::DocumentStore::atomic_batch)
    /// returns `Unsupported`. Internal document and stream writes still use native
    /// metadata transactions when the server supports them, so these handles
    /// cannot bypass another handle's read validation. A true standalone
    /// deployment keeps its existing single-document writes.
    #[must_use]
    pub fn without_transactions(self) -> Self {
        let clock = Arc::clone(&self.shared.clock);
        self.rebuild(clock, false)
    }

    fn rebuild(&self, clock: Clock, transactions: bool) -> Self {
        let shared = Shared::new(
            self.shared.client.clone(),
            self.shared.db.clone(),
            clock,
            transactions,
            self.shared.physical_transactions,
        )
        .with_existing(self.shared.existing);
        Self {
            shared: Arc::new(shared),
        }
    }

    /// Remove the documents of `collection` in `scope` whose expiry time has
    /// passed. Reads already treat them as absent; this reclaims the space.
    /// Their versions are remembered, so a recreated id continues from them.
    ///
    /// # Errors
    ///
    /// An invalid collection name, or a backend error.
    pub async fn sweep_expired(&self, scope: &Scope, collection: &str) -> Result<u64> {
        self.sweep_expired_generation(
            scope,
            tinystoragedrivers_core::ScopeGeneration::default(),
            collection,
        )
        .await
    }

    /// Remove the blobs of `scope` whose lifetime has passed. Reads already
    /// treat them as absent (and reclaim the one they touch); this reclaims the
    /// rest, for a periodic sweeper. Returns how many files were removed.
    ///
    /// # Errors
    ///
    /// A backend error.
    pub async fn sweep_expired_blobs(&self, scope: &Scope) -> Result<u64> {
        self.sweep_expired_blobs_generation(
            scope,
            tinystoragedrivers_core::ScopeGeneration::default(),
        )
        .await
    }

    /// [`sweep_expired_blobs`](Self::sweep_expired_blobs) through an explicitly
    /// issued scope generation, for a scope that has been reset.
    ///
    /// # Errors
    ///
    /// A retired generation, a pending reset, or a backend error.
    pub async fn sweep_expired_blobs_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Result<u64> {
        crate::purge::check(&self.shared, scope, generation, None).await?;
        MongoBlobs::new(Arc::clone(&self.shared), scope.clone(), false)
            .sweep_expired()
            .await
    }

    /// Reclaim expired documents through an explicitly issued scope generation.
    ///
    /// # Errors
    /// Invalid collection, retired generation, pending reset or backend failure.
    pub async fn sweep_expired_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
        collection: &str,
    ) -> Result<u64> {
        crate::purge::check(&self.shared, scope, generation, None).await?;
        validate_collection(collection)?;
        let docs = MongoDocuments::new(Arc::clone(&self.shared), scope.clone(), None);
        if !self.shared.physical_transactions {
            return docs.sweep(collection, None).await;
        }
        self.shared.prepare_validation(Some(scope)).await?;
        crate::fence::in_write!(&self.shared, None, session => {
            self.shared.hold_validation(Some(scope), &mut *session).await?;
            crate::purge::check(&self.shared, scope, generation, Some(&mut *session)).await?;
            self.shared.spec_in_session(collection, &mut *session).await?;
            docs.sweep(collection, Some(session)).await
        })
    }
}

impl MongoStorage {
    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fencing>>) -> ScopedStorage {
        self.handles_generation(
            scope,
            fence,
            tinystoragedrivers_core::ScopeGeneration::default(),
        )
    }

    fn handles_generation(
        &self,
        scope: &Scope,
        fence: Option<&Arc<Fencing>>,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                MongoDocuments::new(Arc::clone(&self.shared), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
            Arc::new(
                MongoStreams::new(Arc::clone(&self.shared), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
            Arc::new(
                MongoBlobs::new(Arc::clone(&self.shared), scope.clone(), fence.is_some())
                    .with_generation(generation),
            ),
        )
        .with_purge(Arc::new(crate::purge::MongoPurge {
            shared: Arc::clone(&self.shared),
            scope: scope.clone(),
            generation,
            fenced: fence.is_some(),
            #[cfg(test)]
            fail_after_chunks: false,
        }))
    }
}

impl fmt::Debug for MongoStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoStorage")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StorageBackend for MongoStorage {
    fn driver(&self) -> &'static str {
        "mongodb"
    }

    fn capabilities(&self) -> Capabilities {
        let base = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Ttl)
            .with(Capability::CollectionEnumeration)
            .with(Capability::ExistingSchema)
            .with(Capability::CollectionValidation)
            .with(Capability::BlobTtl)
            .with(Capability::ScopeEnumeration)
            .with(Capability::StreamSeal)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas);
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
                .with(Capability::ScopePurge)
        } else {
            base
        }
    }

    fn supports_scope_purge_binding_validation(&self) -> bool {
        self.shared.transactions
    }

    fn supports_scope_purge_generation_validation(&self) -> bool {
        self.shared.transactions
    }

    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        crate::scopes::list(&self.shared, prefix).await
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let shared = Shared::new(
            self.shared.client.clone(),
            self.shared.db.clone(),
            Arc::clone(&self.shared.clock),
            self.shared.transactions,
            self.shared.physical_transactions,
        )
        .with_existing(true);
        Ok(Arc::new(Self {
            shared: Arc::new(shared),
        }))
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.shared.existing {
            return Err(StorageError::unsupported(
                Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        for name in [
            "_tsd_validated_batch".to_owned(),
            crate::purge::RECEIPTS.to_owned(),
            crate::purge::MANIFEST.to_owned(),
            STREAMS.to_owned(),
            STREAM_HEADS.to_owned(),
            format!("{BLOBS}.files"),
            format!("{BLOBS}.chunks"),
        ] {
            if !self.shared.collection_exists(&name).await? {
                self.shared
                    .db
                    .create_collection(name.clone())
                    .await
                    .map_err(errors::failed("prepare internal collection"))?;
            }
        }
        self.shared
            .prepare(STREAMS, crate::streams::segment_models())
            .await?;
        self.shared
            .prepare(
                &format!("{BLOBS}.files"),
                vec![
                    crate::blobs::file_index(),
                    crate::blobs::standard_file_index(),
                ],
            )
            .await?;
        self.shared
            .prepare(
                &format!("{BLOBS}.chunks"),
                vec![crate::blobs::chunk_index()],
            )
            .await
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Result<ScopedStorage> {
        if !self.shared.physical_transactions && generation.0 != 0 {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "standalone cannot bind reset generations",
            ));
        }
        Ok(self.handles_generation(scope, None, generation))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fencing needs transactions, which this MongoDB deployment lacks",
            ));
        }
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                shared: Arc::clone(&self.shared),
            })),
        ))
    }

    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        self.for_scope_fenced(scope, fence)?;
        Ok(self.handles_generation(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                shared: Arc::clone(&self.shared),
            })),
            generation,
        ))
    }
}

impl MongoStorage {
    /// Test-only: the same client on a separate MongoDB database named after
    /// `name`, so live tests do not share collections. Kept out of the
    /// production API; a deployment connects to one database.
    #[cfg(test)]
    pub(crate) fn fixture_named(&self, name: &str) -> Result<Self> {
        let database = format!("tsd_fx_{:016x}", crate::naming::fnv1a(name.as_bytes()));
        validate_mongo_database(&database)?;
        Ok(Self {
            shared: Arc::new(
                Shared::new(
                    self.shared.client.clone(),
                    self.shared.client.database(&database),
                    Arc::clone(&self.shared.clock),
                    self.shared.transactions,
                    self.shared.physical_transactions,
                )
                .with_existing(self.shared.existing),
            ),
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "generation_tests.rs"]
mod generation_tests;
