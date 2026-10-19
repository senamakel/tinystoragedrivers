//! [`FileStorage`], the backend, and the state every port handle shares.
//!
//! # Layout
//!
//! ```text
//! <root>/
//! ├── _meta/collections/<collection>.json     collection declarations
//! ├── scopes/<scope>/
//! │   ├── docs/<collection>/<id>.json         {"id", "version", "doc"}
//! │   ├── streams/<name>.jsonl                {"offset", "value"} per line
//! │   ├── streams/<name>.meta.json            {"name", "base"}
//! │   ├── blobs/<key>                         raw bytes
//! │   └── blobs/<key>.meta.json               {"key", "content_type"}
//! └── databases/<name>/                       the same layout per named database
//! ```
//!
//! Names are encoded by the `encode` module. Scopes live under `scopes/`, so
//! none can shadow the `_meta` directory.
//!
//! # Concurrency
//!
//! Every directory has one native mutex shared by independently reopened
//! handles. Guard validation and the write it fences run under that one lock,
//! so `claim` and fenced writes are atomic. The file
//! IO runs on tokio's blocking pool when a runtime is present. Several
//! *processes* writing the same directory are not coordinated: each file
//! replacement is atomic, but a compare-and-swap, claim or fenced write can
//! race another process.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, PurgeMode, PurgeReceipt, PurgeRequest, PurgeStage,
    PurgeStatus, Result, Scope, ScopeGeneration, ScopePurge, ScopedStorage, StorageBackend,
    StorageError,
};

use crate::blobs::FileBlobs;
use crate::documents::FileDocuments;
use crate::encode::dir_components;
use crate::fsio::{io_error, read_json, write_json};
use crate::scopes;
use crate::streams::FileStreams;

/// What this driver provides: expiry, full-text search, fencing and scope
/// listing. Not maintenance (plain files have nothing to reclaim or flush) and not
/// transactions, because a batch spanning several files cannot be made
/// crash-atomic with renames alone. Fencing holds only among the handles of
/// one process, like every other guarantee here: the fence is checked under
/// the process-wide native gate.
pub(crate) const CAPABILITIES: Capabilities = Capabilities::none()
    .with(Capability::Ttl)
    .with(Capability::RecordMetadata)
    .with(Capability::FullText)
    .with(Capability::Fencing)
    .with(Capability::ConditionalStreamAppend)
    .with(Capability::CollectionEnumeration)
    .with(Capability::ExistingSchema)
    .with(Capability::CollectionValidation)
    .with(Capability::BlobTtl)
    .with(Capability::ScopeEnumeration)
    .with(Capability::ScopePurge)
    .with(Capability::StreamSeal);

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// One native lock per canonical storage directory in this process.
fn lock_for(dir: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(locks.entry(dir.to_path_buf()).or_default())
}

/// A validated fence bound by the host when creating a handle.
#[derive(Debug)]
pub(crate) struct Fencing {
    pub(crate) fence: Fence,
}

/// The storage directory and the lock guarding it.
pub(crate) struct Db {
    dir: PathBuf,
    lock: Arc<Mutex<()>>,
    clock: Clock,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FilePurgeState {
    generation: u64,
    #[serde(default)]
    pending: Option<PurgeReceipt>,
    #[serde(default)]
    receipts: HashMap<String, PurgeReceipt>,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl Db {
    fn new(dir: PathBuf, clock: Clock) -> Self {
        let lock = lock_for(&dir);
        Self { dir, lock, clock }
    }

    /// Milliseconds since the Unix epoch, from the injected clock.
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// Where a collection's declaration is kept.
    pub(crate) fn specs_dir(&self) -> PathBuf {
        self.dir.join("_meta").join("collections")
    }

    /// The directory holding every scope's data.
    pub(crate) fn scopes_dir(&self) -> PathBuf {
        self.dir.join("scopes")
    }

    /// The directory holding one scope's data.
    pub(crate) fn scope_dir(&self, scope: &Scope) -> PathBuf {
        self.scopes_dir().join(dir_components(scope.as_str()))
    }

    fn purge_state_path(&self, scope: &Scope) -> PathBuf {
        self.dir
            .join("_meta")
            .join("purges")
            .join(dir_components(scope.as_str()))
            .with_extension("json")
    }

    fn purge_state(&self, scope: &Scope) -> Result<FilePurgeState> {
        Ok(read_json(&self.purge_state_path(scope))?.unwrap_or_default())
    }

    fn save_purge_state(&self, scope: &Scope, state: &FilePurgeState) -> Result<()> {
        write_json(&self.purge_state_path(scope), state)
    }

    pub(crate) fn check_generation(&self, scope: &Scope, expected: ScopeGeneration) -> Result<()> {
        let state = self.purge_state(scope)?;
        if state.pending.is_some() || state.generation != expected.0 {
            return Err(StorageError::new(
                tinystoragedrivers_core::ErrorKind::ScopeGenerationRejected,
                "scope writer generation has been revoked",
            ));
        }
        Ok(())
    }

    /// Hold the directory lock. It guards no in-memory state (everything is on
    /// disk and every file replacement is atomic), so a poisoned lock is
    /// simply taken over.
    fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `work` under the directory lock, on the blocking pool when called
    /// from inside a tokio runtime, inline otherwise.
    pub(crate) async fn run<T, F>(self: &Arc<Self>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Db) -> Result<T> + Send + 'static,
    {
        let db = Arc::clone(self);
        let job = move || {
            let _guard = db.lock();
            work(&db)
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn_blocking(job).await.map_err(|error| {
                StorageError::backend("file storage task did not complete").with_source(error)
            })?,
            Err(_) => job(),
        }
    }
}

/// A [`StorageBackend`] that keeps everything as plain files under one
/// directory: JSON per document, JSONL per stream, raw bytes per blob.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use serde_json::json;
/// use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};
/// use tinystoragedrivers_file::FileStorage;
///
/// let dir = tempfile::tempdir().unwrap();
/// let storage = FileStorage::open(dir.path())?;
/// let local = storage.for_scope(&Scope::local())?;
/// local.documents().put("notes", "n1", json!({"text": "hi"}), Precondition::Absent).await?;
///
/// let reopened = FileStorage::open(dir.path())?.for_scope(&Scope::local())?;
/// assert!(reopened.documents().get("notes", "n1").await?.is_some());
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct FileStorage {
    pub(crate) db: Arc<Db>,
    existing: bool,
}

impl fmt::Debug for FileStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStorage")
            .field("dir", &self.db.dir)
            .finish_non_exhaustive()
    }
}

impl FileStorage {
    /// Open (creating if needed) a file store rooted at `root`, reading the
    /// system clock for document expiry.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend)
    /// when the directory cannot be created or resolved.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_clock(root, Arc::new(system_clock))
    }

    /// [`FileStorage::open`], reading time from `clock` (Unix epoch
    /// milliseconds) so tests can drive expiry deterministically.
    ///
    /// # Errors
    ///
    /// As [`FileStorage::open`].
    pub fn open_with_clock(root: impl Into<PathBuf>, clock: Clock) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(io_error("create the storage directory"))?;
        // Resolve symlinks and relative paths so two handles on one directory
        // share a lock however they spelled it.
        let root = root
            .canonicalize()
            .map_err(io_error("resolve the storage directory"))?;
        let db = Arc::new(Db::new(root, clock));
        Ok(Self {
            db,
            existing: false,
        })
    }

    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fencing>>) -> ScopedStorage {
        self.handles_generation(scope, fence, ScopeGeneration::default())
    }

    fn handles_generation(
        &self,
        scope: &Scope,
        fence: Option<&Arc<Fencing>>,
        generation: ScopeGeneration,
    ) -> ScopedStorage {
        let ports = ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                FileDocuments::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    fence.cloned(),
                    self.existing,
                )
                .with_generation(generation),
            ),
            Arc::new(
                FileStreams::new(Arc::clone(&self.db), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
            Arc::new(
                FileBlobs::new(Arc::clone(&self.db), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
        );
        ports.with_purge(Arc::new(FilePurge {
            db: Arc::clone(&self.db),
            scope: scope.clone(),
            generation,
            fence: fence.cloned(),
        }))
    }

    /// Remove the blobs of `scope` whose lifetime has passed and return how
    /// many went. Reads already hide them and reclaim the one they touch; this
    /// reclaims the rest, for a periodic sweeper.
    ///
    /// # Errors
    ///
    /// A filesystem error.
    pub async fn sweep_expired_blobs(&self, scope: &Scope) -> Result<u64> {
        let scope_for_state = scope.clone();
        let generation = self
            .db
            .run(move |db| {
                let state = db.purge_state(&scope_for_state)?;
                if state.pending.is_some() {
                    return Err(StorageError::scope_generation_rejected(
                        "scope purge is pending",
                    ));
                }
                Ok(ScopeGeneration(state.generation))
            })
            .await?;
        FileBlobs::new(Arc::clone(&self.db), scope.clone(), None)
            .with_generation(generation)
            .sweep_expired()
            .await
    }

    /// The directory this store keeps its data in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.db.dir
    }
}

#[derive(Debug)]
struct FilePurge {
    db: Arc<Db>,
    scope: Scope,
    generation: ScopeGeneration,
    fence: Option<Arc<Fencing>>,
}

#[async_trait::async_trait]
impl ScopePurge for FilePurge {
    fn supports_binding_validation(&self) -> bool {
        true
    }

    fn supports_generation_validation(&self) -> bool {
        true
    }

    async fn validate_generation(&self) -> Result<()> {
        let scope = self.scope.clone();
        let generation = self.generation;
        self.db
            .run(move |db| db.check_generation(&scope, generation))
            .await
    }

    async fn validate_binding(&self) -> Result<()> {
        let scope = self.scope.clone();
        let generation = self.generation;
        let fence = self.fence.clone();
        self.db
            .run(move |db| {
                crate::documents::guard(db, fence.as_deref())?;
                db.check_generation(&scope, generation)
            })
            .await
    }

    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if request.mode == PurgeMode::Atomic {
            return Err(StorageError::unsupported(
                Capability::AtomicScopePurge,
                "file storage cannot atomically purge a scope",
            ));
        }
        let scope = self.scope.clone();
        let operation_id = request.operation_id.clone();
        let generation = self.generation;
        let fence = self.fence.clone();
        self.db
            .run(move |db| {
                crate::documents::guard(db, fence.as_deref())?;
                let mut state = db.purge_state(&scope)?;
                if let Some(receipt) = state.receipts.get(&operation_id) {
                    return Ok(receipt.clone());
                }
                if let Some(pending) = &state.pending {
                    if pending.operation_id != operation_id {
                        return Err(StorageError::unavailable("another scope purge is pending"));
                    }
                } else {
                    db.check_generation(&scope, generation)?;
                    let next = generation.0.checked_add(1).ok_or_else(|| {
                        StorageError::backend("scope generation space is exhausted")
                    })?;
                    state.pending = Some(PurgeReceipt {
                        operation_id: operation_id.clone(),
                        generation: ScopeGeneration(next),
                        status: PurgeStatus::Pending,
                        stage: PurgeStage::Records,
                    });
                    db.save_purge_state(&scope, &state)?;
                }
                let path = db.scope_dir(&scope);
                if path.exists() {
                    std::fs::remove_dir_all(&path).map_err(io_error("remove purged scope"))?;
                }
                let generation = state
                    .pending
                    .as_ref()
                    .map(|receipt| receipt.generation)
                    .ok_or_else(|| StorageError::backend("purge state lost its pending receipt"))?;
                let receipt = PurgeReceipt {
                    operation_id,
                    generation,
                    status: PurgeStatus::Complete,
                    stage: PurgeStage::Complete,
                };
                state.generation = receipt.generation.0;
                state.pending = None;
                state
                    .receipts
                    .insert(receipt.operation_id.clone(), receipt.clone());
                db.save_purge_state(&scope, &state)?;
                Ok(receipt)
            })
            .await
    }

    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        let scope = self.scope.clone();
        let operation_id = operation_id.to_owned();
        let fence = self.fence.clone();
        self.db
            .run(move |db| {
                crate::documents::guard(db, fence.as_deref())?;
                let state = db.purge_state(&scope)?;
                if state
                    .pending
                    .as_ref()
                    .is_some_and(|receipt| receipt.operation_id == operation_id)
                {
                    return Ok(state.pending);
                }
                Ok(state.receipts.get(&operation_id).cloned())
            })
            .await
    }
}

#[async_trait::async_trait]
impl StorageBackend for FileStorage {
    fn driver(&self) -> &'static str {
        "file"
    }

    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    fn supports_scope_purge_binding_validation(&self) -> bool {
        true
    }

    fn supports_scope_purge_generation_validation(&self) -> bool {
        true
    }

    /// Lists the scopes that have a stored file by walking `scopes/`. A walk
    /// under the database lock, so it sees whole writes only.
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let prefix = prefix.to_owned();
        self.db.run(move |db| scopes::list(db, &prefix)).await
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                tinystoragedrivers_core::Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        Ok(())
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let mut storage = self.clone();
        storage.existing = true;
        Ok(Arc::new(storage))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<ScopedStorage> {
        Ok(self.handles_generation(scope, None, generation))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
            })),
        ))
    }

    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles_generation(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
            })),
            generation,
        ))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;
