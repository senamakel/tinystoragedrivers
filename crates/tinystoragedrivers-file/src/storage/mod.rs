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

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, Result, Scope, ScopedStorage, StorageBackend,
    StorageError,
};

use crate::blobs::FileBlobs;
use crate::documents::FileDocuments;
use crate::encode::dir_components;
use crate::fsio::io_error;
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
    .with(Capability::BlobSweep)
    .with(Capability::ScopeEnumeration)
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
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(FileDocuments::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
                self.existing,
            )),
            Arc::new(FileStreams::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
            )),
            Arc::new(FileBlobs::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
            )),
        )
    }

    /// Remove the blobs of `scope` whose lifetime has passed and return how
    /// many went. Reads already hide them and reclaim the one they touch; this
    /// reclaims the rest, for a periodic sweeper.
    ///
    /// # Errors
    ///
    /// A filesystem error.
    pub async fn sweep_expired_blobs(&self, scope: &Scope) -> Result<u64> {
        FileBlobs::new(Arc::clone(&self.db), scope.clone(), None)
            .sweep_expired()
            .await
    }

    /// The directory this store keeps its data in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.db.dir
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

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
            })),
        ))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;
