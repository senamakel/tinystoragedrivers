//! The SQLite [`StorageBackend`]: file location and handle construction.
//!
//! One storage is one database file. A path ending in `.db` or `.sqlite` is
//! that file; any other path is a directory, and the database is
//! `<dir>/storage.db` (the directory is created if it is missing). Any scope
//! may be used; rows carry their scope.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use tinystoragedrivers_core::{
    Capabilities, Checkpoint, Fence, MaintenanceBudget, MaintenanceReport, Result, Scope,
    ScopedStorage, StorageBackend,
};

use crate::blobs::SqliteBlobs;
use crate::connection::Db;
use crate::documents::{SqliteDocuments, capabilities};
use crate::fence::Fencing;
use crate::maintenance;
use crate::native::SqliteNative;
use crate::scopes;
use crate::sql::Tables;
use crate::streams::SqliteStreams;

/// Milliseconds since the Unix epoch, as the driver sees them.
pub use tinystoragedrivers_core::Clock;

/// File name of the root database in directory mode.
pub const ROOT_FILE: &str = "storage.db";

/// A SQLite-backed [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_sqlite::SqliteStorage;
///
/// let dir = tempfile::tempdir().unwrap();
/// let storage = SqliteStorage::open(dir.path())?;
/// assert!(dir.path().join("storage.db").exists());
/// assert_eq!(storage.for_scope(&Scope::local())?.driver(), "sqlite");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct SqliteStorage {
    db: Arc<Db>,
    tables: Arc<Tables>,
    clock: Clock,
    existing: bool,
}

impl fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("path", &self.db.path())
            .finish_non_exhaustive()
    }
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether `path` names a single database file rather than a directory.
fn is_file_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("db") || ext.eq_ignore_ascii_case("sqlite"))
}

impl SqliteStorage {
    /// Open storage at `path` and create the driver's tables. A path ending
    /// in `.db` or `.sqlite` is the database file; any other path is a
    /// directory holding `storage.db`.
    ///
    /// # Errors
    ///
    /// When the directory or file cannot be created or opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = if is_file_path(path) {
            path.to_path_buf()
        } else {
            path.join(ROOT_FILE)
        };
        Self::assemble(&file, Arc::new(system_clock))
    }

    fn assemble(file: &Path, clock: Clock) -> Result<Self> {
        let db = Db::open(file)?;
        let tables = Arc::new(Tables::new());
        let create = Arc::clone(&tables);
        db.run_now(move |conn| create.ensure(conn))?;
        Ok(Self {
            db,
            tables,
            clock,
            existing: false,
        })
    }

    /// Read document and blob expiry time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Raw access to this database's file, for owners that keep their own
    /// tables beside (or instead of) the generic ones.
    #[must_use]
    pub fn native(&self) -> SqliteNative {
        SqliteNative::new(Arc::clone(&self.db))
    }

    fn handles(&self, scope: &Scope, fencing: Option<&Arc<Fencing>>) -> ScopedStorage {
        self.handles_generation(
            scope,
            fencing,
            tinystoragedrivers_core::ScopeGeneration::default(),
        )
    }
    fn handles_generation(
        &self,
        scope: &Scope,
        fencing: Option<&Arc<Fencing>>,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                SqliteDocuments::new(
                    Arc::clone(&self.db),
                    Arc::clone(&self.tables),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fencing.cloned(),
                    self.existing,
                )
                .with_generation(generation),
            ),
            Arc::new(
                SqliteStreams::new(
                    Arc::clone(&self.db),
                    Arc::clone(&self.tables),
                    scope.clone(),
                    fencing.cloned(),
                    Arc::clone(&self.clock),
                )
                .with_generation(generation),
            ),
            Arc::new(
                SqliteBlobs::new(
                    Arc::clone(&self.db),
                    Arc::clone(&self.tables),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fencing.cloned(),
                )
                .with_generation(generation),
            ),
        )
        .with_purge(Arc::new(crate::purge::SqlitePurge {
            db: Arc::clone(&self.db),
            tables: Arc::clone(&self.tables),
            scope: scope.clone(),
            generation,
            fencing: fencing.cloned(),
        }))
    }

    /// The database file this storage reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.db.path()
    }
}

#[tinystoragedrivers_core::async_trait]
impl StorageBackend for SqliteStorage {
    fn driver(&self) -> &'static str {
        "sqlite"
    }

    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    fn supports_scope_purge_binding_validation(&self) -> bool {
        true
    }

    fn supports_scope_purge_generation_validation(&self) -> bool {
        true
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.existing {
            return Err(tinystoragedrivers_core::StorageError::unsupported(
                tinystoragedrivers_core::Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        let tables = Arc::clone(&self.tables);
        self.db.run(move |conn| tables.ensure(conn)).await
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let inspect = Arc::clone(&self.tables);
        self.db.run_now(move |conn| inspect.validate(conn))?;
        let mut storage = self.clone();
        storage.existing = true;
        Ok(Arc::new(storage))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        let fencing = Arc::new(Fencing::new(fence.clone(), Arc::clone(&self.clock)));
        Ok(self.handles(scope, Some(&fencing)))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Result<ScopedStorage> {
        Ok(self.handles_generation(scope, None, generation))
    }
    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        let fencing = Arc::new(Fencing::new(fence.clone(), Arc::clone(&self.clock)));
        Ok(self.handles_generation(scope, Some(&fencing), generation))
    }

    /// Returns freed pages to the filesystem. It acts on the whole database file.
    /// The first call on a file made without `auto_vacuum` runs a one-time
    /// `VACUUM`, which rewrites the whole file, so it waits for a budget that
    /// is unlimited or at least the database size; a smaller bound reclaims
    /// nothing and leaves the conversion pending. `reclaimed_bytes` is how
    /// much the file shrank, so pages a blocked checkpoint has not copied yet
    /// count in the call that completes it (see the crate README).
    async fn maintain(&self, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
        self.db
            .run(move |conn| maintenance::maintain(conn, budget))
            .await
    }

    async fn checkpoint(&self, mode: Checkpoint) -> Result<()> {
        self.db
            .run(move |conn| maintenance::checkpoint(conn, mode))
            .await
    }

    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let tables = Arc::clone(&self.tables);
        let prefix = prefix.to_owned();
        self.db
            .run(move |conn| scopes::list(conn, &tables, &prefix))
            .await
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
