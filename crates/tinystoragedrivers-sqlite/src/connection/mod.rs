//! One shared connection per database file.
//!
//! Handles opened on the same file with the same access mode share one [`Db`],
//! so writers queue on an in-process mutex instead of contending for SQLite's
//! file lock (which would surface as `SQLITE_BUSY`). Read-only connections use
//! a separate registry entry. Calls run on the tokio blocking pool when a
//! runtime is present, so a slow statement never stalls a runtime worker, and
//! inline otherwise.
//!
//! Another process opening the same file is still safe: WAL mode plus a busy
//! timeout make SQLite serialize writers, and lock contention past the timeout
//! is reported as retryable.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};
use tinystoragedrivers_core::{Result, StorageError};

use crate::error::during;

/// How long a statement waits for another process's write lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// An open database file.
pub(crate) struct Db {
    path: PathBuf,
    conn: Mutex<Connection>,
    read_only: bool,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.path)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

fn registry() -> &'static Mutex<HashMap<(PathBuf, bool), Weak<Db>>> {
    static OPEN: OnceLock<Mutex<HashMap<(PathBuf, bool), Weak<Db>>>> = OnceLock::new();
    OPEN.get_or_init(Mutex::default)
}

/// The absolute, symlink-resolved form of `path`, creating its directory.
fn normalize(path: &Path) -> Result<PathBuf> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    std::fs::create_dir_all(parent)
        .map_err(io_error("cannot create the sqlite database directory"))?;
    let parent = parent
        .canonicalize()
        .map_err(io_error("cannot resolve the sqlite database directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| StorageError::invalid_input("sqlite database path has no file name"))?;
    let joined = parent.join(name);
    // An existing file may be a symlink; key it by its target so every alias
    // shares one connection. A new file is created at the joined path.
    Ok(joined.canonicalize().unwrap_or(joined))
}

/// Resolve an existing database file without creating its parent or file.
fn normalize_existing(path: &Path) -> Result<PathBuf> {
    let resolved = path
        .canonicalize()
        .map_err(io_error("cannot resolve the existing sqlite database"))?;
    if !resolved.is_file() {
        return Err(StorageError::invalid_input(
            "existing sqlite database path is not a file",
        ));
    }
    Ok(resolved)
}

/// Map a filesystem failure while preparing the database location.
fn io_error(what: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::backend(what).with_source(error)
}

/// Map a blocking task that panicked or was cancelled.
fn join_error(error: tokio::task::JoinError) -> StorageError {
    StorageError::backend("sqlite call did not complete").with_source(error)
}

impl Db {
    /// Open (or share the already-open) database at `path`, creating it.
    pub(crate) fn open(path: &Path) -> Result<Arc<Self>> {
        Self::open_with_mode(normalize(path)?, false)
    }

    /// Open an existing database without creating files or changing its schema.
    pub(crate) fn open_read_only(path: &Path) -> Result<Arc<Self>> {
        Self::open_with_mode(normalize_existing(path)?, true)
    }

    fn open_with_mode(path: PathBuf, read_only: bool) -> Result<Arc<Self>> {
        let mut open = registry()
            .lock()
            .map_err(|_| StorageError::backend("sqlite connection registry lock poisoned"))?;
        let key = (path.clone(), read_only);
        if let Some(db) = open.get(&key).and_then(Weak::upgrade) {
            return Ok(db);
        }
        let conn = if read_only {
            Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
        } else {
            Connection::open(&path)
        }
        .map_err(during("open"))?;
        conn.busy_timeout(BUSY_TIMEOUT).map_err(during("open"))?;
        if !read_only {
            // Must come before anything else touches a new file: the setting
            // is fixed by the first table (and WAL mode writes the header).
            // It is inert on a file that already has tables;
            // `maintenance::maintain` converts those.
            conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")
                .map_err(during("open"))?;
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(during("open"))?;
            conn.pragma_update(None, "synchronous", "NORMAL")
                .map_err(during("open"))?;
        }
        let db = Arc::new(Self {
            path: path.clone(),
            conn: Mutex::new(conn),
            read_only,
        });
        open.retain(|_, weak| weak.strong_count() > 0);
        open.insert(key, Arc::downgrade(&db));
        Ok(db)
    }

    /// The file this database lives in.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Run `f` with exclusive use of the connection.
    pub(crate) async fn run<T, F>(self: &Arc<Self>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let db = Arc::clone(self);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle
                .spawn_blocking(move || db.run_now(f))
                .await
                .map_err(join_error)?,
            // Without a runtime the call runs here; catch a panic the same
            // way the blocking pool would, so callers see one contract.
            Err(_) => std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.run_now(f)))
                .map_err(|_| StorageError::backend("sqlite call did not complete"))?,
        }
    }

    pub(crate) fn run_now<T>(&self, f: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| StorageError::backend("sqlite connection lock poisoned"))?;
        f(&mut conn)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
