//! [`BlobStore`] for the file driver: raw bytes plus a JSON sidecar.
//!
//! `<key>` holds the bytes exactly as given, so a blob opens in any tool, and
//! `<key>.meta.json` holds `{"key", "content_type"}`: the real key (file names
//! may be hashed) and the content type. The length is the data file's size.
//!
//! A put writes the bytes, then the sidecar, each atomically; a delete removes
//! the sidecar first. The sidecar decides existence, so a crash mid-put leaves
//! either the old blob, or (when replacing) the new bytes under the old
//! content type, never a blob without bytes.
//!
//! A blob written with a lifetime records `expires_at_ms` in its sidecar (the
//! database clock, milliseconds since the epoch). From that instant every read
//! treats it as absent; its files are removed by the next `delete` or write of
//! the key.

use crate::storage::Fencing;

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, Capabilities, Capability, Result, Scope, ScopeGeneration,
    StorageError, clamp_range, is_reserved_blob_key, validate_blob_key, validate_user_blob_key,
    validate_user_blob_prefix,
};

use crate::documents::guard;
use crate::encode::file_stem;
use crate::fsio::{
    files_with_suffix, io_error, open_read, read_json, read_optional, remove_optional,
    write_atomic, write_json,
};
use crate::storage::Db;

const META_SUFFIX: &str = ".meta.json";

/// A blob's sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Sidecar {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    /// The instant the blob stops existing; absent for a permanent blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at_ms: Option<u64>,
}

impl Sidecar {
    fn expired(&self, now: u64) -> bool {
        self.expires_at_ms.is_some_and(|at| at <= now)
    }
}

/// File-backed blobs bound to one scope.
#[derive(Debug, Clone)]
pub struct FileBlobs {
    db: Arc<Db>,
    scope: Scope,
    /// Checked under the database lock before every write, when set.
    fence: Option<Arc<Fencing>>,
    generation: ScopeGeneration,
}

/// The paths of one blob.
struct Paths {
    data: PathBuf,
    meta: PathBuf,
    /// The database clock when the operation started.
    now: u64,
}

impl FileBlobs {
    pub(crate) fn new(db: Arc<Db>, scope: Scope, fence: Option<Arc<Fencing>>) -> Self {
        Self {
            db,
            scope,
            fence,
            generation: ScopeGeneration::default(),
        }
    }

    pub(crate) fn with_generation(mut self, generation: ScopeGeneration) -> Self {
        self.generation = generation;
        self
    }

    fn dir(db: &Db, scope: &Scope) -> PathBuf {
        db.scope_dir(scope).join("blobs")
    }

    /// Remove every lapsed blob of this scope and return how many went.
    ///
    /// Reads already treat a lapsed blob as absent and reclaim the one they
    /// touch; this reclaims the rest, for a periodic sweeper.
    ///
    /// # Errors
    ///
    /// A filesystem error.
    pub async fn sweep_expired(&self) -> Result<u64> {
        let (scope, fence, generation) = (self.scope.clone(), self.fence.clone(), self.generation);
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                guard(db, fence.as_deref())?;
                let dir = Self::dir(db, &scope);
                let now = db.now();
                let mut swept = 0;
                for path in files_with_suffix(&dir, META_SUFFIX)? {
                    let Some(sidecar) = read_json::<Sidecar>(&path)? else {
                        continue;
                    };
                    if sidecar.expired(now) {
                        remove_optional(&dir.join(file_stem(&sidecar.key)))?;
                        remove_optional(&path)?;
                        swept += 1;
                    }
                }
                Ok(swept)
            })
            .await
    }

    /// Run `work` under the database lock with this key's paths.
    async fn with<T, F>(&self, key: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        self.run(key, None, work).await
    }

    /// [`Self::with`] for a write: the fence (if any) is checked first,
    /// under the same lock.
    async fn write<T, F>(&self, key: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        self.run(key, self.fence.clone(), work).await
    }

    async fn run<T, F>(&self, key: &str, fence: Option<Arc<Fencing>>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths, &str) -> Result<T> + Send + 'static,
    {
        validate_user_blob_key(key)?;
        let (scope, key, generation) = (self.scope.clone(), key.to_owned(), self.generation);
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                guard(db, fence.as_deref())?;
                work(&Self::paths(db, &scope, &key), &key)
            })
            .await
    }

    fn paths(db: &Db, scope: &Scope, key: &str) -> Paths {
        let dir = Self::dir(db, scope);
        let stem = file_stem(key);
        Paths {
            data: dir.join(&stem),
            meta: dir.join(format!("{stem}{META_SUFFIX}")),
            now: db.now(),
        }
    }
}

/// The blob's metadata, `None` when it does not exist.
fn head(paths: &Paths, key: &str) -> Result<Option<BlobMeta>> {
    match read_json::<Sidecar>(&paths.meta)? {
        Some(sidecar) if sidecar.key == key && sidecar.expired(paths.now) => {
            // Reclaim on read: the database lock is held, so no writer can
            // have replaced the lapsed blob between the check and the removal.
            // Failure only delays the reclaim; the blob already reads absent.
            // The data file goes first: if that fails the sidecar stays, so a
            // later read or sweep can retry instead of orphaning the data.
            if remove_optional(&paths.data).is_ok() {
                let _ = remove_optional(&paths.meta);
            }
            Ok(None)
        }
        Some(sidecar) if sidecar.key == key => meta(&paths.data, sidecar),
        Some(_) => Err(collision()),
        None => Ok(None),
    }
}

/// Combine a sidecar with its data file's size; `None` when the data file is
/// missing.
fn meta(data: &Path, sidecar: Sidecar) -> Result<Option<BlobMeta>> {
    // `symlink_metadata` does not follow a link, so a planted symlink or
    // directory is refused here exactly as `get` refuses it.
    match std::fs::symlink_metadata(data).and_then(|stat| {
        if stat.file_type().is_file() {
            Ok(stat)
        } else {
            Err(std::io::Error::other("not a regular file"))
        }
    }) {
        Ok(stat) => Ok(Some(BlobMeta {
            key: sidecar.key,
            len: stat.len(),
            content_type: sidecar.content_type,
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read a blob's size")(error)),
    }
}

fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to touch another blob")
}

/// Write the blob at `paths`: the bytes, then the sidecar that makes it exist.
fn store_in(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
    content_type: Option<String>,
    ttl: Option<Duration>,
) -> Result<BlobMeta> {
    let expires_at_ms = ttl.map(|ttl| {
        paths
            .now
            .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
    });
    if let Some(existing) = read_json::<Sidecar>(&paths.meta)?
        && existing.key != key
    {
        return Err(collision());
    }
    write_atomic(&paths.data, bytes)?;
    let sidecar = Sidecar {
        key: key.to_owned(),
        content_type,
        expires_at_ms,
    };
    write_json(&paths.meta, &sidecar)?;
    Ok(BlobMeta {
        key: sidecar.key,
        len: bytes.len() as u64,
        content_type: sidecar.content_type,
    })
}

/// Remove the blob at `paths`, reporting whether a live one existed.
fn delete_in(paths: &Paths, key: &str) -> Result<bool> {
    // A lapsed blob is not there to delete, but its files go too.
    let lapsed = matches!(
        read_json::<Sidecar>(&paths.meta)?,
        Some(sidecar) if sidecar.key == key && sidecar.expired(paths.now)
    );
    if !lapsed && head(paths, key)?.is_none() {
        return Ok(false);
    }
    remove_optional(&paths.meta)?;
    remove_optional(&paths.data)?;
    Ok(!lapsed)
}

/// The blob operations the stream sealing steps run under the database lock
/// they already hold. They skip the fence check: the stream write that calls
/// them has made it.
impl FileBlobs {
    /// Store `bytes` as blob `key` of `scope`.
    pub(crate) fn put_locked(
        db: &Db,
        scope: &Scope,
        key: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<()> {
        validate_blob_key(key)?;
        store_in(
            &Self::paths(db, scope, key),
            key,
            bytes,
            Some(content_type.to_owned()),
            None,
        )
        .map(|_| ())
    }

    /// The bytes of blob `key` of `scope`, `None` when absent.
    pub(crate) fn get_locked(db: &Db, scope: &Scope, key: &str) -> Result<Option<Vec<u8>>> {
        let paths = Self::paths(db, scope, key);
        if head(&paths, key)?.is_none() {
            return Ok(None);
        }
        read_optional(&paths.data)
    }

    /// The size of blob `key` of `scope`, `None` when absent.
    pub(crate) fn len_locked(db: &Db, scope: &Scope, key: &str) -> Result<Option<u64>> {
        Ok(head(&Self::paths(db, scope, key), key)?.map(|meta| meta.len))
    }

    /// Remove blob `key` of `scope`.
    pub(crate) fn delete_locked(db: &Db, scope: &Scope, key: &str) -> Result<bool> {
        delete_in(&Self::paths(db, scope, key), key)
    }
}

impl FileBlobs {
    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
    ) -> Result<BlobMeta> {
        let content_type = content_type.map(str::to_owned);
        self.write(key, move |paths, key| {
            store_in(paths, key, &bytes, content_type, ttl)
        })
        .await
    }
}

#[async_trait]
impl BlobStore for FileBlobs {
    async fn sweep_expired(&self) -> Result<u64> {
        FileBlobs::sweep_expired(self).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::none().with(Capability::BlobTtl)
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None).await
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl)).await
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.with(key, |paths, key| {
            let Some(meta) = head(paths, key)? else {
                return Ok(None);
            };
            Ok(read_optional(&paths.data)?.map(|bytes| Blob {
                meta: BlobMeta {
                    len: bytes.len() as u64,
                    ..meta
                },
                bytes,
            }))
        })
        .await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.with(key, move |paths, key| {
            let Some(meta) = head(paths, key)? else {
                return Ok(None);
            };
            let len = usize::try_from(meta.len).unwrap_or(usize::MAX);
            let range = clamp_range(&range, len)?;
            let read_error = io_error("read a blob");
            let mut file = open_read(&paths.data).map_err(&read_error)?;
            file.seek(SeekFrom::Start(range.start as u64))
                .map_err(&read_error)?;
            let mut bytes = Vec::with_capacity(range.len());
            file.take(range.len() as u64)
                .read_to_end(&mut bytes)
                .map_err(&read_error)?;
            Ok(Some(bytes))
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.with(key, head).await
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        self.write(key, delete_in).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_user_blob_prefix(prefix)?;
        let (scope, prefix, generation) = (self.scope.clone(), prefix.to_owned(), self.generation);
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                let dir = Self::dir(db, &scope);
                let now = db.now();
                let mut out = Vec::new();
                for path in files_with_suffix(&dir, META_SUFFIX)? {
                    let Some(sidecar) = read_json::<Sidecar>(&path)? else {
                        continue;
                    };
                    if sidecar.expired(now) {
                        // Sidecar last, and only once the payload is gone, so a
                        // failed removal stays discoverable for the next pass.
                        if remove_optional(&dir.join(file_stem(&sidecar.key))).is_ok() {
                            let _ = remove_optional(&path);
                        }
                        continue;
                    }
                    if !sidecar.key.starts_with(&prefix) || is_reserved_blob_key(&sidecar.key) {
                        continue;
                    }
                    let data = dir.join(file_stem(&sidecar.key));
                    if let Some(meta) = meta(&data, sidecar)? {
                        out.push(meta);
                    }
                }
                out.sort_by(|a, b| a.key.cmp(&b.key));
                Ok(out)
            })
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
