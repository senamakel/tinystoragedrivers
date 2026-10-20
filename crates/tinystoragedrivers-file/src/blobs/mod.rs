//! [`BlobStore`] for the file driver: raw bytes plus a JSON sidecar.
//!
//! `<key>` holds the bytes exactly as given, so a blob opens in any tool, and
//! `<key>.meta.json` holds the real key (file names may be hashed), content
//! type, and optional absolute expiry. The length is the data file's size.
//!
//! A put stages the bytes and sidecar, journals the old pair, then publishes
//! both with atomic replacements. The journal is the recovery record: an
//! interrupted replacement rolls back before any operation observes the pair.
//! A delete removes the sidecar first.
//!
//! A blob written with a lifetime records `expires_at_ms` in its sidecar (the
//! database clock, milliseconds since the epoch). From that instant every read
//! treats it as absent; its files are removed by the next `delete` or write of
//! the key.

use crate::storage::Fencing;

use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobSource, BlobStore, Capabilities, Capability, Result, Scope,
    ScopeGeneration, StorageError, clamp_range, is_reserved_blob_key, validate_blob_key,
    validate_user_blob_key, validate_user_blob_prefix,
};

use crate::documents::guard;
use crate::encode::{MAX_PLAIN, decode, encode, file_stem};
use crate::fsio::{
    files_with_suffix, io_error, open_read, read_json, read_optional, remove_optional,
    write_atomic, write_atomic_with, write_json,
};
use crate::storage::Db;

const META_SUFFIX: &str = ".meta.json";
const TRANSACTION_SUFFIX: &str = ".txn.json";

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

/// A pending replacement. The staged pair and old pair are durable before the
/// journal appears; `committed` becomes true only after both public files have
/// been atomically published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobJournal {
    key: String,
    token: String,
    staged_len: u64,
    staged_sidecar: Sidecar,
    had_data: bool,
    had_meta: bool,
    committed: bool,
}

struct TransactionPaths {
    marker: PathBuf,
    staged_data: PathBuf,
    staged_meta: PathBuf,
    backup_data: PathBuf,
    backup_meta: PathBuf,
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

fn transaction_paths(paths: &Paths, key: &str, token: &str) -> Result<TransactionPaths> {
    if !valid_transaction_token(token) {
        return Err(StorageError::backend("invalid blob transaction identity"));
    }
    let Some(parent) = paths.data.parent() else {
        return Err(StorageError::backend("blob path has no parent directory"));
    };
    let stem = file_stem(key);
    Ok(TransactionPaths {
        marker: parent.join(format!("{stem}{TRANSACTION_SUFFIX}")),
        staged_data: parent.join(format!("{stem}.txn-{token}.stage-data")),
        staged_meta: parent.join(format!("{stem}.txn-{token}.stage-meta")),
        backup_data: parent.join(format!("{stem}.txn-{token}.backup-data")),
        backup_meta: parent.join(format!("{stem}.txn-{token}.backup-meta")),
    })
}

fn transaction_marker(paths: &Paths, key: &str) -> Result<PathBuf> {
    let Some(parent) = paths.data.parent() else {
        return Err(StorageError::backend("blob path has no parent directory"));
    };
    Ok(parent.join(format!("{}{TRANSACTION_SUFFIX}", file_stem(key))))
}

fn transaction_token() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn valid_transaction_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 32
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-')
}

fn valid_transaction_stem(stem: &str) -> bool {
    if let Some(hash) = stem.strip_prefix('~') {
        return hash.len() == 32
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    }
    stem.len() <= MAX_PLAIN && decode(stem).is_some_and(|decoded| encode(&decoded).as_str() == stem)
}

fn transaction_artifacts_exist(paths: &TransactionPaths) -> bool {
    [
        &paths.staged_data,
        &paths.staged_meta,
        &paths.backup_data,
        &paths.backup_meta,
    ]
    .into_iter()
    .any(|path| std::fs::symlink_metadata(path).is_ok())
}

fn copy_atomic(source: &Path, target: &Path) -> Result<()> {
    let mut input = open_read(source).map_err(io_error("read a blob transaction file"))?;
    write_atomic_with(target, |output| {
        io::copy(&mut input, output)
            .map(|_| ())
            .map_err(io_error("write a blob transaction file"))
    })
}

fn same_file_bytes(left: &Path, right: &Path) -> Result<bool> {
    let mut left = open_read(left).map_err(io_error("read a blob transaction file"))?;
    let mut right = open_read(right).map_err(io_error("read a blob transaction file"))?;
    let mut left_buf = vec![0_u8; 32 * 1024];
    let mut right_buf = vec![0_u8; 32 * 1024];
    loop {
        let left_len = left
            .read(&mut left_buf)
            .map_err(io_error("read a blob transaction file"))?;
        let right_len = right
            .read(&mut right_buf)
            .map_err(io_error("read a blob transaction file"))?;
        if left_len != right_len || left_buf[..left_len] != right_buf[..right_len] {
            return Ok(false);
        }
        if left_len == 0 {
            return Ok(true);
        }
    }
}

fn regular_file_exists(path: &Path) -> Result<bool> {
    Ok(regular_file_len(path)?.is_some())
}

fn regular_file_len(path: &Path) -> Result<Option<u64>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(Some(metadata.len())),
        Ok(_) => Err(StorageError::backend(
            "blob transaction path is not a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("inspect a blob transaction file")(error)),
    }
}

fn cleanup_transaction(paths: &TransactionPaths) {
    let _ = remove_optional(&paths.staged_data);
    let _ = remove_optional(&paths.staged_meta);
    let _ = remove_optional(&paths.backup_data);
    let _ = remove_optional(&paths.backup_meta);
}

fn cleanup_orphan_transactions(dir: &Path) -> Result<()> {
    const ARTIFACT_SUFFIXES: [&str; 4] =
        [".stage-data", ".stage-meta", ".backup-data", ".backup-meta"];
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list a blob directory")(error)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error("list a blob directory"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = ARTIFACT_SUFFIXES.into_iter().find_map(|suffix| {
            name.strip_suffix(suffix).and_then(|prefix| {
                let (stem, token) = prefix.rsplit_once(".txn-")?;
                (valid_transaction_stem(stem) && valid_transaction_token(token)).then_some(stem)
            })
        }) else {
            continue;
        };
        let marker = dir.join(format!("{stem}{TRANSACTION_SUFFIX}"));
        match std::fs::symlink_metadata(&marker) {
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect a blob transaction journal")(error)),
        }
        let artifact = entry.path();
        if regular_file_exists(&artifact)? {
            remove_optional(&artifact)?;
        }
    }
    Ok(())
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
                recover_all(&dir, now)?;
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
                let paths = Self::paths(db, &scope, &key);
                recover_pending(&paths, &key)?;
                work(&paths, &key)
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

/// Read bytes and their deadline while the database lock is held.
fn source(paths: &Paths, key: &str) -> Result<Option<BlobSource>> {
    let Some(sidecar) = read_json::<Sidecar>(&paths.meta)? else {
        return Ok(None);
    };
    if sidecar.key != key {
        return Err(collision());
    }
    if sidecar.expired(paths.now) {
        let _ = head(paths, key)?;
        return Ok(None);
    }
    let Some(meta) = meta(&paths.data, sidecar.clone())? else {
        return Ok(None);
    };
    let Some(bytes) = read_optional(&paths.data)? else {
        return Ok(None);
    };
    Ok(Some(BlobSource {
        blob: Blob {
            meta: BlobMeta {
                len: bytes.len() as u64,
                ..meta
            },
            bytes,
        },
        expires_at_ms: sidecar.expires_at_ms,
    }))
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

/// Recover one pending pair before any code observes or changes its public
/// data/sidecar files. An uncommitted transaction always rolls back; a
/// committed one is re-published from its durable stage before cleanup.
fn recover_pending(paths: &Paths, key: &str) -> Result<()> {
    let marker = transaction_marker(paths, key)?;
    let Some(journal) = read_json::<BlobJournal>(&marker)? else {
        return Ok(());
    };
    if journal.key != key {
        return Err(collision());
    }
    let transaction = transaction_paths(paths, key, &journal.token)?;
    if transaction.marker != marker {
        return Err(collision());
    }

    if journal.committed {
        if regular_file_len(&transaction.staged_data)? != Some(journal.staged_len)
            || !regular_file_exists(&transaction.staged_meta)?
        {
            return Err(StorageError::backend(
                "committed blob transaction is missing its staged pair",
            ));
        }
        let staged_sidecar: Sidecar = read_json(&transaction.staged_meta)?
            .ok_or_else(|| StorageError::backend("committed blob transaction has no sidecar"))?;
        if staged_sidecar != journal.staged_sidecar || staged_sidecar.key != key {
            return Err(collision());
        }
        copy_atomic(&transaction.staged_data, &paths.data)?;
        copy_atomic(&transaction.staged_meta, &paths.meta)?;
        if !same_file_bytes(&transaction.staged_data, &paths.data)?
            || !same_file_bytes(&transaction.staged_meta, &paths.meta)?
        {
            return Err(StorageError::backend(
                "committed blob transaction could not restore its pair",
            ));
        }
        remove_optional(&transaction.marker)?;
        cleanup_transaction(&transaction);
        return Ok(());
    }

    if journal.had_data && !regular_file_exists(&transaction.backup_data)? {
        return Err(StorageError::backend(
            "pending blob transaction is missing its previous data",
        ));
    }
    if journal.had_meta {
        if !regular_file_exists(&transaction.backup_meta)? {
            return Err(StorageError::backend(
                "pending blob transaction is missing its previous sidecar",
            ));
        }
        let previous: Sidecar = read_json(&transaction.backup_meta)?
            .ok_or_else(|| StorageError::backend("previous blob sidecar disappeared"))?;
        if previous.key != key {
            return Err(collision());
        }
    }

    if journal.had_data {
        copy_atomic(&transaction.backup_data, &paths.data)?;
    } else {
        remove_optional(&paths.data)?;
    }
    if journal.had_meta {
        copy_atomic(&transaction.backup_meta, &paths.meta)?;
    } else {
        remove_optional(&paths.meta)?;
    }
    remove_optional(&transaction.marker)?;
    cleanup_transaction(&transaction);
    Ok(())
}

/// Recover transactions before listing a scope. A malformed journal or a
/// journal whose hashed filename belongs to another key fails closed.
fn recover_all(dir: &Path, now: u64) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list a blob directory")(error)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error("list a blob directory"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.ends_with(TRANSACTION_SUFFIX) {
            continue;
        }
        let path = entry.path();
        let journal: BlobJournal = read_json(&path)?
            .ok_or_else(|| StorageError::backend("blob transaction journal disappeared"))?;
        validate_blob_key(&journal.key)?;
        let stem = file_stem(&journal.key);
        let paths = Paths {
            data: dir.join(&stem),
            meta: dir.join(format!("{stem}{META_SUFFIX}")),
            now,
        };
        if transaction_marker(&paths, &journal.key)? != path {
            return Err(collision());
        }
        recover_pending(&paths, &journal.key)?;
    }
    cleanup_orphan_transactions(dir)
}

/// Replace the raw payload and its sidecar as one recoverable pair.
fn store_in(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
    content_type: Option<String>,
    expires_at_ms: Option<u64>,
) -> Result<BlobMeta> {
    store_in_with_sidecar_publisher(paths, key, bytes, content_type, expires_at_ms, copy_atomic)
}

fn store_in_with_sidecar_publisher(
    paths: &Paths,
    key: &str,
    bytes: &[u8],
    content_type: Option<String>,
    expires_at_ms: Option<u64>,
    publish_metadata: impl FnOnce(&Path, &Path) -> Result<()>,
) -> Result<BlobMeta> {
    recover_pending(paths, key)?;
    if let Some(existing) = read_json::<Sidecar>(&paths.meta)?
        && existing.key != key
    {
        return Err(collision());
    }
    let sidecar = Sidecar {
        key: key.to_owned(),
        content_type,
        expires_at_ms,
    };
    let mut selected = None;
    for _ in 0..16 {
        let token = transaction_token();
        let candidate = transaction_paths(paths, key, &token)?;
        if !candidate.marker.exists() && !transaction_artifacts_exist(&candidate) {
            selected = Some((token, candidate));
            break;
        }
    }
    let (token, transaction) = selected
        .ok_or_else(|| StorageError::backend("could not allocate blob transaction paths"))?;

    let prepared = (|| {
        write_atomic(&transaction.staged_data, bytes)?;
        write_json(&transaction.staged_meta, &sidecar)?;
        let had_data = regular_file_exists(&paths.data)?;
        if had_data {
            copy_atomic(&paths.data, &transaction.backup_data)?;
        }
        let old_sidecar = read_json::<Sidecar>(&paths.meta)?;
        if old_sidecar.as_ref().is_some_and(|old| old.key != key) {
            return Err(collision());
        }
        let had_meta = old_sidecar.is_some();
        if had_meta {
            copy_atomic(&paths.meta, &transaction.backup_meta)?;
        }
        Ok((had_data, had_meta))
    })();
    let (had_data, had_meta) = match prepared {
        Ok(previous) => previous,
        Err(error) => {
            cleanup_transaction(&transaction);
            return Err(error);
        }
    };

    let mut journal = BlobJournal {
        key: key.to_owned(),
        token,
        staged_len: bytes.len() as u64,
        staged_sidecar: sidecar.clone(),
        had_data,
        had_meta,
        committed: false,
    };
    if let Err(error) = write_json(&transaction.marker, &journal) {
        if read_json::<BlobJournal>(&transaction.marker)?.is_some() {
            recover_pending(paths, key)?;
        } else {
            cleanup_transaction(&transaction);
        }
        return Err(error);
    }

    let published = (|| {
        copy_atomic(&transaction.staged_data, &paths.data)?;
        publish_metadata(&transaction.staged_meta, &paths.meta)?;
        if !same_file_bytes(&transaction.staged_data, &paths.data)?
            || !same_file_bytes(&transaction.staged_meta, &paths.meta)?
        {
            return Err(StorageError::backend(
                "published blob pair differs from its staged version",
            ));
        }
        journal.committed = true;
        write_json(&transaction.marker, &journal)
    })();
    if let Err(error) = published {
        if let Some(mut current) = read_json::<BlobJournal>(&transaction.marker)?
            && current.committed
        {
            // A failed durability barrier must not be reported as success.
            // First make rollback durable again; if that barrier also fails,
            // leave the journal for recovery. The result of this failed I/O
            // attempt is uncertain, but every later read still sees a pair.
            current.committed = false;
            write_json(&transaction.marker, &current)?;
            recover_pending(paths, key)?;
        } else {
            recover_pending(paths, key)?;
        }
        return Err(error);
    }

    // The committed journal is durable. If unlinking it cannot be confirmed,
    // leave it for the next operation to finish; this write is already valid.
    if remove_optional(&transaction.marker).is_ok() {
        cleanup_transaction(&transaction);
    }
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
        recover_pending(&paths, key)?;
        if head(&paths, key)?.is_none() {
            return Ok(None);
        }
        read_optional(&paths.data)
    }

    /// The size of blob `key` of `scope`, `None` when absent.
    pub(crate) fn len_locked(db: &Db, scope: &Scope, key: &str) -> Result<Option<u64>> {
        let paths = Self::paths(db, scope, key);
        recover_pending(&paths, key)?;
        Ok(head(&paths, key)?.map(|meta| meta.len))
    }

    /// Remove blob `key` of `scope`.
    pub(crate) fn delete_locked(db: &Db, scope: &Scope, key: &str) -> Result<bool> {
        let paths = Self::paths(db, scope, key);
        recover_pending(&paths, key)?;
        delete_in(&paths, key)
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
            let expires_at_ms = ttl.map(|ttl| {
                paths
                    .now
                    .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
            });
            store_in(paths, key, &bytes, content_type, expires_at_ms)
        })
        .await
    }

    async fn store_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        let content_type = content_type.map(str::to_owned);
        self.write(key, move |paths, key| {
            store_in(paths, key, &bytes, content_type, expires_at_ms)
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

    async fn put_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        self.store_until(key, bytes, content_type, expires_at_ms)
            .await
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

    async fn get_with_expiry(&self, key: &str) -> Result<Option<BlobSource>> {
        self.with(key, source).await
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
                recover_all(&dir, now)?;
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
