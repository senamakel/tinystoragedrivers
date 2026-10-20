//! The blob port: opaque byte objects addressed by key.
//!
//! Attachments, exported artifacts and large transcript bodies are blobs. Keys
//! are `/`-separated paths so [`BlobStore::list`] can enumerate a prefix, but no
//! driver promises directories exist.

use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::Capability;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};
use crate::stream::{SEALED_BLOB_PREFIX, is_reserved_blob_key};

mod tree;

pub use tree::{TreeEntry, TreeFile};

/// The format limit for a blob key, in bytes.
/// Drivers may announce a smaller physical limit with [`BlobStore::max_key_len`].
pub const MAX_BLOB_KEY_LEN: usize = 1024;

/// What is known about a blob without reading it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    /// The blob's key.
    pub key: String,
    /// Size in bytes.
    pub len: u64,
    /// MIME type, when the writer gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// A blob's metadata and bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// Metadata.
    pub meta: BlobMeta,
    /// Contents.
    pub bytes: Vec<u8>,
}

/// A blob and the exact deadline stored with it.
///
/// `expires_at_ms` is an absolute Unix epoch millisecond deadline. `None`
/// means the store knows the blob is permanent; stores that cannot determine
/// the deadline must return `Unsupported` instead of using `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobSource {
    /// Metadata and bytes read together with the deadline.
    pub blob: Blob,
    /// The stored absolute deadline, or `None` for a known permanent blob.
    pub expires_at_ms: Option<u64>,
}

/// An owned asynchronous blob reader. Drivers may deliver bytes incrementally.
pub type BlobReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Metadata and an incremental reader for a blob.
pub struct BlobStream {
    /// Metadata obtained before the body is consumed.
    pub meta: BlobMeta,
    /// Contents; a read failure is reported as an I/O error.
    pub reader: BlobReader,
}

/// A streaming blob reader and the exact deadline stored with it.
pub struct BlobSourceStream {
    /// Metadata obtained with the deadline before the body is consumed.
    pub meta: BlobMeta,
    /// The stored absolute deadline, or `None` for a known permanent blob.
    pub expires_at_ms: Option<u64>,
    /// Contents; a read failure is reported as an I/O error.
    pub reader: BlobReader,
}

impl std::fmt::Debug for BlobSourceStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobSourceStream")
            .field("meta", &self.meta)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for BlobStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStream")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// Opaque byte objects, bound to one scope.
#[async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug {
    /// Maximum logical key length in UTF-8 bytes for this scoped handle.
    ///
    /// Object stores count their tenant/database prefix against a physical key
    /// limit, so this may be smaller than the general format limit.
    fn max_key_len(&self) -> usize {
        MAX_BLOB_KEY_LEN
    }

    /// Store `bytes` under `key`, replacing any previous blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta>;

    /// Store exactly the declared bytes, replacing the old blob only on success.
    ///
    /// Object-store drivers override this to upload bounded chunks. The default
    /// buffers the body for existing drivers; use a streaming driver for large objects.
    /// A declared length mismatch fails before publishing the object.
    ///
    /// # Errors
    ///
    /// Invalid key or length, reader failure, or a backend error.
    async fn put_stream(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        validate_user_blob_key(key)?;
        let mut bytes = Vec::new();
        (&mut reader)
            .take(len.saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| StorageError::backend("blob reader failed").with_source(error))?;
        if bytes.len() as u64 != len {
            return Err(StorageError::invalid_input(
                "blob reader length differs from declared length",
            ));
        }
        self.put(key, bytes, content_type).await
    }

    /// Store `bytes` under `key` with a lifetime, replacing any previous blob.
    ///
    /// `ttl` runs from the write. Once it has passed the blob reads as absent
    /// through every read (`get`, `get_range`, `head`, `get_stream`, `list`),
    /// and a later `put` of the same key is an ordinary replacement. A zero
    /// `ttl` is already expired: the write succeeds and the blob is never
    /// visible. A plain [`put`](Self::put) of a key clears any lifetime it had.
    ///
    /// The driver decides when an expired blob's storage is reclaimed; callers
    /// may only rely on it being invisible.
    ///
    /// # Errors
    ///
    /// [`Capability::BlobTtl`] unsupported (the default), an invalid key, or a
    /// backend error.
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        let _ = (key, bytes, content_type, ttl);
        Err(StorageError::unsupported(
            Capability::BlobTtl,
            "this blob driver cannot expire blobs",
        ))
    }

    /// Store bytes with an absolute Unix epoch millisecond deadline.
    ///
    /// A deadline at or before the backend's current clock is expired on
    /// arrival and is not visible to reads. This differs from
    /// [`put_expiring`](Self::put_expiring), which starts a relative lifetime
    /// at write time.
    ///
    /// # Errors
    ///
    /// Unsupported when the driver cannot preserve an absolute deadline, an
    /// invalid key, or a backend error.
    async fn put_until(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        let _ = (key, bytes, content_type, expires_at_ms);
        Err(StorageError::unsupported(
            Capability::BlobTtl,
            "this blob store cannot preserve an absolute expiry",
        ))
    }

    /// Store a stream with an absolute Unix epoch millisecond deadline.
    ///
    /// The default buffers exactly `len` bytes and delegates to
    /// [`put_until`](Self::put_until). Streaming drivers may override it to
    /// keep memory use bounded.
    ///
    /// # Errors
    ///
    /// Unsupported when absolute deadlines are unavailable, an invalid key or
    /// length, a reader failure, or a backend error.
    async fn put_stream_until(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        expires_at_ms: Option<u64>,
    ) -> Result<BlobMeta> {
        validate_user_blob_key(key)?;
        let mut bytes = Vec::new();
        (&mut reader)
            .take(len.saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| StorageError::backend("blob reader failed").with_source(error))?;
        if bytes.len() as u64 != len {
            return Err(StorageError::invalid_input(
                "blob reader length differs from declared length",
            ));
        }
        self.put_until(key, bytes, content_type, expires_at_ms)
            .await
    }

    /// [`put_stream`](Self::put_stream) with a lifetime; see
    /// [`put_expiring`](Self::put_expiring). The default buffers the body like
    /// `put_stream` does and calls `put_expiring`.
    ///
    /// # Errors
    ///
    /// As [`put_stream`](Self::put_stream), or [`Capability::BlobTtl`]
    /// unsupported.
    async fn put_stream_expiring(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        validate_user_blob_key(key)?;
        if !self.capabilities().contains(Capability::BlobTtl) {
            return Err(StorageError::unsupported(
                Capability::BlobTtl,
                "this blob driver cannot expire blobs",
            ));
        }
        let mut bytes = Vec::new();
        (&mut reader)
            .take(len.saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| StorageError::backend("blob reader failed").with_source(error))?;
        if bytes.len() as u64 != len {
            return Err(StorageError::invalid_input(
                "blob reader length differs from declared length",
            ));
        }
        self.put_expiring(key, bytes, content_type, ttl).await
    }

    /// Read a blob incrementally. The default wraps the existing buffered getter.
    ///
    /// # Errors
    ///
    /// An invalid key or backend error. Later reader failures are I/O errors.
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        Ok(self.get(key).await?.map(|blob| BlobStream {
            meta: blob.meta,
            reader: Box::pin(std::io::Cursor::new(blob.bytes)),
        }))
    }

    /// Optional abilities of this blob handle.
    fn capabilities(&self) -> crate::Capabilities {
        crate::Capabilities::none()
    }

    /// A bearer URL for a direct GET, valid for the given duration.
    ///
    /// URLs must be treated as credentials. Presigning does not check existence.
    ///
    /// # Errors
    ///
    /// Unsupported when this handle lacks [`Capability::PresignedUrls`], invalid key/TTL,
    /// or a backend error.
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "this blob driver cannot presign URLs",
        ))
    }

    /// A bearer URL for a direct PUT, valid for the given duration.
    ///
    /// The URL permits replacing the addressed object without a document fence.
    ///
    /// # Errors
    ///
    /// Unsupported when this handle lacks [`Capability::PresignedUrls`], invalid key/TTL,
    /// or a backend error.
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "this blob driver cannot presign URLs",
        ))
    }

    /// Read a whole blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn get(&self, key: &str) -> Result<Option<Blob>>;

    /// Read bytes and their stored absolute expiry from one backend snapshot.
    ///
    /// `expires_at_ms: None` means the store knows the blob is permanent. An
    /// implementation that cannot inspect expiry must return `Unsupported`;
    /// it must not infer permanence from the existing `get` API.
    ///
    /// # Errors
    ///
    /// Unsupported when the driver cannot report a stored deadline, an
    /// invalid key, or a backend error.
    async fn get_with_expiry(&self, key: &str) -> Result<Option<BlobSource>> {
        let _ = key;
        Err(StorageError::unsupported(
            Capability::BlobTtl,
            "this blob store cannot report an absolute expiry",
        ))
    }

    /// Read a blob incrementally together with its stored absolute expiry.
    ///
    /// The default adapts [`get_with_expiry`](Self::get_with_expiry); stores
    /// that need bounded reads may override it with a native streaming path.
    ///
    /// # Errors
    ///
    /// As [`get_with_expiry`](Self::get_with_expiry), or a later reader I/O
    /// error.
    async fn get_stream_with_expiry(&self, key: &str) -> Result<Option<BlobSourceStream>> {
        Ok(self
            .get_with_expiry(key)
            .await?
            .map(|source| BlobSourceStream {
                meta: source.blob.meta,
                expires_at_ms: source.expires_at_ms,
                reader: Box::pin(std::io::Cursor::new(source.blob.bytes)),
            }))
    }

    /// Read the bytes in `range`, clamped to the blob's length.
    ///
    /// # Errors
    ///
    /// An invalid key, an inverted range, or a backend error.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>>;

    /// Read a blob's metadata.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;

    /// Remove a blob and report whether it existed.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Metadata of every blob whose key starts with `prefix`, sorted by key.
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>>;

    /// Reclaim every blob of this scope whose lifetime has passed and return
    /// how many went, for a periodic sweeper.
    ///
    /// Reads already treat a lapsed blob as absent; this only frees the space.
    /// The default reclaims nothing and returns `0`: drivers whose lapsed blobs
    /// are removed by other means (or that have none) need not override it, and
    /// a bus client leaves reclaiming to the host that owns the data. Drivers
    /// with [`Capability::BlobTtl`] that keep lapsed blobs until swept override
    /// it, so a host holding only an `Arc<dyn StorageBackend>` can schedule the
    /// sweep (see [`StorageBackend::sweep_expired_blobs`](crate::StorageBackend::sweep_expired_blobs)).
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn sweep_expired(&self) -> Result<u64> {
        Ok(0)
    }

    /// The directory-like children of `dir` (`""` is the root): blobs directly
    /// inside it and the sub-directories that hold more blobs, sorted by name.
    ///
    /// Built on [`list`](Self::list), so every driver answers it the same way.
    /// `dir` is a prefix without a trailing `/`, and `skills/a` never lists
    /// `skills/ab/...`.
    ///
    /// # Errors
    ///
    /// An invalid `dir`, or a backend error.
    async fn list_dir(&self, dir: &str) -> Result<Vec<TreeEntry>> {
        let prefix = tree::list_prefix(dir)?;
        let listed = self.list(&prefix).await?;
        Ok(tree::children(&prefix, listed))
    }

    /// Write `files` under `dir` (a skill bundle, say), replacing blobs at the
    /// same paths. Every path is validated before the first write; the writes
    /// themselves are **not** atomic across files, so a backend failure part
    /// way leaves the files already written in place. Callers that need an
    /// all-or-nothing bundle publish a manifest document last.
    ///
    /// # Errors
    ///
    /// An invalid or duplicate path, or the first backend error.
    async fn put_tree(&self, dir: &str, files: Vec<TreeFile>) -> Result<Vec<BlobMeta>> {
        let keys = tree::plan(dir, &files, self.max_key_len())?;
        let mut written = Vec::with_capacity(files.len());
        for (key, file) in keys.into_iter().zip(files) {
            written.push(
                self.put(&key, file.bytes, file.content_type.as_deref())
                    .await?,
            );
        }
        Ok(written)
    }

    /// Remove every blob below `dir` and report how many went.
    ///
    /// # Errors
    ///
    /// An invalid `dir` (the root is refused), or a backend error.
    async fn delete_tree(&self, dir: &str) -> Result<u64> {
        if dir.is_empty() {
            return Err(StorageError::invalid_input(
                "delete_tree needs a directory, not the root",
            ));
        }
        let prefix = tree::list_prefix(dir)?;
        let mut removed = 0;
        for meta in self.list(&prefix).await? {
            if self.delete(&meta.key).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// Validate a blob key: 1 to [`MAX_BLOB_KEY_LEN`] bytes, no NUL, no empty,
/// `.` or `..` path segment, and no leading `/`.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_blob_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_BLOB_KEY_LEN || key.contains('\0') {
        return Err(StorageError::invalid_input(format!(
            "blob key must be 1 to {MAX_BLOB_KEY_LEN} bytes without NUL"
        )));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(StorageError::invalid_input(
            "blob key has an empty, `.` or `..` segment",
        ));
    }
    Ok(())
}

/// [`validate_blob_key`] for a key a caller names: additionally refuses the
/// prefix reserved for sealed streams ([`SEALED_BLOB_PREFIX`]).
///
/// Every public blob operation of a driver validates with this, so a caller
/// cannot overwrite, read or delete the blob a sealed stream's entries live
/// in. A driver's own sealing code names that blob with
/// [`sealed_blob_key`](crate::sealed_blob_key) and does not pass through here.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_user_blob_key(key: &str) -> Result<()> {
    validate_blob_key(key)?;
    if is_reserved_blob_key(key) {
        return Err(StorageError::invalid_input(format!(
            "blob keys under `{SEALED_BLOB_PREFIX}` are reserved for sealed streams"
        )));
    }
    Ok(())
}

/// Refuse a [`BlobStore::list`] prefix that lies under the prefix reserved
/// for sealed streams ([`SEALED_BLOB_PREFIX`]). Broader prefixes are allowed;
/// their listings simply omit the reserved keys.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_user_blob_prefix(prefix: &str) -> Result<()> {
    if is_reserved_blob_key(prefix) {
        return Err(StorageError::invalid_input(format!(
            "blob keys under `{SEALED_BLOB_PREFIX}` are reserved for sealed streams"
        )));
    }
    Ok(())
}

/// Clamp `range` to a blob of `len` bytes.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the range
/// is inverted.
pub fn clamp_range(range: &Range<u64>, len: usize) -> Result<Range<usize>> {
    if range.start > range.end {
        return Err(StorageError::invalid_input(
            "blob range start is after its end",
        ));
    }
    let clamp = |at: u64| usize::try_from(at).map_or(len, |at| at.min(len));
    Ok(clamp(range.start)..clamp(range.end))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
