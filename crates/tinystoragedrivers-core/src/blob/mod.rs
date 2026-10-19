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

/// A blob entry from a bounded sweep listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobSweepEntry {
    /// The blob's key, size and content type.
    pub meta: BlobMeta,
    /// When the current blob contents were written, in milliseconds since the
    /// Unix epoch. `None` means the backend cannot establish a trustworthy age.
    pub written_at_ms: Option<u64>,
}

/// One bounded page of physical blobs, including blobs hidden by an expired
/// lifetime so cleanup can still reclaim their stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobSweepPage {
    /// Entries in strictly increasing key order.
    pub items: Vec<BlobSweepEntry>,
    /// The last key when another page may exist, otherwise `None`.
    pub next_after: Option<String>,
}

/// Maximum number of entries in one blob sweep page.
pub const MAX_BLOB_SWEEP_PAGE_ITEMS: usize = 1_000;

/// A blob's metadata and bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// Metadata.
    pub meta: BlobMeta,
    /// Contents.
    pub bytes: Vec<u8>,
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

    /// Page through physical blobs whose key starts with `prefix`.
    ///
    /// Unlike [`list`](Self::list), this cleanup listing also includes blobs
    /// hidden from reads by an expired lifetime. Results are ordered by key;
    /// pass the previous page's `next_after` to continue. A missing write time
    /// is unknown and must be retained by the generic sweep.
    ///
    /// Drivers must implement this with bounded backend pages. The default
    /// fails closed instead of materializing [`list`](Self::list).
    ///
    /// # Errors
    ///
    /// Invalid prefix, cursor or page size, unsupported cleanup listing, or a
    /// backend error.
    async fn list_sweep_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobSweepPage> {
        let _ = (prefix, after, limit);
        Err(StorageError::unsupported(
            Capability::BlobSweep,
            "this blob driver cannot page cleanup metadata",
        ))
    }

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

/// Delete old blobs under `prefix` that the host confirms are unreferenced.
///
/// `older_than` is an exclusive Unix-epoch millisecond cutoff. Callers
/// normally compute it as the current time minus their write-to-document grace
/// period. The reference callback receives owned keys so it can perform async
/// lookups without borrowing the page. Objects with unknown write times are
/// retained. Pages are bounded and processed in key order; an error stops the
/// sweep, though earlier pages may already have been deleted.
///
/// The result counts deletes that reported a visible blob removed. A backend
/// may also reclaim a physical object that has expired while reporting `false`
/// under the existing [`BlobStore::delete`] contract; such reclamation is not
/// included in the count.
///
/// The check and delete are not atomic. Hosts must use immutable blob keys or
/// coordinate writers and sweeping so a key cannot be replaced or referenced
/// between the reference check and delete. The grace period protects an
/// interrupted write only when it exceeds the maximum write-to-document delay.
///
/// # Errors
///
/// Invalid listing data, unsupported paging, a listing or deletion error, or
/// an error from the host's reference callback.
pub async fn sweep_unreferenced<F, Fut>(
    blobs: &dyn BlobStore,
    prefix: &str,
    older_than: u64,
    mut is_referenced: F,
) -> Result<u64>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    validate_user_blob_prefix(prefix)?;
    let mut after: Option<String> = None;
    let mut removed = 0_u64;
    loop {
        let page = blobs
            .list_sweep_page(prefix, after.as_deref(), MAX_BLOB_SWEEP_PAGE_ITEMS)
            .await?;
        validate_sweep_page(prefix, after.as_deref(), MAX_BLOB_SWEEP_PAGE_ITEMS, &page)?;
        for entry in page.items {
            let Some(written_at_ms) = entry.written_at_ms else {
                continue;
            };
            if written_at_ms >= older_than || is_referenced(entry.meta.key.clone()).await? {
                continue;
            }
            if blobs.delete(&entry.meta.key).await? {
                removed = removed.saturating_add(1);
            }
        }
        let Some(next_after) = page.next_after else {
            return Ok(removed);
        };
        after = Some(next_after);
    }
}

fn validate_sweep_page(
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    page: &BlobSweepPage,
) -> Result<()> {
    if page.items.len() > limit {
        return Err(StorageError::backend("blob sweep page exceeds its limit"));
    }
    let mut previous = after;
    for entry in &page.items {
        let key = entry.meta.key.as_str();
        if !key.starts_with(prefix)
            || validate_user_blob_key(key).is_err()
            || previous.is_some_and(|previous| key <= previous)
        {
            return Err(StorageError::backend("blob sweep page keys are invalid"));
        }
        previous = Some(key);
    }
    if let Some(next_after) = &page.next_after
        && (page
            .items
            .last()
            .is_none_or(|entry| entry.meta.key != *next_after)
            || after.is_some_and(|after| next_after.as_str() <= after))
    {
        return Err(StorageError::backend(
            "blob sweep page cursor did not advance",
        ));
    }
    Ok(())
}

/// Validate the shared bounds and cursor rules for a cleanup page request.
/// Drivers should call this before querying their own pagination mechanism.
///
/// # Errors
///
/// Returns `InvalidInput` when the prefix, cursor, or requested page size is
/// outside the blob cleanup contract.
pub fn validate_sweep_page_request(prefix: &str, after: Option<&str>, limit: usize) -> Result<()> {
    validate_user_blob_prefix(prefix)?;
    if !(1..=MAX_BLOB_SWEEP_PAGE_ITEMS).contains(&limit) {
        return Err(StorageError::invalid_input(
            "blob sweep page size is outside the supported range",
        ));
    }
    if let Some(after) = after {
        validate_user_blob_key(after)?;
        if !after.starts_with(prefix) {
            return Err(StorageError::invalid_input(
                "blob sweep cursor is outside the requested prefix",
            ));
        }
    }
    Ok(())
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
