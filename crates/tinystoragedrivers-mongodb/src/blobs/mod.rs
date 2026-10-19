//! [`BlobStore`] on GridFS.
//!
//! Blobs live in the GridFS bucket `<prefix>_tsd_blobs`. A file's `filename`
//! is the blob key and its `metadata` is `{_scope, content_type}`; every
//! lookup filters on `metadata._scope` through [`scoped_filter`].
//!
//! A blob written with a lifetime records `metadata.expires_at_ms`. Once the
//! backend clock passes it the blob reads as absent through every read; the
//! read that notices also reclaims the expired file, and
//! [`MongoBlobs::sweep_expired`] reclaims all of a scope's lapsed files.
//!
//! A replacement uploads the new file first and then deletes every older file
//! with the same key (by `_id`, which orders uploads), and two concurrent
//! writers converge on the one with the greater id. A read picks the newest
//! file; if a replacement deletes that file before or while it is read, the
//! read resolves the key again and reads the replacement, so a reader never
//! sees a blob vanish mid-replacement.
//!
//! Ranged reads fetch only the chunks the range touches, straight from the
//! bucket's `chunks` collection, by the file id resolved under the scope.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use mongodb::IndexModel;
use mongodb::bson::{Bson, Document, doc};
use mongodb::gridfs::{FilesCollectionDocument, GridFsBucket};
use mongodb::options::{FindOptions, GridFsBucketOptions, IndexOptions};
use tinystoragedrivers_core::{
    Blob, BlobMeta, BlobStore, BlobSweepEntry, BlobSweepPage, Capabilities, Capability, Result,
    Scope, StorageError, clamp_range, is_reserved_blob_key, validate_blob_key,
    validate_sweep_page_request, validate_user_blob_key, validate_user_blob_prefix,
};

use crate::backend::Shared;
use crate::errors;
use crate::fence::unfenceable;
use crate::naming::{BLOB_KEY_INDEX, BLOBS, SCOPE, prefix_regex};
use crate::scoped::scoped_filter;

/// Where a file's scope lives.
const FILE_SCOPE: &str = "metadata._scope";

/// Where a file's expiry (epoch milliseconds) lives inside its metadata.
const EXPIRES_AT: &str = "expires_at_ms";

/// The metadata stored with a file.
pub(crate) fn file_metadata(
    scope: &Scope,
    content_type: Option<&str>,
    expires_at_ms: Option<u64>,
) -> Document {
    let mut metadata = doc! {SCOPE: scope.as_str()};
    if let Some(content_type) = content_type {
        metadata.insert("content_type", content_type);
    }
    if let Some(at) = expires_at_ms {
        metadata.insert(EXPIRES_AT, i64::try_from(at).unwrap_or(i64::MAX));
    }
    metadata
}

/// Whether `file` has outlived its lifetime at `now` (epoch milliseconds).
pub(crate) fn lapsed(file: &FilesCollectionDocument, now: u64) -> bool {
    file.metadata
        .as_ref()
        .and_then(|metadata| metadata.get_i64(EXPIRES_AT).ok())
        .is_some_and(|at| at <= i64::try_from(now).unwrap_or(i64::MAX))
}

/// The first file of each key (the newest, as `files` orders them) that is
/// still alive at `now`, plus the ids of every lapsed file seen.
///
/// A key whose newest file has lapsed reads as absent, so its older files are
/// not surfaced either, and they are reported for reclaim with it: otherwise
/// reclaiming the lapsed file alone would let the next read resurrect an older
/// version.
pub(crate) fn live_newest(
    files: Vec<FilesCollectionDocument>,
    now: u64,
) -> (Vec<FilesCollectionDocument>, Vec<Bson>) {
    let mut live = Vec::new();
    let mut dead = Vec::new();
    let mut last: Option<String> = None;
    let mut key_gone = false;
    for file in files {
        let key = file.filename.clone().unwrap_or_default();
        let newest = last.as_deref() != Some(key.as_str());
        if newest {
            last = Some(key);
            key_gone = lapsed(&file, now);
        }
        let gone = key_gone || lapsed(&file, now);
        if gone {
            dead.push(file.id);
        } else if newest {
            live.push(file);
        }
    }
    (live, dead)
}

/// A file's [`BlobMeta`].
pub(crate) fn blob_meta(file: &FilesCollectionDocument) -> BlobMeta {
    BlobMeta {
        key: file.filename.clone().unwrap_or_default(),
        len: file.length,
        content_type: file
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get_str("content_type").ok())
            .map(str::to_owned),
    }
}

/// The chunk numbers `range` touches in chunks of `chunk_size` bytes, and
/// where the range starts inside the first of them.
pub(crate) fn chunk_span(range: &Range<usize>, chunk_size: usize) -> (usize, usize, usize) {
    let size = chunk_size.max(1);
    let first = range.start / size;
    let last = range.end.saturating_sub(1) / size;
    (first, last, range.start - first * size)
}

/// The standard filename/upload-date index required by the GridFS driver.
pub(crate) fn standard_file_index() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {"filename": 1, "uploadDate": 1})
        .build()
}

pub(crate) fn file_index() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {FILE_SCOPE: 1, "filename": 1, "_id": -1})
        .options(
            IndexOptions::builder()
                .name(BLOB_KEY_INDEX.to_owned())
                .build(),
        )
        .build()
}

/// GridFS requires one chunk per file ordinal, enforced by a unique index.
pub(crate) fn chunk_index() -> IndexModel {
    IndexModel::builder()
        .keys(doc! {"files_id": 1, "n": 1})
        .options(IndexOptions::builder().unique(true).build())
        .build()
}

/// MongoDB blobs bound to one scope.
#[derive(Debug)]
pub(crate) struct MongoBlobs {
    shared: Arc<Shared>,
    scope: Scope,
    generation: tinystoragedrivers_core::ScopeGeneration,
    /// Writes are refused: GridFS cannot join a fenced transaction.
    fenced: bool,
    /// The handle a stream's seal uses: it may name keys under the prefix
    /// reserved for sealed streams, which a caller's handle may not.
    sealed_access: bool,
}

impl MongoBlobs {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope, fenced: bool) -> Self {
        Self {
            shared,
            scope,
            generation: tinystoragedrivers_core::ScopeGeneration::default(),
            fenced,
            sealed_access: false,
        }
    }

    /// This handle, allowed to name the sealed-stream blob keys.
    pub(crate) fn with_sealed_access(mut self) -> Self {
        self.sealed_access = true;
        self
    }

    /// Validate `key` for this handle: a caller's handle refuses the
    /// sealed-stream prefix.
    fn check_key(&self, key: &str) -> Result<()> {
        if self.sealed_access {
            validate_blob_key(key)
        } else {
            validate_user_blob_key(key)
        }
    }
    pub(crate) fn with_generation(
        mut self,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Self {
        self.generation = generation;
        self
    }

    /// Refuse a write on a fenced handle rather than run it unfenced.
    fn writable(&self, what: &str) -> Result<()> {
        if self.fenced {
            Err(unfenceable(what))
        } else {
            Ok(())
        }
    }

    async fn bucket(&self) -> Result<GridFsBucket> {
        if self.shared.existing {
            self.shared
                .prepare(&format!("{BLOBS}.chunks"), vec![chunk_index()])
                .await?;
        }
        let name = BLOBS.to_owned();
        let models = if self.shared.existing {
            vec![file_index(), standard_file_index()]
        } else {
            vec![file_index()]
        };
        self.shared
            .prepare(&format!("{BLOBS}.files"), models)
            .await?;
        Ok(self
            .shared
            .db
            .gridfs_bucket(GridFsBucketOptions::builder().bucket_name(name).build()))
    }

    /// Files of this scope matching `inner`, newest first within a key.
    async fn files(
        &self,
        bucket: &GridFsBucket,
        inner: Document,
    ) -> Result<Vec<FilesCollectionDocument>> {
        bucket
            .find(scoped_filter(FILE_SCOPE, &self.scope, inner))
            .sort(doc! {"filename": 1, "_id": -1})
            .await
            .map_err(errors::failed("list blobs"))?
            .try_collect()
            .await
            .map_err(errors::failed("list blobs"))
    }

    async fn newest(
        &self,
        bucket: &GridFsBucket,
        key: &str,
    ) -> Result<Option<FilesCollectionDocument>> {
        let files = self.files(bucket, doc! {"filename": key}).await?;
        let now = self.shared.now();
        let (live, dead) = live_newest(files, now);
        if !dead.is_empty() {
            self.reclaim(bucket, dead).await;
        }
        Ok(live.into_iter().next())
    }

    /// Best-effort removal of lapsed files. They already read as absent, so a
    /// failure here only delays the reclaim to the next read or sweep.
    async fn reclaim(&self, bucket: &GridFsBucket, ids: Vec<Bson>) {
        // Ignored on purpose; see above.
        let _ = self.remove(bucket, ids).await;
    }

    /// Remove every lapsed file of this scope, whether or not a newer file of
    /// the same key exists. Returns how many files were removed.
    ///
    /// # Errors
    ///
    /// A backend error.
    pub(crate) async fn sweep_expired(&self) -> Result<u64> {
        let bucket = self.bucket().await?;
        let now = i64::try_from(self.shared.now()).unwrap_or(i64::MAX);
        let lapsed = self
            .files(&bucket, doc! {"metadata.expires_at_ms": {"$lte": now}})
            .await?;
        // A key whose newest version lapsed loses its older versions with it,
        // or the next read would resurface one of them.
        let mut keys: Vec<String> = lapsed
            .iter()
            .map(|file| file.filename.clone().unwrap_or_default())
            .collect();
        keys.sort();
        keys.dedup();
        let mut ids: Vec<Bson> = Vec::new();
        for key in keys {
            let files = self.files(&bucket, doc! {"filename": key}).await?;
            ids.extend(live_newest(files, self.shared.now()).1);
        }
        let count = ids.len() as u64;
        self.remove(&bucket, ids).await?;
        Ok(count)
    }

    async fn remove(&self, bucket: &GridFsBucket, ids: Vec<Bson>) -> Result<()> {
        for id in ids {
            match bucket.delete(id).await {
                Ok(()) => {}
                // A concurrent delete got there first.
                Err(error) if errors::is_gridfs(&error) => {}
                Err(error) => return Err(errors::map(error, "delete a blob")),
            }
        }
        Ok(())
    }
}

/// How many times a read re-resolves a blob whose file a concurrent
/// replacement removed mid-read.
const READ_ATTEMPTS: usize = 8;

fn replaced_too_often() -> StorageError {
    StorageError::unavailable("blob kept being replaced while it was read; retry")
}

impl MongoBlobs {
    /// The whole file, or `None` when it was deleted (by a concurrent
    /// replacement) before or while it was read.
    async fn download(
        bucket: &GridFsBucket,
        file: &FilesCollectionDocument,
    ) -> Result<Option<Vec<u8>>> {
        let mut download = match bucket.open_download_stream(file.id.clone()).await {
            Ok(download) => download,
            Err(error) if errors::is_gridfs(&error) => return Ok(None),
            Err(error) => return Err(errors::map(error, "read a blob")),
        };
        let mut bytes = Vec::new();
        match download.read_to_end(&mut bytes).await {
            Ok(_) => Ok(Some(bytes)),
            Err(error) if errors::io_is_gridfs(&error) => Ok(None),
            Err(error) => Err(errors::map_io(error, "read a blob")),
        }
    }

    /// The bytes of `range` (already clamped and non-empty), or `None` when
    /// the file's chunks were deleted before they were read.
    async fn read_chunks(
        &self,
        file: &FilesCollectionDocument,
        range: &Range<usize>,
    ) -> Result<Option<Vec<u8>>> {
        let chunk_size = usize::try_from(file.chunk_size_bytes).unwrap_or(usize::MAX);
        let (first, last, skip) = chunk_span(range, chunk_size);
        let chunks = self
            .shared
            .raw(&format!("{BLOBS}.chunks"))
            .find(doc! {
                "files_id": file.id.clone(),
                "n": {"$gte": i64::try_from(first).unwrap_or(i64::MAX), "$lte": i64::try_from(last).unwrap_or(i64::MAX)},
            })
            .with_options(FindOptions::builder().sort(doc! {"n": 1}).build())
            .await
            .map_err(errors::failed("read a blob range"))?
            .try_collect::<Vec<Document>>()
            .await
            .map_err(errors::failed("read a blob range"))?;
        let mut bytes = Vec::new();
        for chunk in &chunks {
            let data = chunk
                .get_binary_generic("data")
                .map_err(|_| StorageError::serialization("malformed blob chunk"))?;
            bytes.extend_from_slice(data);
        }
        if bytes.len() < skip + range.len() {
            return Ok(None);
        }
        Ok(Some(
            bytes.into_iter().skip(skip).take(range.len()).collect(),
        ))
    }
}

impl MongoBlobs {
    /// Store `bytes`, expiring `ttl` after the backend clock's now when given.
    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
    ) -> Result<BlobMeta> {
        let expires_at_ms = ttl.map(|ttl| {
            self.shared
                .now()
                .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
        });
        crate::purge::admitted!(self, {
            self.check_key(key)?;
            self.writable("a blob put")?;
            if self.shared.existing {
                return Err(StorageError::unsupported(
                    Capability::ExistingSchema,
                    "GridFS upload may initialize schema",
                ));
            }
            let bucket = self.bucket().await?;
            let mut upload = bucket
                .open_upload_stream(key)
                .metadata(file_metadata(&self.scope, content_type, expires_at_ms))
                .await
                .map_err(errors::failed("store a blob"))?;
            upload
                .write_all(&bytes)
                .await
                .map_err(errors::failed_io("store a blob"))?;
            upload
                .close()
                .await
                .map_err(errors::failed_io("store a blob"))?;
            let id = upload.id().clone();
            let older: Vec<Bson> = self
                .files(&bucket, doc! {"filename": key, "_id": {"$lt": id.clone()}})
                .await?
                .into_iter()
                .map(|file| file.id)
                .collect();
            self.remove(&bucket, older).await?;
            Ok(BlobMeta {
                key: key.to_owned(),
                len: bytes.len() as u64,
                content_type: content_type.map(str::to_owned),
            })
        })
    }
}

#[async_trait]
impl BlobStore for MongoBlobs {
    async fn sweep_expired(&self) -> Result<u64> {
        crate::purge::admitted!(self, {
            self.writable("a blob sweep")?;
            MongoBlobs::sweep_expired(self).await
        })
    }

    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::none().with(Capability::BlobTtl);
        if self.fenced {
            caps
        } else {
            caps.with(Capability::BlobSweep)
        }
    }

    async fn list_sweep_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobSweepPage> {
        crate::purge::admitted!(self, {
            validate_sweep_page_request(prefix, after, limit)?;
            let bucket = self.bucket().await?;
            let filename = if let Some(after) = after {
                doc! { "$regex": prefix_regex(prefix), "$gt": after }
            } else {
                doc! { "$regex": prefix_regex(prefix) }
            };
            let filter = doc! { "filename": filename };
            let mut files = bucket
                .find(scoped_filter(FILE_SCOPE, &self.scope, filter))
                .sort(doc! { "filename": 1, "_id": -1 })
                .await
                .map_err(errors::failed("page blob sweep metadata"))?;
            let mut items = Vec::with_capacity(limit);
            let mut previous: Option<String> = None;
            let mut has_more = false;
            while let Some(file) = files
                .try_next()
                .await
                .map_err(errors::failed("page blob sweep metadata"))?
            {
                let Some(key) = file.filename.clone() else {
                    return Err(StorageError::backend(
                        "GridFS cleanup listing contained a file without a filename",
                    ));
                };
                if previous.as_deref() == Some(key.as_str()) {
                    continue;
                }
                previous = Some(key.clone());
                if !key.starts_with(prefix) || (!self.sealed_access && is_reserved_blob_key(&key)) {
                    continue;
                }
                if items.len() == limit {
                    has_more = true;
                    break;
                }
                items.push(BlobSweepEntry {
                    meta: blob_meta(&file),
                    written_at_ms: u64::try_from(file.upload_date.timestamp_millis()).ok(),
                });
            }
            let next_after = if has_more {
                items.last().map(|entry| entry.meta.key.clone())
            } else {
                None
            };
            Ok(BlobSweepPage { items, next_after })
        })
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
        crate::purge::admitted!(self, {
            self.check_key(key)?;
            let bucket = self.bucket().await?;
            for _ in 0..READ_ATTEMPTS {
                let Some(file) = self.newest(&bucket, key).await? else {
                    return Ok(None);
                };
                if let Some(bytes) = Self::download(&bucket, &file).await? {
                    return Ok(Some(Blob {
                        meta: blob_meta(&file),
                        bytes,
                    }));
                }
            }
            Err(replaced_too_often())
        })
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        crate::purge::admitted!(self, {
            self.check_key(key)?;
            let bucket = self.bucket().await?;
            for _ in 0..READ_ATTEMPTS {
                let Some(file) = self.newest(&bucket, key).await? else {
                    return Ok(None);
                };
                let len = usize::try_from(file.length).map_err(|_| {
                    StorageError::backend("blob is larger than this platform can address")
                })?;
                let range = clamp_range(&range, len)?;
                if range.is_empty() {
                    return Ok(Some(Vec::new()));
                }
                if let Some(bytes) = self.read_chunks(&file, &range).await? {
                    return Ok(Some(bytes));
                }
            }
            Err(replaced_too_often())
        })
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        crate::purge::admitted!(self, {
            self.check_key(key)?;
            let bucket = self.bucket().await?;
            Ok(self.newest(&bucket, key).await?.as_ref().map(blob_meta))
        })
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        crate::purge::admitted!(self, {
            self.check_key(key)?;
            self.writable("a blob delete")?;
            let bucket = self.bucket().await?;
            let files = self.files(&bucket, doc! {"filename": key}).await?;
            // A lapsed blob is not there to delete, but its files go too.
            let existed = !live_newest(files.clone(), self.shared.now()).0.is_empty();
            let ids: Vec<Bson> = files.into_iter().map(|file| file.id).collect();
            self.remove(&bucket, ids).await?;
            Ok(existed)
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        crate::purge::admitted!(self, {
            if !self.sealed_access {
                validate_user_blob_prefix(prefix)?;
            }
            let bucket = self.bucket().await?;
            let files = self
                .files(&bucket, doc! {"filename": prefix_regex(prefix)})
                .await?;
            let (live, dead) = live_newest(files, self.shared.now());
            if !dead.is_empty() {
                self.reclaim(&bucket, dead).await;
            }
            Ok(live
                .iter()
                .map(blob_meta)
                .filter(|meta| meta.key.starts_with(prefix))
                .filter(|meta| self.sealed_access || !is_reserved_blob_key(&meta.key))
                .collect())
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
