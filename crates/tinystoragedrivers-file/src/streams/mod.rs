//! [`StreamStore`] for the file driver: one JSONL file per stream.
//!
//! `<name>.jsonl` holds one `{"offset": N, "value": ...}` line per retained
//! entry, and `<name>.meta.json` holds `{"name", "base"}`: the real stream name
//! (file names may be hashed) and the first retained offset. The stream's
//! length is never stored; it is the larger of `base` and one past the last
//! line's offset, read from the end of the file. So:
//!
//! - an append is one `write` of whole lines plus an fsync; a crash can leave
//!   at most a torn final line, which readers ignore and the next append cuts
//!   off;
//! - [`truncate_before`](StreamStore::truncate_before) first raises `base`,
//!   then rewrites the file without the dropped lines. A crash between the two
//!   leaves lines below `base`, which readers skip.
//!
//! A sealed stream (see [`StreamStore::seal`]) keeps its entries as one
//! compressed blob in the scope's blob store and records `sealed_len`, its
//! length at the seal, in the meta file; that field is what makes it sealed.
//! The data file is then leftover that sealing removes. [`seal`] holds the
//! protocol.
//!
//! A stream exists once its meta file does: from its first non-empty append
//! until [`delete_stream`](StreamStore::delete_stream), even when truncated
//! empty, like the memory driver's.

use crate::storage::Fencing;

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    Result, Scope, ScopeGeneration, SealOutcome, StorageError, StreamEntry, StreamInfo,
    StreamStore, validate_stream,
};

use crate::documents::guard;
use crate::encode::file_stem;
use crate::fsio::{
    files_with_suffix, io_error, open_append, open_read, read_json, remove_optional, sync_dir,
    write_atomic_with, write_json,
};
use crate::storage::Db;

/// The directory holding a stream file.
fn parent_of(path: &std::path::Path) -> &std::path::Path {
    path.parent().unwrap_or_else(|| std::path::Path::new("."))
}

mod seal;

const META_SUFFIX: &str = ".meta.json";

/// A stream's sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamMeta {
    name: String,
    base: u64,
    /// The stream's length when it was sealed; present exactly when sealed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sealed_len: Option<u64>,
    /// When the stream was sealed, epoch milliseconds. Absent on a stream
    /// sealed before times were recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sealed_ms: Option<i64>,
    /// When a record was last appended, epoch milliseconds. Absent on a
    /// stream written before times were recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_append_ms: Option<i64>,
}

/// One line of a stream file.
#[derive(Debug, Serialize, Deserialize)]
struct Line {
    offset: u64,
    value: Value,
}

/// File-backed streams bound to one scope.
#[derive(Debug, Clone)]
pub struct FileStreams {
    db: Arc<Db>,
    scope: Scope,
    /// Checked under the directory lock before every write, when set.
    fence: Option<Arc<Fencing>>,
    generation: ScopeGeneration,
}

/// The paths of one stream.
struct Paths {
    data: PathBuf,
    meta: PathBuf,
}

impl FileStreams {
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
        db.scope_dir(scope).join("streams")
    }

    fn paths(db: &Db, scope: &Scope, name: &str) -> Paths {
        let dir = Self::dir(db, scope);
        let stem = file_stem(name);
        Paths {
            data: dir.join(format!("{stem}.jsonl")),
            meta: dir.join(format!("{stem}{META_SUFFIX}")),
        }
    }

    /// Run `work` under the directory lock with this stream's paths.
    async fn with<T, F>(&self, name: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths) -> Result<T> + Send + 'static,
    {
        self.with_db(name, move |_, _, paths| work(paths)).await
    }

    /// [`Self::with`] for work that also needs the database and scope, such
    /// as reading the sealed blob.
    async fn with_db<T, F>(&self, name: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Db, &Scope, &Paths) -> Result<T> + Send + 'static,
    {
        validate_stream(name)?;
        let (scope, name, generation) = (self.scope.clone(), name.to_owned(), self.generation);
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                work(db, &scope, &Self::paths(db, &scope, &name))
            })
            .await
    }

    /// [`Self::with`] for a write: the fence (if any) is checked first,
    /// under the same lock.
    async fn write<T, F>(&self, name: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Paths) -> Result<T> + Send + 'static,
    {
        self.write_db(name, move |_, _, paths| work(paths)).await
    }

    /// [`Self::write`] for work that also needs the database and scope.
    async fn write_db<T, F>(&self, name: &str, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Db, &Scope, &Paths) -> Result<T> + Send + 'static,
    {
        validate_stream(name)?;
        let (scope, name, fence, generation) = (
            self.scope.clone(),
            name.to_owned(),
            self.fence.clone(),
            self.generation,
        );
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                guard(db, fence.as_deref())?;
                work(db, &scope, &Self::paths(db, &scope, &name))
            })
            .await
    }
}

/// The stream's sidecar, `None` when the stream does not exist.
fn read_meta(paths: &Paths, name: &str) -> Result<Option<StreamMeta>> {
    match read_json::<StreamMeta>(&paths.meta)? {
        Some(meta) if meta.name == name => Ok(Some(meta)),
        Some(_) => Err(collision()),
        None => Ok(None),
    }
}

/// Remove a stream's files and sealed blob; whether it existed. The caller
/// holds the directory lock.
fn delete_files(db: &Db, scope: &Scope, paths: &Paths, name: &str) -> Result<bool> {
    let existed = read_meta(paths, name)?.is_some();
    if existed {
        // The meta file decides existence; remove it first so a crash
        // leaves at worst an orphaned, invisible data file.
        remove_optional(&paths.meta)?;
        remove_optional(&paths.data)?;
    }
    // The blob goes last, so a crash never leaves a sealed stream
    // without its entries. It goes even when the stream is not there,
    // to clear one a crashed seal left.
    seal::delete_blob(db, scope, name)?;
    Ok(existed)
}

/// The error for a stream whose next offset would not fit a `u64`.
fn exhausted() -> StorageError {
    StorageError::backend("stream offset space is exhausted")
}

fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to touch another stream")
}

/// Where the file's complete lines end, and the last complete line.
///
/// Reads backwards from the end in chunks, so the cost is the size of the
/// last line, not of the file.
fn tail(file: &mut File) -> Result<(u64, Option<Vec<u8>>)> {
    const CHUNK: usize = 8192;
    let read_error = io_error("read a stream file");
    let size = file.seek(SeekFrom::End(0)).map_err(&read_error)?;
    let mut start = size;
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if let Some(last) = buf.iter().rposition(|byte| *byte == b'\n') {
            let complete_end = start + last as u64 + 1;
            if let Some(previous) = buf[..last].iter().rposition(|byte| *byte == b'\n') {
                return Ok((complete_end, Some(buf[previous + 1..last].to_vec())));
            }
            if start == 0 {
                return Ok((complete_end, Some(buf[..last].to_vec())));
            }
        } else if start == 0 {
            return Ok((0, None));
        }
        let step = usize::try_from(start).map_or(CHUNK, |start| start.min(CHUNK));
        start -= step as u64;
        file.seek(SeekFrom::Start(start)).map_err(&read_error)?;
        let mut chunk = vec![0; step];
        file.read_exact(&mut chunk).map_err(&read_error)?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
    }
}

/// The stream's length given its meta and file: one past the last complete
/// line's offset, or `base` when that is larger (or there are no lines).
fn length(meta: &StreamMeta, last_line: Option<&[u8]>) -> Result<u64> {
    let after_last = match last_line {
        Some(line) => serde_json::from_slice::<Line>(line)?
            .offset
            .checked_add(1)
            .ok_or_else(exhausted)?,
        None => 0,
    };
    Ok(meta.base.max(after_last))
}

/// The length of an existing stream.
fn stream_len(paths: &Paths, meta: &StreamMeta) -> Result<u64> {
    if let Some(len) = meta.sealed_len {
        return Ok(len);
    }
    match open_read(&paths.data) {
        Ok(mut file) => {
            let (_, last) = tail(&mut file)?;
            length(meta, last.as_deref())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(meta.base),
        Err(error) => Err(io_error("open a stream file")(error)),
    }
}

/// Every complete line of the stream file at or after `from`, at most `limit`.
fn read_lines(paths: &Paths, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
    let file = match open_read(&paths.data) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("open a stream file")(error)),
    };
    let mut reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut line = Vec::new();
    while out.len() < limit {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(io_error("read a stream file"))?;
        // End of file, or a torn final line a crashed append left behind.
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        let parsed: Line = serde_json::from_slice(&line)?;
        if parsed.offset >= from {
            out.push(StreamEntry {
                offset: parsed.offset,
                value: parsed.value,
            });
        }
    }
    Ok(out)
}

impl FileStreams {
    async fn append_inner(
        &self,
        stream: &str,
        expected: Option<u64>,
        values: Vec<Value>,
    ) -> Result<u64> {
        let name = stream.to_owned();
        self.write_db(stream, move |db, _, paths| {
            let now_ms = i64::try_from(db.now()).unwrap_or(i64::MAX);
            let existing = read_meta(paths, &name)?;
            if existing.as_ref().is_some_and(|meta| meta.sealed_len.is_some()) {
                return Err(StorageError::conflict("stream is sealed"));
            }
            let current = existing.as_ref().map_or(Ok(0), |meta| stream_len(paths, meta))?;
            if expected.is_some_and(|offset| offset != current) {
                return Err(StorageError::conflict("stream next offset changed"));
            }
            if values.is_empty() {
                return existing.map_or(Ok(0), |meta| stream_len(paths, &meta));
            }
            let created = existing.is_none();
            let mut meta = existing.unwrap_or(StreamMeta {
                name,
                base: 0,
                sealed_len: None,
                sealed_ms: None,
                last_append_ms: None,
            });
            meta.last_append_ms = Some(now_ms);
            let mut file = open_append(&paths.data)?;
            // A data file without a meta file is an orphan a failed delete
            // left; a new stream starts empty rather than inheriting it.
            let (complete_end, last) = if created { (0, None) } else { tail(&mut file)? };
            let first = length(&meta, last.as_deref())?;
            if first.checked_add(values.len() as u64).is_none() {
                return Err(exhausted());
            }
            let mut bytes = Vec::new();
            for (offset, value) in (first..).zip(values) {
                serde_json::to_writer(&mut bytes, &Line { offset, value })?;
                bytes.push(b'\n');
            }
            if created {
                // Clear any orphan and persist the new entry before the stream
                // becomes visible, so a crash cannot publish a new stream that
                // still holds a deleted one's records.
                file.set_len(0)
                    .and_then(|()| file.sync_data())
                    .map_err(io_error("append to a stream file"))?;
                sync_dir(parent_of(&paths.data))?;
            }
            // The time goes in before the entries: a failed append rolls the
            // entries back and leaves, at worst, a slightly late time.
            write_json(&paths.meta, &meta)?;
            // Cut off a torn line a crashed append left, so the new lines
            // start on a line boundary, then append and flush.
            let written = file
                .set_len(complete_end)
                .and_then(|()| file.write_all(&bytes))
                .and_then(|()| file.sync_data());
            if let Err(error) = written {
                // Nothing may stay appended on failure: roll the file back to
                // its last complete line, and forget a stream this call
                // created. If the rollback itself fails the stream's state is
                // unknown, and the error says so rather than claiming a clean
                // failure.
                let rolled_back = file
                    .set_len(complete_end)
                    .and_then(|()| file.sync_data())
                    .is_ok();
                let forgotten = !created || remove_optional(&paths.meta).is_ok();
                if !(rolled_back && forgotten) {
                    return Err(StorageError::backend(
                        "file storage could not append to a stream file and could not roll it back; the stream's contents are indeterminate",
                    )
                    .with_source(error));
                }
                return Err(io_error("append to a stream file")(error));
            }
            Ok(first)
        })
        .await
    }
}

#[async_trait]
impl StreamStore for FileStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_inner(stream, None, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.append_inner(stream, Some(expected_next_offset), values)
            .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        let name = stream.to_owned();
        self.with_db(stream, move |db, scope, paths| {
            let Some(meta) = read_meta(paths, &name)? else {
                return Ok(Vec::new());
            };
            if meta.sealed_len.is_some() {
                return Ok(seal::read_sealed(db, scope, &name)?.window(from, limit));
            }
            read_lines(paths, from.max(meta.base), limit)
        })
        .await
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            read_meta(paths, &name)?.map_or(Ok(0), |meta| stream_len(paths, &meta))
        })
        .await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        let name = stream.to_owned();
        self.write(stream, move |paths| {
            let Some(mut meta) = read_meta(paths, &name)? else {
                return Ok(0);
            };
            if meta.sealed_len.is_some() {
                return Err(StorageError::conflict("stream is sealed"));
            }
            let len = stream_len(paths, &meta)?;
            let cut = offset.clamp(meta.base, len);
            let removed = cut - meta.base;
            if removed == 0 {
                return Ok(0);
            }
            meta.base = cut;
            write_json(&paths.meta, &meta)?;
            // Copy the retained lines into the replacement file one at a
            // time: memory stays at one line however long the stream is.
            let source = open_read(&paths.data);
            let source = match source {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(io_error("open a stream file")(error)),
            };
            write_atomic_with(&paths.data, |out| {
                let Some(source) = source else { return Ok(()) };
                let mut reader = BufReader::new(source);
                let mut line = Vec::new();
                loop {
                    line.clear();
                    let read = reader
                        .read_until(b'\n', &mut line)
                        .map_err(io_error("read a stream file"))?;
                    // End of file, or a torn final line, which is dropped.
                    if read == 0 || line.last() != Some(&b'\n') {
                        return Ok(());
                    }
                    let parsed: Line = serde_json::from_slice(&line)?;
                    if parsed.offset >= cut {
                        out.write_all(&line).map_err(io_error("write a file"))?;
                    }
                }
            })?;
            Ok(removed)
        })
        .await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        let name = stream.to_owned();
        self.write_db(stream, move |db, scope, paths| {
            delete_files(db, scope, paths, &name)
        })
        .await
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        let name = stream.to_owned();
        // The check and the delete run under the same directory lock.
        self.write_db(stream, move |db, scope, paths| {
            let sealed =
                read_meta(paths, &name)?.is_some_and(|meta| meta.sealed_ms == Some(sealed_ms));
            if !sealed {
                return Ok(false);
            }
            delete_files(db, scope, paths, &name)
        })
        .await
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.seal_stream(stream).await
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        let name = stream.to_owned();
        self.with(stream, move |paths| {
            let Some(meta) = read_meta(paths, &name)? else {
                return Ok(None);
            };
            Ok(Some(StreamInfo::new(
                meta.base,
                stream_len(paths, &meta)?,
                meta.sealed_ms,
                meta.last_append_ms,
            )))
        })
        .await
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let (scope, prefix, generation) = (self.scope.clone(), prefix.to_owned(), self.generation);
        self.db
            .run(move |db| {
                db.check_generation(&scope, generation)?;
                let mut names = Vec::new();
                for path in files_with_suffix(&Self::dir(db, &scope), META_SUFFIX)? {
                    if let Some(meta) = read_json::<StreamMeta>(&path)?
                        && meta.name.starts_with(&prefix)
                    {
                        names.push(meta.name);
                    }
                }
                names.sort();
                Ok(names)
            })
            .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
