//! Small filesystem helpers: atomic replacement, optional reads, directory
//! listings, and the mapping from [`std::io::Error`] to [`StorageError`].
//!
//! Every write that replaces a whole file goes through [`write_atomic`]: the
//! bytes land in a temporary file in the same directory, are flushed to disk,
//! and are renamed over the target, so a reader (or a crash) sees the old file
//! or the new one, never a torn mix. Temporary files start with `.`, which no
//! encoded name does, so listings skip any a crash left behind.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tinystoragedrivers_core::{Result, StorageError};

/// Build a mapper from an IO error to a [`StorageError`] that says what was
/// being done. Messages name the operation, never a path: paths embed scope
/// names and ids, which may be untrusted.
pub(crate) fn io_error(action: &'static str) -> impl Fn(io::Error) -> StorageError {
    move |error| {
        let message = format!("file storage could not {action}");
        let kind = error.kind();
        let base = if matches!(
            kind,
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
        ) {
            StorageError::unavailable(message)
        } else {
            StorageError::backend(message)
        };
        base.with_source(error)
    }
}

/// Make `options` refuse a symlink at the final path component: `O_NOFOLLOW`
/// on Unix, and on Windows opening the reparse point itself, which
/// [`require_regular`] then rejects.
fn no_follow(options: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT
        options.custom_flags(0x0020_0000)
    }
    #[cfg(not(any(unix, windows)))]
    {
        options
    }
}

/// Fail unless `file` is a regular file, so a symlink, device or directory
/// planted at a data path is refused rather than read or appended to.
fn require_regular(file: File) -> io::Result<File> {
    if file.metadata()?.file_type().is_file() {
        Ok(file)
    } else {
        Err(io::Error::other("not a regular file"))
    }
}

/// Open `path` for reading without following a symlink; only a regular file
/// opens.
pub(crate) fn open_read(path: &Path) -> io::Result<File> {
    require_regular(no_follow(OpenOptions::new().read(true)).open(path)?)
}

/// Read a whole file, or `None` when it does not exist. A symlink is refused.
pub(crate) fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    match open_read(path).and_then(|mut file| file.read_to_end(&mut bytes)) {
        Ok(_) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("read a file")(error)),
    }
}

/// Read and decode a JSON file, or `None` when it does not exist.
pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match read_optional(path)? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Encode `value` as pretty JSON and replace `path` with it atomically.
pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_atomic(path, &bytes)
}

/// A temporary file name, unique within this process, in `dir`.
fn temp_path(dir: &Path) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    dir.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// The directory holding `path`. A bare file name (`state.json`) lives in the
/// current directory; only a path with no parent at all (`/`) is refused.
fn parent(path: &Path) -> Result<&Path> {
    match path.parent() {
        Some(dir) if dir.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(dir) => Ok(dir),
        None => Err(StorageError::backend(
            "file storage path has no parent directory",
        )),
    }
}

/// Replace `path` with `bytes`: write a sibling temporary file, flush it, and
/// rename it into place. Creates the directory when missing.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    write_atomic_with(path, |file| {
        file.write_all(bytes).map_err(io_error("write a file"))
    })
}

/// [`write_atomic`] with the content produced by `fill`, which writes straight
/// into the temporary file so a large file is never held in memory.
///
/// The temporary file is created exclusively (`create_new`), so a name planted
/// beforehand, including a symlink, is never opened or followed.
pub(crate) fn write_atomic_with(
    path: &Path,
    fill: impl FnOnce(&mut File) -> Result<()>,
) -> Result<()> {
    let dir = parent(path)?;
    create_dir_durable(dir)?;
    let (temp, mut out) = create_temp(dir)?;
    let written = (|| {
        fill(&mut out)?;
        out.sync_all().map_err(io_error("write a file"))?;
        drop(out);
        fs::rename(&temp, path).map_err(io_error("write a file"))
    })();
    if let Err(error) = written {
        // Best effort: the temporary file is invisible to listings anyway.
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    sync_dir(dir)
}

/// Exclusively create a fresh temporary file in `dir`, retrying with a new name
/// when one is already taken.
fn create_temp(dir: &Path) -> Result<(PathBuf, File)> {
    const ATTEMPTS: usize = 16;
    for _ in 0..ATTEMPTS {
        let temp = temp_path(dir);
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error("write a file")(error)),
        }
    }
    Err(StorageError::backend(
        "file storage could not find a free temporary file name",
    ))
}

/// `create_dir_all`, then sync the parent of every directory it created, so
/// the whole new hierarchy (not just its leaf) survives a crash.
pub(crate) fn create_dir_durable(dir: &Path) -> Result<()> {
    let mut missing = Vec::new();
    for ancestor in dir.ancestors() {
        if ancestor.as_os_str().is_empty() || ancestor.exists() {
            break;
        }
        missing.push(ancestor);
    }
    fs::create_dir_all(dir).map_err(io_error("create a directory"))?;
    for created in missing {
        sync_dir(parent(created)?)?;
    }
    Ok(())
}

/// Flush a directory entry change (a rename or a new file) to disk. Windows
/// cannot open a directory for this and orders renames itself, so it is a
/// no-op there. Elsewhere a failure is reported: without it the new entry may
/// not survive a crash, which a durable write must not claim.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(io_error("sync a directory"))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Remove a file, reporting whether it existed. The parent directory is
/// synced so the removal survives a crash.
pub(crate) fn remove_optional(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => {
            sync_dir(parent(path)?)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("remove a file")(error)),
    }
}

/// The names of the regular files in `dir` that end with `suffix`, excluding
/// temporary files. A missing directory has none.
pub(crate) fn files_with_suffix(dir: &Path, suffix: &str) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("list a directory")(error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(io_error("list a directory"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') || !name.ends_with(suffix) {
            continue;
        }
        if entry
            .file_type()
            .map_err(io_error("list a directory"))?
            .is_file()
        {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

/// Visit regular files in one directory without materializing the listing.
/// Hidden temporary files are skipped just like [`files_with_suffix`].
pub(crate) fn for_each_file_with_suffix<F>(dir: &Path, suffix: &str, mut visit: F) -> Result<()>
where
    F: FnMut(PathBuf) -> Result<()>,
{
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list a directory")(error)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error("list a directory"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') || !name.ends_with(suffix) {
            continue;
        }
        if entry
            .file_type()
            .map_err(io_error("list a directory"))?
            .is_file()
        {
            visit(entry.path())?;
        }
    }
    Ok(())
}

/// Open `path` for appending, creating it (and its directory) when missing.
/// A symlink at `path` is refused, not followed.
pub(crate) fn open_append(path: &Path) -> Result<File> {
    create_dir_durable(parent(path)?)?;
    no_follow(OpenOptions::new().read(true).append(true).create(true))
        .open(path)
        .and_then(require_regular)
        .map_err(io_error("open a stream file"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
