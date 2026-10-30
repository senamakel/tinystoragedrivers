//! Process-owned file leases for hosts that coordinate through local files.
//!
//! The lease writes the current process id to one create-new lock file. It
//! reclaims dead or self-owned leaked locks after verifying the named file,
//! and releases ownership when the guard drops. This is a local filesystem
//! coordination primitive, not a distributed fencing token.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

const STALE_AFTER: Duration = Duration::from_secs(10);
const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
const LOCK_POLL: Duration = Duration::from_millis(50);
const REMOVE_RETRIES: usize = 5;
/// A lock file holds one short `pid=` line; anything larger is not ours.
const MAX_LOCK_BYTES: u64 = 4096;

/// A lock file backed by local PID ownership.
#[derive(Debug, Clone)]
pub struct LocalLeases {
    lock_path: PathBuf,
}

impl LocalLeases {
    /// Bind a lease to the exact lock-file path. Construction performs no I/O.
    #[must_use]
    pub fn new(lock_path: impl Into<PathBuf>) -> Self {
        Self {
            lock_path: lock_path.into(),
        }
    }

    /// Acquire exclusive ownership, reclaiming dead or leaked local owners.
    ///
    /// # Errors
    /// Returns an I/O error if the lock cannot be created or inspected before
    /// the timeout expires.
    pub fn acquire(&self) -> io::Result<LocalLeaseGuard> {
        self.acquire_with_read_fallback(false)?
            .ok_or_else(|| io::Error::other("lock creation is unavailable on this filesystem"))
    }

    /// Acquire ownership for a read, or return `None` when creating the lock
    /// fails because the filesystem is full or read-only. This fallback is
    /// safe only for readers of atomically published files.
    ///
    /// # Errors
    /// Returns an I/O error for contention, unsafe stale-lock state, or other
    /// lock failures. It never falls back for those cases.
    pub fn acquire_for_read(&self) -> io::Result<Option<LocalLeaseGuard>> {
        self.acquire_with_read_fallback(true)
    }

    fn acquire_with_read_fallback(
        &self,
        read_fallback: bool,
    ) -> io::Result<Option<LocalLeaseGuard>> {
        self.acquire_with_options(read_fallback, LOCK_TIMEOUT, LOCK_POLL)
    }

    fn acquire_with_options(
        &self,
        read_fallback: bool,
        timeout: Duration,
        poll: Duration,
    ) -> io::Result<Option<LocalLeaseGuard>> {
        let parent = self
            .lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        #[cfg(test)]
        let created =
            tests::injected_create_dir_error().map_or_else(|| fs::create_dir_all(parent), Err);
        #[cfg(not(test))]
        let created = fs::create_dir_all(parent);
        match created {
            Ok(()) => {}
            Err(error) if read_fallback && is_unwritable_filesystem(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        let in_process = in_process_lock(&self.lock_path);
        let started = Instant::now();
        let in_process_guard = loop {
            match in_process.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::Poisoned(poisoned)) => break poisoned.into_inner(),
                Err(TryLockError::WouldBlock) if started.elapsed() < timeout => {
                    thread::sleep(poll);
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "lease is held in process",
                    ));
                }
            }
        };

        loop {
            #[cfg(test)]
            let opened = tests::injected_create_error()
                .map_or_else(|| create_lock_file(&self.lock_path), Err);
            #[cfg(not(test))]
            let opened = create_lock_file(&self.lock_path);
            match opened {
                Ok(mut file) => {
                    #[cfg(test)]
                    let write_result = tests::injected_write_error()
                        .map_or_else(|| writeln!(file, "pid={}", std::process::id()), Err);
                    #[cfg(not(test))]
                    let write_result = writeln!(file, "pid={}", std::process::id());
                    if let Err(error) = write_result {
                        let _ = retry_remove(|| fs::remove_file(&self.lock_path));
                        return Err(error);
                    }
                    #[cfg(test)]
                    let sync_result =
                        tests::injected_sync_error().map_or_else(|| file.sync_all(), Err);
                    #[cfg(not(test))]
                    let sync_result = file.sync_all();
                    if let Err(error) = sync_result {
                        let _ = retry_remove(|| fs::remove_file(&self.lock_path));
                        return Err(error);
                    }
                    return Ok(Some(LocalLeaseGuard {
                        lock_path: self.lock_path.clone(),
                        _in_process: in_process_guard,
                    }));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    if self.reclaim_if_stale()? {
                        continue;
                    }
                }
                Err(error) if read_fallback && is_unwritable_filesystem(&error) => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
            if started.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for local lease",
                ));
            }
            thread::sleep(poll);
        }
    }

    fn reclaim_if_stale(&self) -> io::Result<bool> {
        let metadata = match fs::metadata(&self.lock_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        // A FIFO or device at the lock path would block the read below past
        // the acquisition timeout, and an oversized file is not a lock this
        // crate wrote; neither is reclaimed, so the caller times out instead.
        if metadata.len() > MAX_LOCK_BYTES || !(metadata.is_file() || metadata.is_dir()) {
            return Ok(false);
        }
        let age = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok());
        let mut contents = String::new();
        match open_lock_for_read(&self.lock_path).and_then(|mut file| {
            // The path can change type between the metadata check and the
            // open, so the handle itself is checked before it is read.
            if file.metadata()?.is_file() {
                file.read_to_string(&mut contents).map(|_| ())
            } else {
                Err(io::Error::from(io::ErrorKind::InvalidInput))
            }
        }) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        }
        let pid = contents
            .trim()
            .strip_prefix("pid=")
            .and_then(|value| value.parse::<u32>().ok());
        let self_owned_leak = pid == Some(std::process::id());
        let stale = age.is_some_and(|age| age >= STALE_AFTER);
        // An owner that died between creating the file and writing its pid
        // leaves it empty; after the stale window no live owner is still
        // about to write it.
        let abandoned_before_write = stale && contents.trim().is_empty();
        let dead_stale_owner = stale && pid.is_some_and(|pid| !is_pid_alive(pid));
        if !self_owned_leak && !dead_stale_owner && !abandoned_before_write {
            return Ok(false);
        }
        let current = match fs::metadata(&self.lock_path) {
            Ok(current) => current,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        if current.len() != metadata.len() || current.modified().ok() != metadata.modified().ok() {
            return Ok(false);
        }
        match retry_remove(|| fs::remove_file(&self.lock_path)) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error),
        }
    }
}

/// A held local lease. Dropping it removes the owned lock file.
#[derive(Debug)]
pub struct LocalLeaseGuard {
    lock_path: PathBuf,
    _in_process: MutexGuard<'static, ()>,
}

impl Drop for LocalLeaseGuard {
    fn drop(&mut self) {
        let _ = retry_remove(|| fs::remove_file(&self.lock_path));
    }
}

fn in_process_lock(path: &Path) -> &'static Mutex<()> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, &'static Mutex<()>>>> = OnceLock::new();
    let key = path
        .parent()
        .and_then(|parent| fs::canonicalize(parent).ok())
        .and_then(|parent| path.file_name().map(|name| parent.join(name)))
        .unwrap_or_else(|| path.to_path_buf());
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    locks
        .entry(key)
        .or_insert_with(|| Box::leak(Box::new(Mutex::new(()))))
}

/// Check process liveness conservatively. A failed probe means alive, so stale
/// recovery never removes a lock based on an inconclusive OS response.
#[must_use]
pub fn is_pid_alive(pid: u32) -> bool {
    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
    let target = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[target]),
        true,
        ProcessRefreshKind::nothing(),
    );
    if system.process(target).is_some() {
        return true;
    }
    // sysinfo also drops a process whose `/proc/<pid>/stat` it cannot read,
    // which is not proof of death; a surviving `/proc/<pid>` entry is.
    #[cfg(target_os = "linux")]
    if Path::new(&format!("/proc/{pid}")).exists() {
        return true;
    }
    false
}

fn is_unwritable_filesystem(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::ReadOnlyFilesystem
    )
}

/// Open for reading without blocking on a FIFO swapped in at the lock path.
fn open_lock_for_read(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    options.open(path)
}

fn create_lock_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn retry_remove(mut remove: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    let mut attempt = 0;
    loop {
        match remove() {
            Ok(()) => return Ok(()),
            Err(error) if is_transient_remove_error(&error) && attempt < REMOVE_RETRIES - 1 => {
                attempt += 1;
                thread::sleep(Duration::from_millis(20 * (1 << (attempt - 1))));
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_transient_remove_error(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::PermissionDenied
        || matches!(error.raw_os_error(), Some(5 | 32 | 33))
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
