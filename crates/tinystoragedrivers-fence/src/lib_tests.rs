use super::*;

use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

#[test]
fn dead_pid_stale_lock_is_reclaimed() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(&path, "pid=4294967295\n").unwrap();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    let old = std::time::SystemTime::now() - STALE_AFTER - Duration::from_secs(1);
    file.set_times(fs::FileTimes::new().set_modified(old))
        .unwrap();

    let lease = LocalLeases::new(&path).acquire().unwrap();
    let content = fs::read_to_string(&path).unwrap();
    assert_eq!(content, format!("pid={}\n", std::process::id()));
    drop(lease);
    assert!(!path.exists());
}

#[test]
fn same_process_leaked_owner_is_reclaimed_under_the_process_mutex() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(&path, format!("pid={}\n", std::process::id())).unwrap();

    let lease = LocalLeases::new(&path).acquire().unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        format!("pid={}\n", std::process::id())
    );
    drop(lease);
    assert!(!path.exists());
}

#[test]
fn transient_windows_remove_failures_are_retried() {
    let calls = AtomicUsize::new(0);
    retry_remove(|| {
        if calls.fetch_add(1, Ordering::SeqCst) < 2 {
            Err(io::Error::from_raw_os_error(32))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn read_only_unwritable_filesystem_falls_back_to_lock_free_read() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    force_unwritable_create();
    let lease = LocalLeases::new(&path).acquire_for_read().unwrap();
    assert!(lease.is_none());
    assert!(!path.exists());
}

#[test]
fn writers_do_not_use_lock_free_fallback_on_unwritable_filesystem() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    force_unwritable_create();
    let error = LocalLeases::new(&path).acquire().unwrap_err();
    assert!(is_unwritable_filesystem(&error));
}

#[test]
fn in_process_contention_returns_the_lock_timeout() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    let leases = LocalLeases::new(&path);
    let held = leases.acquire().unwrap();
    let error = leases
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    drop(held);
}

#[test]
fn in_process_contention_waits_before_the_timeout_expires() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    let leases = LocalLeases::new(&path);
    let held = leases.acquire().unwrap();
    let error = leases
        .acquire_with_options(false, Duration::from_millis(2), Duration::from_millis(1))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    drop(held);
}

#[test]
fn a_fresh_lock_with_an_unparseable_pid_is_not_stolen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(&path, "pid=not-a-number\n").unwrap();
    let error = LocalLeases::new(&path)
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(fs::read_to_string(path).unwrap(), "pid=not-a-number\n");
}

#[test]
fn a_fresh_lock_with_a_dead_pid_is_not_stolen_before_the_stale_window() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(&path, "pid=4294967295\n").unwrap();
    let error = LocalLeases::new(&path)
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(fs::read_to_string(path).unwrap(), "pid=4294967295\n");
}

fn age_lock(path: &Path) {
    let file = OpenOptions::new().write(true).open(path).unwrap();
    let old = std::time::SystemTime::now() - STALE_AFTER - Duration::from_secs(1);
    file.set_times(fs::FileTimes::new().set_modified(old))
        .unwrap();
}

#[test]
fn an_empty_lock_left_by_a_dead_owner_is_reclaimed_after_the_stale_window() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(&path, "").unwrap();
    let fresh = LocalLeases::new(&path)
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(fresh.kind(), io::ErrorKind::TimedOut);

    age_lock(&path);
    let lease = LocalLeases::new(&path).acquire().unwrap();
    drop(lease);
    assert!(!path.exists());
}

#[test]
fn an_oversized_lock_file_is_never_read_or_reclaimed() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    fs::write(
        &path,
        vec![b'x'; usize::try_from(MAX_LOCK_BYTES).unwrap() + 1],
    )
    .unwrap();
    age_lock(&path);
    let error = LocalLeases::new(&path)
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(path.exists());
}

#[cfg(unix)]
#[test]
fn a_fifo_at_the_lock_path_times_out_instead_of_blocking_the_read() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    let made = std::process::Command::new("mkfifo").arg(&path).status();
    if !made.is_ok_and(|status| status.success()) {
        return;
    }
    let error = LocalLeases::new(&path)
        .acquire_with_options(false, Duration::ZERO, Duration::ZERO)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
}

#[test]
fn a_lock_directory_that_cannot_be_created_on_a_read_only_filesystem_falls_back_for_readers() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("missing").join("profiles.lock");
    force_unwritable_dir();
    assert!(
        LocalLeases::new(&path)
            .acquire_for_read()
            .unwrap()
            .is_none()
    );
    force_unwritable_dir();
    let error = LocalLeases::new(&path).acquire().unwrap_err();
    assert!(is_unwritable_filesystem(&error));
}

#[test]
fn a_lock_sync_failure_removes_the_lock_file() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    force_sync_error();
    let error = LocalLeases::new(&path).acquire().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(!path.exists());
}

#[test]
fn poisoned_process_mutex_is_recovered() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    let lock = in_process_lock(&path);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = lock.lock().unwrap();
        panic!("poison test lock");
    }));
    let lease = LocalLeases::new(&path).acquire().unwrap();
    drop(lease);
}

#[test]
fn a_lock_write_failure_removes_the_partial_lock_file() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("profiles.lock");
    force_write_error();
    let error = LocalLeases::new(&path).acquire().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(!path.exists());
}

#[test]
fn non_transient_remove_errors_return_without_retrying() {
    let calls = AtomicUsize::new(0);
    let error = retry_remove(|| {
        calls.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from_raw_os_error(87))
    })
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(87));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn pid_liveness_is_conservative_for_the_current_process() {
    assert!(is_pid_alive(std::process::id()));
    assert!(!is_pid_alive(u32::MAX));
}

thread_local! {
    static CREATE_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static WRITE_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DIR_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SYNC_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) fn injected_create_error() -> Option<io::Error> {
    CREATE_ERROR.with(|value| {
        value
            .replace(false)
            .then(|| io::Error::from(io::ErrorKind::StorageFull))
    })
}

fn force_unwritable_create() {
    CREATE_ERROR.with(|value| value.set(true));
}

pub(super) fn injected_write_error() -> Option<io::Error> {
    WRITE_ERROR.with(|value| {
        value
            .replace(false)
            .then(|| io::Error::from(io::ErrorKind::Other))
    })
}

fn force_write_error() {
    WRITE_ERROR.with(|value| value.set(true));
}

pub(super) fn injected_sync_error() -> Option<io::Error> {
    SYNC_ERROR.with(|value| {
        value
            .replace(false)
            .then(|| io::Error::from(io::ErrorKind::Other))
    })
}

fn force_sync_error() {
    SYNC_ERROR.with(|value| value.set(true));
}

pub(super) fn injected_create_dir_error() -> Option<io::Error> {
    DIR_ERROR.with(|value| {
        value
            .replace(false)
            .then(|| io::Error::from(io::ErrorKind::ReadOnlyFilesystem))
    })
}

fn force_unwritable_dir() {
    DIR_ERROR.with(|value| value.set(true));
}
