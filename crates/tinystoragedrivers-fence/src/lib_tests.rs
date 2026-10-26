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

thread_local! {
    static CREATE_ERROR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn injected_create_error() -> Option<io::Error> {
    CREATE_ERROR.with(|value| {
        value
            .replace(false)
            .then(|| io::Error::from(io::ErrorKind::StorageFull))
    })
}

fn force_unwritable_create() {
    CREATE_ERROR.with(|value| value.set(true));
}
