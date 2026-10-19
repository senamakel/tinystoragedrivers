//! Connections are shared per file, configured for WAL, and usable with or
//! without a runtime.

use super::*;

#[test]
fn the_same_file_shares_one_connection() {
    let dir = tempfile::tempdir().unwrap();
    let a = Db::open(&dir.path().join("x.db")).unwrap();
    let b = Db::open(&dir.path().join("./x.db")).unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    let other = Db::open(&dir.path().join("y.db")).unwrap();
    assert!(!Arc::ptr_eq(&a, &other));
    assert!(a.path().ends_with("x.db"));
    assert!(format!("{a:?}").contains("x.db"));
}

#[test]
fn a_closed_file_reopens_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("z.db");
    let first = Db::open(&path).unwrap();
    let weak = Arc::downgrade(&first);
    drop(first);
    assert!(weak.upgrade().is_none());
    let again = Db::open(&path).unwrap();
    assert!(again.path().ends_with("z.db"));
}

#[test]
fn read_only_and_writable_handles_use_separate_connections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.db");
    let writable = Db::open(&path).unwrap();
    let read_only = Db::open_read_only(&path).unwrap();
    assert!(!Arc::ptr_eq(&writable, &read_only));
    let error = read_only
        .run_now(|conn| {
            conn.execute("CREATE TABLE forbidden (id INTEGER)", [])
                .map(|_| ())
                .map_err(during("read-only test"))
        })
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
}

#[test]
fn creates_missing_directories_and_uses_wal() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("nested/deeper/w.db")).unwrap();
    let mode: String = db
        .run_now(|conn| {
            conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .map_err(during("test"))
        })
        .unwrap();
    assert_eq!(mode, "wal");
}

#[test]
fn rejects_a_path_without_a_file_name() {
    assert!(normalize(Path::new("/")).is_err());
    assert!(normalize(Path::new("relative.db")).unwrap().is_absolute());
}

#[test]
fn runs_without_a_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("r.db")).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // `block_on` from a plain thread: `run` sees no ambient runtime handle
    // until inside the future, where it uses the blocking pool.
    let n: i64 = runtime
        .block_on(db.run(|conn| {
            conn.query_row("SELECT 41 + 1", [], |row| row.get(0))
                .map_err(during("test"))
        }))
        .unwrap();
    assert_eq!(n, 42);
    let inline: i64 = futures_lite_block_on(db.run(|_| Ok(7)));
    assert_eq!(inline, 7);
}

#[test]
fn reports_unwritable_directories() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("plain");
    std::fs::write(&file, b"x").unwrap();
    assert!(Db::open(&file.join("under-a-file.db")).is_err());
}

/// Poll a future to completion on the current thread without a tokio runtime.
fn futures_lite_block_on<T>(future: impl std::future::Future<Output = Result<T>>) -> T {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value.unwrap();
        }
    }
}

#[test]
fn io_failures_keep_their_cause() {
    let error = io_error("cannot resolve")(std::io::Error::other("gone"));
    assert_eq!(error.message(), "cannot resolve");
    assert!(std::error::Error::source(&error).is_some());
}

#[tokio::test]
async fn a_panicking_call_is_reported_and_poisons_only_its_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("p.db")).unwrap();
    let error = db
        .run(|_| -> Result<()> { panic!("statement exploded") })
        .await
        .unwrap_err();
    assert_eq!(error.message(), "sqlite call did not complete");
    let poisoned = db.run(|_| Ok(())).await.unwrap_err();
    assert_eq!(poisoned.message(), "sqlite connection lock poisoned");
    let other = Db::open(&dir.path().join("q.db")).unwrap();
    other.run(|_| Ok(())).await.unwrap();
}

#[cfg(unix)]
#[test]
fn a_symlinked_file_shares_its_targets_connection() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real.db");
    let target = Db::open(&real).unwrap();
    std::os::unix::fs::symlink(&real, dir.path().join("alias.db")).unwrap();
    let alias = Db::open(&dir.path().join("alias.db")).unwrap();
    assert!(Arc::ptr_eq(&target, &alias));
}

#[test]
fn a_panic_without_a_runtime_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("inline.db")).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        futures_lite_block_on_result(db.run(|_| -> Result<()> { panic!("boom") }))
    }));
    let error = outcome.expect("the panic is caught by run").unwrap_err();
    assert_eq!(error.message(), "sqlite call did not complete");
}

fn futures_lite_block_on_result<T>(
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    use std::task::{Context, Poll, Waker};
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}
