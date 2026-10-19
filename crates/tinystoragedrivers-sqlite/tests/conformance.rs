//! The SQLite driver passes the shared conformance suite in both layouts.

use tinystoragedrivers_core::conformance;
use tinystoragedrivers_sqlite::SqliteStorage;

#[tokio::test]
async fn directory_layout_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    conformance::run(&storage, false).await;
}

#[tokio::test]
async fn single_file_layout_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path().join("all.db")).unwrap();
    conformance::run(&storage, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn conforms_on_a_current_thread_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    conformance::run(&storage, false).await;
}

#[tokio::test]
async fn directory_layout_shares_one_store_across_handles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_owned();
    conformance::run_shared(|| {
        let storage = SqliteStorage::open(&path).unwrap();
        async move { storage }
    })
    .await;
}

#[tokio::test]
async fn single_file_layout_shares_one_store_across_handles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    conformance::run_shared(|| {
        let storage = SqliteStorage::open(&path).unwrap();
        async move { storage }
    })
    .await;
}
