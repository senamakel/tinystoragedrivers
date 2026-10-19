//! Secrets stored as encrypted documents on the SQLite driver.

use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_secrets::testkit::document_secrets_conformance;
use tinystoragedrivers_sqlite::SqliteStorage;

#[tokio::test]
async fn document_secrets_conform_and_stay_ciphertext_at_rest() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    document_secrets_conformance(&storage.for_scope(&Scope::local()).unwrap()).await;
}
