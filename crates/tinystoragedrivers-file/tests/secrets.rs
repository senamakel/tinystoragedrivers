//! Secrets stored as encrypted documents on the file driver.

use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_file::FileStorage;
use tinystoragedrivers_secrets::testkit::document_secrets_conformance;

#[tokio::test]
async fn document_secrets_conform_and_stay_ciphertext_at_rest() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    document_secrets_conformance(&storage.for_scope(&Scope::local()).unwrap()).await;
}
