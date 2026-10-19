//! Secrets stored as encrypted documents on a real MongoDB, named `live_*`.

mod support;

use support::{connect, skip};
use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_secrets::testkit::document_secrets_conformance;

#[tokio::test]
async fn live_document_secrets_conform_and_stay_ciphertext_at_rest() {
    let Some(backend) = connect().await else {
        return skip("live_document_secrets_conform_and_stay_ciphertext_at_rest");
    };
    document_secrets_conformance(&backend.for_scope(&Scope::local()).unwrap()).await;
}
