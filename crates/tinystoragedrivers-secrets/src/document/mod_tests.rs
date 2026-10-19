//! The document driver on the core memory backend.

use serde_json::json;
use tinystoragedrivers_core::{
    ErrorKind, MemoryStorage, Precondition as DocumentPrecondition, StorageBackend,
};

use super::*;
use crate::crypto::tests::{OPENHUMAN_ENC2, fixture_key};
use crate::keys::{DerivedKeys, StaticKey};
use crate::testkit::{secrets_conformance, versioned_secrets_conformance};

fn derived() -> Arc<dyn KeyProvider> {
    Arc::new(DerivedKeys::new(crypto::generate_key().unwrap()))
}

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store = DocumentSecrets::new(&storage, derived());
    secrets_conformance(&store).await;
    versioned_secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "document");
}

#[tokio::test]
async fn independent_document_handles_have_atomic_versioned_writes() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let keys = derived();
    let first = DocumentSecrets::new(&storage, Arc::clone(&keys));
    let second = DocumentSecrets::new(&storage, keys);
    first
        .set_if("shared", b"initial", crate::Precondition::Absent)
        .await
        .unwrap();
    let (_, first_version) = first.get_versioned("shared").await.unwrap().unwrap();
    let (_, second_version) = second.get_versioned("shared").await.unwrap().unwrap();
    assert_eq!(first_version, second_version);
    let version = first_version;

    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let first_write = async {
        first_barrier.wait().await;
        first
            .set_if(
                "shared",
                b"writer-a",
                crate::Precondition::Version(version.clone()),
            )
            .await
    };
    let second_write = async {
        barrier.wait().await;
        second
            .set_if("shared", b"writer-b", crate::Precondition::Version(version))
            .await
    };
    let (first_result, second_result) = tokio::join!(first_write, second_write);
    assert_eq!(
        first_result.is_ok() as usize + second_result.is_ok() as usize,
        1
    );
    let loser = first_result.err().or_else(|| second_result.err()).unwrap();
    assert_eq!(loser.kind(), ErrorKind::Conflict);
    let current = first.get("shared").await.unwrap().unwrap();
    assert!(current.as_slice() == b"writer-a" || current.as_slice() == b"writer-b");
}

#[tokio::test]
async fn stores_only_enc2_ciphertext_in_the_document() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store = DocumentSecrets::new(&storage, derived());
    store.set("api_key", b"sk-live-abc").await.unwrap();
    let doc = storage
        .documents()
        .get(DEFAULT_COLLECTION, "api_key")
        .await
        .unwrap()
        .unwrap()
        .doc;
    let ciphertext = doc["ciphertext"].as_str().unwrap();
    assert!(crypto::is_enc2(ciphertext));
    assert!(!doc.to_string().contains("sk-live"));
    assert_eq!(doc.as_object().unwrap().len(), 1);
}

#[tokio::test]
async fn reads_an_enc2_value_openhuman_wrote() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put(
            DEFAULT_COLLECTION,
            "migrated",
            json!({ "ciphertext": OPENHUMAN_ENC2 }),
            DocumentPrecondition::Absent,
        )
        .await
        .unwrap();
    let store = DocumentSecrets::new(&storage, Arc::new(StaticKey::new(fixture_key())));
    assert_eq!(
        store.get("migrated").await.unwrap().unwrap().as_slice(),
        b"sk-test-123"
    );
}

#[tokio::test]
async fn binary_values_round_trip() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store = DocumentSecrets::new(&storage, derived());
    store.set("bin", &[0, 0xff, 0x80]).await.unwrap();
    assert_eq!(
        store.get("bin").await.unwrap().unwrap().as_slice(),
        [0, 0xff, 0x80]
    );
}

#[tokio::test]
async fn tenants_get_distinct_keys_and_cannot_read_each_other() {
    let backend = MemoryStorage::new();
    let keys = derived();
    let a = DocumentSecrets::new(
        &backend.for_scope(&Scope::new("a").unwrap()).unwrap(),
        Arc::clone(&keys),
    );
    let b_storage = backend.for_scope(&Scope::new("b").unwrap()).unwrap();
    let b = DocumentSecrets::new(&b_storage, Arc::clone(&keys));
    a.set("shared-name", b"for-a").await.unwrap();
    assert!(b.get("shared-name").await.unwrap().is_none());

    // Even a ciphertext copied across tenants does not decrypt under the
    // other tenant's derived key.
    let a_doc = backend
        .for_scope(&Scope::new("a").unwrap())
        .unwrap()
        .documents()
        .get(DEFAULT_COLLECTION, "shared-name")
        .await
        .unwrap()
        .unwrap()
        .doc;
    b_storage
        .documents()
        .put(
            DEFAULT_COLLECTION,
            "shared-name",
            a_doc,
            DocumentPrecondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        b.get("shared-name").await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[tokio::test]
async fn a_malformed_document_is_a_serialization_error() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    storage
        .documents()
        .put(
            DEFAULT_COLLECTION,
            "bad",
            json!({ "ciphertext": 7 }),
            DocumentPrecondition::None,
        )
        .await
        .unwrap();
    let store = DocumentSecrets::new(&storage, derived());
    assert_eq!(
        store.get("bad").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn a_custom_collection_is_used_and_validated() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store =
        DocumentSecrets::from_parts(Arc::clone(storage.documents()), Scope::local(), derived())
            .with_collection("vault")
            .unwrap();
    assert_eq!(store.collection(), "vault");
    assert_eq!(store.scope(), &Scope::local());
    store.set("a", b"1").await.unwrap();
    assert!(
        storage
            .documents()
            .get("vault", "a")
            .await
            .unwrap()
            .is_some()
    );

    let error = store.clone().with_collection("bad name").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[test]
fn debug_names_scope_and_collection_only() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store = DocumentSecrets::new(&storage, Arc::new(StaticKey::new(fixture_key())));
    let debug = format!("{store:?}");
    assert!(debug.contains("local") && debug.contains("secrets"));
    assert!(!debug.contains("0001020304"));
}

/// A provider whose key source is down.
#[derive(Debug)]
struct Outage;

impl KeyProvider for Outage {
    fn data_key(&self, _scope: &Scope) -> Result<Zeroizing<[u8; crypto::KEY_LEN]>> {
        Err(StorageError::unavailable("kms unreachable"))
    }
}

#[tokio::test]
async fn a_key_provider_failure_propagates() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let store = DocumentSecrets::new(&storage, Arc::new(Outage));
    assert_eq!(
        store.set("a", b"1").await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    storage
        .documents()
        .put(
            DEFAULT_COLLECTION,
            "a",
            json!({ "ciphertext": "enc2:00" }),
            DocumentPrecondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get("a").await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
}
