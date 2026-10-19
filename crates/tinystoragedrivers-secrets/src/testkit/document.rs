//! The secrets suite over a storage backend: [`DocumentSecrets`] on any
//! driver's documents.

use std::panic::resume_unwind;
use std::sync::Arc;

use tinystoragedrivers_core::ScopedStorage;

use super::{CatchUnwind, secrets_conformance, unique_prefix};
use crate::store::SecretStore;
use crate::{DEFAULT_COLLECTION, DerivedKeys, DocumentSecrets, crypto};

/// Run [`secrets_conformance`] over [`DocumentSecrets`] on `storage`, then
/// check what the driver holds at rest.
///
/// A driver's tests call this with a scope of a freshly opened backend, so
/// every driver proves it keeps secrets as the memory backend does: the
/// behavioral suite passes, and the stored document is a single `enc2:`
/// ciphertext field that never contains the plaintext.
///
/// # Panics
///
/// On the first check that fails, naming it.
pub async fn document_secrets_conformance(storage: &ScopedStorage) {
    let keys = crypto::generate_key().expect("generate a master key");
    let store = DocumentSecrets::new(storage, Arc::new(DerivedKeys::new(keys)));
    assert_eq!(store.backend_name(), "document");
    secrets_conformance(&store).await;
    ciphertext_at_rest(storage, &store).await;
}

/// The stored document is only `enc2:` ciphertext.
async fn ciphertext_at_rest(storage: &ScopedStorage, store: &DocumentSecrets) {
    let name = format!("{}api_key", unique_prefix());
    let plaintext = "sk-live-conformance-0123456789";
    store
        .set(&name, plaintext.as_bytes())
        .await
        .expect("set a secret");
    let outcome = CatchUnwind(Box::pin(async {
        let doc = storage
            .documents()
            .get(DEFAULT_COLLECTION, &name)
            .await
            .expect("read the stored document")
            .expect("the secret is stored as a document under its name")
            .doc;
        let ciphertext = doc["ciphertext"]
            .as_str()
            .expect("the document holds a ciphertext string");
        assert!(
            crypto::is_enc2(ciphertext),
            "the stored value must be enc2 ciphertext"
        );
        assert!(
            !doc.to_string().contains("sk-live"),
            "the plaintext must not reach the driver"
        );
        assert_eq!(
            doc.as_object().map(serde_json::Map::len),
            Some(1),
            "the document holds the ciphertext and nothing else"
        );
        let read = store.get(&name).await.expect("read the secret back");
        assert_eq!(
            read.as_deref().map(Vec::as_slice),
            Some(plaintext.as_bytes()),
            "the secret decrypts to what was stored"
        );
    }))
    .await;
    // Best effort and unconditional, as in `secrets_conformance`.
    let _ = store.delete(&name).await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
