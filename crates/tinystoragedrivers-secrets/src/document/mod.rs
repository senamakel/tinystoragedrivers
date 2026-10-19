//! [`DocumentSecrets`]: `enc2:` ciphertext in any [`DocumentStore`], one
//! document per secret, keyed per scope.
//!
//! This is the cloud path. Each secret is the document
//! `{"ciphertext": "enc2:<hex>"}` with the secret name as its id, in the
//! collection [`DEFAULT_COLLECTION`] (or one the host picks). The value is
//! encrypted under the data key the [`KeyProvider`] returns for the handle's
//! scope, so with [`DerivedKeys`](crate::DerivedKeys) every tenant's secrets
//! are under a different key, and the database only ever sees ciphertext.
//!
//! The ciphertext is a plain `enc2:` value (no associated data), so an
//! encrypted config field moves into a document unchanged; tenant isolation
//! comes from the per-scope key and the document store's own scoping.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    DocumentStore, DocumentStoreExt, ErrorKind, Precondition as DocumentPrecondition, Query,
    Result, Scope, ScopedStorage, StorageError, Versioned, validate_collection,
};
use zeroize::Zeroizing;

use crate::crypto;
use crate::keys::KeyProvider;
use crate::store::{
    Precondition, SecretStore, SecretVersion, validate_name, validate_prefix, version_from_parts,
};

/// The collection secrets live in unless the host picks another.
pub const DEFAULT_COLLECTION: &str = "secrets";

/// The document field holding the `enc2:` ciphertext.
const CIPHERTEXT_FIELD: &str = "ciphertext";

/// Secrets as encrypted documents.
#[derive(Clone)]
pub struct DocumentSecrets {
    documents: Arc<dyn DocumentStore>,
    scope: Scope,
    keys: Arc<dyn KeyProvider>,
    collection: String,
}

impl DocumentSecrets {
    /// Secrets in `storage`'s documents, under the key `keys` gives its scope.
    #[must_use]
    pub fn new(storage: &ScopedStorage, keys: Arc<dyn KeyProvider>) -> Self {
        Self::from_parts(
            Arc::clone(storage.documents()),
            storage.scope().clone(),
            keys,
        )
    }

    /// Secrets in `documents`, which must be bound to `scope`; the scope picks
    /// the data key.
    #[must_use]
    pub fn from_parts(
        documents: Arc<dyn DocumentStore>,
        scope: Scope,
        keys: Arc<dyn KeyProvider>,
    ) -> Self {
        Self {
            documents,
            scope,
            keys,
            collection: DEFAULT_COLLECTION.to_string(),
        }
    }

    /// Keep the secrets in `collection` instead of [`DEFAULT_COLLECTION`].
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid collection name.
    pub fn with_collection(mut self, collection: impl Into<String>) -> Result<Self> {
        let collection = collection.into();
        validate_collection(&collection)?;
        self.collection = collection;
        Ok(self)
    }

    /// The collection secrets are stored in.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The scope whose data key encrypts these secrets.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
}

impl fmt::Debug for DocumentSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentSecrets")
            .field("scope", &self.scope)
            .field("collection", &self.collection)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SecretStore for DocumentSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        validate_name(name)?;
        let Some(found) = self.documents.get(&self.collection, name).await? else {
            return Ok(None);
        };
        let ciphertext = found
            .doc
            .get(CIPHERTEXT_FIELD)
            .and_then(Value::as_str)
            .ok_or_else(|| {
                StorageError::serialization("secret document has no string ciphertext field")
            })?;
        let key = self.keys.data_key_async(&self.scope).await?;
        crypto::decrypt_enc2(&key, ciphertext).map(Some)
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_name(name)?;
        let Some(found) = self.documents.get(&self.collection, name).await? else {
            return Ok(None);
        };
        let ciphertext = ciphertext(&found)?;
        let version = self.version(name, found.version.0, ciphertext);
        let key = self.keys.data_key_async(&self.scope).await?;
        let value = crypto::decrypt_enc2(&key, ciphertext)?;
        Ok(Some((value, version)))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        validate_name(name)?;
        let key = self.keys.data_key_async(&self.scope).await?;
        let ciphertext = crypto::encrypt_enc2(&key, value)?;
        self.documents
            .put(
                &self.collection,
                name,
                json!({ CIPHERTEXT_FIELD: ciphertext }),
                DocumentPrecondition::None,
            )
            .await?;
        Ok(())
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: Precondition,
    ) -> Result<SecretVersion> {
        validate_name(name)?;
        let key = self.keys.data_key_async(&self.scope).await?;
        let encrypted = crypto::encrypt_enc2(&key, value)?;
        let native_condition = match condition {
            Precondition::Absent => DocumentPrecondition::Absent,
            Precondition::Version(expected) => {
                let Some(found) = self.documents.get(&self.collection, name).await? else {
                    return Err(stale_secret());
                };
                let current = ciphertext(&found)?;
                if self.version(name, found.version.0, current) != expected {
                    return Err(stale_secret());
                }
                DocumentPrecondition::Version(found.version)
            }
        };
        let version = self
            .documents
            .put(
                &self.collection,
                name,
                json!({ CIPHERTEXT_FIELD: encrypted }),
                native_condition,
            )
            .await
            .map_err(map_conditional_write_error)?;
        Ok(self.version(name, version.0, &encrypted))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        self.documents
            .delete(&self.collection, name, DocumentPrecondition::None)
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let all = self
            .documents
            .query_all(&self.collection, &Query::all())
            .await?;
        let mut names: Vec<String> = all
            .into_iter()
            .map(|found| found.id)
            .filter(|id| id.starts_with(prefix))
            .collect();
        names.sort();
        Ok(names)
    }

    fn backend_name(&self) -> &'static str {
        "document"
    }
}

impl DocumentSecrets {
    fn version(&self, name: &str, document_version: u64, ciphertext: &str) -> SecretVersion {
        version_from_parts(&[
            b"document",
            self.scope.as_str().as_bytes(),
            self.collection.as_bytes(),
            name.as_bytes(),
            &document_version.to_be_bytes(),
            ciphertext.as_bytes(),
        ])
    }
}

fn ciphertext(found: &Versioned<Value>) -> Result<&str> {
    found
        .doc
        .get(CIPHERTEXT_FIELD)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            StorageError::serialization("secret document has no string ciphertext field")
        })
}

fn stale_secret() -> StorageError {
    StorageError::conflict("secret changed since it was read")
}

fn map_conditional_write_error(error: StorageError) -> StorageError {
    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) {
        stale_secret()
    } else {
        error
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
