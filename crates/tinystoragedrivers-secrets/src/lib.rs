//! Encrypted secret storage: one [`SecretStore`] port, several places to keep
//! the secrets.
//!
//! API keys, OAuth tokens and encrypted configuration fields need a store that
//! is not a [`DocumentStore`](tinystoragedrivers_core::DocumentStore): values
//! are bytes that must never reach a log, a query, or a backend in plaintext.
//! The port is small (`get`, `get_versioned`, `set`, `set_if`, `delete`,
//! `list`), and the drivers decide where the bytes live:
//!
//! | Driver | Where | Deployment |
//! | --- | --- | --- |
//! | [`MemorySecrets`] | process memory | tests |
//! | [`EncryptedFileSecrets`] | one ChaCha20-Poly1305 file, `secrets.enc` | desktop, CLI |
//! | [`DocumentSecrets`] | `enc2:` ciphertext in any `DocumentStore` | cloud, multi-tenant |
//! | `KeyringSecrets` (feature `keyring`) | the OS credential store | desktop |
//!
//! Every format is OpenHuman's. [`crypto`] reproduces its `enc2:` encoding
//! (`enc2:` followed by lowercase hex of `nonce ‖ ciphertext ‖ tag`) and
//! [`EncryptedFileSecrets`] reads and writes its `secrets.enc` file, so a
//! secret OpenHuman already stored decrypts here unchanged, and the reverse.
//!
//! [`DocumentSecrets`] encrypts each value under a data key a [`KeyProvider`]
//! picks for the handle's [`Scope`](tinystoragedrivers_core::Scope):
//! [`StaticKey`] for one key everywhere, [`DerivedKeys`] for a distinct
//! HKDF-SHA256 key per tenant from one master key, or [`KmsKeyProvider`] for
//! per-scope envelope keys wrapped by a cloud KMS and persisted on storage.
//! `get_versioned` pairs a plaintext value with a redacted opaque token, while
//! `set_if` uses an atomic absent or version precondition. The encrypted-file
//! driver derives its token from the current encrypted map snapshot, so an
//! unrelated write to that file can conservatively conflict.
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use std::sync::Arc;
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//! use tinystoragedrivers_secrets::{DerivedKeys, DocumentSecrets, SecretStore, crypto};
//!
//! let storage = MemoryStorage::new().for_scope(&Scope::new("tenant-a")?)?;
//! let keys = Arc::new(DerivedKeys::new(crypto::generate_key()?));
//! let secrets = DocumentSecrets::new(&storage, keys);
//!
//! secrets.set("openai:api_key", b"sk-live-123").await?;
//! let value = secrets.get("openai:api_key").await?.unwrap();
//! assert_eq!(value.as_slice(), b"sk-live-123");
//! assert_eq!(secrets.list("openai:").await?, ["openai:api_key"]);
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! # Features
//!
//! - `keyring`: `KeyringSecrets`, over the `keyring` crate's native stores.
//! - `keyring-secret-service`: explicit Linux Secret Service with encrypted D-Bus sessions.
//! - `kms-aws`: `AwsKms`, AWS KMS with `SigV4` and refreshable credentials.
//! - `kms-gcp`: `GcpKms`, Google Cloud KMS with scope AAD and refreshable tokens.
//! - `testkit`: `testkit::secrets_conformance`, the suite every driver runs.
//!
//! This crate holds no policy: which secrets exist, how names are namespaced
//! per user, and when a legacy value is migrated belong to the host.

pub mod crypto;
mod document;
mod file;
#[cfg(feature = "keyring")]
mod keyring;
mod keys;
mod kms;
mod memory;
mod store;
mod task;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

pub use document::{DEFAULT_COLLECTION, DocumentSecrets};
pub use file::{
    CleanupAcknowledgment, EncryptedFileSecrets, FileImportSource, FileTransactionBudget,
    FileTransactionGuard, FileTransactionLease, FileTransactionOperation, FileTransactionPhase,
    FileTransactionResource, FileTransactionResult, HexKeyFileSource, KEY_FILE_NAME,
    MAX_HEX_KEY_FILE_BYTES, ProtectedHexKeyFile, PublicationBoundary, SECRETS_FILE_NAME,
    VerifiedFileImport, load_or_create_key_file,
};
#[cfg(feature = "keyring")]
pub use keyring::KeyringSecrets;
pub use keys::{DerivedKeys, KeyProvider, StaticKey};
#[cfg(feature = "kms-aws")]
pub use kms::{AwsCredentials, AwsCredentialsProvider, AwsKms};
pub use kms::{ENVELOPE_COLLECTION, ENVELOPE_ID, KmsClient, KmsKeyProvider};
#[cfg(feature = "kms-gcp")]
pub use kms::{GcpAccessTokenProvider, GcpKms};
pub use memory::MemorySecrets;
pub use store::{
    MAX_SECRET_NAME_LEN, Precondition, SecretStore, SecretVersion, validate_name, validate_prefix,
};

/// The `Zeroizing` wrapper every secret value and key is returned in,
/// re-exported so callers need not name the dependency.
pub use zeroize::Zeroizing;
