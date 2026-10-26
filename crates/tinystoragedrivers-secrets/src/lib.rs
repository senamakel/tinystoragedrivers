//! Encrypted secret storage: one [`SecretStore`] port, several places to keep
//! the secrets.
//!
//! API keys, OAuth tokens and encrypted configuration fields need a store that
//! is not a [`DocumentStore`](tinystoragedrivers_api::DocumentStore): values
//! are bytes that must never reach a log, a query, or a backend in plaintext.
//! The port is small (`get`, `set`, `delete`, `list`), and the drivers decide
//! where the bytes live:
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
//! picks for the handle's [`Scope`](tinystoragedrivers_types::Scope):
//! [`StaticKey`] for one key everywhere, [`DerivedKeys`] for a distinct
//! HKDF-SHA256 key per tenant from one master key, or [`KmsKeyProvider`] for
//! per-scope envelope keys wrapped by a cloud KMS and persisted on storage.
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

#[cfg(any(
    feature = "file",
    feature = "document",
    feature = "kms-aws",
    feature = "kms-gcp"
))]
pub mod crypto;
#[cfg(feature = "document")]
mod document;
#[cfg(feature = "file")]
mod file;
#[cfg(feature = "keyring")]
mod keyring;
#[cfg(any(feature = "document", feature = "kms-aws", feature = "kms-gcp"))]
mod keys;
#[cfg(any(feature = "kms-aws", feature = "kms-gcp"))]
mod kms;
#[cfg(feature = "memory")]
mod memory;
mod store;
#[cfg(any(feature = "file", feature = "keyring"))]
mod task;
#[cfg(any(feature = "file", feature = "document"))]
mod version;

#[cfg(any(test, feature = "testkit"))]
pub mod testkit;

#[cfg(feature = "document")]
pub use document::{DEFAULT_COLLECTION, DocumentSecrets};
#[cfg(all(feature = "file", unix))]
pub use file::sync_parent_dir;
#[cfg(feature = "file")]
pub use file::{
    CleanupAcknowledgment, CorruptFilePolicy, EncryptedFileSecrets, FileImportSource,
    HexKeyFileSource, KEY_FILE_NAME, Locked, MAX_HEX_KEY_FILE_BYTES, ProtectedHexKeyFile,
    PublicationBoundary, SECRETS_FILE_NAME, VerifiedFileImport, load_or_create_key_file, lock_file,
    write_atomic as write_atomic_private,
};
#[cfg(feature = "keyring")]
pub use keyring::KeyringSecrets;
#[cfg(any(feature = "document", feature = "kms-aws", feature = "kms-gcp"))]
pub use keys::{DerivedKeys, KeyProvider, StaticKey};
#[cfg(feature = "kms-aws")]
pub use kms::{AwsCredentials, AwsCredentialsProvider, AwsKms};
#[cfg(any(feature = "kms-aws", feature = "kms-gcp"))]
pub use kms::{ENVELOPE_COLLECTION, ENVELOPE_ID, KmsClient, KmsKeyProvider};
#[cfg(feature = "kms-gcp")]
pub use kms::{GcpAccessTokenProvider, GcpKms};
#[cfg(feature = "memory")]
pub use memory::MemorySecrets;
pub use store::SecretStore;
pub use tinystoragedrivers_types::{
    MAX_SECRET_NAME_LEN, SecretPrecondition, SecretVersion, validate_secret_name,
    validate_secret_prefix,
};

/// The `Zeroizing` wrapper every secret value and key is returned in,
/// re-exported so callers need not name the dependency.
pub use zeroize::Zeroizing;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod testkit_tests;
