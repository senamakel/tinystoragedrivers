//! The [`SecretStore`] port and the name rules every driver enforces.

use std::fmt::{self, Debug, Formatter};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{Capability, Result, StorageError};
use zeroize::Zeroizing;

/// The longest secret name, in bytes.
pub const MAX_SECRET_NAME_LEN: usize = 256;

/// An opaque revision token for one stored secret.
///
/// A version is valid only for the secret name and storage domain that
/// returned it. Another handle to the same persistent store may use it. Its
/// representation is deliberately hidden and its debug output never reveals
/// the token.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecretVersion([u8; 32]);

impl SecretVersion {
    /// Rebuild a token from its opaque wire representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the opaque wire representation without assigning meaning to it.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consume the token into its opaque wire representation.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for SecretVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SecretVersion(..)")
    }
}

/// The condition that must hold before [`SecretStore::set_if`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precondition {
    /// The secret must not exist.
    Absent,
    /// The secret must still have the revision returned by
    /// [`SecretStore::get_versioned`].
    Version(SecretVersion),
}

/// Derive a store-specific token from unambiguous, length-delimited parts.
/// Callers pass only non-plaintext identity, ciphertext, and revision bytes.
pub(crate) fn version_from_parts(parts: &[&[u8]]) -> SecretVersion {
    let mut digest = Sha256::new();
    digest.update(b"tinystoragedrivers-secrets/version/v1\0");
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    SecretVersion(digest.finalize().into())
}

/// Secret bytes by name.
///
/// A store is bound to one owner when it is opened (one OS keychain service,
/// one file, one document scope), so no method takes a scope. Names are
/// validated by [`validate_name`] before any backend sees them; how a host
/// namespaces them (OpenHuman uses `"{user_id}:{key}"`) is its own business.
///
/// Values come back wrapped in [`Zeroizing`] so the plaintext is wiped when the
/// caller drops it.
#[async_trait]
pub trait SecretStore: Send + Sync + Debug {
    /// Read one secret, or `None` when it does not exist.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name,
    /// [`ErrorKind::Crypto`](tinystoragedrivers_core::ErrorKind::Crypto) when
    /// the stored ciphertext does not decrypt under this store's key, or a
    /// backend error.
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>>;

    /// Read one secret and the opaque revision of the same snapshot.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name,
    /// [`ErrorKind::Unsupported`](tinystoragedrivers_core::ErrorKind::Unsupported)
    /// when this store cannot compare revisions atomically, or the same
    /// storage and decryption errors as [`SecretStore::get`].
    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_name(name)?;
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "this secret store does not support versioned reads",
        ))
    }

    /// Store `value` under `name`, replacing any previous value.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name or a value the backend cannot hold (the file and
    /// keyring drivers store UTF-8 text only), or a backend error.
    async fn set(&self, name: &str, value: &[u8]) -> Result<()>;

    /// Store `value` only when `condition` still holds, atomically.
    ///
    /// A stale revision and an already-present secret under
    /// [`Precondition::Absent`] return
    /// [`ErrorKind::Conflict`](tinystoragedrivers_core::ErrorKind::Conflict).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name or unsupported value,
    /// [`ErrorKind::Conflict`](tinystoragedrivers_core::ErrorKind::Conflict)
    /// when the precondition no longer holds,
    /// [`ErrorKind::Unsupported`](tinystoragedrivers_core::ErrorKind::Unsupported)
    /// when this store cannot compare and write atomically, or a backend
    /// error.
    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: Precondition,
    ) -> Result<SecretVersion> {
        validate_name(name)?;
        let _ = (value, condition);
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "this secret store does not support conditional writes",
        ))
    }

    /// Remove one secret and report whether it existed.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name, or a backend error.
    async fn delete(&self, name: &str) -> Result<bool>;

    /// The names starting with `prefix`, sorted. An empty prefix lists every
    /// secret.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid prefix, [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend)
    /// from a store that cannot enumerate (see [`SecretStore::enumerable`]),
    /// or a backend error.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Whether [`SecretStore::list`] works. The OS credential stores cannot
    /// enumerate their entries, so the keyring driver reports `false`.
    fn enumerable(&self) -> bool {
        true
    }

    /// A short, stable name for logs: `memory`, `encrypted_file`, `document`,
    /// `keyring`.
    fn backend_name(&self) -> &'static str;
}

/// Check a secret name: 1 to [`MAX_SECRET_NAME_LEN`] bytes, no control
/// characters (NUL included).
///
/// The message never repeats the name: a misplaced secret value passed as a
/// name must not reach a log.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput).
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name must be 1 to {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_printable(name)
}

/// Check a [`SecretStore::list`] prefix: empty, or a prefix of some valid name.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput).
pub fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name prefix must be at most {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_printable(prefix)
}

fn validate_printable(text: &str) -> Result<()> {
    if text.chars().any(char::is_control) {
        return Err(StorageError::invalid_input(
            "secret names must not contain control characters",
        ));
    }
    Ok(())
}

/// Require `value` to be UTF-8, for the drivers whose formats hold text.
pub(crate) fn require_utf8<'a>(value: &'a [u8], driver: &str) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|_| {
        StorageError::invalid_input(format!("the {driver} secret store holds utf-8 text only"))
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
