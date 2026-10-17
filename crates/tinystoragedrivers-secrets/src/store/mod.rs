//! The [`SecretStore`] port and the name rules every driver enforces.

use std::fmt::Debug;

use async_trait::async_trait;
use tinystoragedrivers_core::{Result, StorageError};
use zeroize::Zeroizing;

pub use tinystoragedrivers_core::{
    MAX_SECRET_NAME_LEN, validate_secret_name as validate_name,
    validate_secret_prefix as validate_prefix,
};

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

    /// Store `value` under `name`, replacing any previous value.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for an invalid name or a value the backend cannot hold (the file and
    /// keyring drivers store UTF-8 text only), or a backend error.
    async fn set(&self, name: &str, value: &[u8]) -> Result<()>;

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

/// Require `value` to be UTF-8, for the drivers whose formats hold text.
pub(crate) fn require_utf8<'a>(value: &'a [u8], driver: &str) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|_| {
        StorageError::invalid_input(format!("the {driver} secret store holds utf-8 text only"))
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
