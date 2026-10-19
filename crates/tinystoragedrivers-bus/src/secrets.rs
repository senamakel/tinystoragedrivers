//! Wire shapes for the Secrets object.
//!
//! Confidential: servers admit only attested in-process callers, so every call
//! uses the confidential route. Values and names never appear in `Debug`
//! output, logs or errors; hosts and clients zeroize buffers after use.
use serde::{Deserialize, Serialize};

/// Secrets object name under [`crate::STORAGE_INTERFACE`].
pub const SECRETS_OBJECT: &str = "Secrets";
/// Finite selector set; servers must reject every other selector.
pub const SECRETS_SELECTORS: [&str; 6] = ["Get", "GetVersioned", "Set", "SetIf", "Delete", "List"];

/// Return whether `selector` belongs to the fixed Secrets selector set.
#[must_use]
pub fn is_secrets_selector(selector: &str) -> bool {
    SECRETS_SELECTORS.contains(&selector)
}

/// Request addressing one secret (`Get`, `Delete`).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretNameRequest {
    /// Name inside the captured grant.
    pub name: String,
}
/// Store a secret value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetSecretRequest {
    /// Name inside the captured grant.
    pub name: String,
    /// Secret bytes.
    pub value: Vec<u8>,
}
/// Secret value reply; `None` when absent.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretValue {
    /// Secret bytes.
    pub value: Vec<u8>,
}
/// Secret value and opaque revision from the same read.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedSecret {
    /// Secret bytes.
    pub value: Vec<u8>,
    /// Opaque revision token; callers must not interpret its bytes.
    pub version: [u8; 32],
}
/// Secret revision required by a conditional write.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretRevision(pub [u8; 32]);
/// Condition required by `SetIf`.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "version", rename_all = "snake_case")]
pub enum SecretPrecondition {
    /// The secret must not exist.
    Absent,
    /// The secret must still have the supplied revision.
    Version(SecretRevision),
}
/// Store a secret only while its precondition holds.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetSecretIfRequest {
    /// Name inside the captured grant.
    pub name: String,
    /// Secret bytes.
    pub value: Vec<u8>,
    /// Required current state.
    pub condition: SecretPrecondition,
}
/// List secret names by prefix.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSecretsRequest {
    /// Literal name prefix.
    #[serde(default)]
    pub prefix: String,
}

macro_rules! redacted_debug {
    ($($ty:ident),*) => {$(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($ty)).finish_non_exhaustive()
            }
        }
    )*};
}
redacted_debug!(
    SecretNameRequest,
    SetSecretRequest,
    SecretValue,
    VersionedSecret,
    SetSecretIfRequest,
    ListSecretsRequest
);

impl std::fmt::Debug for SecretRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretRevision(..)")
    }
}

impl std::fmt::Debug for SecretPrecondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("Absent"),
            Self::Version(_) => f.write_str("Version(SecretRevision(..))"),
        }
    }
}

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
