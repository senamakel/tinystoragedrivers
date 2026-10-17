//! Data types for storage grant policy, authority, and request decisions.

use std::fmt;

use serde_json::Value;
use tinystoragedrivers_bus::CollectionSpec;

/// Policy the host applies when turning requested needs into authority.
#[derive(Default)]
pub struct GrantPolicy<'a> {
    /// Whether granted resources may be written.
    pub writable: bool,
    /// Whether requested secret prefixes may be admitted.
    pub allow_secrets: bool,
    /// Additional collection names reserved by this host.
    pub reserved_collections: &'a [&'a str],
}

impl fmt::Debug for GrantPolicy<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrantPolicy")
            .field("writable", &self.writable)
            .field("allow_secrets", &self.allow_secrets)
            .field("reserved_collections", &self.reserved_collections.len())
            .finish()
    }
}

/// A validated, owned grant. Its fields cannot be widened after construction.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageGrant {
    pub(super) collections: Vec<CollectionSpec>,
    pub(super) stream_prefixes: Vec<String>,
    pub(super) blob_prefixes: Vec<String>,
    pub(super) secret_prefixes: Vec<String>,
    pub(super) exact_secret_names: Vec<String>,
    pub(super) writable: bool,
}

impl fmt::Debug for StorageGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageGrant")
            .field("collections", &self.collections.len())
            .field("stream_prefixes", &self.stream_prefixes.len())
            .field("blob_prefixes", &self.blob_prefixes.len())
            .field("secret_prefixes", &"[REDACTED]")
            .field("exact_secret_names", &"[REDACTED]")
            .field("writable", &self.writable)
            .finish()
    }
}

/// A content-free grant construction failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrantError {
    /// A collection declaration is malformed, reserved, or duplicated.
    Collection,
    /// A resource prefix or exact secret name is malformed or duplicated.
    Prefix,
    /// Secret resources were requested without host permission.
    SecretsDenied,
}

impl fmt::Display for GrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Collection => "invalid collection grant",
            Self::Prefix => "invalid storage prefix grant",
            Self::SecretsDenied => "secret storage grant denied by host policy",
        })
    }
}

impl std::error::Error for GrantError {}

/// Why a storage request was refused, without exposing request data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Denied {
    /// The object is not granted.
    Object,
    /// The requested target is not granted.
    Target,
    /// The request is a write through a read-only grant.
    ReadOnly,
    /// Collection schema and lifecycle are host controlled.
    Schema,
    /// The selector, request shape, or reply is malformed.
    Malformed,
}

impl Denied {
    /// A stable, content-free label suitable for logs and transport errors.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Object => "object-not-granted",
            Self::Target => "target-not-granted",
            Self::ReadOnly => "read-only-grant",
            Self::Schema => "schema-is-host-only",
            Self::Malformed => "malformed-request",
        }
    }
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl std::error::Error for Denied {}

/// How an allowed call should be executed.
#[derive(Debug, PartialEq)]
pub enum Plan {
    /// Forward to the caller's already-bound storage endpoint.
    Forward {
        /// Granted target for auditing; secret names are redacted.
        target: String,
        /// Reply transformation required by the grant.
        filter: ReplyFilter,
        /// Whether the request mutates storage.
        write: bool,
    },
    /// Answer locally without touching the backend.
    Answer(Value),
}

/// A post-processing rule for an allowed reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplyFilter {
    /// No filtering is required.
    None,
    /// Keep only granted collection names.
    GrantedCollections,
}
