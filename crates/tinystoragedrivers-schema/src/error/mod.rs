//! The one error type every port and driver returns.
//!
//! A caller branches on [`ErrorKind`], never on the message: the message is for
//! logs, and the kind is the contract. Drivers map their backend's failures onto
//! the kinds (a SQLite `SQLITE_BUSY` and a MongoDB network timeout are both
//! [`ErrorKind::Unavailable`]) and keep the original error as the source.

use std::error::Error as StdError;
use std::fmt;

use crate::capabilities::Capability;

/// What went wrong, in terms a repository can act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The addressed document, stream or blob does not exist and the operation
    /// required it to.
    NotFound,
    /// A `Precondition` did not hold: the document
    /// changed (or appeared, or vanished) since the caller read it.
    Conflict,
    /// A write would duplicate a value in a unique index.
    AlreadyExists,
    /// The driver does not implement the requested capability.
    Unsupported(Capability),
    /// The caller passed something the port cannot accept: an empty name, a
    /// malformed scope, a filter on an impossible path.
    InvalidInput,
    /// A value could not be encoded to, or decoded from, the backend's format.
    Serialization,
    /// Encryption or decryption failed.
    Crypto,
    /// The backend is temporarily unreachable or busy. Retrying may succeed.
    Unavailable,
    /// Any other backend failure.
    Backend,
    /// A write through a fenced handle was refused because its
    /// `Fence` no longer holds: the guard document is gone,
    /// expired, or moved on (a newer lease epoch). Nothing was written.
    /// Not retryable: the caller lost the right to write.
    Fenced,
    /// This handle's scope generation is retired or unissued, or a scope reset
    /// is pending. This does not imply that an independent native lease was
    /// lost. Not retryable through the same handle: only a trusted owner may
    /// complete the reset and bind an explicitly issued generation.
    ScopeGenerationRejected,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::Conflict => f.write_str("conflict"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::Unsupported(capability) => write!(f, "unsupported capability {capability}"),
            Self::InvalidInput => f.write_str("invalid input"),
            Self::Serialization => f.write_str("serialization"),
            Self::Crypto => f.write_str("crypto"),
            Self::Unavailable => f.write_str("unavailable"),
            Self::Backend => f.write_str("backend"),
            Self::Fenced => f.write_str("fenced"),
            Self::ScopeGenerationRejected => f.write_str("scope generation rejected"),
        }
    }
}

/// The error every port returns.
///
/// ```
/// use tinystoragedrivers_schema::{ErrorKind, StorageError};
///
/// let error = StorageError::conflict("version 3 expected, found 4");
/// assert_eq!(error.kind(), ErrorKind::Conflict);
/// assert!(!error.is_retryable());
/// assert_eq!(error.to_string(), "conflict: version 3 expected, found 4");
/// ```
#[derive(Debug, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct StorageError {
    kind: ErrorKind,
    message: String,
    #[source]
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl StorageError {
    /// Build an error of `kind` with a log-friendly `message`.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Attach the backend error that caused this one.
    #[must_use]
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// [`ErrorKind::NotFound`].
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    /// [`ErrorKind::Conflict`].
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }

    /// [`ErrorKind::AlreadyExists`].
    #[must_use]
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::AlreadyExists, message)
    }

    /// [`ErrorKind::Unsupported`] for `capability`.
    #[must_use]
    pub fn unsupported(capability: Capability, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported(capability), message)
    }

    /// [`ErrorKind::InvalidInput`].
    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }

    /// [`ErrorKind::Serialization`].
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Serialization, message)
    }

    /// [`ErrorKind::Crypto`].
    #[must_use]
    pub fn crypto(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Crypto, message)
    }

    /// [`ErrorKind::Unavailable`].
    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }

    /// [`ErrorKind::Backend`].
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Backend, message)
    }

    /// [`ErrorKind::Fenced`].
    #[must_use]
    pub fn fenced(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Fenced, message)
    }

    /// [`ErrorKind::ScopeGenerationRejected`].
    #[must_use]
    pub fn scope_generation_rejected(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ScopeGenerationRejected, message)
    }

    /// What went wrong.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The log-friendly description, without the kind prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Whether repeating the same call may succeed without any change by the
    /// caller. Only [`ErrorKind::Unavailable`] qualifies: a conflict needs a
    /// fresh read first.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Unavailable
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(error: serde_json::Error) -> Self {
        Self::serialization(error.to_string()).with_source(error)
    }
}

/// The result every port returns.
pub type Result<T> = std::result::Result<T, StorageError>;
