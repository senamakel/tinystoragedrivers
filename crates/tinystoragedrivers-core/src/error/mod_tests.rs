//! Error kinds, constructors, display and retry classification.

use super::*;
use crate::Capability;

#[test]
fn every_constructor_sets_its_kind() {
    let cases = [
        (StorageError::not_found("x"), ErrorKind::NotFound),
        (StorageError::conflict("x"), ErrorKind::Conflict),
        (StorageError::already_exists("x"), ErrorKind::AlreadyExists),
        (
            StorageError::unsupported(Capability::FullText, "x"),
            ErrorKind::Unsupported(Capability::FullText),
        ),
        (StorageError::invalid_input("x"), ErrorKind::InvalidInput),
        (StorageError::serialization("x"), ErrorKind::Serialization),
        (StorageError::crypto("x"), ErrorKind::Crypto),
        (StorageError::unavailable("x"), ErrorKind::Unavailable),
        (StorageError::backend("x"), ErrorKind::Backend),
        (StorageError::fenced("x"), ErrorKind::Fenced),
        (
            StorageError::scope_generation_rejected("x"),
            ErrorKind::ScopeGenerationRejected,
        ),
    ];
    for (error, kind) in cases {
        assert_eq!(error.kind(), kind);
        assert_eq!(error.message(), "x");
        assert_eq!(error.is_retryable(), kind == ErrorKind::Unavailable);
    }
}

#[test]
fn display_prefixes_the_kind() {
    let rendered = [
        StorageError::not_found("a").to_string(),
        StorageError::conflict("a").to_string(),
        StorageError::already_exists("a").to_string(),
        StorageError::unsupported(Capability::Transactions, "a").to_string(),
        StorageError::invalid_input("a").to_string(),
        StorageError::serialization("a").to_string(),
        StorageError::crypto("a").to_string(),
        StorageError::unavailable("a").to_string(),
        StorageError::backend("a").to_string(),
        StorageError::fenced("a").to_string(),
        StorageError::scope_generation_rejected("a").to_string(),
    ];
    assert_eq!(
        rendered,
        [
            "not found: a",
            "conflict: a",
            "already exists: a",
            "unsupported capability transactions: a",
            "invalid input: a",
            "serialization: a",
            "crypto: a",
            "unavailable: a",
            "backend: a",
            "fenced: a",
            "scope generation rejected: a",
        ]
    );
}

#[test]
fn keeps_the_source_error() {
    let io = std::io::Error::other("disk gone");
    let error = StorageError::unavailable("write failed").with_source(io);
    assert_eq!(
        std::error::Error::source(&error)
            .map(ToString::to_string)
            .as_deref(),
        Some("disk gone")
    );
    assert!(std::error::Error::source(&StorageError::backend("plain")).is_none());
}

#[test]
fn json_errors_become_serialization_errors() {
    let parse = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
    let error = StorageError::from(parse);
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(std::error::Error::source(&error).is_some());
}
