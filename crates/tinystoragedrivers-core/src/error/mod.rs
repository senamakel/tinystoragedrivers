//! Compatibility exports of the pure error vocabulary.

pub use tinystoragedrivers_schema::{ErrorKind, Result, StorageError};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
