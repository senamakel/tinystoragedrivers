//! Lease ownership and write-fence composition for tinystoragedrivers.
//!
//! This crate combines the document-backed [`tinystoragedrivers_core::Leases`]
//! primitive with host-selectable lease stores and fenced backend views.
//! Enable `file` to use [`LocalLeases`] when no shared storage backend exists.

mod backend;
mod document;
mod lease;

pub use backend::for_scope_fenced;
pub use document::DocumentLeases;
pub use lease::{LeaseError, LeaseGrant, LeaseRecord, LeaseStore, is_live, retry_after_ms};

#[cfg(feature = "file")]
mod local;

#[cfg(feature = "file")]
pub use local::LocalLeases;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
