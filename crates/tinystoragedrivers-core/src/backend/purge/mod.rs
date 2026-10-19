//! Scope reset addresses, admission generations and optional purge port.
use crate::{Capability, Result, Scope, StorageError, async_trait};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Native data-access admission generation, issued by purge rather than model input.
/// A successful purge bound to generation `N` issues `N + 1`; implementations
/// reject the operation if that increment overflows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ScopeGeneration(pub u64);

/// Guarantees requested for a purge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PurgeMode {
    /// Erase every port and revoke existing handles in one native operation.
    Atomic,
    /// Trusted host has stopped and drained writers. Stages may resume.
    Quiescent,
}

/// An idempotent reset request. Hosts persist this identity before starting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeRequest {
    /// Stable, host-issued operation identity; never tenant data or model input.
    pub operation_id: String,
    /// Required failure guarantees.
    pub mode: PurgeMode,
}
impl PurgeRequest {
    /// Validate the durable operation identity.
    ///
    /// # Errors
    /// Invalid operation identity.
    pub fn validate(&self) -> Result<()> {
        if self.operation_id.is_empty()
            || self.operation_id.len() > 128
            || !self
                .operation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        {
            return Err(StorageError::invalid_input(
                "purge operation id must be 1 to 128 ASCII letters, digits, underscores or hyphens",
            ));
        }
        Ok(())
    }
}

/// Whether all ports of this one addressed database have been erased.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PurgeStatus {
    /// Reset admission remains closed; the operation must resume.
    Pending,
    /// All ports have finished; the host may persist and issue the generation.
    Complete,
}

/// The next resumable stage, or completed native erasure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PurgeStage {
    /// Erase scoped documents and streams.
    Records,
    /// Persist every owned blob identity before deletion.
    BlobManifest,
    /// Remove scoped chunks and files using the durable manifest.
    Blobs,
    /// All ports have finished.
    #[default]
    Complete,
}

/// Durable native receipt kept outside erased tenant data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeReceipt {
    /// Host-issued operation identity.
    pub operation_id: String,
    /// Generation a successful operation issues for subsequent handles. A purge
    /// bound to generation `N` issues exactly `N + 1`; a pending receipt carries
    /// this candidate generation while admission remains closed. Replay returns
    /// the originally issued generation.
    pub generation: ScopeGeneration,
    /// Completion of this database only, never an implicit root-wide reset.
    pub status: PurgeStatus,
    /// Durable next stage for a pending operation.
    #[serde(default)]
    pub stage: PurgeStage,
}

/// Optional reset port bound to a scope, database, generation and native fence.
///
/// Atomic drivers erase documents (including tombstones and secret envelopes),
/// stream records and lifetime cursors, and blobs together. Shared collection
/// declarations and native admission/receipt metadata remain. Staged drivers
/// deny all generations while pending and require trusted writer quiescence.
#[async_trait]
pub trait ScopePurge: Send + Sync + fmt::Debug {
    /// Whether this bound handle implements generation and fence validation.
    /// Defaults to false for existing purge ports that predate this contract.
    fn supports_binding_validation(&self) -> bool {
        false
    }

    /// Whether this port can validate generation revocation independently of
    /// an optional writer fence. Read admission may remain open when a fence
    /// is absent or stale, while a purged generation must still be rejected.
    fn supports_generation_validation(&self) -> bool {
        false
    }

    /// Validate only that the bound generation is still admitted.
    ///
    /// # Errors
    /// Unsupported generation validation, revoked generation or backend failure.
    async fn validate_generation(&self) -> Result<()> {
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "this driver cannot validate a scope generation",
        ))
    }

    /// Validate that the bound generation is still admitted and that its
    /// native fence (when present) remains current, without changing state.
    ///
    /// # Errors
    /// Unsupported validation, a pending/revoked generation, stale fence or
    /// backend failure.
    async fn validate_binding(&self) -> Result<()> {
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "this driver cannot validate a scope purge binding",
        ))
    }

    /// Erase the bound scope and revoke its old handles. On success, the receipt
    /// carries the next generation, exactly one greater than the generation
    /// bound to this purge operation.
    ///
    /// # Errors
    /// Unsupported guarantees, rejected scope generation or native fence, invalid input,
    /// or backend failure. Partial staged failures never return Complete.
    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt>;
    /// Read the bound scope's durable receipt without starting a new reset.
    ///
    /// # Errors
    /// Unsupported status queries, stale native fence or backend failure.
    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        let _ = operation_id;
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "this driver cannot read purge status",
        ))
    }
}

/// Refuse a generation that the backend cannot bind.
pub(super) fn unsupported_generation<T>(scope: &Scope, generation: ScopeGeneration) -> Result<T> {
    let _ = (scope, generation);
    Err(StorageError::unsupported(
        Capability::ScopePurge,
        "this driver cannot bind scope generations",
    ))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
