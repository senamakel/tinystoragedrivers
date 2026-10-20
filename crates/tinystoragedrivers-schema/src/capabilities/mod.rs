//! What a driver can do beyond the baseline every driver must implement.
//!
//! The baseline is the whole `DocumentStore`,
//! `StreamStore` and `BlobStore`
//! surface except the operations named here. A repository that needs one of
//! these checks [`Capabilities::require`] when it opens, so a deployment on the
//! wrong backend fails at boot rather than on the first request that needs it.

use std::fmt;

use crate::error::{Result, StorageError};

/// One optional ability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// The backend can issue native handles that never initialize or repair schema.
    ExistingSchema,
    /// Read-only validation of persisted document collection declarations.
    CollectionValidation,
    /// `DocumentStore::search` over the
    /// fields a collection's `SearchSpec` names.
    FullText,
    /// `DocumentStore::atomic_batch`:
    /// several writes across documents and collections that land together or
    /// not at all.
    Transactions,
    /// `DocumentStore::atomic_batch_with` and
    /// `DocumentStore::atomic_batch_checked_with` preserve write options for
    /// every `Put` in the same atomic operation.
    AtomicWriteOptions,
    /// Documents expire on their collection's
    /// `ttl_field`.
    Ttl,
    /// `StorageBackend::for_scope_fenced`:
    /// handles whose writes the driver checks against a
    /// `Fence` in the same atomic step.
    Fencing,
    /// Direct object-store GET and PUT URLs, on the blob handle.
    PresignedUrls,
    /// Blob reads and writes use bounded buffers rather than whole objects.
    StreamingBlobs,
    /// `DocumentStore::collections` enumerates collections with records in
    /// the handle's scope.
    CollectionEnumeration,
    /// Atomic stream append guarded by its persistent next offset.
    ConditionalStreamAppend,
    /// Document batches validate read-only dependencies atomically with writes.
    ValidatedBatch,
    /// Scoped reset with persistent generation admission.
    ScopePurge,
    /// Every storage port is purged atomically.
    AtomicScopePurge,
    /// Native scoped document invalidation subscriptions.
    Watch,
    /// `BlobStore::put_expiring`: a blob
    /// written with a lifetime reads as absent once it has passed.
    BlobTtl,
    /// The driver keeps `Meta` for every document: the declared
    /// schema, creation and update times and an expiry
    /// (`DocumentStore::put_with`), and it
    /// answers the reserved `_schema`, `_created_ms`, `_updated_ms` and
    /// `_expires_ms` filter and sort paths.
    RecordMetadata,
    /// `StorageBackend::maintain` and
    /// `StorageBackend::checkpoint` run
    /// rather than refuse. A driver with nothing to reclaim may still report
    /// it: the calls succeed and report zero.
    Maintenance,
    /// `StorageBackend::scopes` lists the
    /// scopes that hold records.
    ScopeEnumeration,
    /// `StreamStore::seal`: close a stream to
    /// appends and keep its entries as one compressed blob.
    StreamSeal,
    /// Independently opened backends on the same store agree: they see each
    /// other's writes at once, and the compare-and-swap rules (version
    /// preconditions, `Absent` creates, claims, fences) hold between them, so
    /// racing handles, in this process or another, get exactly one winner.
    /// Only a driver whose engine arbitrates between connections reports it
    /// (`SQLite` and `MongoDB`); the memory driver is one process's heap, and the
    /// file driver serializes only among the handles of one process.
    SharedAcrossProcesses,
    /// Version preconditions, `Absent` creates, claims and fences arbitrate
    /// atomically across processes, so racing conditional writes have exactly
    /// one winner. This capability names that subset of the broader
    /// [`Capability::SharedAcrossProcesses`] guarantee explicitly.
    CrossProcessCas,
}

impl Capability {
    const ALL: [Self; 22] = [
        Self::ExistingSchema,
        Self::CollectionValidation,
        Self::FullText,
        Self::Transactions,
        Self::AtomicWriteOptions,
        Self::Ttl,
        Self::Fencing,
        Self::PresignedUrls,
        Self::StreamingBlobs,
        Self::CollectionEnumeration,
        Self::ConditionalStreamAppend,
        Self::ValidatedBatch,
        Self::ScopePurge,
        Self::AtomicScopePurge,
        Self::Watch,
        Self::BlobTtl,
        Self::RecordMetadata,
        Self::Maintenance,
        Self::ScopeEnumeration,
        Self::StreamSeal,
        Self::SharedAcrossProcesses,
        Self::CrossProcessCas,
    ];

    const fn bit(self) -> u32 {
        match self {
            Self::ExistingSchema => 1 << 13,
            Self::CollectionValidation => 1 << 14,
            Self::FullText => 1,
            Self::Transactions => 1 << 1,
            Self::AtomicWriteOptions => 1 << 22,
            Self::Ttl => 1 << 2,
            Self::Fencing => 1 << 3,
            Self::PresignedUrls => 1 << 4,
            Self::StreamingBlobs => 1 << 5,
            Self::CollectionEnumeration => 1 << 6,
            Self::ConditionalStreamAppend => 1 << 7,
            Self::ValidatedBatch => 1 << 9,
            Self::ScopePurge => 1 << 10,
            Self::AtomicScopePurge => 1 << 11,
            Self::Watch => 1 << 12,
            Self::BlobTtl => 1 << 15,
            Self::RecordMetadata => 1 << 16,
            Self::Maintenance => 1 << 17,
            Self::ScopeEnumeration => 1 << 18,
            Self::StreamSeal => 1 << 19,
            Self::SharedAcrossProcesses => 1 << 20,
            Self::CrossProcessCas => 1 << 21,
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ExistingSchema => "existing_schema",
            Self::CollectionValidation => "collection_validation",
            Self::FullText => "full_text",
            Self::Transactions => "transactions",
            Self::AtomicWriteOptions => "atomic_write_options",
            Self::Ttl => "ttl",
            Self::Fencing => "fencing",
            Self::PresignedUrls => "presigned_urls",
            Self::StreamingBlobs => "streaming_blobs",
            Self::CollectionEnumeration => "collection_enumeration",
            Self::ConditionalStreamAppend => "conditional_stream_append",
            Self::ValidatedBatch => "validated_batch",
            Self::ScopePurge => "scope_purge",
            Self::AtomicScopePurge => "atomic_scope_purge",
            Self::Watch => "watch",
            Self::BlobTtl => "blob_ttl",
            Self::RecordMetadata => "record_metadata",
            Self::Maintenance => "maintenance",
            Self::ScopeEnumeration => "scope_enumeration",
            Self::StreamSeal => "stream_seal",
            Self::SharedAcrossProcesses => "shared_across_processes",
            Self::CrossProcessCas => "cross_process_cas",
        })
    }
}

/// A set of [`Capability`] values.
///
/// ```
/// use tinystoragedrivers_schema::{Capabilities, Capability};
///
/// let caps = Capabilities::none().with(Capability::Ttl);
/// assert!(caps.contains(Capability::Ttl));
/// assert!(caps.require(Capability::FullText).is_err());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Capabilities(u32);

impl Capabilities {
    /// The empty set: a baseline driver.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Every capability this version of the crate defines.
    ///
    /// The set grows in later releases, so a backend should list the
    /// capabilities it actually has rather than report `all()`. In
    /// particular, a backend that reports [`Capability::Fencing`] must
    /// implement (or forward)
    /// `StorageBackend::for_scope_fenced`;
    /// the trait's default refuses with `Unsupported(Fencing)`.
    #[must_use]
    pub const fn all() -> Self {
        Self(
            Capability::ExistingSchema.bit()
                | Capability::CollectionValidation.bit()
                | Capability::FullText.bit()
                | Capability::Transactions.bit()
                | Capability::AtomicWriteOptions.bit()
                | Capability::Ttl.bit()
                | Capability::Fencing.bit()
                | Capability::PresignedUrls.bit()
                | Capability::StreamingBlobs.bit()
                | Capability::CollectionEnumeration.bit()
                | Capability::ConditionalStreamAppend.bit()
                | Capability::ValidatedBatch.bit()
                | Capability::ScopePurge.bit()
                | Capability::AtomicScopePurge.bit()
                | Capability::Watch.bit()
                | Capability::BlobTtl.bit()
                | Capability::RecordMetadata.bit()
                | Capability::Maintenance.bit()
                | Capability::ScopeEnumeration.bit()
                | Capability::StreamSeal.bit()
                | Capability::SharedAcrossProcesses.bit()
                | Capability::CrossProcessCas.bit(),
        )
    }

    /// This set plus `capability`.
    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }

    /// This set minus `capability`.
    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self(self.0 & !capability.bit())
    }

    /// Whether `capability` is in the set.
    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    /// Succeed when `capability` is in the set.
    ///
    /// # Errors
    ///
    /// `ErrorKind::Unsupported` naming the
    /// missing capability.
    pub fn require(self, capability: Capability) -> Result<()> {
        if self.contains(capability) {
            Ok(())
        } else {
            Err(StorageError::unsupported(
                capability,
                format!("this storage driver does not provide {capability}"),
            ))
        }
    }

    /// The capabilities in the set, in a stable order.
    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |capability| self.contains(*capability))
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}
