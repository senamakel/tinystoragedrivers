# Scope purge contract

This module defines the optional native reset port, explicit logical database
addresses, host-issued operation requests, durable receipts and scope
generations. Public exports remain centralized in the core crate root.
`mod_tests.rs` pins request validation and optional capability refusal.

The trusted host persists an operation identity, authorizes its database and
scope manifest, stops and drains writers for quiescent resets, and issues
replacement handles only after completion. Native implementations retain
admission and receipts outside erased tenant data. Ordinary constructors keep
generation zero; they do not discover a newer generation and revive an old
caller. A retired generation returns `ScopeGenerationRejected`; a stale native
lease fence remains `Fenced`.

Atomic and Quiescent modes express different guarantees. Drivers must refuse
unsupported modes before mutation. CompositeStorage provides a durable staged
coordinator across its record and blob ports, but never claims atomicity across
those independent stores. Its reserved coordinator scopes and collections are
internal, stable across nested reconstruction, and excluded from public scope
enumeration. With `ExistingSchema`, trusted bootstrap must prepare the
coordinator collection; restricted handles validate its presence and
compatibility without creating it.

`BackendBlobs` records nonzero logical-to-physical generation mappings in its
reserved `__tsd_composite_blob_scope_map/scopes` collection. ExistingSchema
backends must prepare that collection through trusted bootstrap before those
generations are used; restricted handles validate it and fail closed if it is
missing or incompatible. Generation zero retains the legacy physical scope.

The port does not promise root-wide atomic erasure or external key revocation.
S3 scope purge requires a signer with `s3:ListBucketVersions` and
`s3:DeleteObjectVersion`; signer-less S3 and custom blob backends without
generation-safe purge support do not enable composite purge. Quiescent mode
requires the host to drain writers. Late uploads that complete in an old
physical generation may leave unreachable orphans for separate cleanup.
S3 version-list and version-delete requests have bounded connect and total
request timeouts, and transport errors do not include signed URLs.
