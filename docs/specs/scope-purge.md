# Native scope purge

Status: Implementation candidate; publication and hosted gates remain in progress.
Owner: TinyStorageDrivers. Host integration: OpenHuman issue #7432.

## Problem

Removing a local directory does not erase data held by another backend.
Enumerating current records also misses stream lifetime state, document
tombstones, encrypted secret envelopes and blob chunks. A reset must retire
existing handles so they cannot read or write the replacement data.

## Boundary

The optional native port resets one scope of the connected backend.
The trusted host owns the complete backend/scope manifest, authorization,
writer shutdown, operation journal and issuance of replacement handles.
The host resolves the root backend itself; the port never discovers owner
namespaces.

The port does not revoke external KMS keys, remove unrelated filesystem paths,
or implement a root-wide transaction. CompositeStorage coordinates its record
and blob ports with a durable staged receipt in the record backend. It reports
ScopePurge only when both halves can bind and validate generations; it never
reports AtomicScopePurge. Coordinator collections and scopes are reserved
internal metadata and are hidden from public scope enumeration. Nested
composites use deterministic, bounded coordinator layers so reopening the same
composition finds the same state. ExistingSchema handles validate the
coordinator declaration and refuse if it is absent or incompatible; trusted
bootstrap must prepare that declaration before restricted composite purge is
used. Purge does not erase coordinator receipts.

S3 and generic BackendBlobs purge use generation-isolated physical namespaces
after generation zero. The trusted host is responsible for stopping and
draining writers during quiescent reset. A writer that violates that contract
and finishes in an old generation can leave unreachable orphan objects;
reclamation remains separate maintenance.

BackendBlobs keeps its logical-scope-to-physical-generation index in the
reserved `__tsd_composite_blob_scope_map/scopes` collection of the wrapped
backend. Generation-zero storage keeps its legacy scope name. An
ExistingSchema backend used for nonzero BackendBlobs generations must have
that internal collection prepared by trusted bootstrap; restricted use
validates it and fails closed if it is absent or incompatible. The index is
data metadata and does not alter application collection declarations.

## Contract

PurgeRequest contains a stable host-issued operation_id and Atomic or Quiescent
mode. IDs contain 1–128 ASCII letters, digits, underscores or hyphens. The host
persists an ID before its first attempt and reuses it after interruption.

ScopePurge is bound to a backend, scope, generation and any native fence.
StorageBackend generation constructors preserve those bindings. Ordinary
constructors select generation zero, rather than discovering the newest
generation and silently reviving old callers.

PurgeReceipt reports that operation's generation, status and resumable stage.
Complete means every owned native port in this backend has finished. Pending
means admission remains closed and the same operation must resume. Native
admission and receipts live outside erased tenant data.

Completed receipts remain queryable after subsequent resets and reconnects.
Retrying a completed operation returns its original receipt and never erases
replacement data or issues a newer generation. Receipt retention is unbounded
in this version; there is no automatic pruning policy. An old schema can
backfill its latest completed admission, but cannot reconstruct earlier
receipts that the old implementation never retained.

Memory and SQLite support atomic all-port deletion and generation advancement.
SQLite also persists the receipt in that transaction. File storage supports
durable staged deletion and generation advancement; it does not claim crash
atomicity across files. MongoDB replica sets support explicitly quiescent
staged reset: records, durable blob manifest, chunks/files, then completion.
Composite storage is likewise staged across its independently owned record and
blob backends. Every generation is denied while a staged reset is pending.
GridFS uploads must be stopped and drained by the trusted host first; a late
file keeps the operation pending and causes manifest replanning.

S3 advertises blob scope purge only when an object-store signer is configured.
Purge lists every version and delete marker under the exact physical
generation prefix, follows version pagination, and deletes each version by
its VersionId. AWS credentials need `s3:ListBucketVersions` on the bucket and
`s3:DeleteObjectVersion` on the scoped object resources. A signer-less S3
backend, or a custom blob backend without truthful generation-purge support,
withholds composite ScopePurge before mutation. S3 erasure is staged and is not
atomic with record deletion. Version-list and version-delete HTTP requests use
bounded connect and total request timeouts; transport and client-construction
errors are returned without including signed URLs or credentials.

MongoDB standalone deployments refuse ScopePurge. MongoDB Atomic mode and
native fenced GridFS purge refuse their unsupported guarantees before starting.
Native error kinds and causes remain intact. Corrupt authority fails closed.
A durable MongoDB reset witness is committed independently of the admission
row in the same transaction that first closes admission. Lost or recreated
admission remains corruption while that first reset is pending; completed
receipt history also detects authority loss from older schema versions.
A pre-reset registration may omit reset fields; once any reset marker exists,
all authority fields are required. Manifest publication, chunk deletion,
file/manifest removal and completion validate and write the same native
authority row in their transactions. A delayed overlapping reset stage cannot
commit destructive work against replacement data after completion.

## Invariants

- Sibling scopes survive every reset stage and retry.
- Documents, tombstones, streams, stream lifetime cursors and blobs are erased.
- Shared collection declarations and native authority metadata remain.
- Old generations cannot read replacement payloads or mutate any port. Revoked
  handles report `ScopeGenerationRejected`; stale native lease guards retain
  their distinct `Fenced` error.
- Native fence validation still precedes historical receipt replay.
- Failed atomic erasure commits neither generation nor receipt.
- Partial staged work never returns Complete or opens admission early.
- Status queries do not start a reset; they can report an earlier generation.

## Acceptance

Conformance must exercise supported mode selection,
all-port replacement preservation, and historical status/retry after two resets.
Native tests must also prove persisted reopen, upgrade backfill, corruption,
rollback, staged interruption/resume, late GridFS files and stale-generation
reads. Missing reset authority fields and delayed overlapping planner/eraser
stages must fail closed while replacement blobs remain intact. Live MongoDB checks use local replica and standalone fixtures.

Before publication, run the owning workspace gates, strict Clippy/rustdoc and
per-file coverage. Host end-to-end reset, external KMS key revocation, host
writer coordination and orphan cleanup remain separate acceptance work, not
established by native conformance alone.

## Open questions

MongoDB fenced GridFS reset remains unsupported; host takeover-safe shared reset
must not bypass that refusal. Future watch ports must validate generations
before every delivery and terminate on reset, or refuse the combination.
