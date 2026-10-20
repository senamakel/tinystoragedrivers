# Storage ports

Status: accepted (v0.3, ports, memory driver and conformance suite).

## Problem

OpenHuman persists data in more than a dozen places, and each one talks to its
backend directly:
- rusqlite databases opened per call: sessions, run ledger, flows, cron,
  approvals, devices, notifications, task sources, graph checkpoints
- JSON and JSONL files: turn states, threads, artifacts, costs, credentials
- the OS keyring

Some of the libraries it hosts (tinyagents, tinyflows) hard-wire SQLite in the
same way. Nothing can run against another backend, and a multi-tenant cloud
deployment on MongoDB is impossible without rewriting every store.

## Outcome

- One set of backend-neutral ports that every persisting crate builds on.
- A driver per backend: memory, SQLite, MongoDB, files, and secrets (the
  `SecretStore` contract is in [`secret-storage.md`](secret-storage.md)). Each is
  compiled in by Cargo feature and chosen at boot by URL.
- The same behavior on every driver, enforced by one conformance suite.
- Tenant isolation as a property of the port handle, not of each call site.

Non-goals:
- typed repositories (they stay with the crates that own the records)
- a query language richer than what both SQL and MongoDB evaluate natively
- cross-backend replication

## Ports

### `DocumentStore`

Versioned JSON **objects** in named collections.

- **Names.** Collection names are 1–120 bytes of `[A-Za-z0-9_.-]`, and the
  prefix `_tsd` is reserved for drivers. Ids are 1–512 bytes without NUL.
- **Versions.** A new document is version 1, and every write produces a
  strictly greater version. A write that would exceed `u64::MAX` fails with
  `ErrorKind::Backend` and changes nothing; a version is never reused. That
  holds across deletion: a deleted (or dropped) id that is written again
  continues from its last version, so a compare-and-swap prepared before the
  deletion fails instead of overwriting the new document.
- **Preconditions.** `Precondition::None` upserts. `Absent` inserts. `Version(v)`
  is a compare-and-swap. A failed precondition is `ErrorKind::Conflict`.
- **Filters.** `Filter` supports `eq`, `ne`, `in`, `range`, `exists`, `and`,
  `or` and `not`, over dotted paths. `_id` addresses the document id.
  - Values compare by the cross-type order in `value::compare`: null < bool <
    number < string < array < object. Numbers compare numerically.
  - A range bound of a different JSON type never matches.
  - `ne` matches documents where the field is absent.
- **Sorting.** Any number of keys. Ties, and an empty sort, fall back to id
  ascending. A missing field sorts as null.
- **Paging.** `Query.limit` caps a page and must be at least 1. Sort fields
  follow the same path rules as filters. `Page.next` is an opaque keyset cursor
  containing the last returned sort values and id, bound to the issuing driver,
  scope, collection, filter and sort; any other cursor is
  `ErrorKind::InvalidInput`. Legacy native offset cursors are invalid because
  their contents cannot be interpreted as a keyset position.
  Each continuation re-evaluates the query and starts strictly after that saved
  total-order position. Paging does not promise a snapshot: inserts before the
  saved position are not returned, inserts after it can be returned, and
  deletions or sort-key changes affect later pages according to their current
  values. A document moved from before the cursor to after it can therefore
  appear, while one moved from after it to before it can be omitted.
- **Declarations.** `ensure_collection` merges into what is already declared:
  indexes and search fields accumulate, and an expiry field can be added but
  not changed. Redeclaring an index name with different fields is
  `InvalidInput`. Adding a unique index that stored documents already violate
  (in any scope) is `AlreadyExists`, and nothing is declared. A declaration is schema, written by host code rather than
  tenants, so it applies to the collection in every scope, the way a SQL or
  MongoDB index spans the whole table or collection.
- **Indexes.** Any field is queryable. An `IndexSpec` is a performance hint;
  when it is `unique`, every driver enforces it with `ErrorKind::AlreadyExists`.
  Documents missing an indexed field are unconstrained.
- **Claims.** `claim(filter, sort, patch)` picks the first match, applies an
  RFC 7396 merge patch, and returns the updated document, all as one atomic
  step. Two claims never return the same document when the patch makes it
  stop matching.
  - *Run-ledger patterns.* A task or run ledger is built from the document
    port alone: enqueue with `Precondition::Absent`, claim the next eligible
    task with `claim(eligible, [priority, seq], patch)`, complete with
    `Precondition::Version` of the claimed version (a stale holder gets
    `Conflict`), release a worker's claims on shutdown by claiming
    `state = running AND owner = me` until `None` with a patch that sets the
    state back and nulls `owner`, and prune finished tasks past retention with
    `delete_where`. Conformance (`conformance/ledger.rs`) pins these on every
    driver.
- **Optional capabilities.** A capability the driver lacks is
  `ErrorKind::Unsupported(capability)`.
  - `Ttl`: documents whose `ttl_field` (epoch ms) has passed read as absent.
    Expired documents are invisible to `get`, `query`, `count`, `claim` and
    `search` alike, and rewriting a document with a later expiry keeps it
    alive.
  - `BlobTtl`: `put_expiring` and `put_stream_expiring` start a relative blob
    lifetime at write time. Blob schema migration uses the additive
    `BlobStore::get_with_expiry` and `put_until` methods to retain the stored
    absolute deadline instead of restarting a relative TTL. A missing deadline
    means known permanence; defaults for drivers that cannot inspect or retain
    expiry refuse as unsupported rather than treating it as permanent. No
    absolute-expiry capability is advertised separately.
  - `RecordMetadata`: every document carries `Versioned::meta`
    (`Meta { schema, created_ms, updated_ms, expires_ms }`), described in
    [`schema-v2.md`](schema-v2.md). `put_with(.., WriteOptions { schema,
    expires_ms })` declares the schema version and expiry; a plain `put`
    declares neither, so it clears an earlier expiry, and `claim` carries both
    over. The driver stamps `created_ms` on the first write (an update keeps
    it; creating the id again after a delete or expiry starts it afresh) and
    `updated_ms` on every write (never backwards). A record at or past
    `expires_ms` reads as absent from `get`, `query`, `count`, `claim` and
    `search`. Filters and sorts address the fields as `_schema`,
    `_created_ms`, `_updated_ms` and `_expires_ms`, next to `_id`; a reserved
    path never reads the body. A driver without the capability returns
    `Meta::unknown()` (all zero, no expiry) and refuses a `put_with` that
    declares a schema or expiry with `Unsupported(RecordMetadata)`. The legacy
    batch methods keep their default-options behavior. `AtomicWriteOptions`
    separately marks drivers that preserve one `WriteOptions` value on every
    put in the new atomic methods; non-default options also require
    `RecordMetadata`.
  - `Transactions`: `atomic_batch`, all or nothing.
  - `AtomicWriteOptions`: `atomic_batch_with` and
    `atomic_batch_checked_with` preserve metadata for every put inside the
    same transaction. Deletes are unchanged. Drivers without this capability
    refuse the new calls without a nonatomic fallback or post-commit update.
  - `CollectionEnumeration`: `collections(prefix)` lists collection names with
    stored records in this handle's scope. It filters by a literal string prefix
    and sorts names lexicographically. Tombstones count as stored records so
    complete scoped cleanup can discover deleted versions; declared collections
    with no records are omitted. Without the capability, the method returns
    `Unsupported(CollectionEnumeration)`. Enumeration never exposes another
    scope.

  - `FullText`: `search` over the declared `SearchSpec` fields. Hits come
    best first: scores never increase down the list, equal scores order by
    document id, a document matching more of the query ranks ahead of one
    matching less, and `limit` keeps the best prefix of the full ranking. Score
    values are backend-local. Declaring a field searchable later indexes
    documents written before the declaration (an idempotent reindex), and the
    index follows updates and deletes. Conformance
    (`conformance/semantics.rs`) pins all of this.
  - Vector search is not a port here; it stays with TinyMemory (see
    [`storage-bus.md`](storage-bus.md)).
  - `Fencing`: `StorageBackend::for_scope_fenced`, see
    [`write-fencing.md`](write-fencing.md).

### `StreamStore`

Append-only logs of JSON values.
- Offsets are dense, start at 0, and are never reused.
- `truncate_before` discards old records but leaves `len` unchanged.
- `seal` (capability `StreamSeal`) closes a stream to appends and stores its entries as one zstd blob at `_tsd/sealed/<escaped name>` in the same scope. Reads are unchanged; appends and `truncate_before` are `Conflict`; sealing is idempotent; a missing stream is `NotFound`; `delete_stream` removes the blob. See `schema-v2.md` section 6.

Optional ConditionalStreamAppend provides append_batch_if_offset(stream,
expected_next_offset, values). Compare the persistent next offset and append
the entire batch in the same atomic step; return its first offset. A missing
stream has next offset 0. An empty batch still checks the expected offset and
any native fence.

A mismatched offset returns Conflict without appending. Validation errors and
aborted operations append nothing. Native fence validation participates in the
same lock/transaction. Scope isolation matches other stream
operations. Unsupported adapters return Unsupported(ConditionalStreamAppend).
MongoDB advertises the capability only when transactions are available; file
atomicity remains within its documented process boundary.

A transport failure during MongoDB commit can leave the outcome unknown,
reported explicitly as a backend error. Read before retrying; repeating the
same expected offset cannot duplicate a committed batch. delete_stream resets
next offset to 0; truncate_before never does. A caller retaining a cursor across
deletion must separately track identity.

### `BlobStore`

Bytes by `/`-separated key, without empty, `.` or `..` segments, and with no
leading `/`. Supports ranged reads clamped to the blob's length, and prefix
listing.

**Tree view.** `list_dir(dir)` lists one level (blobs and the sub-directories
that hold more blobs, sorted by name, `a` never matching `ab/...`),
`put_tree(dir, files)` writes a bundle such as a skill (every path is validated
before the first write; the writes are not atomic across files, so publish a
manifest document last when all-or-nothing matters), and `delete_tree(dir)`
removes everything below a directory (the root is refused). They are provided
methods built on `list`/`put`/`delete`, so every driver, the S3 driver and the
bus client answer them identically.

### `StorageBackend` and `Scope`

- `for_scope(scope)` returns handles bound to one tenant. No port method takes
  a scope, so a call site cannot forget it.
- `Scope::local()` is the single-operator scope.
- A backend that serves only one scope (a desktop SQLite file) rejects other
  scopes in `for_scope`.
- A backend is one database; there are no named databases (removed in schema
  v2, see [`schema-v2.md`](schema-v2.md) section 7).
- `for_scope_fenced(scope, fence)` returns handles whose writes the driver
  checks atomically against a guard document (`Capability::Fencing`,
  `ErrorKind::Fenced`). See [`write-fencing.md`](write-fencing.md).
- OpenHuman's scope is the agent id. The cloud deployment runs one agent per
  user, so this is the user.

### Errors

`StorageError { kind, message, source }`.
- Callers branch on `ErrorKind` only.
- Only `Unavailable` is retryable.
- Messages never contain credentials. `StorageConfig`'s `Display` and `Debug`
  redact the password in a connection string.

### Synchronous callers

`Blocking` (feature `blocking`) runs a port future on a runtime thread it owns.
It is safe from plain threads and from inside any tokio runtime. A future
running on the bridge must not call back into it; doing so is a `Backend`
error, not a deadlock. The bridge is transitional: synchronous consumer traits
should become async.

## Acceptance

- `conformance::run` passes on every driver. The memory driver passes it in
  this release.
- `tinystoragedrivers-core` depends on no database client or transport, and CI
  asserts this.
- Every source file has at least 90% line coverage.

## Host conformance

A host that composes several scopes, shares one store between handles and
migrates its own collections should not repeat the driver matrix in its own
tests. `conformance::run` therefore also covers, on every driver:

- **Nested scopes** (`isolation.rs`, `nested`): a tenant `S`, `S/agent:a` and
  `S/module:m` hold the same collection, id, stream and blob key without
  seeing each other, and collection and scope listings do not leak between
  parent and child.
- **Document migrations** (`migrations.rs`): a two-step chain over several
  batches, a crash part way, a resume that writes no record twice, and a no-op
  re-run (drivers with `RecordMetadata`).
- **Holder fences**: see [`write-fencing.md`](write-fencing.md).

External drivers should call the public
`conformance::run_with_process_capabilities(backend, single_scope,
shared_across_processes, cross_process_cas)` entry point with their declared
process guarantees. It checks both `StorageBackend::capabilities()` and the
`DocumentStore::capabilities()` on the local handle and, for multi-scope
drivers, the `conformance-b` handle. The declarations must match the supplied expectations
on both surfaces; `SharedAcrossProcesses` requires `CrossProcessCas` on each.
This catches a backend whose top-level declaration promises a guarantee that
its scoped document port does not expose, or vice versa. Built-in native
drivers use `conformance::run`, which supplies their fixed expectations.

`conformance::run_shared(open)` takes a closure that opens independent
backends on one store. A driver declaring `CrossProcessCas` must arbitrate
racing document claims, version-guarded puts, create-only (`Absent`) puts and
lease acquisitions across handles: each race has exactly one winner, and a
fresh lease starts at epoch 1. When it also advertises `Fencing`, a fenced
handle must reject a write after another handle changes its guard. CAS-only
drivers make no immediate-visibility promise for the other storage ports.

A driver declaring `SharedAcrossProcesses` promises all of those CAS and
fencing guarantees, and also immediate cross-handle visibility: document
creates, updates and deletes, stream appends and blob writes are visible to
other handles. The suite verifies these reads after the corresponding writes
and checks that all handles observe the winning claim, lease and version race.
Every driver, including ones with neither process capability, must show that
a backend opened after another was dropped reads the stored data and continues
its document versions, stream offsets and lease epochs. The memory driver (one
heap) and file driver (one process) do not report either process capability.
`tinystoragedrivers_secrets::testkit::document_secrets_conformance` runs the
secrets suite and the `enc2`-ciphertext-at-rest check over any driver's
documents; the SQLite, file and MongoDB crates run it.


## Validated document batches

A document port advertising `ValidatedBatch` implements
`atomic_batch_checked(read_checks, write_ops)`. A `ReadCheck` names a collection
and document id in the bound scope. Its `ExpectedDocument` is
either `Absent` or an exact live `Version`. Expired documents count as absent.
All checks inspect the pre-write state, followed by the writes in their input
order, inside one native lock or transaction. Contradictory duplicate checks
return `Conflict`; equivalent duplicates are allowed. Empty and read-only
batches still validate the native fence. Dependency bodies and logical versions
are never rewritten. A failed check or write applies none of the batch.

The default trait implementation returns `Unsupported(ValidatedBatch)`.
Memory and SQLite support this operation. File storage refuses it until its
multi-document persistence has a crash-atomic commit contract. Mongo supports
it on transaction-capable deployments when public transactions are enabled;
standalone servers do not advertise it.

`atomic_batch_checked_with(read_checks, write_ops, options)` keeps the same
pre-write check and rollback guarantees while applying one shared
`WriteOptions` value to every put. `atomic_batch_with(write_ops, options)` is
the unchecked counterpart. The existing `WriteOp` shape and old batch methods
remain unchanged; the latter continue to declare default options. Both new
methods default to `Unsupported(AtomicWriteOptions)`. Drivers advertise that
capability only when both preserve options atomically.

Mongo uses reserved metadata counters per storage scope.
Every document mutation, including claims, deletion, expiry sweeping and writes
through independently opened handles, increments its scope counter inside the
same physical transaction. This prevents snapshot write skew for both version
and absence checks, without changing user documents. Ordinary writes in
unrelated scopes touch different counters. Collection-wide schema changes and
expiry burial lock registration and all registered scope counters atomically.
First admission of a scope also locks registration, so it cannot bypass a
concurrent schema change. Mongo does not create server TTL indexes; expiry is
filtered on reads and buried by driver mutations.

On replica sets, `without_transactions()` disables public transactional
capabilities, including validated batches and fencing. Ordinary document
mutations still use internal physical transactions to participate in validation
serialization. This narrows the former promise that the handle never used any
transactions. True standalone deployments keep ordinary single-document writes.

Mongo bounds transient transaction retries to 64 attempts with staggered,
bounded backoff. Exhaustion returns the typed last error; a changed dependency
returns nonretryable `Conflict`. An unknown commit result returns a
nonretryable backend error because the mutation may have committed: callers
must read their durable outcome before retrying. The API has no deadline
parameter. Scope serialization trades same-scope mutation throughput for
cross-document serializability.

## Scoped reset and writer admission

Reset targets one scope of the connected backend. A receipt from one backend
says nothing about another; a host reports a full reset only after every owned
backend and external owner stage completes.

`ScopedStorage::purge(PurgeRequest { operation_id, mode })` is optional.
Unknown adapters return `Unsupported(ScopePurge)` before erasing data.
`ScopePurge` advertises native scope admission and reset support;
`AtomicScopePurge` additionally promises one atomic all-port operation.
A custom `ScopePurge` implementation must explicitly opt in to generation
validation before CompositeStorage can rely on its generation check. Full
binding validation is a separate opt-in that also checks a native writer fence.
This distinction lets composite blob reads reject retired generations while
retaining the existing fenced-read contract; writes still enforce their fence.
Both trait defaults are unsupported for compatibility with older custom ports;
advertising purge alone does not prove that a handle rejects retired
generations. A composite refuses purge capability if its record or blob half
cannot validate the required generation binding.
A stable operation identity makes retries idempotent and its receipt survives
the erased data. `PurgeMode::Quiescent` is a trusted host assertion that every
writer has been stopped and drained; it is never a model-selectable option.
`PurgeMode::Atomic` requires native atomic erasure of all storage ports.

Memory and SQLite implement atomic erasure of documents, deleted-document
versions, document-backed secrets and wrapped-key envelopes, import receipts,
stream records and lifetime cursors, and blobs for exactly one scope.
SQLite also removes that scope's search mirror. Shared collection declarations,
indexes and native admission/receipt metadata remain: removing shared schemas
would alter other scopes. Query results become empty while declarations remain.
Standalone secrets backends, OS keyrings, arbitrary native SQL tables and other
external owner stores are outside these ports and require explicit owner stages.

The operation issues a monotonically increasing `ScopeGeneration`.
Generation zero is the legacy handle default. On memory and SQLite, every data
read and mutation, including empty operations and schema
declarations, validates its issued generation under the same native
lock/transaction as the data access. SQLite reads pin generation and data in
one deferred read transaction. Retired handles cannot read replacement data.
Revoked generations fail with `ScopeGenerationRejected`, even if the operation
would otherwise change nothing; an actual stale native lease guard continues
to fail with `Fenced`.
Hosts persist the returned generation in authoritative profile state before
reopening through `for_scope_generation` or
`for_scope_fenced_generation`. Generations come from trusted
native receipts and profile admission, never model arguments or document
metadata. A matching receipt retry does not erase data written in the new
generation. Reusing an older operation identity after a later reset cannot
revive the old generation.

A fenced purge checks and holds the native guard through the operation. The
guard's scope must differ from the erased scope, including when the guard is
in another scope. A stale guard changes nothing.
Memory and SQLite support this natively. File purge is durable but staged
across filesystem entries and makes no crash-atomicity claim. CompositeStorage
uses a durable coordinator in the record backend and stages records and blob
deletion separately; it never advertises AtomicScopePurge. Its coordinator
collection must already be present and compatible when the record backend is
opened through ExistingSchema. Trusted bootstrap prepares that internal
collection; restricted use validates it and refuses if it is missing, without
creating or repairing schema. Nested compositions reserve deterministic
internal coordinator scopes, which public scope constructors and enumeration
hide.

S3 advertises ScopePurge only when a signer is installed. It enumerates every
object version and delete marker beneath the exact generation prefix and
deletes each VersionId. AWS permissions must include `s3:ListBucketVersions`
for the bucket and `s3:DeleteObjectVersion` for the scoped object keys. A
signer-less S3 backend or a custom blob driver without generation isolation
withholds the composite capability before record or blob mutation. File,
Mongo GridFS, S3 and composite storage do not advertise atomic all-port purge;
they must not turn an unsupported native guarantee into an unfenced
best-effort success.

Reset invalidates application query caches, old version preconditions, stream
cursors, import receipts and scoped secret providers. Shared declaration caches
remain valid because declarations are retained. Hosts stop and drain KMS loads
and secret writers, call `KmsKeyProvider::invalidate(scope)` to zeroize the
local cached key, then rebuild/rebind providers over the admitted generation.
KMS async reads already revalidate the durable envelope before using cached
plaintext; the synchronous KMS path refuses to use unvalidated cached keys.
The new envelope must be created through a generation-admitted handle.

The reset coordinator may call `purge_status(operation_id)` through its bound
reset port even when its data generation has retired. This reads native receipt
authority and checks its native lease; it cannot start another erase. Pending
receipts identify the next `PurgeStage`; a typed stage failure is followed by a
status query and a retry of the same operation identity.

### Mongo quiescent purge

Replica sets advertise ScopePurge, with PurgeMode::Quiescent only. GridFS
prevents atomic all-port erasure and fenced all-port reset: Atomic and fenced
requests return typed Unsupported before changing admission or tenant data.
Standalone and without_transactions backends do not advertise reset support.
An independent replica handle with public transactions disabled still obeys
the persistent admission row; normal document and stream mutations retain
internal transactions.

The host must stop and drain every scoped writer, including GridFS uploads,
before starting. GridFS chunks carry no scope before a completed file manifest,
so the driver cannot prove that assertion. Reset persists the issued generation,
operation identity, Pending flag and next stage outside tenant data. Pending
blocks both old and newly issued generations from data reads and mutations.
Document and stream writes hold that admission row and check their generation
inside the same transaction as the mutation. Shared declaration changes check
the actor generation inside the schema metadata transaction; indexes remain
shared administrative declarations.

Records and stream lifetime heads are erased transactionally for the exact
default/named namespace. Blob planning saves one durable manifest row per file.
Each blob's chunks are erased before its file and manifest row. A stage failure
keeps the receipt Pending; retrying the same identity resumes without losing
ownership of orphan chunks. Complete is persisted only after no scoped files
or manifest rows remain. It covers this one backend, and is not a
cross-address, cross-port atomicity claim.

All Mongo payload and admission reads select the primary. Port reads validate
generation and Pending both before and after reading; a changed generation
rejects the result instead of exposing replacement data. Existing returned
bytes/values cannot be revoked, so hosts must gate and drain readers too before
publishing a full reset. Each document/stream mutation serializes on its scope
metadata row; unrelated scopes use separate rows. Scoped expiry reclamation
after a reset uses sweep_expired_generation with the issued generation.

### Record driver identity in composites

`StorageBackend::driver` remains its diagnostic label (`composite` for a
composition). `document_driver` identifies the originating native document and
stream driver, defaulting to `driver` for ordinary backends. Compositions and
transparent backend wrappers forward it recursively. It performs no opening or I/O. Hosts must use the record identity when
applying their own coordination/recovery rules rather than classifying a wrapper
or independent blob driver. The identity grants no capability or actor authority;
native capabilities and admission still govern operations.
