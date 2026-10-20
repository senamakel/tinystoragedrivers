# Storage bus

Status: accepted for contract design; implementation is staged by
[`../plans/storage-bus.md`](../plans/storage-bus.md). Owner: tinystoragedrivers.

## Problem and boundary

Compiled TinyBus modules need storage that follows the same tenant, schema,
atomicity and capability rules as native callers. `DocumentCollection1` is an
existing confined Get/Put/Delete callback, but it is not the complete
`StorageBackend` port. This contract defines the complete
`ai.tinyhumans.tinystorage.Storage` object family, with Documents, Streams,
Blobs, Secrets and Queues objects. The new Documents wire types
are synchronous serde data and do not depend on native ports, an async runtime,
or a database. The existing crate still houses the legacy TinyBus CRUD proxy
for compatibility; moving transport adapters out of the wire crate is part of
the staged implementation, not a property claimed for the current dependency
graph.

The proxy is behind a default-on `client` Cargo feature, so a data-only
consumer can disable defaults and link the wire declarations without TinyBus;
see the [client feature spec](bus-client-feature.md).

The `tinystoragedrivers-bus` package supports Rust 1.85 in either feature
profile. The storage workspace and implementation packages remain at Rust
1.88; see the [bus contract MSRV spec](bus-contract-msrv.md).

Goals are full Documents, Streams, Blobs and Secrets parity; one host-captured
caller authority; mechanically adapted clients and servers; and shared
conformance proving same-port behavior. The existing `tinystoragedrivers`
name remains. This does not move domain repositories into storage, let modules
choose storage URLs/scopes, or allow a module to create a host grant.

## Authority and dispatch

The host authenticates the module caller and binds the call to one principal,
scope, admitted backend, and immutable grant. The request has no
scope, URL, backend, generation, or grant selector. Unknown selectors
and unknown request fields fail closed. The host checks caller rights, object
rights, declared schema and resource limits on every call; a module's claimed
identity is not authority. Scope is never serialized in a request or inferred
from a document body.

Grants enumerate allowed object families, collections, stream names, blob key
prefixes, secret name prefixes, and operations. Documents schema rights list
permitted indexes, TTL fields, and search fields; modules may only use host
approved declarations. Host fences are captured at registration and checked
atomically by native writes, including empty checked batches. Stale authority
returns `Fenced`. Secrets always use the encrypted `SecretStore` port and never
cross a non-confidential route. Failure payloads carry only the sanitized
storage error category.

## Objects and behavior

`Storage` contains `Documents`, `Streams`, `Blobs`, and `Secrets`. Collection
names are explicit in Documents operation messages and are checked against the
captured host grant on every call. They select data within that grant; they do
not select a scope, backend, or schema authority. Each object
has a fixed versioned interface, finite selector list, and typed synchronous
serde messages. TinyBus request/reply provides bounded control traffic. Bulk
payloads over 16 MiB use an authenticated TinyBus `Stream`; there is no inline
fallback above that limit. Stream capabilities are bound to the same caller
grant and one operation, enforce byte limits, and are closed on completion or
failure. Watch delivery uses TinyBus `SIGNAL`s: `WatchOpen` and `WatchClose`
control a host-owned subscription, while `WatchChanged` carries only a
host-issued watch id and a content-free invalidation/reset notice. Its cursor
matches native `WatchOptions.after` resume semantics and its bounded quiet
interval is 1–60,000 milliseconds. Clients reread authoritative data through
Documents. Signals never carry documents, secrets, blobs, or stream contents.

Documents mirrors `DocumentStore`: get/put/delete, filter/sort keyset query,
count, delete-where, atomic claim, atomic batch, checked atomic batch, search,
schema ensure/validate, collection drop and enumeration. Requests preserve
native versions and preconditions. Cursors are opaque, query-bound and
host-grant-bound. Search uses declared fields and returns best matches first;
the contract promises stable ordering within one result list, not cross-driver
score equality. Schema changes are host-authorized only. Full-text index
rebuild/reindex is a host/driver maintenance operation, never a module selector.

FTS providers must apply a documented deterministic tokenization and ranking
strategy per driver; conformance pins token membership and ordering for ties,
while relevance scores remain backend-local. Reindexing a changed approved
search declaration is idempotent and does not expose index internals.

Vector search is intentionally not a Documents capability. Semantic memory
indexes, embeddings, candidate retrieval and relevance policy belong to
TinyMemory, whose storage implementation and TinyBus module contract own the
end-to-end meaning of a vector query. Keeping that query inside TinyMemory
ensures every host uses its same embedding model, metadata filters, recall
policy and ranking, while Storage supplies durable documents, streams and
blobs. A generic vector port here would otherwise leave embedding generation,
dimensions, distance metrics, filtering, ranking and index lifecycle undefined
across drivers. TinyMemory can propose a separately versioned vector capability
only if it needs a driver-neutral acceleration seam and defines conformance for
those semantics; the Storage bus will then adapt that accepted capability.

Streams mirrors append/read/range/length/truncate/delete and conditional batch
append when supported. Offsets are dense and never reused by truncation.
Blobs mirrors put/get/ranged-read/delete/prefix-list and sends large byte
payloads only over TinyBus Stream. Secrets mirrors get/set/delete/prefix-list
through encrypted native storage; secret bytes and names are only returned to
authorized confidential callers. Every operation reports unsupported
capabilities explicitly; adapters must not claim unimplemented capabilities.

The S3 driver remains a host-side driver. Hosts forward S3 configuration and
credentials into their own storage backend construction; neither these secrets
nor S3 endpoints are passed through a module request or module-controlled URL.

## Wire reference

Contract version: `CONTRACT_VERSION = 7` (crate `tinystoragedrivers-bus`).
Interface `ai.tinyhumans.tinystorage.Storage`; objects and finite selectors:

| Object | Selectors | Route |
| --- | --- | --- |
| `Documents` | 19, see `DOCUMENTS_SELECTORS` | private |
| `Streams` | `Append`, `AppendBatch`, `AppendBatchIfOffset`, `ReadWindow`, `Len`, `TruncateBefore`, `DeleteStream`, `Streams`, `Seal`, `Info` | private |
| `Blobs` | `Put`, `PutStream`, `Get`, `GetStream`, `GetRange`, `Head`, `Delete`, `List`, `PresignGet`, `PresignPut`, `Capabilities`, `GetWithExpiry`, `GetStreamWithExpiry`, `PutUntil`, `PutStreamUntil` | private |
| `Secrets` | `Get`, `Set`, `Delete`, `List` | confidential (`#[tinybus(confidential)]`) |
| `Queues` | `Features`, `EnsureQueue`, `Enqueue`, `Receive`, `Ack`, `Nack`, `Extend`, `Stats`, `DeadLetters`, `Redrive`, `Purge` | private |

Size limits (constants in the crate, enforced by the server before native I/O):

- `MAX_FRAME_BYTES` is 16 MiB, the `TinyBus` frame cap.
- `MAX_INLINE_BYTES` is 8 MiB: encoded cap of one page, batch or window, so
  escaping overhead cannot reach the frame.
- `MAX_PAGE_ITEMS` and `MAX_BATCH_OPS` are 1,000 records or operations.
- `MAX_INLINE_BLOB_BYTES` is 1 MiB (a JSON byte array can cost 4 bytes per byte): larger blobs use `PutStream` / `GetStream`
  over a `TinyBus` stream in `BLOB_STREAM_CHUNK_BYTES` (512 KiB) chunks. The
  expiry-aware `GetStreamWithExpiry` / `PutStreamUntil` variants use the same
  streaming limit and bind the deadline to the same read or write as the body.
- `MAX_PRESIGN_TTL_SECS` is 3,600.
- `MAX_QUEUE_MESSAGE_BYTES` is 256 KiB of payload JSON text per queue message.

Stream records and document bodies are inline JSON text (`*_json`), never
native stream references. Streamed blob calls carry only metadata
(`PutBlobStreamRequest`, `BlobStreamHead`); the body is a separate `TinyBus`
stream argument bound to one operation, whose length must equal the declared
`len`. A page that would exceed `MAX_INLINE_BYTES` is cut short and continued
with the returned cursor or last offset plus one. `Secrets` messages redact
`Debug` output; presigned URLs are credentials and are likewise never printed.
Watch signals are `WatchChanged` (Documents) and carry no payload.

## Client and reference server

`tinystoragedrivers-client` holds both ends, with no `TinyBus` dependency:

- `BusStorage` implements `StorageBackend`; its `ScopedStorage` hands out
  `BusDocuments` (`DocumentStore`), `BusStreams` (`StreamStore`) and
  `BusBlobs` (`BlobStore`), and `BusStorage::secrets()` is a `SecretStore`.
  Calls go through the `StorageTransport` trait (`call`, `put_blob_stream`,
  `get_blob_stream`, `signals`); a `TinyBus` proxy is one implementation.
- The caller is bound to one host-chosen scope. `for_scope` accepts only that
  scope, the fenced and generation variants are refused, and
  host-only capabilities (fencing, scope purge, existing-schema) are never
  reported. `Documents.Capabilities` and `Blobs.Capabilities` tell the client
  what the admitted backend provides; an `Unsupported` refusal is attributed to
  the capability the call needs, since the wire carries only the category.
- Large values: blobs over `MAX_INLINE_BLOB_BYTES` use `PutStream` / `GetStream`
  and `GetRange` is split into inline spans, so a blob above the 16 MiB frame
  works; windows and batches above their caps are rejected or continued as
  described under Wire reference.
- `StorageServer` is the reference host adapter: generic over a `ScopedStorage`
  the host already bound, it enforces the limits before native I/O, answers
  `Secrets` only to `Admission::Confidential` callers, and projects native
  watches to `WatchChanged` signals (at most 64 per binding). `Loopback` joins
  a client to a server in one process through JSON text frames capped at
  `MAX_FRAME_BYTES`, so the shared `conformance::run_ports` suite runs against
  the client exactly as against a native driver.

`WatchOpen` replies with `WatchOpenResponse { watch_id }`; `Capabilities`
replies with `CapabilitiesReply`. Adding that selector made the contract
version 2.

### Blob lifetimes

`BlobStore::put_expiring` and `put_stream_expiring` write a blob with a
lifetime counted from the write (`Capability::BlobTtl`). From that instant
every read (`get`, `get_range`, `head`, `get_stream`, `list`) treats the blob
as absent; a zero lifetime is expired on arrival; a plain `put` of the key
clears the lifetime; reclaiming the storage is the driver's business. The
memory, SQLite, file, MongoDB and S3 drivers provide it (S3 as `x-amz-meta-`
user metadata, so `list` already pays one HEAD per object; MongoDB as
`metadata.expires_at_ms` on the GridFS file). Reclaiming is each driver's
business: file and MongoDB reclaim the lapsed blob on the read that notices it
(and expose `sweep_expired_blobs` / `sweep_expired_blobs_generation` for a
sweeper), S3 only reclaims through `S3Blobs::sweep_expired` because object
deletes cannot be conditional. The conformance suite asserts expiry on every
driver that claims the capability. On the wire `PutBlobRequest` and
`PutBlobStreamRequest` gain an optional `ttl_ms` and `BlobCapabilities` gains
`ttl`; adding them made the contract version 3. A
SQLite database written by an earlier release gains the `expires_at_ms` column
on the next migrating open; a read-only `existing_schema` open refuses the old
layout like any other schema drift.

### Record metadata, sealing and host-only operations (version 4)

Uniform schema v2 ([`schema-v2.md`](schema-v2.md) sections 3 and 6) adds four
things to the ports. The wire carries two of them and deliberately not the
other two.

Carried (additive; every new field is optional and defaulted):

- `Meta { schema, created_ms, updated_ms, expires_ms? }` is an optional `meta`
  field on every `Versioned` a host returns: `Get`, `Query` and `Claim`. Its
  JSON is the native `Meta`'s. `Watch` is content-free and carries none; a
  module rereads. An absent `meta` (a host that predates version 4, or the
  legacy `DocumentCollection1` callback) reads as `Meta::unknown()`.
- `PutDocumentRequest` gains optional `schema` (omitted when `0`) and
  `expires_ms`: `DocumentStore::put_with`. A plain put serializes exactly as
  before. The client reports `Unsupported(RecordMetadata)` when the host
  refuses an options-bearing put. `WriteOptions::created_ms` is a host-only
  migration option: the request has no such field (a request naming it fails to
  decode and is refused `InvalidInput`), and `BusDocuments::put_with` refuses it
  locally with `InvalidInput`. A module can never choose a record's creation
  time; `created_ms` and `updated_ms` always come from the host driver's clock.
  Batch `Put` operations carry no options, as the native `WriteOp` has none.
- `Streams.Seal` takes `StreamRequest { stream }` and replies
  `SealResponse { base, len, blob_bytes }` (the native `SealOutcome`). Sealing
  a stream the module may append to is within its grant, so it is exposed;
  `Conflict` (append to a sealed stream) and `NotFound` cross as usual.
- `Streams.Info` takes `StreamRequest { stream }` and replies
  `StreamInfoResponse { base, len, sealed_ms, last_append_ms }` (the native
  `StreamInfo`), or `null` for a stream that does not exist. A module may read
  the times of its own streams; they are epoch milliseconds from the host's
  clock, so a module learns nothing it could not infer from its appends. It
  rides the `StreamSeal` capability: a host without it answers `Unsupported`.
- `Documents.Capabilities` now lists `record_metadata` and `stream_seal` when
  the backend has them.

Host-only (no selector exists; `HOST_ONLY` in `tinystoragedrivers-client`
keeps `maintenance`, `scope_enumeration`, `shared_across_processes` and
`cross_process_cas` out of every capability list):

- `shared_across_processes` means independently opened backends on the same
  store observe one another's writes and agree on the compare-and-swap rules:
  version preconditions, `Absent` creates, claims and fences arbitrate between
  processes, so racing handles get one winner. `cross_process_cas` names that
  conditional-write guarantee explicitly for policy that needs to query it
  separately; it does not weaken or replace the broader shared-store contract.
  SQLite and MongoDB declare both; memory and file declare neither. These
  describe host-side storage coordination, so they have no bus selector and
  are not exposed to modules.

- `StorageBackend::maintain` and `checkpoint` run vacuum, WAL checkpoints and
  the seal and retention policies across the whole database. A module could
  use them to spend the host's I/O budget or stall other tenants, so the host
  runs them on its own schedule.
- `StreamStore::delete_sealed_stream` is the conditional delete retention uses so it never removes a stream recreated after it was inspected. It has no selector: a bus client answers `Unsupported(StreamSeal)`, and a module deletes its own streams with `DeleteStream`.
- `StorageBackend::scopes` enumerates every tenant. A module is bound to one
  scope and must not learn that others exist.

Compatibility. A new module on an old host works for everything it did before:
a default put sends no new field, and a missing `meta` decodes to unknown. An
options-bearing put or `Seal` fails closed on an old host (`deny_unknown_fields`
and the finite selector set), never silently dropping the option. An old module
on a new host decodes only replies without `meta`: the wire types reject
unknown fields, so a host serving version 4 metadata must be paired with a
module built against version 4. This is the existing rule that contract
changes bump `CONTRACT_VERSION`; the constant is informational and no
handshake compares it.

### Queues (version 5)

The `Queues` object carries the `QueueStore` port
([`queue-store.md`](queue-store.md)), one selector per method. Payloads are
inline JSON text (`payload_json`); `Retry` is `{"kind":"after","delay_ms":n}`
or `{"kind":"dead_letter","reason":"..."}`. `Features` replies
`QueueFeaturesReply`, which `BusStorage::connect` reads once, so a
`BusQueues` handle reports the host queue's features.

- The server answers from `ScopedStorage::queues()`: a `DocumentQueue` over
  the bound scope's documents unless the host built the storage
  `with_queues` (a broker). Leases, delays and dedupe windows run on the
  host's clock; a module cannot set or read it beyond the times a delivery
  carries.
- Limits, checked before native I/O: at most `MAX_BATCH_OPS` messages per
  `Enqueue` (the client splits larger batches, since messages are stored
  independently), at most `MAX_QUEUE_MESSAGE_BYTES` per payload. `Receive`
  leases in rounds sized so one more maximal message always fits
  `MAX_INLINE_BYTES`, so the host never takes a lease its reply cannot carry;
  the reply may hold fewer than `max`. `DeadLetters` is clamped to
  `MAX_PAGE_ITEMS` and cut at the inline cap.
- **Manifest.** `StorageServer::with_queue_manifest(manifest)` makes a
  `QueueManifest` the host's authority: the host applies it on the caller's
  first `Queues` call, a request naming an undeclared queue is refused
  `NotFound`, and `EnsureQueue` succeeds only with the declared spec
  (`Conflict` otherwise). A module that applies the same manifest file
  therefore agrees with its host. Without a manifest a caller may declare any
  queue in its scope.
- Compatibility: a version 5 module calls `Queues.Features` while connecting,
  so it needs a version 5 host; a version 4 module never names the object.

Version 6 adds expiry-aware blob selectors without changing existing payload
shapes. A version 6 client can continue using legacy blob operations against
an older host; absolute-expiry migration fails closed if the new selector is
not implemented. Version 5 clients never name the new selectors.

The four selectors are `GetWithExpiry`, `GetStreamWithExpiry`, `PutUntil`, and
`PutStreamUntil`. They do not change `BlobInfo`, `BlobBody`, `BlobStreamHead`,
`PutBlobRequest`, `PutBlobStreamRequest`, or `BlobCapabilities`, all of which
keep their existing `deny_unknown_fields` wire shape. Buffered and streamed
source replies carry bytes and their stored absolute `expires_at_ms` from the
same backend read; a missing deadline means known permanence. The write
selectors store the supplied absolute deadline rather than restarting a
relative TTL. Deadlines at or before the backend clock are invisible on
arrival.

The client transport and native blob port default to `Unsupported` for these
new methods. A client receiving that refusal from an old server or driver
returns an error; it must not retry through legacy `Get`, `Put`, or a relative
TTL. No absolute-expiry field is added to the existing capability reply. Each
selector is scoped to the same captured blob-key prefixes as its legacy
operation.

### Atomic batch metadata (version 7)

`Documents.AtomicBatchWith` and `Documents.AtomicBatchCheckedWith` use new
strict request types with one shared `BatchWriteOptions { schema, expires_ms }`
value. The host applies it to every put inside the native atomic operation;
deletes are unchanged. The checked form authorizes every dependency and write
collection before forwarding and still checks dependencies against pre-write
state. Both requests are bounded by `MAX_BATCH_OPS` and `MAX_INLINE_BYTES`.

The DTO deliberately has no `created_ms`: that remains a trusted host-side
migration option. Unknown fields, including `created_ms` or a forged scope,
fail decoding before native I/O. `atomic_write_options` appears in the
capability reply only when the backend implements the new atomic methods.
Existing `AtomicBatch` and `AtomicBatchChecked` payloads and default-options
semantics remain byte-for-byte unchanged. A client calling either new selector
against an older server or an unsupported driver gets a refusal; it never
falls back to ordinary puts or patches metadata after commit.

## Real TinyBus transport

`tinystoragedrivers-busproxy` is the `TinyBus` half that `-client` leaves out:

- **Module side.** `connect_storage(connection)` returns the module's
  `BusStorage` over a `BusTransport`: ordinary calls are proxy calls to
  `/ai/tinyhumans/tinystorage/<Object>` (`Secrets` on the confidential route),
  `PutStream` and `PutStreamUntil` open a bus stream on the host and write the
  body in `BLOB_STREAM_CHUNK_BYTES` chunks while the call is outstanding;
  `GetStream` and `GetStreamWithExpiry` accept the stream the host opens on the
  caller, and watch invalidations
  arrive as `WatchChanged` signals.
- **Host side.** `serve(connection, endpoint)` registers the five objects,
  claims `ai.tinyhumans.tinystorage.Storage` and hands every call, with the
  broker-authenticated sender, to a `StorageEndpoint`. The endpoint owns the
  decisions (who the caller is, scope, grant, fence, audit);
  `PerCallerEndpoint` is the ready-made one that binds each caller to its own
  `StorageServer`. Failures cross as `MethodFailed` errors named
  `ai.tinyhumans.tinystorage.Error.Denied` (a content-free label) or
  `...Error.Storage` (a snake_case `StorageKind`).
- **Signals.** Watch ids are minted per caller binding, so a host emits each
  caller's signals on `/ai/tinyhumans/tinystorage/Watch/<caller>` (derived from
  the caller's unique name) and a module accepts them only from the host's
  unique name.

Peer binding. Streams are taken with `Connection::accept_stream_from`: the host
reads an upload only from the authenticated caller that opened it, and a client
reads a download only from the host's unique name. Watch signals are unicast
with `Connection::emit_to` to the subscribing caller, so another peer matching
the same path or interface never receives them.

## Acceptance

- With default features disabled, the wire types and storage-needs declarations
  use serde-only data dependencies, with no core, transport, async-trait or
  runtime. The default-on legacy client adds TinyBus only for consumers that
  retain `DocumentClient`; the independent consumer proof checks the
  no-default normal/build dependency graph.
- Every selector and message shape is pinned by wire roundtrip and unknown
  selector/field rejection tests; no message accepts scope selectors.
- Native and module callers run the same Documents/Streams/Blobs/Secrets/Queues
  conformance cases against one backend and observe the same committed state.
- Authentication, grant, schema, confidentiality, fencing and size checks run
  before native I/O. Watch signals contain no payload.
- Calls larger than 16 MiB use TinyBus Stream with caller-bound limits.
- Unsupported capabilities remain unsupported; no adapter fabricates success.

## Open questions

None for the accepted boundary. Selector versioning and any future streaming
extension require a new interface version rather than changing existing wire
shapes.
