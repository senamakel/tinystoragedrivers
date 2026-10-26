# Storage primitives v2

Status: proposed contract for the v2.0.0-alpha.1 implementation.

This spec splits the current storage surface into static, consumer-owned
primitives. It does not prescribe a model registry, migration framework,
repository helper, distributed lease policy, or host configuration format.
The local file lease helper is documented separately below. The exhaustive
source-to-destination audit is in [the public item inventory](primitives-v2-inventory/README.md).

## Crate map

| Crate | Owns | Engine features |
| --- | --- | --- |
| `tinystoragedrivers-types` | Serializable values and errors: `Scope`, validated opaque `Key`, filters, query/page/cursor, versions/preconditions, collection specs, metadata, capability vocabulary, and primitive request/result values | None; normal dependencies are only serde, serde_json, and thiserror |
| `tinystoragedrivers-api` | Async object-safe ports and the `StorageBackend` / `Storage` bundle | None; depends on `-types`, `async-trait`, `bytes`, `futures-core`, `serde`, `serde_json`, and `zeroize` |
| `tinystoragedrivers-document` | `DocumentStore` and `StreamStore` engines | `memory` (default), `sqlite`, `mongodb`, `file` |
| `tinystoragedrivers-kv` | JSON value `KvStore` | `memory` (default), `sqlite`, `mongodb`, `file` |
| `tinystoragedrivers-blob` | Opaque `BlobStore`, streaming and tree operations | `memory` (default), `fs`, `sqlite`, `mongodb`, `s3` |
| `tinystoragedrivers-queue` | At-least-once `QueueStore`; document-backed queue engine and broker engine | `memory` (default), `document`, `rabbitmq` |
| `tinystoragedrivers-secrets` | Secret engine adapters implementing the API crate's `SecretStore` | `memory` (default), `file`, `document`, `keyring`, `kms-aws`, `kms-gcp` |
| `tinystoragedrivers-fence` | Process-local PID lockfile leases with stale-owner recovery; no distributed fencing | None |
| `tinystoragedrivers-connect` | Private shared engine connection handles. One configured storage URL yields one `storage.db` connection shared by document, stream, blob and KV adapters | `publish = false`; `sqlite`, `mongodb`, `s3`, and `amqp` connection features |
| `tinystoragedrivers` | Composition-root facade: parse URLs and assemble explicitly selected engines | Forwards `sqlite`, `mongodb`, `file`, `s3`, `rabbitmq`, `secrets`, `keyring`, `kms-aws`, and `kms-gcp` |
| `tinystoragedrivers-testkit` | Per-port conformance harness, in-memory stores, and fault-injecting decorators | Development dependency only; memory engines enabled |
| `tinystoragedrivers-bus` | Versioned, serde wire types and member names for module storage calls | No engine or implementation dependency; contracts re-export `-types` values |
| `tinystoragedrivers-busproxy` | Module-side connection and host-side scoped serving adapters | `tinybus` transport selected by host; built on `-api` and `-bus` |

The `-types` allowlist is exactly `serde`, `serde_json`, and `thiserror`.
`Key` is a validated opaque value with `new`, `as_str`, `split`, and
`is_hashed`. Compound-key construction (`Key::of`), its SHA-256/base32 hashed
form, and ULID generation are consumer-owned helpers that preserve the v1
stored bytes; the vocabulary crate does not gain SHA, encoding, or ID-generator
dependencies. The `-api` dependency allowlist is `tinystoragedrivers-types`, `async-trait`,
`bytes`, `futures-core`, `serde`, `serde_json`, and `zeroize`. Serde and JSON
support the existing typed `DocumentStoreExt` methods. `zeroize` is required because the
existing `SecretStore::get` contract returns `Zeroizing<Vec<u8>>`; plaintext
must retain its wipe-on-drop guarantee. It belongs in `-api`, not `-types`,
which remains limited to serde, serde_json, and thiserror. The ordinary
dependency rule for every consumer is `tinystoragedrivers-types`
and `tinystoragedrivers-api`. A loadable module may additionally depend on
`tinystoragedrivers-busproxy`; it must not depend on an engine crate, the
facade, or `-bus` directly. Consumers put the memory-enabled primitive crates
and `-testkit` in dev-dependencies. Only a composition root such as OpenHuman
selects production engine features.

`-document` owns both documents and streams because their SQLite/file/Mongo
adapters share the same connection and lifecycle. `-connect` is the single
owner of that connection setup; no primitive opens a second database for the
same configured URL. Queue broker URLs are separate from the storage URL.

## API contract

All ports are `Send + Sync + Debug`, object safe, and async. Fallible operations
return `tinystoragedrivers_types::Result<T>`. `StorageBackend` reports backend
capabilities; document and blob handles report their own capabilities, while
queues report `QueueFeatures`. A handle returned by
`StorageBackend::for_scope` is already bound to a scope; individual port calls
never accept a scope argument. Engines report optional behavior through
`Capabilities`; a missing capability returns `ErrorKind::Unsupported`.
`Capability` also has primitive-availability markers `Documents`, `Streams`,
`Kv`, `Blobs`, `Queues`, and `Secrets`. An unconfigured handle in the stable
`Storage` bundle returns `Unsupported` with its matching marker for every
operation; optional operations on a configured handle continue to report
their operation-specific capability.

### Bundle and scope

`StorageBackend` exposes `capabilities() -> Capabilities` and
`for_scope(&Scope) -> Result<Storage>`. `Storage` is a cloneable
bundle with `Arc<dyn DocumentStore>`, `Arc<dyn StreamStore>`,
`Arc<dyn KvStore>`, `Arc<dyn BlobStore>`, `Arc<dyn QueueStore>`, and
`Arc<dyn SecretStore>` handles. Its shape is stable regardless of enabled
engines: an unconfigured port returns `ErrorKind::Unsupported` from operations.
It exposes accessors and
`async ensure_collection(&CollectionSpec) -> Result<()>`, which delegates to
its scope-bound document port for trusted host bootstrap. It does not own model
or lifecycle policy. The bundle
always has all primitive handles, including in the testkit memory bundle. A
port without a configured engine returns a typed `Unsupported` error; Cargo
features never change the bundle's Rust type. The backend has no composite
routing, purge, lease, generation-admission, or write-fencing policy. It may
reject unsupported scopes while opening the bundle.

`Scope` is opaque to API consumers and exposes exactly:
`Scope::new(root: &str) -> Result<Scope>`, `as_str(&self) -> &str`, and
`child(&self, segment: &str) -> Result<Scope>`. `new` rejects an empty value,
values longer than 256 UTF-8 bytes, and whitespace/control characters; every
accepted legacy scope string is preserved byte-for-byte. `child` rejects an
empty segment and appends one generic segment, percent-encoding every UTF-8
byte outside RFC 3986 unreserved characters (including `/` and `%`) with
uppercase hex; it rejects a final encoded scope longer than 256 bytes. It does
not normalize or hash child identifiers. The host owns `local`/`tenant`/
`cluster`/`agent`/`module` constructors and the prior v1 hashing grammar, and
must preserve those stored names when constructing scopes. Serialized scope
remains the isolation key used by every engine and bus proxy.

### Documents

`DocumentStore` preserves the v1 port surface while moving its shared argument
and result types to `-types`. The required methods are:

```rust,ignore
fn capabilities(&self) -> Capabilities;
async fn watch(&self, collection: &str, filter: &Filter,
               options: &WatchOptions) -> Result<Box<dyn DocumentWatch>>;
async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()>;
async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()>;
async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>>;
async fn put(&self, collection: &str, id: &str, value: Value,
             precondition: Precondition) -> Result<Version>;
async fn put_with(&self, collection: &str, id: &str, value: Value,
                  precondition: Precondition, options: WriteOptions) -> Result<Version>;
async fn delete(&self, collection: &str, id: &str,
                precondition: Precondition) -> Result<bool>;
async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>>;
async fn count(&self, collection: &str, filter: &Filter) -> Result<u64>;
async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64>;
async fn claim(&self, collection: &str, filter: &Filter, sort: &[Sort],
               patch: &Value) -> Result<Option<Versioned<Value>>>;
async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>>;
async fn atomic_batch_checked(&self, checks: Vec<ReadCheck>, ops: Vec<WriteOp>)
    -> Result<Vec<WriteResult>>;
async fn search(&self, collection: &str, text: &str, limit: usize)
    -> Result<Vec<SearchHit>>;
async fn drop_collection(&self, collection: &str) -> Result<()>;
async fn collections(&self, prefix: &str) -> Result<Vec<String>>;
```

`DocumentStoreExt` supplies `get_as<T>(collection, id) ->
Result<Option<Versioned<T>>>`, `put_as<T>(collection, id, &T, Precondition) ->
Result<Version>`, `query_as<T>(collection, &Query) ->
Result<Page<Versioned<T>>>`, and `query_all(collection, &Query) ->
Result<Vec<Versioned<Value>>>` using serde, without changing persistence.
`watch` defaults to `Unsupported(Watch)`,
`validate_collection_spec` to `Unsupported(CollectionValidation)`, and
`put_with` to `Unsupported(RecordMetadata)` when non-default metadata is
requested. `atomic_batch` defaults to `Unsupported(Transactions)`;
`atomic_batch_checked` to `Unsupported(ValidatedBatch)`; `search` to
`Unsupported(FullText)`; and `collections` to
`Unsupported(CollectionEnumeration)`. Required semantics: JSON object bodies,
monotonically increasing versions including across delete/recreate; `None`
upsert, `Absent` insert-only and `Version(v)` compare-and-swap preconditions;
deterministic total ordering and opaque keyset cursors; atomic claim with RFC
7396 merge patch. Collection declarations describe storage constraints, never
an owning application's schema version or migration function.

### Streams

`StreamStore` provides `append(stream: &str, value: Value) -> Result<u64>`,
`append_batch(stream: &str, values: Vec<Value>) -> Result<u64>`,
`append_batch_if_offset(stream: &str, expected_next_offset: u64, values: Vec<Value>) -> Result<u64>`,
`read_window(stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>>`,
`len(stream: &str) -> Result<u64>`,
`truncate_before(stream: &str, offset: u64) -> Result<u64>`,
`delete_stream(stream: &str) -> Result<bool>`, and
`streams(prefix: &str) -> Result<Vec<String>>`.
Offsets start at zero and count every append; truncation does not reduce or
reuse offsets, while deleting the stream resets its length. Conditional append
is optional under `ConditionalStreamAppend` and is atomic, including for an
empty batch; its default returns `Unsupported(ConditionalStreamAppend)`.
Optional `seal(stream: &str) -> Result<SealOutcome>`,
`delete_sealed_stream(stream: &str, sealed_ms: i64) -> Result<bool>`, and
`info(stream: &str) -> Result<Option<StreamInfo>>` share `StreamSeal` and
default to `Unsupported(StreamSeal)`. `info` records retained base, next
offset, and seal/growth times. `seal` is idempotent, and sealing leaves
`read_window` results unchanged.

### Key/value

`KvStore` is new in `-api`. Its required contract is:

```rust,ignore
fn capabilities(&self) -> Capabilities;
async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>>;
async fn put(&self, key: &str, value: Value) -> Result<Version>;
async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version>;
async fn delete(&self, key: &str, expected: Precondition) -> Result<bool>;
async fn list(&self, prefix: &str, cursor: Option<Cursor>, limit: usize)
    -> Result<Page<Versioned<Value>>>;
async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration)
    -> Result<Version>; // optional: Capability::Ttl
```

`put` is an unconditional replace/upsert. `cas` accepts `Absent` or
`Version(v)` and returns `Conflict` when its precondition fails. `delete` uses
the same precondition vocabulary. Values may be any JSON value, not only JSON
objects. Listing uses a stable key order and opaque keyset cursor; TTL entries
past expiry read as absent; TTL is optional and gated by `Capability::Ttl`.
Keys are opaque strings validated by `-types`.
The port does not encode owner/namespace policy: a consumer that needs those
coordinates owns its key construction.

### Blobs

`BlobMeta { key: String, len: u64, content_type: Option<String> }` and
`Blob { meta: BlobMeta, bytes: Vec<u8> }` are `-types` values. `BlobReader` and
`BlobStream { meta: BlobMeta, reader: BlobReader }` are `-api` streaming types.
`BlobStore` preserves the v1 operation semantics and uses these v2 signatures:
`capabilities() -> Capabilities`, `max_key_len() -> usize`,
`put(key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta>`,
`put_stream(key: &str, reader: BlobReader, len: u64, content_type: Option<&str>) -> Result<BlobMeta>`,
`get(key: &str) -> Result<Option<Blob>>`,
`get_stream(key: &str) -> Result<Option<BlobStream>>`,
`get_range(key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>>`,
`head(key: &str) -> Result<Option<BlobMeta>>`,
`delete(key: &str) -> Result<bool>`,
`list(prefix: &str) -> Result<Vec<BlobMeta>>`, and
`sweep_expired() -> Result<u64>`.
Tree helpers
`list_dir`, `put_tree`, and `delete_tree` are default operations over that
surface. `BlobMeta` carries key, byte length, and optional content type.
Streaming must enforce the declared length and must not publish a partial
replacement. Optional `put_expiring(key, bytes, content_type, ttl)` and
`put_stream_expiring(key, reader, len, content_type, ttl)` are gated by
`BlobTtl`; `presign_get(key, ttl)` and `presign_put(key, ttl)` by
`PresignedUrls`. `sweep_expired` defaults to zero.

`BlobReader = Pin<Box<dyn futures_core::Stream<Item = Result<bytes::Bytes>> + Send>>`.
`put_stream` and
`put_stream_expiring` receive a declared `u64` length; the engine rejects a
short or long body and publishes no partial replacement. The optional feature
methods return `Unsupported` when their capability is absent. This
intentionally replaces v1's Tokio `AsyncRead` blob reader so `-api` stays
runtime-free; engine adapters convert their native streaming interfaces. The
default `put_stream` implementation consumes with `std::future::poll_fn`,
checks the declared length before publishing, and buffers for engines that do
not override it; `get_stream` wraps the buffered `get` result by default.

### Queues

`QueueStore` in `-api` keeps the current queue contract and exposes
`features() -> QueueFeatures`, `ensure_queue(queue: &str, spec: &QueueSpec) -> Result<()>`,
`enqueue(queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>>`,
`receive(queue: &str, max: u32, visibility_ms: u64, consumer: &str) -> Result<Vec<Delivery>>`,
`ack(queue: &str, receipt: &Receipt) -> Result<bool>`,
`nack(queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool>`,
`extend(queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool>`,
`stats(queue: &str) -> Result<QueueStats>`,
`dead_letters(queue: &str, limit: usize) -> Result<Vec<DeadLetter>>`,
`redrive(queue: &str, id: &str) -> Result<bool>`, and
`purge(queue: &str) -> Result<u64>`.
Queue records and result types are `NewMessage`, `Enqueued`, `Delivery`,
`Receipt`, `Retry`, `DeadLetter`, `QueueSpec`, `QueueFeatures`, and `QueueStats`
from the current port. `QueueSpec` carries `max_attempts` and
`dedupe_window_ms`; `Retry` is `After` or `DeadLetter`; `QueueFeatures` reports
dedupe, groups, priorities and delay. Receive leases messages at-least-once;
ack/nack/extend require the current receipt. The JSON queue manifest and
schema are host configuration and move to OpenHuman. The memory and document queue
engines share the same conformance tests; RabbitMQ implements the same
observable contract within its documented broker guarantees.

### Secrets

`SecretStore` moves to `-api` and preserves the existing exact method surface:
`get(name) -> Result<Option<Zeroizing<Vec<u8>>>>`,
`set(name, &[u8]) -> Result<()>`, `delete(name) -> Result<bool>`,
`list(prefix) -> Result<Vec<String>>`, `enumerable() -> bool`, and
`backend_name() -> &'static str`. It is `Send + Sync + Debug`; `Zeroizing` is
from the existing zeroize dependency and plaintext is wiped when dropped.
Names and prefixes retain the current printable, bounded validation rules.
Engine-specific encryption envelopes, key files,
migration journals, bounded transactions, and host credential naming are not
part of this port; the owning consumer implements them over the port or its
chosen OS/KMS facility.

## Feature and dependency rules

| Primitive | Default | Optional engine features | Shared storage connection |
| --- | --- | --- | --- |
| document + stream | `memory` | `sqlite`, `mongodb`, `file` | Yes, via `-connect` |
| KV | `memory` | `sqlite`, `mongodb`, `file` | Yes |
| blob | `memory` | `fs`, `sqlite`, `mongodb`, `s3` | Yes where the engine uses storage.db |
| queue | `memory` | `document`, `rabbitmq` | Document engine reuses document handle; RabbitMQ uses separate URL |
| secrets | `memory` | `file`, `document`, `keyring`, `kms-aws`, `kms-gcp` | Document engine reuses document handle |

Feature names are additive and primitive-local. `default = ["memory"]` means
tests and embedders have an explicit in-process engine; it does not authorize a
consumer to use memory as a production fallback. The facade's default feature
set is empty. URL parsing returns an actionable unsupported-engine error when
the URL names a disabled feature. Optional engine dependencies are absent from
`-types`, `-api`, all consumer normal/build dependency graphs, `-bus`, and the
default facade graph.

The `document` crate may additionally enable `polling-watch`, which supplies
`PollingWatch` as a conservative implementation adapter and depends on
`tokio/time`. It is not part of `-api`; native `Watch` remains an engine
capability. API and type crates remain free of a Tokio runtime dependency.

## Wire contract and versioning

`tinystoragedrivers-api` exports `CONTRACT_VERSION: u32` (initially `1`) and
`is_compatible(version: u32) -> bool` for Rust API participants; compatibility
is exact equality with the current version. A test pins the value.
`tinystoragedrivers-bus::CONTRACT_VERSION` is a distinct `u32` wire
constant shared by host and module. Every bus request carries that version and
is rejected before decoding when it differs. The bus crate declares only interface/member names
and serde types; it has no engine, filesystem, database, or network code.
`-busproxy` performs scope admission and adapts calls to `-api`; a module never
receives a raw engine handle.

The API's `CONTRACT_VERSION` is bumped for incompatible port-level behavior or
type changes. The bus wire contract is currently version `6`; #32 must increment
it to `7` for its incompatible wire changes. API
versioning is separate from the bus contract version: an internal engine
or Rust-only API change does not itself alter the bus wire. `is_compatible`
accepts exactly versions the current API can safely interoperate with.

## Testkit and bus proxy entry points

`tinystoragedrivers-testkit` exports `memory_storage() -> Storage` and
`memory_backend() -> impl StorageBackend`. It exposes `run_documents`,
`run_streams`, `run_blobs`, `run_kv`, `run_queue`, `run_secrets`, `run_watch`,
`run_isolation`, and `run_semantics` conformance entry points. Its generic
decorators are `Failing<T>::new(inner).on_nth_call(n, ErrorKind)` and
`.on_operation(operation, ErrorKind)`, `NoNativeTtl<T>::new(inner)`,
`Slow<T>::new(inner, delay: Duration)`, and
`ConflictOnce<T>::new(inner, operation)`. Each wrapper forwards every API method
except the injected behavior; capability reporting is adjusted by
`NoNativeTtl`. A `clock` helper returns a deterministic, manually advanced
test clock. The testkit contains no production policy.

`tinystoragedrivers-busproxy` exports
`connect_storage(Connection) -> Result<Storage>`, which returns the
host-admitted scoped bundle, and
`serve(&Connection, Storage) -> Result<ServedStorage>`, which serves every
primitive from a bundle. Both sides check the same `CONTRACT_VERSION` before
decoding or invoking a primitive. `-busproxy` depends on `-types`, `-api`,
`-bus`, and TinyBus only; it has no engine or secret implementation
dependency.

Semver applies to each crate independently. Adding a defaulted optional field,
a new method with a behaviorally safe default, a capability, or a feature is
minor. Removing/renaming an item, changing serialized field spelling or
meaning, strengthening a guarantee, or changing error classification is
major. Bus wire changes that cannot interoperate increment
`CONTRACT_VERSION`; additive optional fields and new members preserve it only
when old hosts and modules safely ignore them. Rust API semver does not replace
the wire-version check. Pre-1.0 alpha tags may change incompatibly, but still
increment the wire version whenever old/new module pairs cannot interoperate.

## Historical contracts

Earlier v1 schema and migration contracts were removed with their host-specific
implementations. The v2 contract above is normative. Historical on-disk
fixtures remain only where a current engine test proves compatibility.
