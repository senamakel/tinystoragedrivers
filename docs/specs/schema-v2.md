# Uniform schema (v2)

Status: Implemented (v1.0.0). Owner: TinyStorageDrivers. Host integration: OpenHuman.
Implementation order: [`../plans/schema-v2.md`](../plans/schema-v2.md).

## Problem

The ports in [`storage-ports.md`](storage-ports.md) fix how records are stored.
They say nothing about what the records look like, and every crate that
adopted them invented its own conventions. A survey of OpenHuman and the
modules it hosts (2026-10, tinystoragedrivers v0.11.0) found the following.

**One tenant addressed by five scope grammars.**
- Domain stores use `profile:<id>`, `profile-sha256:<hex>` or `local`.
- Sessions use `<profile>~<agent>`, or `default` when there is no agent.
- Conversations use `profile:<id>` and drop the agent.
- Modules use `module:<id>@<base>`, `<profile>~module:<id>` or `module-sha256:<hex>`.
- Leases use `cluster`.
- Legacy importers write to `local`, where a profile-scoped reader never looks.
- A hashed session scope (`sha256:<hex>`) falls outside the profile fence, which guards the prefix `<profile>~`.

**Four hash fallbacks and six key schemes.** Fallbacks are `sha256:`, `profile-sha256:`, `module-sha256:` and `h:<hex>`. Keys are built as:
- `<len>:<ns>/<key>`
- `<len>:<prefix><name>`
- `<len>:p/…`
- sha256 of length-prefixed parts
- in-body `scope_key`/`workspace` fields
- collection-name prefixes

Stream names use `/`, `:` and `.` as separators interchangeably.

**Five time encodings, often for the same instant.** Seconds, ms, µs, ns and RFC 3339 strings all appear. Examples of one instant stored twice or three times: `next_run_ms` + `next_run_ns`, `ingested_ms` + `ingested_ns` + `ingested_at`. Several indexes are built on RFC 3339 strings.

**Mixed casing and wrappers.**
- snake_case and camelCase bodies coexist, sometimes in one record (`created_at` next to `updatedAt`).
- Values are wrapped as `{"value": …}`, `{"json": "<string>"}` or `record: {…}`.
- Journal rows repeat `run_id`, `root_run_id` and `event_id` in every entry.

**Collection names follow no rule.** Some have a domain prefix and some do not (`approvals`, `artifacts`, `leases`, `config`). Siblings disagree: `channels_*` and `channel_remote_*`, `wf_*` and `flows_*`. One name is kebab case (`composio-user-scopes`) and one family carries a version suffix (`conversation_*_v1`).

**Primitives implemented again and again.**
- Leases and claims (7 shapes): `leases`, `cron_occurrence_claims`, an embedded `occurrence_lease`, `<prefix>_leases`, `wf_locks`, `#claim` documents, `reserved_at`.
- Import markers (10 collections), and the marker `version` is never read.
- Key-value stores (5).
- Counters (3).
- Graph checkpointers (2).
- CAS retry loops (budgets of 16, 32 and 64).

**Growth nobody bounds.** Finished run journals are kept forever as one row per event; a desktop profile reached 3.6M rows and 1.4 GB. Deleted rows never return space, because SQLite never vacuums.

The consequence: data cannot be moved between backends or users without per-domain code, a tenant cannot be listed or purged by prefix, and every new store re-decides the same questions.

## Goals

- One addressing scheme: scope, collection, id, stream and blob key.
- One body convention: bare snake_case JSON, epoch-ms times.
- Record metadata (created, updated, expiry, schema version) kept by tsd, not by each body.
- A schema registry: every owner declares its collections, and tsd validates, prepares and migrates them.
- Shared primitives for leases, migrations, key-value, counters and CAS updates, so owners stop reimplementing them.
- Sealing for finished streams, and space reclamation on SQLite.
- A user's SQLite file is a complete, portable unit: it loads into a shared MongoDB with no rewrite.
- A deterministic, resumable migration from every v1 shape listed in the [catalogue](#catalogue).

## Non-goals

- Typed repositories. They stay with the owning crates; this spec fixes their naming and storage shape only.
- Cross-backend replication or live dual-writes.
- The host's on-disk layout and the legacy-file importers (OpenHuman owns those). This spec only requires that a host can place one database file per user.
- Changing RPC or UI wire formats. Only the stored form changes.

## Proposed behavior

### 1. Scopes

```text
scope     = tenant [ "/" principal ]  |  "cluster"
tenant    = "local"  |  "u:" segment
principal = ( "agent:" | "module:" ) segment
segment   = 1*( ALPHA / DIGIT / "-" / "_" / "." / "@" )   ; at most 96 bytes
          | "~" 26( base32-lower )                         ; hashed form
```

- **`tenant` is the isolation and fence boundary.** `local` is the desktop operator; `u:<id>` is a user or profile. Every scope that begins `<tenant>` or `<tenant>/` belongs to that tenant.
- **`principal` narrows a record to one agent or one module inside the tenant.** No principal means the record is shared across the tenant.
- **`cluster` is the only scope with no tenant.** It holds the profile registry and process-placement leases on shared backends.
- **Hashing.** A segment that is too long or holds characters outside the set is replaced by `~` plus the first 26 characters of the lowercase base32 SHA-256 of the raw segment. Only the segment is hashed, never the whole scope, so a hashed agent still sits under its tenant and its fence.
- **Providers take a tenant.** A store that scopes per agent (for example tinyagents' `DriverSessionStores`) is constructed with the tenant `Scope` and derives `tenant.agent(id)` itself; it never uses a raw agent id as a scope.
- **API.** `Scope::tenant(id)`, `Scope::local()`, `Scope::cluster()`, `scope.agent(id)`, `scope.module(id)`, `scope.tenant_scope()` and `Scope::parse(text) -> Result<Parsed>`. `Scope::new` continues to accept any valid v1 string, so v1 data stays readable during migration. It is deprecated for new writes.
- **Tenant purge.** A tenant purge covers the tenant scope and every principal under it. `StorageBackend::scopes(prefix)` (capability `ScopeEnumeration`) lists them, and the host drives the purge (see [`scope-purge.md`](scope-purge.md)).

### 2. Names

| Thing | Form | Example |
|---|---|---|
| Collection | `<owner>.<entity>` | `harness.run_status`, `cron.jobs` |
| Stream | `<owner>.<stream>` or `<owner>.<stream>/<key>` | `mcp.audit`, `harness.journal/run.0006…` |
| Blob key | `<owner>/<path>` or `cas/<sha256-hex>` | `juice/ccr/<token>` |
| Document id | `Key::of([parts])` or a ULID | `job.7f…/2026-10-16T10:00Z` |

- **`owner`** is the crate or domain that defines the record type: lowercase, `[a-z][a-z0-9_]{1,23}`.
- **`entity`** is a lowercase snake_case plural noun.
- **No version suffixes.** The version lives in the [registry](#4-schema-registry).
- **The owner `tsd` is reserved** for the shared primitives (§5). The prefix `_tsd` stays reserved for driver internals.
- **`Key::of(parts)`** joins parts with `/`, escaping `%`, `/` and NUL as `%25`, `%2F` and `%00`, and a leading `~` of the first part as `%7E` so only a hashed key starts with `~`. If the result exceeds 512 bytes it becomes `~` plus the full 52-character lowercase base32 SHA-256 of the escaped key (an id is a primary key, so the digest is not truncated). `Key::split` reverses an unhashed key and returns `None` for a hashed one.
- **The length-prefixed and `h:` schemes are retired.**
- **New entity ids are ULIDs** (`Key::ulid()`; `Key::ulid_at(ms)` for a migration that keeps a record's creation time): 26 characters, sortable by creation time. That makes `_ns` tie-break fields unnecessary.

### 3. Bodies and record metadata

**Bodies**
- A document body is a JSON object holding the record's own fields at top level. No `value`, `json` or `record` wrapper, and no JSON encoded inside a string. Bytes are stored as a blob and referenced by key.
- Field names are snake_case. A type whose RPC form is camelCase uses a separate storage DTO, or `#[serde(rename_all = "snake_case")]` when the type is storage-only.
- A time is an `i64` count of Unix epoch milliseconds, in a field named `<what>_ms`, for example `next_run_ms`. One field per instant. Durations are `<what>_ms` too. Storage never holds seconds, µs, ns or formatted dates.
- Stream entries do not repeat their stream key. A journal entry is `{kind, ts_ms, …}`: its run id is the stream name and its offset is the sequence number.

**Metadata.** tsd keeps four fields per document, outside the body:

| Field | Meaning | Set by |
|---|---|---|
| `schema` | body version the writer declared (§4) | the handle, from the registry |
| `created_ms` | first write | the driver |
| `updated_ms` | last write | the driver |
| `expires_ms` | optional expiry; past it, the record reads as absent | the writer |

- **Reading.** `Document` gains `meta: Meta { schema, created_ms, updated_ms, expires_ms }`. Filters and sorts address the fields as `_schema`, `_created_ms`, `_updated_ms` and `_expires_ms`, alongside `_id`.
- **Precedence over bodies.** A body keeps its own time field only when it means something other than creation, update or expiry (`next_run_ms`, `decided_ms`).
- **Expiry.** `expires_ms` replaces `CollectionSpec::ttl(field)`. A v1 TTL field is honoured until its collection migrates, and the migration step moves its value into `expires_ms`.
- **Who sets the times.** `created_ms` and `updated_ms` come from the driver clock, never from the caller. A migration step can supply `created_ms` when it rewrites a v1 record, so history keeps its original times. **Expiry survives a migration.** A document step's output carries the source record's `expires_ms` when it is the same record (the output's collection is the step's item and its id is the source's), so an in-place rewrite never turns a time-limited record into a permanent one. A step overrides it with `TargetRecord::expires_ms(Some(ms))` (for example moving a legacy TTL body field into metadata) or `expires_ms(None)` (no expiry); a record under a new id carries nothing implicitly, and a step that renames records passes `record.meta.expires_ms` itself.

### 4. Schema registry

```rust
pub struct Schema { owner: Owner, items: Vec<Item>, steps: Vec<Step> }
pub enum Item {
    Collection { name, version: u32, indexes, search, scope: ScopeClass },
    Stream     { name, scope: ScopeClass, seal: SealPolicy },
    Blobs      { prefix, scope: ScopeClass },
    Secrets    { prefix, scope: ScopeClass },
}
pub enum ScopeClass { Tenant, Agent, Module, Cluster }
pub struct Step { id: &'static str, from: u32, to: u32, run: StepFn }
```

**What every owner provides.** Each owning crate exports `pub fn storage_schema() -> Schema`.

**Composition.** The host builds `Registry::compose([schemas])`. Composition refuses:
- a name outside its owner's prefix
- a name claimed twice
- a version that goes down
- a step gap

Composition then runs `ensure_collection` once for each item, from trusted bootstrap. This is the only place declarations are made.

**Migration steps**
- A `Step` rewrites one collection, stream family or blob prefix from version `from` to version `to`. There are three kinds: `Step::new` (documents), `Step::stream` and `Step::blobs`. Compose refuses a step whose kind does not match its item.
- **Document steps** receive a batch of v1 records (body, meta, scope, id) and return the replacement records with their new ids and names. They may also emit records for other items of the same owner. The runner pages through the source with a keyset cursor and writes each output batch with `Precondition::Absent`. It stores progress `{step, cursor, written}` in `tsd.migrations` after every batch, and deletes the source only when the step completes. A crash resumes from the cursor. The output writes and the progress record are not one atomic commit, so a resumed run re-produces the batch the crash interrupted. An output that already exists counts as written only when its stored body equals the step's output, its `meta.schema` equals the schema the runner stamps on that output (the step's `to` for the item, the target collection's version otherwise), its `meta.expires_ms` equals the expiry the runner resolved, and, when the step supplies `created_ms`, its `meta.created_ms` equals it; a different body, schema, expiry or creation time under that id would leave the record wrongly versioned while the step reads as complete, so it stops the step like a different document under that id, or a unique-index collision with another document, stops the step with the driver's error. An in-place step (source and target are the same collection) that returns a record under a new id has the replaced source record deleted once the batch's outputs are written; a source record the step does not return is removed the same way.
- **Stream steps** name their legacy streams with `.source_prefix(prefix)` (required; it must not overlap the item's own name). For each stream under the prefix, in name order, the runner reads every retained entry in order (paged with `read_window`) and calls `fn(source_name, entries) -> Option<(target_name, entries)>`; `None` drops the stream. The target must be the item or under `item/`. The runner appends the result in batches, each conditional on the target length it observed (`append_batch_if_offset`), so a second migrator on the same stream fails with `Conflict` instead of duplicating entries. It seals the target if the stream backend (`StreamStore::info`) reports the source sealed, and deletes the source. Progress for each source is a `tsd.migrations` document keyed `[owner, item, step, source]` holding the target name and entries appended; it is saved before the first append and after each batch, and deleted with the source. On resume the target's length decides where to continue, so no entry is duplicated; a target that has entries but no recorded progress, is shorter than recorded, or longer than the mapped result is refused with `Conflict`.
- **Offsets.** A migrated stream starts at offset 0. A driver cannot set a stream's base, so a source that had been truncated keeps only its retained entries, renumbered from 0. Owners that store offsets as external references must account for the source's `base`. A sealed source whose mapped result is empty is dropped rather than sealed.
- **Blob steps** name their legacy prefix the same way and call `fn(key, content_type, bytes) -> Option<(target_key, content_type, bytes)>` per blob. The runner puts the target, then deletes the source. An existing target counts as the interrupted write of an earlier run only when its bytes and content type equal the step output; anything else (an unrelated blob, or two sources mapping to one key) fails with `Conflict`, never overwrites, and keeps the source. The runner stores the running count after every blob. A resume lists what is left.
- Stream and blob items carry no declared version, so their step chains only have to start at version 1 and not skip.
- Steps are pure functions of their input, so a re-run produces the same output.
- A collection whose recorded version is below the declared one is not opened for writes until its steps finish (`ErrorKind::Unavailable`, message `migrating <owner>.<item>`). Reads of an unmigrated collection go to the v1 source.
- **Readiness is per scope.** Progress lives in each scope's `tsd.migrations`, so `Registry::migrate` records which items are still pending for the scope it ran over, and `Registry::check_ready_in(scope, item)` answers for that scope; a scope `migrate` has not run over is not ready, and migrating one scope never opens another. `Registry::check_ready(item)` is the unscoped form for a host with one scope: it is open once `migrate` has run over at least one scope and none it has seen still has the item pending. It cannot vouch for scopes it was never asked to migrate.
- Legacy-file importers are steps too: steps with no source item, whose completion is recorded in `tsd.migrations`. This replaces every `*_import` marker collection.

**Module grants are the module's schema.** A host grants a loadable module exactly the items in its `Schema`, scoped to `ScopeClass::Module`, so grant tables are generated rather than hand-written.

### 5. Shared primitives (owner `tsd`)

| Collection | API | Body |
|---|---|---|
| `tsd.leases` | `Leases::acquire(owner, key, holder, ttl_ms) / renew(owner, key, holder, epoch, ttl_ms) / release(owner, key, holder, epoch) / current`; `Lease::fence(scope)`; id `Key::of([owner, key])` | `{holder, epoch, expires_ms}` plus optional `endpoint` and `released` (absent unless used); `epoch` bumps on every acquisition after a lapse or release (even by the same holder), stays on renew and on a live re-acquire, and doubles as the write fence ([`write-fencing.md`](write-fencing.md)); `renew` and `release` take the epoch of the caller's `Lease` and refuse (`None` / `false`) when the live record is at another epoch, so a stalled actor of an earlier tenure of the same holder string cannot extend or end the new one |
| `tsd.migrations` | used by the registry runner; `Migrations::done(owner, step)` for importers | `{owner, step, from, to, cursor, written, done}` |
| `tsd.kv` | `Kv::ns(docs, owner, ns).get/put/delete/list(prefix)` | `{owner, ns, key, value}` (a body must be an object, so the value sits in a field and may be any JSON shape: scalar, string, array or object; a migration that "unwraps `value`" moves the legacy value into this field); id `Key::of([owner, ns, key])`; index over `(owner, ns, key)` |
| `tsd.counters` | `Counters::next(owner, name) -> u64` (atomic), `peek`, `set_at_least` | `{value}`; counts stay in `0..=i64::MAX` on every driver (`InvalidInput` past it) |
| `tsd.secrets` | `SecretStore` over documents (exists as `secrets`) | `{ciphertext}` |
| `tsd.secret_keys` | KMS envelope (exists as `secret_keys`) | `{format, key_id, wrapped}` |

`Registry::prepare` declares `tsd.migrations` and `tsd.leases`, `tsd.kv` (with its `kv_by_key` index) and `tsd.counters` (`primitives::collection_specs()`) along with the owners' collections, so a host that opens the backend with `existing_schema` reaches the primitives without declaring anything itself.

**Lease cluster features.** `Leases::acquire` is the minimal rule. Process placement on a shared backend needs three more things, all additive on the same record:
- **Endpoint.** `Leases::acquire_with(Acquire { owner, key, holder, ttl_ms, endpoint, held_epoch })` stores where the holder can be reached, so a caller refused with `Acquisition::Held(LeaseRecord)` can redirect. `renew` keeps it.
- **Released versus expired.** `release` sets `released = true` and expires the record at once. `Leases::inspect` returns the record whether it is live, lapsed or released (`current` hides the last two), so an observer can tell a clean hand-over from an abandoned lease.
- **Unclean takeover.** `acquire_with` returns `previous_unclean` with a grant. It is `true` when the acquisition replaced a record that was not released and that the caller did not hold: the record lapsed, or the same `holder` string left a still-live record (an earlier incarnation died before expiry). A same-holder live record is re-entrant (epoch kept, clean) only when `held_epoch` equals the record's epoch; otherwise the new incarnation takes it over at once with `epoch + 1`, which fences out the old one (its `renew`, `release` and `Lease::fence` all refuse the new epoch). A fresh key, a released record and a re-entrant acquire are clean.

**Lease compaction.** `Leases::compact(owner, key, holder, epoch)` deletes the record only when it still belongs to that holder and epoch, as one compare-and-swap (`Repo::delete_if`). It exists for keys with one lease each and no second tenure, such as a cron occurrence, where records otherwise pile up forever.

Why it is safe: tsd keeps expired and released leases so that a takeover reads the record and continues at `epoch + 1`, which keeps a stale holder's fence dead. Compaction does not weaken that for a live tenure, because the delete names `(holder, epoch)`: any takeover has bumped the epoch, so a record that moved on is never deleted. While the key stays absent, a fence on the deleted tenure refuses every write (a missing fence document fails the check). What compaction gives up is the epoch history: if the key were acquired again it would restart at epoch 1, and a stale fence at epoch 1 would match the new record. A caller may therefore only compact a key that it guarantees will not be acquired again (for a cron occurrence: once the job's next run has moved past it) and whose stale fenced writers can no longer run.

Two shared helpers are not collections:
- **`Repo<T>`** is a typed collection handle: `get`, `put`, `insert`, `update(id, f)` and `query`. `update` runs one compare-and-swap loop (budget and backoff in `RetryPolicy`, default 32 attempts, 1 to 8 ms jittered). The closure returns `Some(value)` to write or `None` to leave the record alone. A lease keeps its expiry in the body, not in `expires_ms`, because a takeover must read the lapsed record to continue its epoch. Every owner uses it instead of its own loop.
- **The graph checkpointer** exists once, in tinyagents-graph (`graph.checkpoints`, `graph.threads`, `graph.writes`). Each user passes a namespace field instead of copying the checkpointer.

### 6. Streams: sealing and retention

- **Sealing.** `StreamStore::seal(name)` closes a stream to appends. It writes the entries as one zstd-compressed JSON array blob under `_tsd/sealed/<stream>`, records that in the stream head, and deletes the entry rows.
  - Reads (`read`, `read_window`, `len`) behave exactly as before and return the same offsets and values.
  - Appending to a sealed stream is `ErrorKind::Conflict`.
  - Sealing is idempotent and survives a crash at any point: the blob is written before the head changes, and rows are deleted only after.
  - **Sealing is serialized with appends.** An append racing a seal either lands before it, and is in the blob, or after it, and is refused with `Conflict`; an acknowledged append is never lost with the entry rows. A driver that can make the steps one atomic unit does (memory under its lock, SQLite in one immediate transaction, File under the directory lock). One that cannot (MongoDB) marks conditionally: the mark is refused when the stream's base or length differs from the snapshot the blob was written from (MongoDB inserts the seal marker at the snapshot's end, through the unique `(_scope, s, g, o)` index that serializes appenders), and the seal then takes a fresh snapshot, a bounded number of times, before it reports `Conflict`.
  - **Sealing is serialized with truncation.** A `truncate_before` that completes after a seal's snapshot never lets the seal publish the older blob: the truncation is ordered before the seal (the seal snapshots the truncated stream) or after it (the truncation is refused with `Conflict`, as for any operation on a sealed stream), so an acknowledged truncation is never undone. Memory, SQLite and File get this from the same atomic unit as appends. MongoDB's truncation counts itself on the stream head (`truncate_epoch`) when it changes segments, and the snapshot records the count. With transactions the marker's transaction holds the head at that count, so a truncation completed since the snapshot (or racing it) refuses the marker; without them the count is checked right after the marker insert and the inserted marker is taken back, with `Conflict`, when it moved, and a truncation that finishes after the marker finds the stream sealed and is refused. Without transactions a reader can briefly see the retracted marker.
  - **The blob prefix is reserved.** Blobs under `_tsd/sealed/` belong to the drivers. The blob port refuses them (`InvalidInput`) for `put`, `get`, `get_range`, `head`, `delete`, trees and `list` of a prefix beneath them (`validate_user_blob_key`, `validate_user_blob_prefix`), and omits them from broader listings. Only a driver's own sealing code names them.
  - **Retention deletes only the incarnation it inspected.** `StreamStore::delete_sealed_stream(name, sealed_ms) -> bool` deletes the stream (and its blob) only if it is currently sealed with exactly that `sealed_ms`, atomically with the check, and returns `false` and changes nothing otherwise (missing, unsealed, or sealed at another instant). A stream deleted and recreated after `info` therefore survives retention. Memory checks and deletes under its lock, SQLite in one immediate transaction, File under the directory lock; MongoDB reads the seal marker of the current generation and advances the generation with a compare-and-swap on the value it read (inside the write transaction where the deployment has them), so a newer incarnation matches nothing. The default is `Unsupported(StreamSeal)`. It is host-only like `maintain`: the storage bus has no selector for it, so a bus client reports `Unsupported(StreamSeal)`, and its conformance check runs for backends (`conformance::run`) but not through `run_ports`. A recreated stream sealed again within the same millisecond is indistinguishable; the sealed instant is the incarnation's identity.
  - **A sealed blob belongs to one incarnation of its stream.** MongoDB keeps the blob at `_tsd/sealed/<stream>/g<generation>`, so a seal that stalled while the stream was deleted and recreated cannot replace the recreated stream's blob; `delete_stream` and a failed seal remove the blobs of earlier generations.
  - A `SealedLog` whose `base` plus entry count overflows `u64` cannot be constructed (`SealedLog::new` returns `InvalidInput`) or decoded.
- **Stream times.** Every driver's stream head records `last_append_ms` (stamped on every non-empty append by the driver's clock; truncation does not touch it) and `sealed_ms` (stamped when the seal marks the stream). `StreamStore::info(name) -> Option<StreamInfo { base, len, sealed_ms, last_append_ms }>` reads them; `None` means the stream does not exist. Reading times is part of the `StreamSeal` capability, not a capability of its own: a driver that can seal records both, and the default `info` is `Unsupported(StreamSeal)`. A stream written before times existed reports `None` for them, and the policy leaves it alone rather than guess.
  - memory: fields of the stored stream, stamped from the injectable clock.
  - SQLite: nullable `stream_heads.sealed_ms` and `last_append_ms`, added by `ensure()` with `ALTER TABLE ... ADD COLUMN` when a layout-2 file lacks them (layout stays 2).
  - File: `sealed_ms` and `last_append_ms` in the `.meta.json` sidecar (an append now rewrites the sidecar before appending the entries).
  - MongoDB: `ms` on every segment document and on the seal marker (`ms` is the seal time; `la` carries the last append time, because sealing deletes the entry segments).
  - Bus: `Streams.Info` (`StreamRequest` in, `StreamInfoResponse` or `null` out). A module may read the times of its own streams. Contract version stays 4 (unreleased).
- **Seal policy.** A `SealPolicy` on a `Stream` item states when the owner seals:
  - `OnFinish`: the owner calls `seal`
  - `Idle(ms)`: tsd seals streams untouched for that long, during maintenance
  - `Never`

  It also states a retention period: `delete_stream` after the stream has been sealed for N ms. `retain_ms` is unsigned (`u64`, like `Idle`): a negative period cannot be stated, in code or on the wire (deserializing one fails), so a sealed stream is never due for deletion before the instant it was sealed. A period beyond `i64::MAX` never comes due.
- **Applying policy.** `Registry::maintain(&ScopedStorage, now_ms, MaintenanceBudget) -> PolicyReport` applies every `Item::Stream` policy to one scope. For each declared base name it lists `<base>` and `<base>/...` with `streams(base)` (the `<base>` prefix alone would also match `<base>x`, so the family is filtered) and reads `info` for each:
  - not sealed, `Idle(ms)`, and `now_ms - last_append_ms > ms`: `seal`. A stream appended to after inspection is still sealed: idleness was the trigger and `seal` keeps every entry;
  - sealed, `retain_ms` set, and `sealed_ms + retain_ms < now_ms`: `delete_sealed_stream(name, sealed_ms)` with the inspected `sealed_ms` (which removes the blob too); `false` means the stream is no longer the one inspected and counts as a conflict;
  - `OnFinish` and `Never` streams are never sealed here, but retention applies to any sealed stream whose policy states it. A family whose policy can never act (`Never` or `OnFinish` without retention) is not even listed.
  - `MaintenanceBudget::max_streams` caps the streams sealed or deleted per call (a lost race counts); `PolicyReport { scopes, inspected, sealed, deleted, conflicts, exhausted }` says what happened and whether work remained. A `Conflict` or `NotFound` from a racing writer is counted, not returned; any other error stops the pass.
  - The policy is applied to the scope handed in, whatever the item's `ScopeClass`: the host chooses the scopes.
  - `Registry::maintain_all(&dyn StorageBackend, now_ms, budget)` runs it over `backend.scopes("")` with one budget shared across scopes. It needs `ScopeEnumeration`; without it, enumerate the scopes and call `maintain` for each.
  - **Order for a host:** policies first, then space reclamation, so the vacuum sees the freed rows: `registry.maintain_all(...)`, then `backend.maintain(budget)`.
- **Maintenance.** `StorageBackend::maintain(budget)` (capability `Maintenance`) reclaims space; it does not apply policies, because the backend does not know the registry:
  - SQLite runs `PRAGMA incremental_vacuum` with `auto_vacuum=INCREMENTAL`, set on creation. A migrated v0.x file is still `auto_vacuum=NONE`; SQLite only changes that with a full `VACUUM`, which cannot run in the migration transaction, so the first `maintain` with an unlimited budget (or one at least the database size) runs it once. A smaller bound reclaims nothing and leaves the conversion pending.
  - `reclaimed_bytes` is how much the database file shrank, measured around a passive checkpoint. A reader that keeps the checkpoint from finishing delays the report to the call that completes it.
  - MongoDB and File return `Unsupported`, which callers treat as nothing to do.
- **`checkpoint(Truncate)`** flushes the SQLite WAL. Call it before moving a database file.

### 7. Physical layout

**SQLite**: one database file.

```text
docs(scope, coll, id, version, schema, created_ms, updated_ms, expires_ms,
     deleted, body)                     PK (scope, coll, id)
streams(scope, name, seq, body)         PK (scope, name, seq)
stream_heads(scope, name, base, len, sealed, sealed_ms, last_append_ms)
blobs(scope, key, content_type, size, sha256, expires_ms, bytes)
doc_fields(scope, coll, id, path, kind, value)   PK (scope, coll, id, path)
fts                                     FTS5, searchable collections only
meta(key, value)                        registry snapshot, versions, layout
```

- `deleted` replaces `_tsd_tombstones`: a deleted id keeps its row with an empty body, so versions continue across deletion as the ports require.
- `body` is UTF-8 JSON text. A body whose serialized JSON is over 4096 bytes is stored as a `BLOB`: one marker byte (`0x01`) and a zstd frame (level 3) of the JSON. Smaller bodies, and any that would not shrink, stay `TEXT`. Drivers decode transparently, telling the two apart by the SQLite type of the value.
- SQL never reads `body`. The paths of a collection's declared indexes and search fields are projected in Rust, before compression, into the side table `doc_fields(scope, coll, id, path, kind, value)`: one row per path the document has, `kind` the JSON type rank used by sorting and `value` the SQL value of a scalar. One generic index `(scope, coll, path, value, id)` serves every collection. Equality, membership and range filters (numbers, strings, booleans), sorts and keyset cursors on a projected path are answered from it (a limited query is ordered and limited in SQL through a `LEFT JOIN` per sort key, falling back to Rust for array or object sort values and for `u64` above `i64::MAX`), and unique indexes are checked through the same lookup. A filter or sort on any other path is evaluated in Rust over decoded rows, as before; the Rust check decides every row either way. The table is rewritten on every put, cleared on delete, rebuilt when a collection's projected paths change, and built when a layout-2 file written without it is opened (`meta.layout` stays `2`; the body-expression indexes of that file are dropped).
- Generated columns were rejected: they are computed from other columns of the same row, and the only column holding the document is the compressed one, which no SQL function can decode. They would also need an `ALTER TABLE` per declared index on a table shared by all collections. The side table reuses `value::lookup`, so the projection and the filter agree on every path.
- Named databases (`StorageBackend::database`) are removed: scope plus owner prefix give the separation they were meant for, and no caller uses them.
  - **Done.** the database selector on `StorageBackend` and `BlobBackend`, the guard-database locator and `for_scope_fenced_in_database` (the generation-bound case is now `for_scope_fenced_generation(scope, generation, fence)`), the purge database address, and the cross-database fencing capability (its bit, `1 << 8`, is left unused; bits are in-process only) are gone from the ports and every driver. SQLite is one file (a directory path maps to `<dir>/storage.db`) with unprefixed tables, and the v0 upgrade handles only the default database; files, Mongo collections and S3 objects carry no database segment (S3 keys are `<prefix>/scopes/<hex scope>/`, no longer `<prefix>/<database>/scopes/...`). The storage bus contract never carried a database name.

**MongoDB**:
- The same scopes, names and metadata.
- Document fields are `_scope`, `_key`, `_v`, `_schema`, `_c`, `_u`, `_x` (expires) and `_del`, with the body under `d`.
- TTL indexes use `_x`.
- A record written before the metadata fields existed lacks `_schema`, `_c` and `_u`, and reads them as `0`. Filters and sorts on `_schema`, `_created_ms` and `_updated_ms` treat a missing field as `0` (a predicate that `0` satisfies also accepts a missing field; a sort reads `$ifNull(field, 0)`), so `_schema < 2` finds legacy records and an in-place migration cannot finish with them unmigrated. A missing `_x` stays "no expiry".

**File driver**: `scopes/<scope>/{docs/<coll>/<id>.json, streams/<name>.jsonl, blobs/<key>}`, with metadata in a `.meta.json` sidecar. It exists for tests and inspection, not production.

**Physical migration.** On open, the SQLite driver upgrades a v0.x file (`_tsd_*` tables):
- it renames tables in place
- it folds tombstones into `deleted`
- it adds the metadata columns, backfilling `created_ms`/`updated_ms` from the body when a recognised v1 field exists, else from the open time (a numeric field not named `*_ms` below 1e11 is epoch seconds and is multiplied by 1000, since v0.x kept some, such as `profiles.created_at`, in seconds; a field named `*_ms` is always milliseconds; a fractional number follows the same rule and is rounded to the nearest millisecond, halves away from zero)
- it leaves `auto_vacuum` alone: a `NONE` file is converted to `INCREMENTAL` by the first `maintain` that can afford the one-time `VACUUM` (out of the transaction, so the layout is recorded first and the conversion can be retried; see section 6)

The upgrade runs as one transaction, records `meta.layout = 2` and is not reversible. A backup copy is the host's responsibility.

## Compatibility

v2 ships as **v1.0.0** and is a breaking release on purpose:

- **On-disk.** A SQLite file opened by v1.0.0 is upgraded in place to layout 2 and cannot be opened by v0.x again.
- **Bus contract version 4.** Its wire types deny unknown fields, so a module built against contract 3 cannot decode a version-4 reply that carries `meta`. Every module re-pins onto v1.0.0 together with its host; mixed versions are unsupported. A module on contract 4 talking to an older host still works for default writes. An options-bearing write or `Seal` fails closed.
- **S3 object keys.** The S3 driver drops the database segment: objects move from `<prefix>/<database>/scopes/…` to `<prefix>/scopes/…`. Blobs written by v0.x must be copied or re-keyed.
- **`WriteOptions::created_ms` is host-only.** The bus refuses it with `InvalidInput`.

## Invariants

1. Every record's scope parses under §1. Tenants never nest, and no tenant's scope is a prefix of another tenant's: `u:a` and `u:ab` are distinct because the tenant ends at `/`.
2. No two owners declare the same name. Every name starts with its owner's prefix.
3. Every stored time is epoch ms in a field named `_ms`, or tsd metadata.
4. A migration step is deterministic and resumable. Running it twice changes nothing.
5. Sealing never changes what a reader observes: offsets, values and lengths stay the same.
6. A v2 database copied to another backend of the same major version reads identically.

## Acceptance criteria

- The conformance suite gains `schema` cases, and they pass on memory, SQLite, File and MongoDB:
  - scope grammar and hashing
  - `Key::of` and its reversal
  - metadata on write and read, including `_created_ms` filters and sorts
  - `expires_ms`
  - registry composition refusals
  - step resume after an injected failure
  - leases, kv and counters under contention
  - seal and read equality
  - retention
- The SQLite physical migration passes against a v0.11.0 fixture database with every table populated.
- `StorageBackend::maintain` reduces a SQLite file's size after a bulk delete.
- Every collection in the [catalogue](#catalogue) has a v2 name, an owner and a migration step in its owning repository.
- OpenHuman's `scripts/ci/check-storage-schema.mjs` passes. It checks names, single ownership, casing and the `_ms` rule, and generates `docs/storage-schema.md`.

## Open questions

1. Decided: a body over 4 KiB is stored as zstd, not SQLite JSONB. JSONB keeps `json_extract` cheap but saves little space (about the size of the text), and the 8b measurement (1000 compressible 20 KiB documents: 21.5 MB raw, 0.2 MB stored) favours zstd. `json_extract` over a compressed body is impossible, so indexed fields are projected into `doc_fields` instead of generated columns (see the storage section above).
2. Whether `harness.journal` retention (default 30 days) belongs in the owner's `SealPolicy` or in host configuration. The proposal is owner default, host override.

## Catalogue

The table maps every v1 name to its v2 name. Each owner implements the step for its rows, in its own repository.

**tsd and OpenHuman host stores**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `secrets`, `secret_keys` | `tsd.secrets`, `tsd.secret_keys` | Tenant | rename only |
| `leases` (cluster) | `tsd.leases` | Cluster | `expires_at_ms` becomes `expires_ms` |
| `storage_import`, `threads_import`, `*_host_import`, `session_import_markers`, `*_state_import`, `subagent_sessions_import` | `tsd.migrations` | Tenant | one entry per completed importer |
| `storage_agents` | `agents.registry` | Tenant | |
| `profiles` | `profiles.registry` | Cluster | `created_at` (seconds) moves to metadata |
| `config` | `config.documents` | Tenant | `toml_enc` stays (an encrypted value) |
| `approvals`, `approval_flow_trust` | `approvals.requests`, `approvals.flow_trust` | Tenant | RFC 3339 becomes `_ms`; uuid becomes ULID for new rows only |
| `paired_devices` | `devices.paired` | Tenant | |
| `integration_notifications`, `notification_dedup`, `notification_settings`, `core_notifications` | `notifications.integration`, `.dedup`, `.settings`, `.core` | Tenant | drop `received_at`; keep `received_ms` |
| `task_sources`, `ingested_tasks` | `task_sources.sources`, `.ingested` | Tenant | drop `_ns`/`_at` copies |
| `composio_state` | `tsd.kv` ns `composio` | Tenant | unwrap `value` |
| `artifacts`, `artifact_args` | `artifacts.records`, `.args` | Tenant | unwrap `args` |
| `subagent_sessions` | `subagents.sessions` | Agent | `updatedAt` becomes `updated_ms` metadata |
| `cost_records` | `cost.records` | Tenant | `ts_ms` kept |
| `tinymemes_documents` | `memes.state` | Tenant | parse the `json` string into the body |
| `threads_transcript_keys` | `threads.transcript_keys` | Tenant | |
| `cron_occurrence_claims` | `tsd.leases` | Tenant | key `Key::of(["cron", job, occurrence_ms])` |
| `delegation_graph_*` | `graph.*` ns `delegation` | Agent | |

**tinyagents**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `session_kv` | `tsd.kv` (owner `session`, ns as today) | Agent | unwrap `value`; drop `<len>:` keys |
| stream `16:session_journal/<run>` | stream `harness.journal/<run>` | Agent | drop repeated `run_id`, `root_run_id` and `event_id`; seal finished runs |
| `harness_run_status` | `harness.run_status` | Agent | unwrap `record`; id is the run id |
| `harness_embedding_cache` | `harness.embedding_cache` | Agent | |
| `attachment_stash_index`, blobs `attachments/…` | `harness.attachment_stash`, blobs `harness/attachments/…` | Agent | |
| blobs `tool-results/…`, `outputs/…`, `workspace/…`, `generated-media/…` | `harness/tool_results/…` and so on | Agent | |
| `session_transcripts`, `session_transcript_entries` | `session.transcripts`, `session.transcript_entries` | Agent | `created_at` and `reserved_at` move to metadata and `tsd.leases` |
| `session_turn_states` | `session.turn_states` | Agent | |
| `run_ledger_*` | `ledger.*` (`workflow_runs`, `telemetry`, `agent_runs`, `teams`, `members`, `tool_effects`, `event_outbox`, `event_heads`, `tasks`, `entry_heads`, `entries`, `entry_labels`, `entry_indexes`) | Agent | unwrap `record`; camelCase becomes snake_case; drop `*Sql` copies |
| `run_ledger_agent_imports` | `tsd.migrations` | Agent | |
| stream `run_events:<sha>` | `ledger.events/<run>` | Agent | |
| `session_history_{sessions,messages,tool_calls,fts}` | `history.{sessions,messages,tool_calls,search}` | Agent | µs becomes ms |
| `session_history_counters` | `tsd.counters` | Agent | |
| `conversation_threads_v1`, `conversation_appends_v1`, streams `conversation_messages_v1:<sha>:<inc>` | `conversations.threads`, `.appends`, streams `conversations.messages/<thread>/<inc>` | Tenant | an append intent's `record` (a JSON string today) becomes an object |
| `<prefix>_{checkpoints,threads,writes,leases}` | `graph.{checkpoints,threads,writes}` plus `tsd.leases` | Agent | prefix becomes a `ns` field |
| `WorkflowRun.lease_owner` / `lease_expires_at` (embedded in `run_ledger_workflow_runs`) | `tsd.leases` key `Key::of(["ledger", "workflow", run_id])` | Agent | lease fields removed from the record |
| `orchestration_tasks`, `orchestration_task_history`, `orchestration_completions` | `tasks.records`, `tasks.history`, `tasks.completions` | Agent | `updatedAtMs` moves to metadata |
| `harness_store`, `harness_namespaced_store`, `harness_response_cache`, `graph_task_cache` | `tsd.kv` (owner `harness` or `graph`) | Agent | |

**tinyflows**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `flows_{definitions,revisions,runs,suggestions,drafts}` | `flows.{definitions,revisions,runs,suggestions,drafts}` | Tenant | drop `*_ns`; keep `_ms`; ULIDs give ordering |
| `flows_kv`, `flows_state` | `tsd.kv` (owner `flows`) | Tenant | |
| `flows_graph_*` | `graph.*` ns `flows` | Tenant | |
| `cron_jobs`, `cron_runs` | `cron.jobs`, `cron.runs` | Tenant | drop `next_run_ns`; embedded `occurrence_lease` moves to `tsd.leases` |
| `cron_counters`, `adaptive_counters` | `tsd.counters` | Tenant | |
| `adaptive_*` | `flows.adaptive_*` | Tenant | `scope_key` becomes a key part |
| `wf_*` | `flows.*` | Tenant | unused library store, mapped to the same collections |

**Modules**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `mcp_servers`, `mcp_server_names`, `mcp_registry_cache`, `mcp_tool_cache`, `mcp_registry_index`, `mcp_registry_index_state`; stream `mcp_audit` | `mcp.servers`, `.server_names`, `.registry_cache`, `.tool_cache`, `.registry_index`, `.registry_index_state`; stream `mcp.audit` | Module | `installed_at`, `cached_at`, `synced_at` and `claimed_at` (ms) become `_ms` or metadata |
| `channels_delivery_*`, `channel_remote_*`, `whatsapp_*` | `channels.delivery_*`, `channels.remote_*`, `channels.whatsapp_*` | Module | |
| `composio-user-scopes`, streams `triggers/<date>` | `connectors.user_scopes`, streams `connectors.triggers/<date>` | Module | unwrap `value` |
| blobs `tinyjuice_ccr/<token>` | blobs `juice/ccr/<token>` | Module | |
| `x402_budget`, stream `x402.payments` | `wallet.budgets`, stream `wallet.payments` | Module | |
| `tinybox_boxes`, `tinybox_jail_index` | `box.boxes`, `box.jail_index` | Module | |
| tinyskills bundle blobs | `skills/bundles/…` | Tenant | |
