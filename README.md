# tinystoragedrivers

Pluggable storage for OpenHuman and the libraries it hosts. Every crate that
persists data (sessions, workflow runs, approvals, schedules) builds its records
on one small set of ports. The host chooses the backend once at boot, from a URL:

| Deployment | URL | Driver |
| --- | --- | --- |
| Tests, stateless embedders | `memory` | in-process maps |
| Desktop and CLI | `sqlite:<workspace dir>` | SQLite, one file (feature `sqlite`) |
| Cloud, multi-tenant | `mongodb://…/<db>` | MongoDB, every record scoped to its tenant (feature `mongodb`) |
| Cloud records and object blobs | `mongodb://…/<db>?blobs=s3%3A%2F%2Fbucket%2Fprefix%3Fregion%3Dus-east-1` | MongoDB records with S3 blobs (features `mongodb`, `s3`) |
| Plain files | `file:<dir>` | JSON documents, JSONL streams and raw blobs on disk (`tinystoragedrivers-file`, feature `file`) |
| Secrets | none (features `secrets`, `keyring`, `keyring-secret-service`) | `SecretStore`: OS keyring, encrypted `secrets.enc`, or `enc2:` ciphertext in any `DocumentStore` |

Drivers are separate crates. A host compiles in only the ones it enables, as
Cargo features on the `tinystoragedrivers` facade. A desktop build never links
a MongoDB client, and a cloud build never links SQLite.

Add one percent-encoded `blobs=<URL>` query option to any records URL to keep
documents and streams on that backend while storing blobs independently. Other
records query options stay on the records URL. The blob URL may name `s3:` or
another supported storage backend; nested composite blob URLs are rejected.
For example, with the `mongodb` and `s3` features enabled:

```text
mongodb://db.example/openhuman?retryWrites=true&blobs=s3%3A%2F%2Fbucket%2Fattachments%3Fregion%3Dus-east-1
```

## The ports

- **`DocumentStore`** holds versioned JSON objects in named collections. It
  supports:
  - filters, sorting, and cursor paging;
  - compare-and-swap through `Precondition`;
  - atomic `claim` for queues and leases;
  - unique indexes;
  - optional expiry, cross-document transactions, and full-text search.
- **`StreamStore`** holds append-only logs with dense offsets, for transcripts,
  journals, and audit trails.
- **`BlobStore`** holds opaque bytes by key, for attachments and artifacts.

A `StorageBackend` hands out a `ScopedStorage` for each `Scope`, the tenant key
every record carries. Two scopes on one backend never see each other's data.
A backend is one database: a SQLite file, a MongoDB database, a directory of
files. Owners keep their data apart with the scope they open, not with a
separate database.

This repository holds no typed repository for any particular record. Those
belong to the crate that owns the record, which keeps this crate free of every
consumer's schema.

## The schema

Owners needing only declarations and wire contracts can use
[`tinystoragedrivers-schema`](crates/tinystoragedrivers-schema/README.md) without
linking the storage runtime. Core and the facade reexport these shared types
while keeping their existing executable schema and migration APIs.

The optional `migration-delivery` feature adds transport-independent confidential
fragmentation for external migration executors. Blob bytes and identity-stream
entries stay local; document and independent-entry requests/replies use bounded
frames without a new total size ceiling. See the
[delivery contract](docs/specs/migration-delivery.md) and
[runtime adapter requirements](crates/tinystoragedrivers-core/src/schema/delivery/README.md).

Since v1.0.0, every record follows one uniform schema
([`docs/specs/schema-v2.md`](docs/specs/schema-v2.md)):

- **Scopes.** `local` or `u:<id>` for the tenant, optionally narrowed with `/agent:<id>` or `/module:<id>`; `cluster` for placement. Build them with `Scope::tenant`, `.agent` and `.module`.
- **Names.** Collections are `<owner>.<entity>`, streams `<owner>.<stream>/<key>`, blobs `<owner>/<path>`. Ids come from `Key::of(parts)` or `Key::ulid()`.
- **Bodies.** Bare snake_case JSON objects, every time an `*_ms` epoch millisecond. tsd keeps `schema`, `created_ms`, `updated_ms` and `expires_ms` per record (`Meta`, `WriteOptions`).
- **Registry.** Each owning crate exports `storage_schema()`. The host composes them with `Registry::compose`, prepares them, runs their resumable migration steps (`Registry::migrate`) and applies their seal and retention policies (`Registry::maintain`).
- **Primitives.** `Repo<T>`, `Leases`, `Kv` and `Counters` replace hand-written CAS loops, lease documents, key-value stores and counters.
- **Streams seal.** A finished stream is folded into one compressed blob and reads exactly as before (`StreamStore::seal`).

```rust
use serde_json::json;
use tinystoragedrivers::{Filter, Precondition, Scope, StorageConfig};

async fn demo() -> tinystoragedrivers::Result<()> {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory")?).await?;
    let jobs = backend.for_scope(&Scope::local())?;
    let docs = jobs.documents();

    docs.put("jobs", "nightly", json!({"state": "queued", "run_at": 0}), Precondition::Absent)
        .await?;
    let next = docs
        .claim("jobs", &Filter::eq("state", "queued"), &[], &json!({"state": "running"}))
        .await?;
    assert_eq!(next.unwrap().id, "nightly");
    Ok(())
}
```

## Workspace

```text
crates/
├── tinystoragedrivers-core/    # ports, Scope, filters, errors, memory driver,
│                               # blocking bridge, conformance suite
├── tinystoragedrivers-file/    # `file:<dir>`: JSON, JSONL and raw files
├── tinystoragedrivers-sqlite/  # SQLite: generic tables, FTS5, native access
├── tinystoragedrivers-mongodb/ # MongoDB: multi-tenant, scope on every record
├── tinystoragedrivers-secrets/ # SecretStore port: memory, secrets.enc file,
│                               # DocumentStore and OS keyring drivers
└── tinystoragedrivers/         # facade: StorageConfig URL parsing, open(),
                                # driver features, re-exports the core
```

Each driver gets its own crate (`tinystoragedrivers-sqlite`,
`-mongodb`, `-file`, `-secrets`) and must pass
`tinystoragedrivers_core::conformance::run`. The suite runs the same checks
against every driver:
- precondition conflicts
- unique-index violations
- paging
- claim ordering
- stream offsets
- blob ranges
- scope isolation
- scope isolation

## Develop

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-features
.github/scripts/check-file-coverage.sh 90 target/coverage.json
```

The MongoDB driver's `live_*` tests run against the server named by
`TSD_MONGO_URL` and skip without it. The coverage gate needs them, so run it
with a single-node replica set up:

```sh
docker run -d --rm --name tsd-mongo -p 27017:27017 mongo:7 --replSet rs0 --bind_ip_all
docker exec tsd-mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
export TSD_MONGO_URL='mongodb://localhost:27017/tsd_test?directConnection=true'
```

The design and its guarantees are in
[`docs/specs/storage-ports.md`](docs/specs/storage-ports.md). The order in which
drivers land and consumers migrate is in
[`docs/plans/storage-rollout.md`](docs/plans/storage-rollout.md). Native restricted handles and trusted schema bootstrap are described in
[`docs/specs/existing-schema.md`](docs/specs/existing-schema.md). Restricted Mongo reset uses trusted prepared metadata; missing reset collections refuse before initialization or data erasure rather than being created by the restricted handle.

## Releases

`.github/workflows/release.yml` (manual dispatch) validates, bumps the single
workspace version, refreshes the root and discovered proof-workspace lockfiles,
tags `vX.Y.Z`, and creates a GitHub release. Before staging the exact version
and lockfile paths, it validates each workspace with locked metadata using
default and all features. Failed updates or validation restore the affected
files to their original bytes and existence. See the
[release runbook](AGENTS.md#releases). Consumers pin the tag through a git
dependency or a submodule gitlink.

## License

GPL-3.0-only. See [`LICENSE`](LICENSE).

### Mongo live-test database permissions

Run the complete live suite on a disposable MongoDB server. Transaction race
fixtures each use their own database on the server named in `TSD_MONGO_URL`
(`tsd_fx_*`, `tsd_<label>_<pid>` and `tsd_schema_default_*`; the URL's own
database, or `tsd_test` when it has none, backs the rest). This prevents
parallel schema changes from invalidating unrelated transactions.

The test account must be allowed to create databases, collections and indexes,
and read, write and delete documents in them. On an authenticated disposable
server, `readWrite` on any database (or on those name patterns) is sufficient.
The URL's `authSource` continues to select the authentication database
independently.
