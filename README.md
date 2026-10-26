# TinyStorageDrivers

TinyStorageDrivers provides storage values, async ports, independent primitive
engines, and adapters for hosts and loadable modules. The v2 architecture keeps
storage policy with the consumer that owns its data model. See the
[primitives v2 contract](docs/specs/primitives-v2.md) and the
[v2 item inventory](docs/specs/primitives-v2-inventory/README.md).

## Crates

The workspace contains twelve crates:

| Crate | Role |
| --- | --- |
| `tinystoragedrivers-types` | Shared values, errors, scopes, capabilities, and request/result types |
| `tinystoragedrivers-api` | Async storage ports and the `StorageBackend` / `Storage` bundle |
| `tinystoragedrivers-document` | Document and stream primitives; memory, SQLite, MongoDB, and file engines |
| `tinystoragedrivers-kv` | Key/value primitive; memory, SQLite, MongoDB, and file engines |
| `tinystoragedrivers-blob` | Blob primitive; memory, filesystem, SQLite, MongoDB, and S3 engines |
| `tinystoragedrivers-queue` | Queue primitive; memory, document-backed, and RabbitMQ engines |
| `tinystoragedrivers-secrets` | Secret-store engines, including memory, file, document, keyring, and KMS |
| `tinystoragedrivers-connect` | Shared engine connection setup used by primitive adapters |
| `tinystoragedrivers` | Optional composition-root facade for URL parsing and engine assembly |
| `tinystoragedrivers-testkit` | In-memory bundles, conformance suites, and test decorators |
| `tinystoragedrivers-bus` | Versioned storage module wire types |
| `tinystoragedrivers-busproxy` | Module-side connection and host-side scoped serving adapters |

Consumer libraries normally depend on `tinystoragedrivers-types` and
`tinystoragedrivers-api`. Loadable modules may additionally use
`tinystoragedrivers-busproxy`. They select neither engines nor the facade.
Production engine features belong at a composition root; primitive engines
and the testkit belong in consumer dev-dependencies when needed.

## Composition facade

The facade is optional and intended for application composition roots.
`StorageConfig::parse` with `open` selects a complete backend from a storage
URL. `BlobConfig` with `open_blobs` selects a blob port independently, and
`QueueConfig` with `open_queues` selects a scoped queue port. Memory storage is
always available; production features are opt-in and the default feature set
is empty.

The facade does not define consumer schemas, migrations, tenant naming,
retention policy, distributed fencing, or secret-key ownership. Those remain
with the host or module that owns the records and lifecycle. The separate
`tinystoragedrivers-fence` crate supplies only a local PID lockfile lease for
host coordination. Durable backends expose an
unsupported secret port until the host supplies an explicit secret store and
key provider. Shared durable connections come from
`tinystoragedrivers-connect`.

## Development

Run `.github/scripts/test-features.sh` from the repository root to check the
memory-only build and each single-feature build (`file`, `sqlite`, `mongodb`,
`s3`, `rabbitmq`, `keyring`, `kms-aws`, and `kms-gcp`). CI runs this same
locked feature matrix.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo deny check all
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
```

The CI coverage gate requires at least 90% line coverage in every source file.
MongoDB and RabbitMQ live tests need disposable services; see the relevant
engine documentation and [AGENTS.md](AGENTS.md) for local setup guidance.

## Documentation

- [Primitive contracts](docs/specs/primitives-v2.md)
- [Storage ports and host boundaries](docs/specs/storage-ports.md)
- [Specifications index](docs/specs/README.md)
- [Repository contributor guide](AGENTS.md)

## License

GPL-3.0-only. See [`LICENSE`](LICENSE).
