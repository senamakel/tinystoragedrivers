# Repository Guidelines

TinyStorageDrivers is a Rust 2024 workspace of storage values, async ports,
primitive engines, and loadable-module adapters. OpenHuman and other hosts
compose these primitives; this repository does not own application schemas,
migrations, tenant policy, or secret-key acquisition.

## Crates

The workspace contains thirteen crates:

| Crate | Responsibility |
| --- | --- |
| `tinystoragedrivers-types` | Scope, errors, capabilities, keys, filters, and value types |
| `tinystoragedrivers-api` | Async primitive ports and the `Storage` bundle |
| `tinystoragedrivers-document` | Document and stream ports with memory, file, SQLite, and MongoDB engines |
| `tinystoragedrivers-kv` | Key/value port with memory and durable engines |
| `tinystoragedrivers-blob` | Blob port with memory, file, SQLite, MongoDB, and S3 engines |
| `tinystoragedrivers-queue` | Queue port with memory, document, and RabbitMQ engines |
| `tinystoragedrivers-secrets` | Secret port with memory, file, document, keyring, and KMS engines |
| `tinystoragedrivers-connect` | Shared connection handles for primitive engines |
| `tinystoragedrivers-fence` | Local process-owned lockfile leases for host coordination |
| `tinystoragedrivers` | Optional application composition facade |
| `tinystoragedrivers-testkit` | In-memory bundles, conformance suites, and test decorators |
| `tinystoragedrivers-bus` | Versioned storage module wire contract |
| `tinystoragedrivers-busproxy` | Module-side connections and host-side scoped serving |

Consumers normally depend on `-types` and `-api`; loadable modules also use
`-busproxy`. Consumer libraries do not link engines or depend on the facade.
The composition root selects engines. Keep per-record repositories, schema
declarations, migration policy, queue manifests, and key acquisition with the
host or module that owns them.

The facade's `StorageConfig` opens a complete backend selected by `memory`,
`file:`, `sqlite:`, or `mongodb://` URLs. S3 is a blob-only engine exposed by
`BlobConfig`/`open_blobs`; RabbitMQ is a queue-only engine exposed by
`QueueConfig`/`open_queues`. Both helpers take an explicit scope. The facade
does not invent memory fallbacks for unconfigured durable ports. Durable
backends return unsupported secrets until the host supplies an explicit secret
store and key provider.

Read [the v2 primitive contract](docs/specs/primitives-v2.md) before changing a
public port, engine behavior, storage format, or bus contract.

## Development

Run commands from the repository root. CI is the acceptance source of truth:

```sh
cargo fmt --all -- --check
cargo check --locked -p tinystoragedrivers --no-default-features
cargo check --locked -p tinystoragedrivers --no-default-features --features file
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
cargo deny check all
```

The facade's production features are individually checked with
`--no-default-features`: `file`, `sqlite`, `mongodb`, `s3`, `rabbitmq`,
`keyring`, `kms-aws`, and `kms-gcp`. Memory ports remain available with no
default features. Avoid enabling keyrings or cloud providers in tests that do
not explicitly mock them.

Run the live MongoDB, RabbitMQ, and MinIO suites only with disposable services
and explicit test credentials. Unit tests do not contact external services.
Do not log keys, credentials, or stored secret bytes.

## Rust conventions

- Put module tests in sibling `*_tests.rs` files declared at the end of the
  production module. Do not use inline test modules or files named `tests.rs`.
- Name tests for observable behavior and cover error paths as well as success.
- Keep public exports deliberate and document public fallible APIs with
  `# Errors` sections. Error messages must not contain connection credentials.
- Maintain at least 90% line coverage in every production source file. The
  native CI coverage inventory and changed-line accounting must remain intact.
- Keep optional clients and platform bindings behind the narrowest engine
  feature. The normal no-default facade build must not pull in keyring, KMS, or
  encryption-provider libraries.
- Avoid `unwrap`, `expect`, and `panic` in production code. Workspace lints
  forbid unsafe Rust and CI treats warnings as errors.

## Documentation and release

Keep crate docs, the README, `docs/specs/`, and examples aligned with the
current v2 APIs. Superseded v1 schema and migration specs are not normative;
retain only historical fixtures that are exercised by compatibility tests.

The release workflow updates the inherited workspace version and publishes a
tag with a source archive and its `checksum.toml`. Do not bump package
versions by hand. Release dispatches are restricted to canonical `main` and
the exact validated fork epic branches listed in `.github/workflows/release.yml`.
Only the maintainer may dispatch a release, after CI and coverage pass.
